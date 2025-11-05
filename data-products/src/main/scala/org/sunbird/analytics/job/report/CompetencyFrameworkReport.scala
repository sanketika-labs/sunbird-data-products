package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.UserCacheSupport
import org.sunbird.analytics.exhaust.collection.UDFUtils
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}
import scala.collection.JavaConverters._


object CFSummaryReport extends IJob with BaseReportsJob with UserCacheSupport {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val reportCols = Seq("userid", "firstname", "lastname", "username", "email", "usertype", "cin", "fmpsid", "province", "designation", "training_group", "orgname", "competency_framework_name", "competency_level_name", "num_courses_enrolled", "num_courses_started", "num_courses_completed", "course_completed_name", "courses_enrolled_name", "course_started_name", "course_completed_codes", "courses_enrolled_codes", "course_started_codes")
  
  private val collectionBatchDBSettings = Map("table" -> "batches", "keyspace" -> AppConf.getConfig("sunbird.collection.keyspace"), "cluster" -> "LMSCluster")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.collection.keyspace"), "cluster" -> "LMSCluster")
  private val userCourseDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "ReportCluster")
  private val collectionTrackingSettings = Map("table" -> "sb_collection_tracking", "keyspace" -> AppConf.getConfig("sunbird.collection.keyspace"), "cluster" -> "LMSCluster")

  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
  val db: String = AppConf.getConfig("postgres.db")
  val url: String = AppConf.getConfig("postgres.url") + s"$db"
  val requestsTable: String = "cf_summary_report"

  implicit val className: String = "org.sunbird.analytics.job.report.CFSummaryReport"
  val jobName = "CFSummaryReport"

  case class ActivityMetadata(
                               activityId: String,
                               activityType: String,
                               name: String,
                               code: String,
                               primaryCategory: String
                             )

  case class UserActivityInfo(
                               userId: String,
                               cfActivityId: String,
                               clActivityId: String,
                               cfName: String,
                               cfCode: String,
                               clName: String,
                               clCode: String
                             )

  case class CourseInfo(
                         courseId: String,
                         courseName: String,
                         courseCode: String, 
                         status: Int
                       )

  // $COVERAGE-OFF$
  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    try {
      val res = CommonUtil.time(prepareReport(fetchData))
      val reportData = res._2
      saveToPostgres(reportData)
      reportData.unpersist()
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
  }
  // $COVERAGE-ON$

  def prepareReport(fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    import spark.implicits._

    println("[STEP 1] Get user cache and collection tracking data")
    val userCachedDF = getUserCacheDF(fetchData)
    val collectionTrackingDF = getCollectionTrackingDF(fetchData)

    println("[STEP 2] Extract CF and CL activity IDs per user")
    // Extract latest CF and CL activity IDs per user
    val userActivityDF = collectionTrackingDF
      .filter(lower(col("activity_type")).isin("competency framework", "competency level"))
      .groupBy("userid").agg(
        max(when(lower(col("activity_type")).contains("competency framework"), col("activityid"))).as("cf_activity_id"),
        max(when(lower(col("activity_type")).contains("competency level"), col("activityid"))).as("cl_activity_id")
      )

    println("[STEP 3] Fetch activity metadata from Redis")
    val redisHost = AppConf.getConfig("redis.host")
    val redisPort = AppConf.getConfig("redis.port").toInt
    val redisDb = AppConf.getConfig("redis.db").toInt

    // Broadcast Redis host, port, and db for worker nodes
    val broadcastRedisHost = spark.sparkContext.broadcast(redisHost)
    val broadcastRedisPort = spark.sparkContext.broadcast(redisPort)
    val broadcastRedisDb = spark.sparkContext.broadcast(redisDb)

    // Get activity metadata - CF and CL names, codes, and child nodes
    val activityMetadataRDD = userActivityDF.rdd.mapPartitions { partition =>
      val jedis = new Jedis(broadcastRedisHost.value, broadcastRedisPort.value)
      jedis.select(broadcastRedisDb.value)
      partition.map { row =>
        val userId = row.getAs[String]("userid")
        val cfActivityId = Option(row.getAs[String]("cf_activity_id")).getOrElse("")
        val clActivityId = Option(row.getAs[String]("cl_activity_id")).getOrElse("")

        val cfMetadata = if (cfActivityId.nonEmpty) getActivityMetadataFromRedis(jedis, cfActivityId) else None
        val clMetadata = if (clActivityId.nonEmpty) getActivityMetadataFromRedis(jedis, clActivityId) else None

        (userId, cfActivityId, clActivityId, cfMetadata, clMetadata)
      }
    }

    println("[STEP 4] Get courses linked to CF child nodes")
    // Get courses from CF child nodes - CF has the childNodes array
    val coursesRDD = activityMetadataRDD.mapPartitions { partition =>
      val jedis = new Jedis(broadcastRedisHost.value, broadcastRedisPort.value)
      jedis.select(broadcastRedisDb.value)
      partition.flatMap { case (userId, cfId, clId, cfMeta, clMeta) =>
        val courses = scala.collection.mutable.ListBuffer[CourseInfo]()

        if (cfMeta.isDefined && cfId.nonEmpty) {
          val cfData = JSONUtils.deserialize[Map[String, Any]](
            jedis.get(s"content:$cfId")
          )
          val childNodes = cfData.get("childNodes").asInstanceOf[Option[List[String]]].getOrElse(List.empty)

          println(s"[DEBUG] CF $cfId has ${childNodes.size} child nodes")

          // Get CL from child nodes (excluding other CLs for this user)
          childNodes.foreach { childId =>
            val childData = JSONUtils.deserialize[Map[String, Any]](
              jedis.get(s"content:$childId")
            )
            val primaryCategory = childData.get("primaryCategory").map(_.toString).getOrElse("")

            // Check if this is the CL we're looking for
            if (primaryCategory == "Competency Level" && childId == clId) {
              println(s"[DEBUG] Found CL $clId in CF child nodes")
              
              // Now get courses from this CL's child nodes
              val clChildNodes = childData.get("childNodes").asInstanceOf[Option[List[String]]].getOrElse(List.empty)
              println(s"[DEBUG] CL $clId has ${clChildNodes.size} child nodes (courses)")

              clChildNodes.foreach { courseChildId =>
                val courseData = JSONUtils.deserialize[Map[String, Any]](
                  jedis.get(s"content:$courseChildId")
                )
                val coursePrimaryCategory = courseData.get("primaryCategory").map(_.toString).getOrElse("")

                // Validate parent is CL and primary category is Course
                if (coursePrimaryCategory == "Course") {
                  val courseId = courseData.get("identifier").map(_.toString).getOrElse("")
                  val courseName = courseData.get("name").map(_.toString).getOrElse("")
                  val courseCode = courseData.get("code").map(_.toString).getOrElse("")

                  if (courseId.nonEmpty) {
                    println(s"[DEBUG] Found course: $courseId ($courseName) under CL $clId")
                    courses += CourseInfo(courseId, courseName, courseCode, 0) // status defaults to 0
                  }
                }
              }
            }
          }
        }

        courses.map(course => (userId, cfId, clId, cfMeta, clMeta, course))
      }
    }

    println("[STEP 5] Fetch user course enrollments and status")
    val userEnrolmentDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(userCourseDBSettings)
      .load()
      .filter(lower(col("active")).equalTo("true"))
      .select("userid", "courseid", "status")

    // Create DataFrame from courses RDD
    val coursesDF = coursesRDD.toDF("userid", "cf_activity_id", "cl_activity_id", "cf_metadata", "cl_metadata", "course_info")

    println("[STEP 6] Join user enrollments with courses")
    // Extract course info and join with enrollments
    val coursesExpandedDF = coursesDF
      .withColumn("courseid", col("course_info.courseId"))
      .withColumn("course_name", col("course_info.courseName"))
      .withColumn("course_code", col("course_info.courseCode"))

    val enrolledCoursesDF = coursesExpandedDF
      .join(userEnrolmentDF, Seq("userid", "courseid"), "left")
      .withColumn("status", coalesce(col("status"), lit(0)))

    println("[STEP 7] Aggregate courses by status per CF activity ID")
    // Aggregate courses by status (0=enrolled, 1=started, 2=completed) per CF activity ID
    val aggregatedDF = enrolledCoursesDF.groupBy(
      "userid", "cf_activity_id", "cf_metadata", "cl_metadata"
    ).agg(
      collect_list(
        when(col("status") === 0, struct(col("course_name"), col("course_code")))
      ).as("enrolled_courses"),
      collect_list(
        when(col("status") === 1, struct(col("course_name"), col("course_code")))
      ).as("started_courses"),
      collect_list(
        when(col("status") === 2, struct(col("course_name"), col("course_code")))
      ).as("completed_courses")
    )

    println("[STEP 8] Create course name and code aggregations")
    // Create course names and codes columns
    val withCourseDetailsDF = aggregatedDF
      .withColumn("num_courses_enrolled", size(col("enrolled_courses")))
      .withColumn("num_courses_started", size(col("started_courses")))
      .withColumn("num_courses_completed", size(col("completed_courses")))
      .withColumn("courses_enrolled_name", concat_ws(", ", 
        transform(col("enrolled_courses"), x => col("x.course_name"))))
      .withColumn("course_started_name", concat_ws(", ", 
        transform(col("started_courses"), x => col("x.course_name"))))
      .withColumn("course_completed_name", concat_ws(", ", 
        transform(col("completed_courses"), x => col("x.course_name"))))
      .withColumn("courses_enrolled_codes", concat_ws(", ", 
        transform(col("enrolled_courses"), x => col("x.course_code"))))
      .withColumn("course_started_codes", concat_ws(", ", 
        transform(col("started_courses"), x => col("x.course_code"))))
      .withColumn("course_completed_codes", concat_ws(", ", 
        transform(col("completed_courses"), x => col("x.course_code"))))

    println("[STEP 9] Extract CF and CL names and join with user data")
    // Extract CF and CL metadata and join with user cache
    val extractMetadataUDF = udf((metadata: String, field: String) => {
      if (metadata != null && metadata.nonEmpty) {
        try {
          val map = JSONUtils.deserialize[Map[String, Any]](metadata)
          map.get(field).map(_.toString).getOrElse("")
        } catch {
          case _: Exception => ""
        }
      } else {
        ""
      }
    })

    val finalDF = withCourseDetailsDF
      .withColumn("cf_name", extractMetadataUDF(col("cf_metadata"), lit("name")))
      .withColumn("cf_code", extractMetadataUDF(col("cf_metadata"), lit("code")))
      .withColumn("cl_name", extractMetadataUDF(col("cl_metadata"), lit("name")))
      .withColumn("cl_code", extractMetadataUDF(col("cl_metadata"), lit("code")))
      .join(userCachedDF, "userid", "left")
      .select(
        col("userid"),
        col("cf_activity_id"),
        col("firstname"),
        col("lastname"),
        col("username"),
        col("email"),
        col("usertype"),
        col("cin"),
        col("fmpsid"),
        col("province"),
        col("designation"),
        col("training_group"),
        col("orgname"),
        col("cf_name").as("competency_framework_name"),
        col("cl_name").as("competency_level_name"),
        col("num_courses_enrolled"),
        col("num_courses_started"),
        col("num_courses_completed"),
        col("course_completed_name"),
        col("courses_enrolled_name"),
        col("course_started_name"),
        col("course_completed_codes"),
        col("courses_enrolled_codes"),
        col("course_started_codes")
      )

    println(s"[STEP 10] Final DataFrame created with ${finalDF.count()} records")
    finalDF
  }

  def getCollectionTrackingDF(fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    fetchData(spark, collectionTrackingSettings, cassandraUrl, new StructType())
      .select("userid", "activityid", "activity_type")
  }

  def getActivityMetadataFromRedis(jedis: Jedis, activityId: String): Option[String] = {
    try {
      val key = s"content:$activityId"
      val data = jedis.get(key)
      if (data != null) Some(data) else None
    } catch {
      case e: Exception =>
        JobLogger.log(s"Error fetching from Redis for activityId: $activityId", Option(Map("error" -> e.getMessage)), INFO)
        None
    }
  }

  def saveToPostgres(reportData: DataFrame): Unit = {
    val reportDataWithUpdate = reportData.withColumn("updated_date", current_timestamp())
    reportDataWithUpdate.write
      .mode("overwrite")
      .jdbc(url, requestsTable, connProperties)
  }

  def getDateFormat(): SimpleDateFormat = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dateFormatter.setTimeZone(TimeZone.getTimeZone("IST"))
    dateFormatter
  }
}