package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.UserCacheSupport

import java.util.Properties

// Case classes for API responses
case class ObservableElementResponse(id: String, ver: String, ts: String, params: Map[String, Any], responseCode: String, result: ObservableElementResult)

case class ObservableElementResult(framework: ObservableElementFramework)

case class ObservableElementFramework(identifier: String, code: String, name: String, status: String, category: String)

object UserSkillSummaryReport extends IJob with BaseReportsJob with UserCacheSupport {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  // Database settings
  private val collectionTrackingDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.collection.keyspace"), "cluster" -> "LMSCluster")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "ReportCluster");
  private val hierarchyStoreSettings = Map("table" -> "content_hierarchy", "keyspace" -> AppConf.getConfig("sunbird.hierarchy.keyspace"), "cluster" -> "LMSCluster")
  // Report columns for the final output
  private val reportCols = Seq("userid", "firstname", "lastname", "username", "email", "usertype", "cin", "fmpsid", "province", "designation", "training_group", "orgname", "createddate", "courseid", "course_status", "observable_elements")
  // PostgreSQL connection settings
  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
  val db: String = AppConf.getConfig("postgres.db")
  val url: String = AppConf.getConfig("postgres.url") + s"$db"
  val requestsTable: String = "skill_summary_report"

  implicit val className: String = "org.sunbird.analytics.job.report.UserSkillSummaryReport"
  val jobName = "UserSkillSummaryReport"

  // API Configuration - Read from application.conf
  val frameworkApiUrl = AppConf.getConfig("framework.api.url")
  val authToken = AppConf.getConfig("framework.api.auth.token")

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()

    try {
      val res = CommonUtil.time(prepareReport(spark, fetchData))
      val reportData = res._2
      saveToPostgres(reportData)
      reportData.unpersist()
      JobLogger.end(s"$jobName completed successfully", "SUCCESS", Option(Map("timeTaken" -> res._1)))
    } catch {
      case ex: Exception =>
        JobLogger.log(s"$jobName failed", Option(Map("error" -> ex.getMessage)), INFO)
        JobLogger.end(s"$jobName failed", "FAILED", Option(Map("error" -> ex.getMessage)))
        throw ex
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    //spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    //spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
  }

  /**
   * Step 1: Get user enrollments from sb_collection_tracking.user_enrolments
   * Extract userid and activityid
   */
  def getCollectionTrackingEnrolments(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val df = fetchData(spark, collectionTrackingDBSettings, cassandraUrl, new StructType())
    df.select("userid", "activityid")
      .filter(col("activityid").isNotNull && col("activityid") =!= "")
      .filter(col("userid").isNotNull && col("userid") =!= "")
      .distinct()
  }

  /**
   * Step 2: Fetch hierarchy from content_hierarchy table
   * Join on activityid → identifier
   */
  def getHierarchyData(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val df = fetchData(spark, hierarchyStoreSettings, cassandraUrl, new StructType())
    df.select("identifier", "hierarchy")
      .filter(col("identifier").isNotNull && col("identifier") =!= "")
      .filter(col("hierarchy").isNotNull && col("hierarchy") =!= "")
  }

  /**
   * Step 3: Extract courseid from hierarchy JSON where primaryCategory == "Course"
   * activityid = identifier (used to fetch hierarchy)
   * courseid = extracted from hierarchy JSON where primaryCategory == "Course"
   */
  def extractCourseIdFromHierarchy(hierarchyJson: String): String = {
    try {
      val hierarchy = JSONUtils.deserialize[Map[String, Any]](hierarchyJson)

      // Debug: Log the hierarchy structure
      JobLogger.log(s"DEBUG: Parsing hierarchy JSON", Option(Map("hierarchy_keys" -> hierarchy.keys.mkString(", "))), INFO)

      // Recursively search for Course in the hierarchy - works for any depth and structure
      def findCourseRecursively(node: Any, depth: Int = 0, maxDepth: Int = 20): String = {
        // Prevent infinite recursion
        if (depth > maxDepth) {
          JobLogger.log(s"DEBUG: Maximum depth reached ($maxDepth), stopping search", Option(Map("depth" -> depth)), INFO)
          return ""
        }

        node match {
          case map: Map[String, Any] =>
            // Check current node for Course
            map.get("primaryCategory") match {
              case Some("Course") =>
                val courseId = map.get("identifier").map(_.toString).getOrElse("")
                JobLogger.log(s"DEBUG: Found Course at depth $depth", Option(Map("courseId" -> courseId, "depth" -> depth)), INFO)
                return courseId
              case _ => // Continue searching
            }

            // Search in children recursively - handle different children structures
            val children = map.get("children").orElse(map.get("childNodes")).orElse(map.get("items"))
            children match {
              case Some(childrenList: List[Any]) =>
                JobLogger.log(s"DEBUG: Searching in children at depth $depth", Option(Map("children_count" -> childrenList.size, "depth" -> depth)), INFO)
                for (child <- childrenList) {
                  val result = findCourseRecursively(child, depth + 1, maxDepth)
                  if (result.nonEmpty) return result
                }
                ""
              case Some(childrenSeq: Seq[Any]) =>
                JobLogger.log(s"DEBUG: Searching in children (Seq) at depth $depth", Option(Map("children_count" -> childrenSeq.size, "depth" -> depth)), INFO)
                for (child <- childrenSeq) {
                  val result = findCourseRecursively(child, depth + 1, maxDepth)
                  if (result.nonEmpty) return result
                }
                ""
              case _ =>
                JobLogger.log(s"DEBUG: No children found at depth $depth", Option(Map("depth" -> depth)), INFO)
                ""
            }
          case _ =>
            JobLogger.log(s"DEBUG: Non-map node at depth $depth", Option(Map("node_type" -> node.getClass.getSimpleName, "depth" -> depth)), INFO)
            ""
        }
      }

      val courseId = findCourseRecursively(hierarchy)
      if (courseId.isEmpty) {
        JobLogger.log(s"DEBUG: No Course found in entire hierarchy", None, INFO)
      }
      courseId

    } catch {
      case e: Exception =>
        JobLogger.log(s"Error parsing hierarchy JSON for courseId", Option(Map("error" -> e.getMessage, "hierarchyJson" -> hierarchyJson.take(200))), INFO)
        ""
    }
  }

  /**
   * Step 4: Get completed courses from sunbird_courses.user_enrolments (status = 2)
   */
  def getCompletedEnrolments(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val df = fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
    df.select("userid", "courseid", "status")
      .filter(col("status") === 2) // Only completed courses
      .filter(col("courseid").isNotNull && col("courseid") =!= "")
      .filter(col("userid").isNotNull && col("userid") =!= "")
      .distinct()
  }

  /**
   * Step 5: Extract targetObservableElementIds from hierarchy JSON
   */
  def extractTargetObservableElementIds(hierarchyJson: String): List[String] = {
    try {
      val hierarchy = JSONUtils.deserialize[Map[String, Any]](hierarchyJson)

      // Recursively collect all targetObservableElementIds from the hierarchy - works for any depth and structure
      def collectObservableElementIds(node: Any, depth: Int = 0, maxDepth: Int = 20): List[String] = {
        // Prevent infinite recursion
        if (depth > maxDepth) {
          JobLogger.log(s"DEBUG: Maximum depth reached ($maxDepth), stopping search", Option(Map("depth" -> depth)), INFO)
          return List.empty
        }

        node match {
          case map: Map[String, Any] =>
            var allIds = List.empty[String]

            // Check current node for targetObservableElementIds - handle multiple field name variations
            val currentNodeIds = map.get("targetobservableElementIds")
              .orElse(map.get("targetObservableElementIds"))
              .orElse(map.get("targetObservableelementIds"))
              .orElse(map.get("observableElementIds"))
              .orElse(map.get("se_observableElementIds"))
              .orElse(map.get("observableElement"))

            currentNodeIds match {
              case Some(idList: List[String]) =>
                allIds = allIds ++ idList.filter(_ != null).filter(_ != "")
                JobLogger.log(s"DEBUG: Found observable element IDs at depth $depth", Option(Map("ids" -> idList.mkString(", "), "depth" -> depth)), INFO)
              case Some(idList: Seq[String]) =>
                allIds = allIds ++ idList.toList.filter(_ != null).filter(_ != "")
                JobLogger.log(s"DEBUG: Found observable element IDs at depth $depth", Option(Map("ids" -> idList.mkString(", "), "depth" -> depth)), INFO)
              case Some(idString: String) =>
                allIds = allIds ++ List(idString).filter(_ != null).filter(_ != "")
                JobLogger.log(s"DEBUG: Found single observable element ID at depth $depth", Option(Map("id" -> idString, "depth" -> depth)), INFO)
              case _ => // No IDs at this level
            }

            // Recursively search in children - handle different children structures
            val children = map.get("children").orElse(map.get("childNodes")).orElse(map.get("items"))
            children match {
              case Some(childrenList: List[Any]) =>
                for (child <- childrenList) {
                  allIds = allIds ++ collectObservableElementIds(child, depth + 1, maxDepth)
                }
              case Some(childrenSeq: Seq[Any]) =>
                for (child <- childrenSeq) {
                  allIds = allIds ++ collectObservableElementIds(child, depth + 1, maxDepth)
                }
              case _ => // No children
            }

            allIds
          case _ =>
            JobLogger.log(s"DEBUG: Non-map node at depth $depth", Option(Map("node_type" -> node.getClass.getSimpleName, "depth" -> depth)), INFO)
            List.empty
        }
      }

      val allIds = collectObservableElementIds(hierarchy)
      val uniqueIds = allIds.distinct

      JobLogger.log(s"DEBUG: Collected observable element IDs", Option(Map("total_found" -> allIds.size, "unique_count" -> uniqueIds.size, "ids" -> uniqueIds.mkString(", "))), INFO)

      uniqueIds
    } catch {
      case e: Exception =>
        JobLogger.log(s"Error parsing hierarchy JSON for targetObservableElementIds", Option(Map("error" -> e.getMessage)), INFO)
        List.empty
    }
  }

  /**
   * Step 6: Fetch observable element details from framework API
   * Makes API call to: https://dev.maharat.fmps.ma/api/framework/v1/read/{elementId}
   * Returns Map of elementId -> (code, name)
   */
  def getObservableElementDetails(elementIds: List[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Map[String, (String, String)] = {
    if (elementIds.isEmpty) return Map.empty

    JobLogger.log(s"Fetching details for ${elementIds.size} observable elements from framework API", Option(Map("count" -> elementIds.size)), INFO)

    elementIds.flatMap { elementId =>
      try {
        val headers = Map("Authorization" -> authToken)
        val apiUrl = s"$frameworkApiUrl$elementId"

        val response = RestUtil.get[ObservableElementResponse](apiUrl, Option(headers))

        val code = response.result.framework.code
        val name = response.result.framework.name

        JobLogger.log(s"Successfully fetched element details", Option(Map("elementId" -> elementId, "code" -> code, "name" -> name)), INFO)

        Some(elementId -> (code, name))
      } catch {
        case e: Exception =>
          JobLogger.log(s"Error fetching observable element: $elementId", Option(Map("error" -> e.getMessage, "url" -> s"$frameworkApiUrl$elementId")), INFO)
          None
      }
    }.toMap
  }

  /**
   * Main report preparation method
   * Flow:
   * 1. Get collection tracking enrolments (userid, activityid)
   * 2. Join with hierarchy on activityid = identifier to get hierarchy JSON
   * 3. Extract courseid from hierarchy JSON
   * 4. Get completed courses (status=2) from user_enrolments
   * 5. Filter to only include completed courses
   * 6. Extract observable elements and enrich with API data
   * 7. Join with user data on userid ONLY
   */
  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sparkSession: SparkSession = spark
    import spark.implicits._

    JobLogger.log("=== Starting UserSkillSummaryReport Data Preparation ===", None, INFO)

    // Step 1: Get collection tracking enrolments (userid and activityid)
    JobLogger.log("Step 1: Fetching collection tracking enrolments from sb_collection_tracking.user_enrolments", None, INFO)
    val collectionEnrolmentsDF = getCollectionTrackingEnrolments(spark, fetchData)
    val collectionCount = collectionEnrolmentsDF.count()
    JobLogger.log(s"Step 1: Fetched collection tracking enrolments", Option(Map("count" -> collectionCount)), INFO)

    // Show sample data
    JobLogger.log("Step 1: Sample collection tracking enrolments data:", None, INFO)
    collectionEnrolmentsDF.show(10, truncate = false)

    if (collectionCount == 0) {
      JobLogger.log("WARNING: No collection tracking enrolments found!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // Step 2: Get hierarchy data from content_hierarchy table
    // content_hierarchy has columns: identifier and hierarchy (JSON)
    JobLogger.log("Step 2: Fetching hierarchy data from content_hierarchy table", None, INFO)
    val hierarchyDF = getHierarchyData(spark, fetchData)
    val hierarchyCount = hierarchyDF.count()
    JobLogger.log(s"Step 2: Fetched hierarchy data from content_hierarchy table", Option(Map("count" -> hierarchyCount)), INFO)

    // Show sample data
    JobLogger.log("Step 2: Sample hierarchy data:", None, INFO)
    hierarchyDF.show(10, truncate = false)

    if (hierarchyCount == 0) {
      JobLogger.log("WARNING: No hierarchy data found!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // Step 2b: Match activityid with identifier to get the hierarchy JSON
    // activityid (from collection_tracking) = identifier (from content_hierarchy)
    JobLogger.log("Step 2b: Joining collection enrolments with hierarchy data on activityid = identifier", None, INFO)
    val collectionWithHierarchyDF = collectionEnrolmentsDF
      .join(
        hierarchyDF,
        collectionEnrolmentsDF("activityid") === hierarchyDF("identifier"),
        "inner"
      )
      .select(
        col("userid"),
        col("activityid"),
        col("hierarchy")
      )

    val joinedCount = collectionWithHierarchyDF.count()
    JobLogger.log(s"Step 2b: Matched activityid with identifier", Option(Map("count" -> joinedCount)), INFO)

    // Show sample data
    JobLogger.log("Step 2b: Sample joined collection with hierarchy data:", None, INFO)
    collectionWithHierarchyDF.show(10, truncate = false)

    if (joinedCount == 0) {
      JobLogger.log("WARNING: No matches between activityid and identifier!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // Step 3: Extract courseid from hierarchy JSON where primaryCategory == "Course"
    JobLogger.log("Step 3: Extracting courseId from hierarchy JSON where primaryCategory == 'Course'", None, INFO)

    // Debug: Show sample hierarchy JSON before processing
    JobLogger.log("Step 3: Sample hierarchy JSON before courseId extraction:", None, INFO)
    collectionWithHierarchyDF.select("hierarchy").show(5, truncate = false)

    val extractCourseIdUDF = udf((hierarchyJson: String) => {
      if (hierarchyJson == null || hierarchyJson.isEmpty) ""
      else extractCourseIdFromHierarchy(hierarchyJson)
    })

    // Debug: Show what courseIds are being extracted (including empty ones)
    val debugCourseIdDF = collectionWithHierarchyDF
      .withColumn("courseid", extractCourseIdUDF(col("hierarchy")))

    JobLogger.log("Step 3: Debug - All extracted courseIds (including empty):", None, INFO)
    debugCourseIdDF.select("userid", "activityid", "courseid").show(10, truncate = false)

    val userCourseDataDF = debugCourseIdDF
      .filter(col("courseid") =!= "")
      .select(
        col("userid"),
        col("activityid"),
        col("courseid"),
        col("hierarchy")
      )

    val courseIdCount = userCourseDataDF.count()
    JobLogger.log(s"Step 3: Extracted courseId from hierarchy", Option(Map("count" -> courseIdCount)), INFO)

    // Show sample data
    JobLogger.log("Step 3: Sample data after courseId extraction:", None, INFO)
    userCourseDataDF.show(10, truncate = false)

    if (courseIdCount == 0) {
      JobLogger.log("WARNING: No courseIds extracted from hierarchy!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // Step 4: Get completed enrollments (status = 2) from sunbird_courses
    JobLogger.log("Step 4: Fetching completed enrolments (status=2) from sunbird_courses.user_enrolments", None, INFO)
    val completedEnrolmentsDF = getCompletedEnrolments(spark, fetchData)
    val completedCount = completedEnrolmentsDF.count()
    JobLogger.log(s"Step 4: Fetched completed enrolments (status=2)", Option(Map("count" -> completedCount)), INFO)

    // Show sample data
    JobLogger.log("Step 4: Sample completed enrolments data:", None, INFO)
    completedEnrolmentsDF.show(10, truncate = false)

    if (completedCount == 0) {
      JobLogger.log("WARNING: No completed enrolments found!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // Step 4b: Join to filter only completed courses
    // Join on BOTH userid and courseid to ensure we only get completed courses
    JobLogger.log("Step 4b: Joining user course data with completed enrolments on userid and courseid", None, INFO)
    val completedCoursesWithHierarchyDF = userCourseDataDF
      .join(
        completedEnrolmentsDF,
        userCourseDataDF("userid") === completedEnrolmentsDF("userid") &&
          userCourseDataDF("courseid") === completedEnrolmentsDF("courseid"),
        "inner"
      )
      .select(
        userCourseDataDF("userid"),
        userCourseDataDF("activityid"),
        userCourseDataDF("courseid"),
        completedEnrolmentsDF("status"),
        userCourseDataDF("hierarchy")
      )

    val completedJoinCount = completedCoursesWithHierarchyDF.count()
    JobLogger.log(s"Step 4b: Filtered to completed courses only", Option(Map("count" -> completedJoinCount)), INFO)

    // Show sample data
    JobLogger.log("Step 4b: Sample data after filtering to completed courses:", None, INFO)
    completedCoursesWithHierarchyDF.show(10, truncate = false)

    if (completedJoinCount == 0) {
      JobLogger.log("WARNING: No matches between extracted courseids and completed enrolments!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // Step 5: Extract targetObservableElementIds from hierarchy JSON
    JobLogger.log("Step 5: Extracting targetObservableElementIds from hierarchy JSON", None, INFO)
    val extractObservableIdsUDF = udf((hierarchyJson: String) => {
      if (hierarchyJson == null || hierarchyJson.isEmpty) List.empty[String]
      else extractTargetObservableElementIds(hierarchyJson)
    })

    val coursesWithObservableElementsDF = completedCoursesWithHierarchyDF
      .withColumn("targetObservableElementIds", extractObservableIdsUDF(col("hierarchy")))
      .select(
        col("userid"),
        col("activityid"),
        col("courseid"),
        col("status"),
        col("targetObservableElementIds")
      )

    val observableCount = coursesWithObservableElementsDF
      .filter(size(col("targetObservableElementIds")) > 0)
      .count()

    JobLogger.log(s"Step 5: Extracted targetObservableElementIds",
      Option(Map("total_courses" -> completedJoinCount, "courses_with_elements" -> observableCount)), INFO)

    // Show sample data
    JobLogger.log("Step 5: Sample data after extracting observable element IDs:", None, INFO)
    coursesWithObservableElementsDF.show(10, truncate = false)

    // Step 6: Get all unique observable element IDs and fetch their details
    JobLogger.log("Step 6: Collecting unique observable element IDs for API enrichment", None, INFO)
    val allObservableElementIds = coursesWithObservableElementsDF
      .filter(size(col("targetObservableElementIds")) > 0)
      .select(explode(col("targetObservableElementIds")).as("element_id"))
      .distinct()
      .as[String]
      .collect()
      .toList

    JobLogger.log(s"Step 6: Found unique observable elements", Option(Map("count" -> allObservableElementIds.size)), INFO)
    JobLogger.log(s"Step 6: Unique observable element IDs: ${allObservableElementIds.take(10).mkString(", ")}", None, INFO)

    // Fetch observable element details (code, name) from API
    JobLogger.log("Step 6: Fetching observable element details from framework API", None, INFO)
    val observableElementDetailsMap = if (allObservableElementIds.nonEmpty) {
      getObservableElementDetails(allObservableElementIds)
    } else {
      Map.empty[String, (String, String)]
    }

    JobLogger.log(s"Step 6: Successfully fetched details for ${observableElementDetailsMap.size} observable elements", None, INFO)

    val broadcastElementDetailsMap = spark.sparkContext.broadcast(observableElementDetailsMap)

    // Step 6b: Enrich observable elements with code and name
    JobLogger.log("Step 6b: Enriching observable elements with code and name from API data", None, INFO)
    val enrichObservableElementsUDF = udf((elementIds: Seq[String]) => {
      if (elementIds == null || elementIds.isEmpty) {
        "[]"  // Return empty JSON array string
      } else {
        val detailsMap = broadcastElementDetailsMap.value
        val enrichedElements = elementIds.filter(_ != null).distinct.map { elementId =>
          val (code, name) = detailsMap.getOrElse(elementId, ("", ""))
          Map(
            "element_id" -> elementId,
            "code" -> code,
            "name" -> name
          )
        }
        JSONUtils.serialize(enrichedElements)
      }
    })

    val skillDataDF = coursesWithObservableElementsDF
      .withColumn("observable_elements", enrichObservableElementsUDF(col("targetObservableElementIds")))
      .withColumn("course_status", lit("Completed"))
      .select(
        col("userid"),
        col("courseid"),
        col("course_status"),
        col("observable_elements")
      )

    val skillDataCount = skillDataDF.count()
    JobLogger.log(s"Step 6b: Created skill data DF with observable elements", Option(Map("count" -> skillDataCount)), INFO)

    // Show sample data
    JobLogger.log("Step 6b: Sample skill data after enrichment:", None, INFO)
    skillDataDF.show(10, truncate = false)

    // Step 7: Get user details and join with skill data (join ONLY on userid)
    JobLogger.log("Step 7: Fetching user details from user cache", None, INFO)
    val userCachedDF = getUserCacheDF(spark, fetchData)
    val decryptedUserDF = decryptUserInfo(userCachedDF)
    val userCount = decryptedUserDF.count()

    JobLogger.log(s"Step 7: Fetched user details", Option(Map("count" -> userCount)), INFO)

    // Show sample user data
    JobLogger.log("Step 7: Sample user data:", None, INFO)
    decryptedUserDF.show(10, truncate = false)

    if (userCount == 0) {
      JobLogger.log("WARNING: No user details found!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // CRITICAL: Join ONLY on userid
    // This creates one row per user per completed course (with status=2)
    JobLogger.log("Step 7: Joining user data with skill data on userid", None, INFO)
    val finalReportDF = decryptedUserDF
      .join(skillDataDF, Seq("userid"), "inner")
      .na.fill("[]", Seq("observable_elements"))
      .na.fill("Completed", Seq("course_status"))

    val finalCount = finalReportDF.count()
    JobLogger.log(s"Step 7: Final report after joining with users on userid", Option(Map("count" -> finalCount)), INFO)

    // Show sample final data
    JobLogger.log("Step 7: Sample final report data:", None, INFO)
    finalReportDF.show(10, truncate = false)

    if (finalCount == 0) {
      JobLogger.log("WARNING: Final report is empty after joining with user details!", None, INFO)
      return createEmptyReportDF(spark)
    }

    // Select final columns
    JobLogger.log("Final: Selecting final report columns", None, INFO)
    val reportDF = finalReportDF.select(reportCols.head, reportCols.tail: _*)

    val finalReportCount = reportDF.count()
    JobLogger.log(s"Final: Generated report successfully", Option(Map("total_rows" -> finalReportCount)), INFO)

    // Show final sample data
    JobLogger.log("Final: Sample final report data with all columns:", None, INFO)
    reportDF.show(10, truncate = false)

    JobLogger.log("=== UserSkillSummaryReport Data Preparation Completed ===", None, INFO)

    reportDF
  }

  /**
   * Create an empty DataFrame with the correct schema when no data is found
   */
  def createEmptyReportDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.emptyDataFrame.select(
      lit(null).cast("string").as("userid"),
      lit(null).cast("string").as("firstname"),
      lit(null).cast("string").as("lastname"),
      lit(null).cast("string").as("username"),
      lit(null).cast("string").as("email"),
      lit(null).cast("string").as("usertype"),
      lit(null).cast("string").as("cin"),
      lit(null).cast("string").as("fmpsid"),
      lit(null).cast("string").as("province"),
      lit(null).cast("string").as("designation"),
      lit(null).cast("string").as("training_group"),
      lit(null).cast("string").as("orgname"),
      lit(null).cast("string").as("createddate"),
      lit(null).cast("string").as("courseid"),
      lit(null).cast("string").as("course_status"),
      lit(null).cast("string").as("observable_elements")
    ).limit(0)
  }

  /**
   * Save the report to PostgreSQL
   */
  /**
   * Save the report to PostgreSQL
   * Creates table with schema even if data is empty
   */
  def saveToPostgres(reportData: DataFrame): Unit = {
    import org.apache.spark.sql.functions.current_timestamp

    val reportCount = reportData.count()

    try {
      // Add updated_date column to the dataframe
      val reportDataWithUpdate = reportData.withColumn("updated_date", current_timestamp())

      if (reportCount == 0) {
        JobLogger.log(s"No data to save - creating empty table with schema in PostgreSQL", None, INFO)

        // Write empty dataframe with mode "overwrite" to create table structure
        reportDataWithUpdate.write
          .mode("overwrite")
          .jdbc(url, requestsTable, connProperties)

        JobLogger.log(s"Successfully created empty table in PostgreSQL: $requestsTable with schema", None, INFO)
      } else {
        // Normal write operation when data exists
        reportDataWithUpdate.write
          .mode("overwrite")
          .jdbc(url, requestsTable, connProperties)

        JobLogger.log(s"Successfully saved report to PostgreSQL table: $requestsTable",
          Option(Map("rows" -> reportCount)), INFO)
      }
    } catch {
      case ex: Exception =>
        JobLogger.log(s"Error saving report to PostgreSQL",
          Option(Map("error" -> ex.getMessage, "table" -> requestsTable)), INFO)
        throw ex
    }
  }

  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30))
    dateFormat.print(System.currentTimeMillis())
  }
}