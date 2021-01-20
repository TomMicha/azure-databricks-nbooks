// Databricks notebook source
// MAGIC %md Extention Library for Spark Imports, File Paths and Utility Functions

// COMMAND ----------

import java.sql._
import org.apache.spark.sql.DataFrame
import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.LocalDateTime
import scala.collection.mutable._
import io.delta.tables._

// COMMAND ----------

// MAGIC %run "/Shared/Utility/Scala/_staticPathUtil"

// COMMAND ----------

// MAGIC %run "/Shared/Utility/Scala/_sparkFileUtil"

// COMMAND ----------

// MAGIC %run "/Shared/Utility/Scala/_mdmUtil"

// COMMAND ----------

// replace checkIfUpdated below :: Retrieve date of last table 'update' run to see if persisted should run (should use mainly in persisted zone or where applicable)
//**** may be issue for date leakage timezones?
def dailyZoneUpdateCheck(tablePath: String): Boolean = {
  val deltaTable = DeltaTable.forPath(spark, tablePath)
  val hist = deltaTable.history(1)
  val lastOperation = hist.select("timestamp").first.getTimestamp(0).toLocalDateTime.toLocalDate
  val currentDate = LocalDateTime.now.toLocalDate
  if (lastOperation == currentDate){
    return true
  } else { return false}
}

// COMMAND ----------

/** function to check current day's update for transient/persisted/curated areas if Date has already been added, override available
* @param: targetPath path to table
* @param: runDate - date to check if data updated
* @param: force : default(false) else can force to rerun for day
*/
def checkIfDateUpdated(targetPath: String, runDate: String, force: Boolean = false) : Boolean = {
  if (force) { return false}
  
  val lastDateDF :DataFrame = optionOpenDF(targetPath).select("MODIFIED")
    .withColumn("MODIFIEDX", to_date(col("MODIFIED")))
    .drop("MODIFIED")
    .filter(to_date(col("MODIFIEDX")) === runDate)

  if(lastDateDF.head(1).isEmpty){
    return false
  }
  var lastUpdate = lastDateDF.first.get(0)
  lastUpdate = LocalDate.parse(lastUpdate.toString)
  println(s"extracted date $lastUpdate")
  if (lastUpdate == runDate){
    println(s"This $runDate date has already been last updated on $lastUpdate")
    return true
  } else { return false }
}

// COMMAND ----------

// function to custom get DATE DF starting from 2000 into perpetuity within XDW
def get2000DateDF(startISOdate: String): DataFrame = {
  // create date table for all time windows 
  import java.time.temporal.ChronoUnit

  val start = LocalDate.parse(startISOdate)
  val todayTime = LocalDate.now()
  val numMonths = ChronoUnit.MONTHS.between(start.withDayOfMonth(1),todayTime.withDayOfMonth(1))

  lazy val dateStream: Stream[LocalDate] = start #:: dateStream.map(_.plusMonths(1))
  val dates = dateStream.take(numMonths.toInt+1).toSeq.map(t => (t.getYear(), t.getMonth().getValue()))
  // create BASE date table starting at 2000/01
  var dfdates = dates.toDF("YEAR","MONTH")
  dfdates = dfdates.withColumn("DATE_KEY", concat($"YEAR",$"MONTH")).drop("YEAR","MONTH")
  return dfdates
}

// COMMAND ----------

// func to help print distinct vs total counts
def printCounts(df: DataFrame, field: String): Unit = {
  println("total: "+df.count)
  println(s"total $field distinct: "+df.select(field).distinct.count)
}

// COMMAND ----------

// add index / new UniqueID (PK) for new EntityID received from Source
def addNewUIDs(df : DataFrame, maxSK: Long, nameSK: String) : DataFrame = {
  val nextSK = maxSK + 1
  val schema = df.schema
  val rows = df.rdd.zipWithIndex.map{
     case (r: Row, id: Long) => Row.fromSeq((id+ nextSK) +: r.toSeq)}
  // get DF back with PKs
  val newDFWithUID = sqlContext.createDataFrame(
    rows, StructType(StructField("id", LongType, false) +: schema.fields)).withColumnRenamed("id",nameSK)
  return newDFWithUID
}

// COMMAND ----------

/* function to drop all records past a certain point and resave - to run and drop duplicates from table zones
* @param filePath - full path to file/table
* @param fileType - file type - parquet, delta, csv
* @param targetCol - String for target col to pull date field
* @param afterDate - Timestamp for date in time to remove all date on /after this date
*/
def dropAfterDate(filePath: String, fileType: String, targetCol: String, afterDate: Timestamp): Unit = {
  val openDF = optionOpenDF(filePath, fileType)
    .filter(col(targetCol) < afterDate)
  println(openDF.distinct.count)
  val saveDF = openDF.distinct
  println(saveDF.distinct.count)
  // save over file dropping 
  optionSaveDF(saveDF,filePath, fileType)
}

// COMMAND ----------

// function to take list of columns for data null checks and validate this subset against dataframe 
def checkIfNulls(df: DataFrame, checkedCols: List[String]): Boolean = {
  var failed = false
  for (c <- checkedCols){
    val nulls = df.filter(df(c).isNull).count
    println(s"$c : $nulls")
    val nullfail = if (nulls > 0) true else false
    failed = nullfail
  }
  return failed
}

// COMMAND ----------

// function to check DBFS target path (mount) check for new files in the given targetPath   
def getNewDBFSFiles(targetPath: String) : MutableList[String] = {
  val new_files = dbutils.fs.ls(targetPath)
  val updated_entities = MutableList[String]()
  for (x <- new_files) {
    var res = x.name
    // can remove when txt files dropped
    if (!res.endsWith(".txt")){
      res = res.replace("/","")
      updated_entities += res
    } 
  }
  return updated_entities
}

// COMMAND ----------

// Add function to take 2 dataFrames and key field
// this function left anti-joins set A {original} with set B {updates} to get set C, then unions set C with set B (updates) to get newly updated DF
// can also select the columns from old df, how return TUPLE with (DF, updates, new)
def mergeDFUpdates(df_main: DataFrame, df_updates: DataFrame, fieldKey: String): DataFrame = {
  val true_cols = df_main.columns
  var df_B = df_updates.select(true_cols.map(col): _*)
  
  // get set C = main without any updated rows
  val df_C = df_main.join(df_updates, Seq(fieldKey),"leftanti")
  // join final
  val newDF = df_C.unionByName(df_B)
  return newDF
}

// COMMAND ----------

// function for taking dataframe and printing num nulls for each column

// COMMAND ----------

// check last delta version of path quickly
def getDeltaVersion(pathToTable: String): Long = {
  val tempStrMod = "'"+ pathToTable + "'"
  val query = s"SELECT max(version) FROM (DESCRIBE HISTORY $tempStrMod)"
  val curr_version = spark.sql(query).distinct.collect.last.getLong(0)
  return curr_version
}

// COMMAND ----------

/** Function to revert Delta Table back 1 version
* @param pathToTable : String - path to the table to revert version
* @param numVersions : default(1) - can override to roll back more than 1 version
*/
def rollBackDelta(pathToTable: String, numVersions: Int = 1): Unit = {
  val tempStrMod = "'"+ pathToTable + "'"
  val query = s"SELECT max(version) FROM (DESCRIBE HISTORY $tempStrMod)"
  val curr_version = spark.sql(query).distinct.collect.last.getLong(0)
  println(curr_version)
  val prev_version = curr_version - numVersions // * i collect the 2nd to last item instead
  println(prev_version)
  val readDF  : DataFrame = spark.read.format("delta")
    .option("versionAsOf", prev_version)
    .load(pathToTable)
// save over with prev version
  optionSaveDF(readDF,pathToTable, "delta","overwrite","Rollback to previous version due to sequential update procedure error")
  
}

// COMMAND ----------

import java.io._ 
import scala.io.Source

// COMMAND ----------

// helper function to save in scala using File and PrintWriter to a file location and close file
def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

// COMMAND ----------

// function that take local date, returns a path dir for this date
def dateDirPath(date: LocalDate): String = {
  val year = date.getYear()
  var month = date.getMonth().toString.toLowerCase
  month = month.substring(0,1).toUpperCase() + month.substring(1)
  val day = date.getDayOfMonth()
  val returnPath = s"$year/$month/$day/"
  return returnPath
}

// COMMAND ----------

// function to take a list and write the list to the file path given
def writeSummaryLogs(zone: String, filepath: String, data: List[String]): Unit = {
  val break_start = List("***************************")
  val data_list = break_start.++(data)
  dbutils.fs.mkdirs(filepath)
  val filename = filepath + zone +".txt"
  printToFile(new File(filename)) { p =>
  data_list.foreach(p.println)
  }
}

// COMMAND ----------

// function to capture nested notebook run info
def getNotebookInfo(): (String, String) = {
  val tags = dbutils.notebook.getContext.tags
  val jobID = tags.get("jobId").getOrElse("0000")
  val runID = tags.get("runId").getOrElse("0000")
  return (jobID, runID)
}

// COMMAND ----------

/** New (finish) function to save a message to file // change to get list instead of map and set schema here
* @param  runResults = Map[String] - schema defined by k,v passed by zone
*/
// could this take a map and organize the messages based on agreed upon sequence? val returning = s"$status,$entity,$runId,$jobId,$date,$new,$updates,$before,$after"
// PASS,dimPerson,1432,1433,2020-10-21,2,12,75999,76001,
def getZoneStats(runResults: Map[String,Any]): String = {
  // extract from map given the correct keys

  var returnStr = ""
  for ((k,v) <- runResults) {
    val result = k + " : " + v.toString + " "
    returnStr = returnStr ++ result
  }
    /*val returnList = MutableList[String]()
  for ((k,v) <- runResults) {
    val resultStr = k + " : " + v.toString
    returnList+= resultStr
  }*/
  return returnStr
}

// COMMAND ----------

// helper functions for logging etc
def test_sublist[A](list1:List[A], list2:List[A]):Boolean = {
      list1.forall(list2.contains)
 }
// helper function to convert types in dataframe to those required by logging entry table
def helpLoggingEntryTypes(df: DataFrame): DataFrame = {
  return df.withColumn("date", to_date($"date", "yyyy-MM-dd"))
  .withColumn("runID", $"runID".cast(LongType))
  .withColumn("jobID", $"jobID".cast(LongType))
  .withColumn("created", $"created".cast(LongType))
  .withColumn("updated", $"updated".cast(LongType))
  .withColumn("before", $"before".cast(LongType))
  .withColumn("after", $"after".cast(LongType))
}

// COMMAND ----------

/** New Logging function to / get map of values and return a dataframe 
* @param  runResults = Map[String] - schema defined by k,v passed by zone
*/
def createLoggingEntry(runResults: Map[String,String]): DataFrame = {
  // not most efficient, additional copy map holding the req and not required, empty variables
  val masterKeysMap = Map("table" -> "true",  "status" -> "true", "zone" -> "true", "date" -> "true", "runID" -> "true", "jobID" -> "true", "created" -> "", "updated" -> "", "before" -> "", "after" -> "", "stdout"-> "")
  val sequencedKeys = List("table","status","zone","date","runID","jobID","created","updated","before","after","stdout")
  val requiredKeys = List("table","status","zone","date","runID","jobID")
  println(requiredKeys)
  // check you have all req fields, 
  val hasAllReqKeys = test_sublist(requiredKeys, runResults.keys.toList)
  println(s"Has all required fields: $hasAllReqKeys")
  require(hasAllReqKeys, "Must have all required fields")
  // if none in a required field, throw exception
  for ((k,v) <- runResults){
    if (masterKeysMap.contains(k)){
      print("in master list")
      require(masterKeysMap.contains(k), "Caller Must have one of the accepted column values in passed map")
    }
    println(s"$k , $v")
    // check required fields
    if (requiredKeys.contains(k)){
      println("required value checked")
      require(v != null, "Cannot have a null in a required field")
    }
  }
  // append null for remaining columns
  var notIncludedMap = Map[String,String]()
  for ((k,v) <- masterKeysMap){
    if(!runResults.contains(k)){
      notIncludedMap += (k -> v)
    }
  }

  println(notIncludedMap)
  val finalMap = runResults ++= notIncludedMap
  //notIncludedCols.map(x => finalMap += (x, ""))
  // all columns should match master now
  println(finalMap)
  println("almost done")
  
  val rdd = sc.parallelize(Seq(Row.fromSeq(finalMap.values.toSeq)))
  val schema = StructType(finalMap.keys.toSeq.map(StructField(_, StringType)))
  val returnDF = spark.createDataFrame(rdd,schema)
    .select(sequencedKeys.map(col):_*)
  // call helper function to format types for lgogging entry
  return helpLoggingEntryTypes(returnDF)
}

// COMMAND ----------

// helper func to search thru map with string keys and check if kv pair exists, if so return stringified value, else empty string
def mapExtractLogsHelper(searchKey: String, map: Map[String,Any]): String = {
  if (map.isEmpty){
    return ""
  }
  val mapContainsFlag = map.contains(searchKey)
  if (mapContainsFlag){
    val mapValue = map(searchKey)
    return mapValue.toString
  }
  else {
    return ""
  }
}
/** New  function to save a schema for csv to file // get map of values and return comma delimited text string
* @param  runResults = Map[String] - schema defined by k,v passed by zone
*/
def getZoneLogStats(runResults: Map[String,Any]): String = {
  // schema strings
  val status = mapExtractLogsHelper("status",runResults)
  val entity = mapExtractLogsHelper("entity",runResults)
  val jobID = mapExtractLogsHelper("jobID",runResults)
  val runID = mapExtractLogsHelper("runID",runResults)
  val date = mapExtractLogsHelper("date",runResults)
  val created = mapExtractLogsHelper("created",runResults)
  val updated = mapExtractLogsHelper("updated",runResults)
  val before = mapExtractLogsHelper("before",runResults)
  val after = mapExtractLogsHelper("after",runResults)
  
  val returningStr = s"$status,$entity,$runID,$jobID,$date,$created,$updated,$before,$after"
  return returningStr
}

// COMMAND ----------

/* helper funciton for Curated - create DateKey from chosen date column - able to group and join on this or use as composite key
* @param df - dataframe
* @param targetDate - date field for creating date key column
*/
def createDateKeyCol(df: DataFrame, targetDate: String) : DataFrame = {
  val returnDF = df.withColumn("YEAR", year(col(targetDate)))
    .withColumn("MONTH", month(col(targetDate)))
    .withColumn("MONTH", addMonthXudf($"MONTH"))
    .withColumn("DATE_KEY", concat($"YEAR", $"MONTH"))
    .drop("YEAR", "MONTH")
  return returnDF
}
// helper UDF for Month to change to 01
// UDF to change Def/Ind to PI/PD for FIRM paid dollars
def updateMonth(data: String): String = {
  if (data.toInt < 10) return "0"+data
  else return data
}
val addMonthXudf = udf(updateMonth _)

// COMMAND ----------

// function to check designated primary key has no duplicates
def checkDistinctPK(df: DataFrame, targetCol: String) : Boolean = {
  val count = df.select(targetCol).count
  val uniq_count = df.select(targetCol).distinct.count
  if (count == uniq_count){
    return true
  }
  return false
}

// COMMAND ----------

/* function to return df from file - 
* @param : path
* @param : column target for query
* @param : target Ids of target col to query by
def openTableByTargetFields(path: String, col: String, targets: List[String]): DataFrame // maybe adjust to List[Any]

*/