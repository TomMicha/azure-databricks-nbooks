// Databricks notebook source
// MAGIC %md Ingestion Notebook for factMatterFinancialActivity - update this table

// COMMAND ----------

// MAGIC %run "/Test/Utility/Scala/_sparkUtil"

// COMMAND ----------

// Store my path to notebooks here
val entity = "CustomMatterAudit"
val target = "factMatterFinancialActivity" // get easily SK 

val targetSK = "MATTER_FINANCIAL_ACTIVITY_SK"

// COMMAND ----------

dbutils.widgets.text("DATE", "", "DateForUpdate")

// COMMAND ----------

val parameter = dbutils.widgets.get("DATE")
if (parameter == ""){
  dbutils.notebook.exit("Error: No Data parameter passed, notebook must know what date to update for.. ")
}
val DATE = LocalDate.parse(parameter)
print(DATE)

// COMMAND ----------

// get notebook's job and run ID
val (jobID, runID) = getNotebookInfo()

// COMMAND ----------

// dates to get dataframe
val startDate = Timestamp.valueOf(DATE.atStartOfDay)
val endDate = Timestamp.valueOf((DATE.plusDays(1)).atStartOfDay)
println(startDate)
println(endDate)

// COMMAND ----------

// read in new records // check if new records exist?
val newrecords = dailyRead(truncatedPath+entity,"updatedAt",startDate, endDate)   //.where($"MATTER_ID".isNotNull)
printCounts(newrecords, "MATTER_ID")

// COMMAND ----------

display(newrecords.distinct)

// COMMAND ----------

val recordsWithIDs = newrecords.where($"MATTER_ID".isNotNull).count
print(recordsWithIDs)
if (recordsWithIDs == 0){
  var exit_results = getZoneLogStats(Map("entity" -> target, "jobID" -> jobID.toString, "runID" -> runID.toString, "status" -> "SKIP", "date" -> DATE))
  dbutils.notebook.exit(exit_results)
}

// COMMAND ----------

val DSM = getDSMMapping("MATTER_ID","MATTER_SK")


// COMMAND ----------

val APPEND_EXTERNAL_COLS = List[String]("MATTER_SK","FIRM_SK","POLICY_YEAR_SK","POLICY_VERSION_SK","LOB_SK","AOP_SK","POLICY_COVERAGE_START_DATE","POLICY_COVERAGE_START_DATE_SK","POLICY_COVERAGE_END_DATE","POLICY_COVERAGE_END_DATE_SK")

// COMMAND ----------

// load current FMFA table from transient delta to append to
var FMFA_current = spark.read.format("delta").load(transientPath+target)
val FMFA_MAX_SK = FMFA_current.orderBy(col(targetSK).asc).select(targetSK).collect.last.getLong(0)
// load subset of this table for static columns (as long as matter is not NEW), can add these values df.select(columns.map(col): _*)
var FMFA_subset = FMFA_current.select(APPEND_EXTERNAL_COLS.map(col): _*).distinct
//display(FMFA_current.orderBy($"MODIFIED".desc))

// COMMAND ----------

//val FMFA_current = FMFA_current1.where($"MATTER_FINANCIAL_HISTORY_DATE_SK" =!= 20201026)
//print(FMFA_current.count)

// COMMAND ----------

// function to loop over category, type to determine ACTIVITY_TYPE
// need a UDF to extract ints 
def interpretActivity(string: String): String = {
   val code = string match  { 
    case "11" => "PI"
      case "12" => "PD"
      case "21" => "RI"
      case "22" => "RD"
      case _ => "-1"
    }
  return code
}
val interpretActID = udf(interpretActivity _)

// COMMAND ----------

// function to loop over columns category, type and parse ID string
// need a UDF to extract ints 
def extractColID(string: String): Int = {
  var result = string.split("/").last.toInt
  return result
}
val extractIDUDF = udf(extractColID _)

// COMMAND ----------

// get matterActivity to join on ActivityType to get Matter_Activity_SK
val matterActivity = spark.read.format("delta").load(transientPath+"dimMatterActivity").select("MATTER_ACTIVITY_SK","ACTIVITY_TYPE")


// COMMAND ----------

var cleanRecords = newrecords.withColumn("type", extractIDUDF($"type"))
  .withColumn("category", extractIDUDF($"category"))
  .orderBy($"updatedAt".asc)
// remove all null matterIDs since we cant connect these
cleanRecords = cleanRecords.filter(($"MATTER_ID" =!= "") || ($"MATTER_ID".isNotNull))

printCounts(cleanRecords, "MATTER_ID")

// COMMAND ----------

var newDF = cleanRecords.select($"MATTER_ID",$"category",$"type",$"auditDate".alias("MATTER_FINANCIAL_HISTORY_DATE"),$"updatedAt".alias("MODIFIED"),$"netOfCoinsAmount_amount".alias("COINS_NET_ACTIVITY_AMOUNT").cast(DecimalType(20,6)),$"createdAt".alias("CREATED"),$"coinsAmount_amount".alias("COINS_AMOUNT").cast(DecimalType(20,6)),$"amount_amount".alias("ACTIVITY_AMOUNT").cast(DecimalType(20,6)))
  .withColumn("ACTIVITY_COMBINED", concat($"category",$"type"))
  .withColumn("ACTIVITY_TYPE", interpretActID(col("ACTIVITY_COMBINED")))
  .withColumn("MATTER_FINANCIAL_ACTIVITY_PROCESS_DATE", $"MODIFIED")
  .withColumn("EFFECTIVE_ON_DATE", $"MODIFIED")
  .withColumn("EXPIRED_ON_DATE", lit(null).cast(TimestampType))
  .drop("ACTIVITY_COMBINED","category","type")
printCounts(newDF, "MATTER_ID")

// COMMAND ----------

val newJoinDF = newDF.withColumn("MATTER_FINANCIAL_HISTORY_DATE_SK", date_format($"MATTER_FINANCIAL_HISTORY_DATE", "yyyyMMdd").cast(LongType))
  .withColumn("MATTER_FINANCIAL_ACTIVITY_PROCESS_DATE_SK", date_format($"MATTER_FINANCIAL_ACTIVITY_PROCESS_DATE", "yyyyMMdd").cast(LongType))
  .join(matterActivity, Seq("ACTIVITY_TYPE"), "left")
  .join(DSM, Seq("MATTER_ID"), "left")
  .drop("MATTER_ID")
//printCounts(newJoinDF, "MATTER_SK")

// COMMAND ----------

// add columns from FMFA_subset to get LOB, SK's from previous dates
var dfUpdates = newJoinDF.join(FMFA_subset, Seq("MATTER_SK"), "left")
  .orderBy($"MODIFIED".asc)
val updateMatterSKs = dfUpdates.select("MATTER_SK").distinct.map(r => r.getLong(0)).collect.toList 

// COMMAND ----------

var FMD = optionOpenDF(transientPath+"factMatterDetail")
  .select("MATTER_SK","FIRM_SK","POLICY_YEAR_SK","POLICY_VERSION_SK","LOB_SK","AOP_SK","POLICY_COVERAGE_START_DATE_SK","POLICY_COVERAGE_END_DATE_SK")
  .filter($"MATTER_SK".isin(updateMatterSKs:_*))

// COMMAND ----------

// check if any joined records have nulls in SK dependent columns save as newMatterRecords -> first time matter/claim enters FMFA table
var firstTimeMatters = false
val newMatterRecords = dfUpdates.where(col("FIRM_SK").isNull || col("POLICY_YEAR_SK").isNull || col("POLICY_VERSION_SK").isNull || col("LOB_SK").isNull || col("AOP_SK").isNull || col("POLICY_COVERAGE_START_DATE").isNull || col("POLICY_COVERAGE_END_DATE").isNull)
  .drop(APPEND_EXTERNAL_COLS.drop(1): _*)
if (newMatterRecords.isEmpty){ println("No New Matters added to FMFA") }
else {  // modify colunns below for to_timestamp conversion
  firstTimeMatters = true 
  FMD = FMD.withColumn("POLICY_COVERAGE_START_DATE_SK", $"POLICY_COVERAGE_START_DATE_SK".cast(StringType))
    .withColumn("POLICY_COVERAGE_END_DATE_SK", $"POLICY_COVERAGE_END_DATE_SK".cast(StringType))
    .withColumn("POLICY_COVERAGE_START_DATE", to_timestamp($"POLICY_COVERAGE_START_DATE_SK", "yyyyMMdd"))
    .withColumn("POLICY_COVERAGE_END_DATE", to_timestamp($"POLICY_COVERAGE_END_DATE_SK", "yyyyMMdd"))
  // redo columns with appropriate types
  FMD = FMD.withColumn("POLICY_COVERAGE_START_DATE_SK", $"POLICY_COVERAGE_START_DATE_SK".cast(LongType))
    .withColumn("POLICY_COVERAGE_END_DATE_SK", $"POLICY_COVERAGE_END_DATE_SK".cast(LongType))
 }

// COMMAND ----------

// if new, join with FMD to get required columns
if (firstTimeMatters){
  // drop matters that are first time added to FMFA via anti join on matters in newMatterRecords
  dfUpdates = dfUpdates.join(newMatterRecords, Seq("MATTER_SK"), "leftanti")
  print(dfUpdates.columns.size)
  // join newMatterRecords on FMD to get cols required from external references
  val dfNewMatters = newMatterRecords.join(FMD, Seq("MATTER_SK"), "left")
  print(dfNewMatters.columns.size)
  dfUpdates = dfUpdates.unionByName(dfNewMatters)
}

// COMMAND ----------

//add MATTER_FINANCIAL_ACTIVITY_SK
val fullDF = addNewUIDs(dfUpdates, FMFA_MAX_SK, targetSK)
//display(fullDF)

// COMMAND ----------

val saveDF = mergeDFUpdates(FMFA_current, fullDF, targetSK)
println(fullDF.count)
printCounts(saveDF, targetSK)

// COMMAND ----------

var failed = false
val count = saveDF.count.toLong
print(count)
// use failed variable to fail depending on counts above

// COMMAND ----------

display(saveDF)

// COMMAND ----------

// return results exiting notebook about run
var exit_results = getZoneLogStats(Map("entity" -> target, "jobID" -> jobID.toString, "runID" -> runID.toString, "status" -> "PASS", "date" -> DATE, "before" -> FMFA_current.count, "after" -> count, "created" -> recordsWithIDs))

// COMMAND ----------

saveTransientDF(saveDF, target, failed, exit_results)

// COMMAND ----------

