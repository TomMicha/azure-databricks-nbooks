// Databricks notebook source
// MAGIC %md Notebook to Coordinate Which notebooks to call based on ingested data

// COMMAND ----------

// MAGIC %run "/Shared/Utility/Scala/_sparkUtil"

// COMMAND ----------

// MAGIC %md  Run library loading concurrent notebook processing

// COMMAND ----------

// MAGIC %run "/Shared/Utility/Scala/_concLib"

// COMMAND ----------

//Cycle DATE parameter for daily updates
val DATE = LocalDate.now().minusDays(3)
val dateTimestamp = Timestamp.valueOf(DATE.atStartOfDay)
val dateForPull = DATE.toString

// COMMAND ----------

// path to each area where notebooks are called, need NB path
val nbParsePath = "/Shared/SourceExtract/PassportAPI/convertParseExtract"
val nbProcessingPath = "/Shared/SourceExtract/PassportAPI/entityProcessing/"
val nbTransientPath = "/Shared/Ingestion/"
val nbPersistedPath = "/Shared/Persisted/"
val nbCuratedPath = "/Shared/Curated/"

// COMMAND ----------

// MAGIC %md TRANSIENT/INGESTION RUN

// COMMAND ----------

// temp function to get list of values from Zone DF
def getZoneList(df: DataFrame, field: String): List[String] = {
  val result = df.select(explode(col(field))).rdd.map(r=> r(0).asInstanceOf[String]).collect.toList
  return result
}

// COMMAND ----------

val jsonPath = refPath+"entityLevels.json"
val ZONES = spark.read.json(jsonPath)

display(ZONES)

// COMMAND ----------

//Set Ingestion Summary Logs path
val dirLogsPath = dateDirPath(DATE)
val fullLogsPath = passLogsPath+dirLogsPath

// COMMAND ----------

val TRANSIENT = ZONES.where($"ZONE" === "TRANSIENT").select("TABLES.*", "*")
  .drop("ZONE","TABLES","SOURCE")
display(TRANSIENT)

// COMMAND ----------

//val transientLevels = List[String]("L5","L6") //TRANSIENT.columns.toList //
//val L1 = getZoneList(TRANSIENT,"L1")
val transientLevels = List[String]("L2","L3","L4","L5","L6")

// COMMAND ----------

// this pulls all notebookData objects into a list of lists depending the order the transient/Ingestion notebooks must be executed
val transient_notebooks = MutableList[List[NotebookData]]()

for (lev <- transientLevels){
  // what if null in a level?
  val level = getZoneList(TRANSIENT,lev)
  if (!level.isEmpty){
    val testnb = createNotebookDataList(level, nbTransientPath, dateForPull)
    transient_notebooks += testnb
  }
}

transient_notebooks.length

// COMMAND ----------

transient_notebooks.get(0)

// COMMAND ----------

// !* Parallel Execution of Transient Notebooks
var transientReturns = MutableList[List[String]]()
var continue = true

for (notebooks <- transient_notebooks){
  if (continue){
    val result = parallelExecute(notebooks)
   // println(result)
    if (result.toString.contains("FAILED") || result.toString.contains("ERROR,TIMEDOUT")){
      continue = false   
    }
    transientReturns+=result
  }
  else {
    println(s"Failing on $notebooks")  
  }
}
// flatten list of lists, save resulting values into array
transientReturns


// COMMAND ----------

var transient_returns = transientReturns.flatten.toList

// COMMAND ----------

// add regex pattern to remove unwanted chars from responses from parallel notebook execution
val regex = "['()[\\[\\]]]"
transient_returns = transient_returns.map( nbString => nbString.replaceAll("List", ""))
transient_returns = transient_returns.map( nbString => nbString.replaceAll(regex, ""))

// COMMAND ----------

// write summary logs for transient run
print(fullLogsPath)
writeSummaryLogs("transient_run","/dbfs"+fullLogsPath, transient_returns )

// COMMAND ----------

/* if need to roll back previous version of delta here    DESCRIBE HISTORY "/mnt/alasdeltalake/transient/XDW/factMatterCategoryHistory"  .option("versionAsOf", 5).
val dorg = spark.read.format("delta").option("versionAsOf",28).load(transientPath+"dimOrganization")
display(dorg.orderBy($"MODIFIED".desc))
//rollBackDelta(transientPath+"factBillingDetail",1)*/

// COMMAND ----------

// MAGIC %md PERSISTED START

// COMMAND ----------

var PERSISTED = ZONES.where($"ZONE" === "PERSISTED").select("TABLES.*", "*")
  .drop("ZONE","TABLES","SOURCE")
val persistedLevels = PERSISTED.columns.toList
val persistedTemp = List("dimPerson", "dimOrganization", "dimMatter","dimMatterHistory", "factMatterDetail", "factMatterFinancialActivity", "factMatterPersonInvolved", "factMatterOrganizationInvolved", "factMatterCategoryHistory", "factBillingHeader", "factBillingDetail", "factPersonOrgRelation") 
display(PERSISTED)

// COMMAND ----------

// this pulls all notebookData objects into a list of lists depending the order the Persisted notebooks must be executed
var persisted_notebooks = List[NotebookData]()

val testnb = createNotebookDataList( persistedTemp, nbPersistedPath, dateForPull )
persisted_notebooks = testnb
  
var proceed = true
persisted_notebooks.length
if (persisted_notebooks.length == 0) proceed = false

// COMMAND ----------

// !* Parallel Execution of Persisted Notebooks
// save return values into list
var persistedReturns = List[String]()
if (proceed){
  println(s"Parallel Persisted Zone $dateForPull")
  val results_pers = parallelExecute(persisted_notebooks) // blocking call
// no dependencies in peristed
  persistedReturns = results_pers
} else { println("STOPPED ON PERSISTED ZONE")}


// COMMAND ----------

// add regex pattern to remove unwanted chars from responses from parallel notebook execution
val regex = "['()[\\[\\]]]"
persistedReturns = persistedReturns.map( nbString => nbString.replaceAll("List", ""))
persistedReturns = persistedReturns.map( nbString => nbString.replaceAll(regex, ""))

// COMMAND ----------

// parse and check how many "PASS" and how many "FAIL"

// COMMAND ----------

// write Summary logs for Persisted
println(fullLogsPath)
writeSummaryLogs("persisted_run","/dbfs"+fullLogsPath, persistedReturns )
// persisted complete flag
val persisted_complete = true

// COMMAND ----------

// MAGIC %md UPDATE CoreEntityRelation here when Ready

// COMMAND ----------

if (persisted_complete) {
  val result = dbutils.notebook.run(path = "/Shared/MDM/updateCoreEntityRelation", timeoutSeconds=1000, arguments=Map("DATE" -> DATE.toString))
  println(result)
}


// COMMAND ----------

// MAGIC %md CURATED

// COMMAND ----------

// get curated mapping of tables & priority levels
val CURATED = ZONES.where($"ZONE" === "CURATED").select("TABLES.*", "*")
  .drop("ZONE","TABLES","SOURCE")
val curatedLevels = CURATED.columns.toList

display(CURATED)

// COMMAND ----------

// CURATED - this pulls all notebookData objects into a list of lists depending the order the Curated notebooks must be executed
val curated_notebooks = MutableList[List[NotebookData]]()

for (lev <- curatedLevels){
  // what if null in a level?
  val level = getZoneList(CURATED,lev)
  print(level)
  if (!level.isEmpty){
    val testnb = createNotebookDataList(level, nbCuratedPath, dateForPull )
    curated_notebooks+= testnb
  }
}
curated_notebooks.length

// COMMAND ----------

// save return values into list
val curatedReturns = MutableList[List[String]]()

for (notebooks <- curated_notebooks){
  println(s"Updated Curated notebooks on $dateForPull")
  val results_cur = parallelExecute(notebooks)
  curatedReturns+= results_cur
}
// flatten list of lists to return results
curatedReturns

// COMMAND ----------

// write Summary logs for Curated
val curated_returns = curatedReturns.flatten.toList
writeSummaryLogs("curated_run","/dbfs"+fullLogsPath, curated_returns )

// COMMAND ----------

/* past before : PASS, measureFinancialActivityMerge, 2020-10-24, 169056, PASS, summaryMatterMeasure_financial, 2020-10-24, 68495, PASS, measureMatterPaidBase, 2020-10-24, 68495 , " measureClaimFrequencyWindows ran successfully status:PASS", " PASS", factPolicyFinancialActivity, 2020-10-24, 11954 status:PASS, " PASS", summaryAOPtable, 2020-10-24, 79524 status:PASS, " PASS", summaryMatterCauseOfLoss, 2020-10-24, 82379 status:PASS, " PASS", summaryMatterTypeCount, 2020-10-24, 69853))
*/

// COMMAND ----------

// MAGIC %md FIN DU PROCES

// COMMAND ----------

// write to warehouse here
// send the notebooks that need to be written to warehouse 
// zone in which they're found
// which database - option for both 