// Databricks notebook source
// MAGIC %md 
// MAGIC ### Building Curated summaryMatterCauseOfLoss (part of summary/measures)
// MAGIC Dependent Tables:
// MAGIC  - pull persisted/XDW/dimPolicyYear
// MAGIC  - pull persisted/XDW/dimCauseOfLoss
// MAGIC  - pull persisted/XDW/dimMatter
// MAGIC  - pull persisted/XDW/factBillingHeader
// MAGIC  - pull persited/XDW/factMatterDetail
// MAGIC > Actions: matter and claim counts by type - summary view

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//pull necessary tables from persisted delta tables
val dfPolicyYear = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyYear")
val dfCauseOfLoss = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimCauseOfLoss")


// ADD measureMatterPaidBase helper tables - this will have all MATTER_SKs and aggregated financial sums needed 
val financialDF = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_financial")
val matterCounts = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_counts")

// COMMAND ----------

display(matterCounts)

// COMMAND ----------

// create partial dfs selecting needed columns out of tables
val causeoflossDF = dfCauseOfLoss.select("PRIMARY_CAUSE_OF_LOSS_SK","FULL_NAME")

// maybe keep matterDetail and compare MATTER_SK counts with DF financial activity- may need different way for counts

val policyDF = dfPolicyYear.select("POLICY_YEAR_SK","POLICY_PERIOD_STARTS","POLICY_PERIOD_END","LOB","FIRM_KEY")

// COMMAND ----------

display(causeoflossDF)

// COMMAND ----------

// REMOVE FOR NOW (add later 4/25) join with CoL to get subtype of loss groupBY (join with both Counts and Finances DFs)
//var financeCoL = financialDF.join(causeoflossDF, Seq("PRIMARY_CAUSE_OF_LOSS_SK"), "left")

//var countsCoL = matterCounts.join(causeoflossDF, Seq("PRIMARY_CAUSE_OF_LOSS_SK"), "left")

//display(countsCoL)

// COMMAND ----------

// func to help print distinct vs total counts
def printCounts(df: DataFrame, field: String): Unit = {
  println("total: "+df.count)
  println(s"total $field: "+df.select(field).count)
  println(s"total distinct: "+df.select(field).distinct.count)
}


// COMMAND ----------

// join matters counts with CoL and group by to get counts of matter_sks
var joined_counts = countsCoL.groupBy("DATE_KEY", "POLICY_YEAR_SK","PRIMARY_CAUSE_OF_LOSS_SK").agg(count("MATTER_SK").alias("REPORTED_MATTERS"))
display(joined_counts)

// COMMAND ----------

printCounts(joined_counts, "PRIMARY_CAUSE_OF_LOSS_SK")
printCounts(joined_counts,"DATE_KEY")


// COMMAND ----------

val financeDropExtra = financeCoL.drop("ALAS_INDEMNITY_PAID","ALAS_DEFENSE_PAID","RESERVE_INDEMNITY","RESERVE_DEFENSE")

// COMMAND ----------

// join finances with CoL and group to get aggregate values for financial sums 	ALAS_RESERVE	FIRM_PAID	TOTAL_GROUND_UP	TOTAL_INCURRED
var finance_counts = financeDropExtra.groupBy("DATE_KEY","POLICY_YEAR_SK","PRIMARY_CAUSE_OF_LOSS_SK").agg(sum("ALAS_PAID").alias("ALAS_PAID"), sum("ALAS_RESERVE").alias("ALAS_RESERVE"), sum("FIRM_PAID").alias("FIRM_PAID"), sum("TOTAL_GROUND_UP").alias("TOTAL_GROUND_UP"), sum("TOTAL_INCURRED").alias("TOTAL_INCURRED"))
display(finance_counts)

// COMMAND ----------

printCounts(finance_counts, "PRIMARY_CAUSE_OF_LOSS_SK")
printCounts(finance_counts,"DATE_KEY")

// COMMAND ----------

// testing
//var counts = finance_counts.groupBy("DATE_KEY", "POLICY_YEAR_SK").count
//display(counts.where($"count">1))

// COMMAND ----------

// join both finance_counts and joined_counts tables
val finalJoin = finance_counts.join(joined_counts, Seq("DATE_KEY","POLICY_YEAR_SK","PRIMARY_CAUSE_OF_LOSS_SK"), "outer")
display(finalJoin)

// COMMAND ----------

printCounts(finalJoin, "PRIMARY_CAUSE_OF_LOSS_SK")


// COMMAND ----------

// add PRIMARY CAUSE OF LOSS SK back in
var saveDF = finalJoin.join(causeoflossDF, Seq("PRIMARY_CAUSE_OF_LOSS_SK"), "left")
display(saveDF)


// COMMAND ----------

printCounts(saveDF, "PRIMARY_CAUSE_OF_LOSS_SK")

// COMMAND ----------

saveDF = saveDF.join(policyDF, Seq("POLICY_YEAR_SK"), "left")
saveDF = saveDF.na.fill(0, Seq("REPORTED_MATTERS"))
saveDF = saveDF.na.fill(0, Seq("ALAS_PAID"))
saveDF = saveDF.na.fill(0, Seq("ALAS_RESERVE"))
saveDF = saveDF.na.fill(0, Seq("FIRM_PAID"))
saveDF = saveDF.na.fill(0, Seq("TOTAL_GROUND_UP"))
saveDF = saveDF.na.fill(0, Seq("TOTAL_INCURRED"))
display(saveDF)

// COMMAND ----------

printCounts(saveDF, "PRIMARY_CAUSE_OF_LOSS_SK")

// COMMAND ----------

// join back FIRM_SK, CoL_SK, PERIOD START/END
// this doesnt seem to be working
//val temp1 = matterCounts.select("POLICY_YEAR_SK", "DATE_KEY","FIRM_SK")
//saveDF = saveDF.join(temp1, Seq("DATE_KEY","POLICY_YEAR_SK"), "left")
//display(saveDF)

// COMMAND ----------

printCounts(saveDF, "PRIMARY_CAUSE_OF_LOSS_SK")

// COMMAND ----------



// COMMAND ----------

//for reference use these columns
val columns = List[String]("POLICY_YEAR_SK","FIRM_SK","CAUSE_OF_LOSS_KEY","CYCLE_KEY"
"POLICY_COVERAGE_START",
"POLICY_COVERAGE_END",
"CAUSE_OF_LOSS_TYPE",
"CAUSE_OF_LOSS_SUBTYPE",
"REPORTED MATTERS COUNT",
"FIRM_PAID",
"ALAS PAID",
"ALAS_INCURRED_AMOUNT")

// COMMAND ----------

var failed = false

// COMMAND ----------

//save curate version of table
if (!failed){
  val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  val tableName = "summaryMatterCauseOfLoss"
  saveDF.write.format("delta").save(curatedDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

