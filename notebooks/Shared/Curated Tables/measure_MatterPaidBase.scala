// Databricks notebook source
// MAGIC %md 
// MAGIC ### Building Curated measureMatterPaidBase 
// MAGIC Dependent Tables:
// MAGIC - pull curated/XDW/summaryMatterMeasure_financial
// MAGIC - pull curated/XDW/summaryMatterMeasure_counts
// MAGIC - pull persisted/XDW/factBillingHeader
// MAGIC - pull persisted/XDW/factPolicyHistory
// MAGIC > Building: Matter level table including policyYear that has paid amounts for all matters (necessary for other tables)
// MAGIC 
// MAGIC > Actions: group by matter key, policyVersion and extrapolate most of the information from fields

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// func to help print distinct vs total counts
def printCounts(df: DataFrame, field: String): Unit = {
  println("total: "+df.count)
  println(s"total $field: "+df.select(field).count)
  println(s"total distinct: "+df.select(field).distinct.count)
}

// COMMAND ----------

// ADD summary helper tables import
val financialDF = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_financial")

// need PolicyHistory table
val dfPolicyHistory = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factPolicyHistory")
val dfPolicy = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyYear")

// COMMAND ----------

display(financialDF)

// COMMAND ----------

var saveDF = financialDF.drop("PRIMARY_CAUSE_OF_LOSS_SK","AOP_SK")
saveDF = saveDF.na.fill(0, Seq("REPORTED_MATTERS"))
saveDF = saveDF.na.fill(0, Seq("ALAS_PAID"))
saveDF = saveDF.na.fill(0, Seq("ALAS_RESERVE"))
saveDF = saveDF.na.fill(0, Seq("FIRM_PAID"))
saveDF = saveDF.na.fill(0, Seq("TOTAL_GROUND_UP"))
saveDF = saveDF.na.fill(0, Seq("TOTAL_INCURRED"))
display(saveDF)

// COMMAND ----------

printCounts(saveDF, "MATTER_SK")

// COMMAND ----------

var failed = false

// COMMAND ----------

//save curate version of table
if (!failed){
  val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  val tableName = "measureMatterPaidBase"
  saveDF.write.format("delta").save(curatedDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

