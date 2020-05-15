// Databricks notebook source
// MAGIC %md 
// MAGIC ### Building Curated summaryAOP (part of summary/measures)
// MAGIC Dependent Tables:
// MAGIC  - pull curated/XDW/summarMatterMeasure_financial
// MAGIC  - pull curated/XDW/summarMatterMeasure_counts
// MAGIC  - pull persisted/XDW/dimAOP
// MAGIC 
// MAGIC > Actions: matter and claim counts by type - summary view

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

// ADD summary table helpers - this will have all MATTER_SKs and aggregated financial sums needed 
val financialDF = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_financial")
val matterCounts = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_counts")
// need AOP table
val dimAOP = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimAOP")
val dfPolicy = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyYear")

// COMMAND ----------

display(dimAOP)

// COMMAND ----------

// slice out what is needed form the tables
val aopDF = dimAOP.select("AOP_SK","PASSPORT_AOP_NAME")
val policyDF = dfPolicy.select("POLICY_YEAR_SK","POLICY_PERIOD_STARTS","POLICY_PERIOD_END","LOB","FIRM_KEY")

// COMMAND ----------

// REMOVE FOR NOW, join back later::join both finance and matter count tabels with AOP type
//var financeAOP = financialDF.join(aopDF, Seq("AOP_SK"), "left")
//var countsAOP = matterCounts.join(aopDF, Seq("AOP_SK"), "left")
//display(countsAOP)

// COMMAND ----------

// start finishing the Join Matters count by GroupBy to get counts of matterSKs for each AOP
var joined_counts = countsAOP.groupBy("DATE_KEY","POLICY_YEAR_SK","AOP_SK").agg(count("MATTER_SK").alias("REPORTED_MATTERS"))
display(joined_counts)

// COMMAND ----------

printCounts(joined_counts, "AOP_SK")

// COMMAND ----------

val financeDropExtra = financeAOP.drop("ALAS_INDEMNITY_PAID","ALAS_DEFENSE_PAID","RESERVE_INDEMNITY","RESERVE_DEFENSE")

// COMMAND ----------

// start on Finances with AOP grouBY to get aggregate values for sums of ALAS RESERVE FIRM PAID TOTAL GROUND UP TOTAL INCURRED
var finance_counts = financeDropExtra.groupBy("DATE_KEY", "POLICY_YEAR_SK","AOP_SK").agg(sum("ALAS_PAID").alias("ALAS_PAID"), sum("ALAS_RESERVE").alias("ALAS_RESERVE"), sum("FIRM_PAID").alias("FIRM_PAID"), sum("TOTAL_GROUND_UP").alias("TOTAL_GROUND_UP"), sum("TOTAL_INCURRED").alias("TOTAL_INCURRED"))
display(finance_counts)

// COMMAND ----------

printCounts(finance_counts, "PASSPORT_AOP_NAME")

// COMMAND ----------

val finalJoin = finance_counts.join(joined_counts, Seq("DATE_KEY","POLICY_YEAR_SK","AOP_SK"), "outer")
display(finalJoin)

// COMMAND ----------

printCounts(finalJoin, "AOP_SK")

// COMMAND ----------

// join back FIRM_SK, and AOP_SK, PERIOD start/end

var saveDF = finalJoin.join(aopDF, Seq("AOP_SK"),"left")
display(saveDF)

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

printCounts(saveDF, "PASSPORT_AOP_NAME")

// COMMAND ----------

// ISSUE addign this..x8 row count after joining - for now adding FIRM_KEY
//val addfirmsk = matterCounts.select("POLICY_YEAR_SK", "FIRM_SK")
//saveDF = saveDF.join(addfirmsk, Seq("POLICY_YEAR_SK"), "inner")

// COMMAND ----------

printCounts(saveDF, "PASSPORT_AOP_NAME")

// COMMAND ----------

var failed = false

// COMMAND ----------

//save curate version of table
if (!failed){
  val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  val tableName = "summaryAOPtable"
  saveDF.write.format("delta").save(curatedDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

// note of columns in table
val newcols = List[String]("POLICY_YEAR_SK","FIRM_SK","LINE_OF_BUSINESS_SK","AOP_SK",
"POLICY COVERAGE START",
"POLICY COVERAGE END",
"PASSPORT_AOP_NAME",
"REPORTED MATTERS ",
"FIRM_PAID (WIR BASE)",
"ALAS PAID",
"ALAS_INCURRED_AMOUNT")

// COMMAND ----------

