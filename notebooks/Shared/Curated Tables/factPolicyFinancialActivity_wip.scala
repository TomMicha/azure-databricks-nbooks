// Databricks notebook source
// MAGIC %md 
// MAGIC ### Building Curated factPolicyFinancialActivity (part of summary/measures)
// MAGIC Dependent Tables:
// MAGIC - pull persisted/XDW/dimPolicyYear
// MAGIC - pull persisted/XDW/factPolicyHistory
// MAGIC - pull persisted/XDW/factBillingHeader
// MAGIC - pull persisted/XDW/factMatterDetail
// MAGIC > Actions: group by policyVersion and extrapolate most of the information from fields

// COMMAND ----------

// imports
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

// pull necessary tables from persisted delta tables
val dfPolicyYear = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyYear")
val dfFinancial = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_financial")
val dfBilling = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/measureMattersHelper_bh")
val dfCounts = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/measureMattersHelper_cnt")
// need fact policy history for multiple fields
val dfPolicyHistory = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factPolicyHistory")
// need dimMatter to get Active Status Code
val dfMatter = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimMatter")
// pull dimPolicyVersion to get versions under policyYear
val dfVersion = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyVersion")

// COMMAND ----------

display(dfCounts)

// COMMAND ----------

// slice out necessary columns from policyHistory (take latest policyCoverageEndDate)
val historyDF = dfPolicyHistory.select("POLICY_YEAR_SK","FIRM_SK","LOB_SK","POLICY_VERSION_STATUS_CODE","POLICY_COVERAGE_END_DATE_SK","RETENTION_AGGREGATE_AMOUNT","LIMIT_AGGREGATE_AMOUNT","GROSS_PREMIUM_AMOUNT","NET_PREMIUM_AMOUNT")
// need financial summary helper table for amounts
val financialDF = dfFinancial.select("DATE_KEY","POLICY_YEAR_SK","MATTER_SK","ALAS_RESERVE")
// counts of matters
var countsDF = dfCounts.select("DATE_KEY", "MATTER_SK")
val matterH = dfMatter.select("MATTER_SK","MATTER_STATUS_CODE")
countsDF = countsDF.join(matterH, Seq("MATTER_SK"), "inner")

// billing df for WIR spend
val renameCols = Seq("DATE_KEY","MATTER_SK", "COINS_PAID_BASE", "WIR")
var billingDF = dfBilling.select("DATE_KEY","MATTER_SK","CUST_FIRM_COINS_PAYMENT_AMOUNT","TOTAL_NET_AMOUNT")
billingDF = billingDF.toDF(renameCols: _*)

val policyDF = dfPolicyYear.select("POLICY_YEAR_SK","FIRM_KEY","LOB","POLICY_PERIOD_STARTS","POLICY_PERIOD_END")


// COMMAND ----------

printCounts(dfPolicyHistory, "POLICY_YEAR_SK")

// COMMAND ----------

display(dfPolicyHistory.filter($"POLICY_YEAR_SK" === 11265)) // 8440 19 1211 11434

// COMMAND ----------

// roll up ALAS_RESERVE on policyYearSK then join
val finance_agg = financialDF.groupBy("POLICY_YEAR_SK").agg(sum("ALAS_RESERVE").alias("ALAS_RESERVE"))
display(finance_agg)                                                   

// COMMAND ----------

// join temp fMD to get policyYearSk into billingHeader df
var fMD = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterDetail")
fMD = fMD.select("MATTER_SK","POLICY_YEAR_SK")
var billing = billingDF.join(fMD, Seq("MATTER_SK"), "left")
display(billing)

// COMMAND ----------

printCounts(billing, "POLICY_YEAR_SK")

// COMMAND ----------

// roll up WIR and COINS_PAID_BASE on policyYearSK then join, get 2800 distinct rolled up policyYearSKs
billing = billing.drop("DATE_KEY","MATTER_SK")
billing = billing.groupBy("POLICY_YEAR_SK").agg(sum("WIR").alias("WIR"), sum("COINS_PAID_BASE").alias("COINS_PAID_BASE"))
printCounts(billing, "POLICY_YEAR_SK")

// COMMAND ----------

// roll up counts on POlicy, but need to check if active
// filter out all closed matters since this is current policy activity view, then add policyYearSk and do count
countsDF = countsDF.filter($"MATTER_STATUS_CODE" === "Open")
countsDF = countsDF.join(fMD, Seq("MATTER_SK"), "left")
countsDF = countsDF.groupBy("POLICY_YEAR_SK").agg(countDistinct("MATTER_SK").alias("REMAINING_MATTERS"))
display(countsDF)

// COMMAND ----------

printCounts(countsDF, "POLICY_YEAR_SK")

// COMMAND ----------

// test
display(historyAggPrem.filter($"POLICY_YEAR_SK" === 1211))

// COMMAND ----------

// test
display(historyDF.filter($"POLICY_YEAR_SK" === 1211))

// COMMAND ----------

// check for duplicates
val duplicates = histTemp.groupBy("POLICY_YEAR_SK").count
display(duplicates)

// COMMAND ----------

historyDF.printSchema

// COMMAND ----------

// modify historyDF based on active, replaced pending
var historyAgg = historyDF.filter($"POLICY_VERSION_STATUS_CODE" =!= "P")
printCounts(historyAgg, "POLICY_YEAR_SK")

// COMMAND ----------

val historyAggPrem = historyAgg.groupBy("POLICY_YEAR_SK","FIRM_SK").agg(sum("GROSS_PREMIUM_AMOUNT").alias("GROSS_PREMIUM_DOLLARS"),sum("NET_PREMIUM_AMOUNT").alias("TOTAL_PREMIUM_DOLLARS"))
printCounts(historyAggPrem, "POLICY_YEAR_SK")

// COMMAND ----------

// now that have agg premium above, drop all replaced policies and other columns
var histTemp = historyAgg.filter($"POLICY_VERSION_STATUS_CODE" === "A")

histTemp = histTemp.drop("GROSS_PREMIUM_AMOUNT","NET_PREMIUM_AMOUNT","POLICY_VERSION_STATUS_CODE")
histTemp = histTemp.groupBy("POLICY_YEAR_SK","FIRM_SK").agg(max("POLICY_COVERAGE_END_DATE_SK").alias("POLICY_COVERAGE_END"))
printCounts(histTemp, "POLICY_YEAR_SK")

// COMMAND ----------

// UDF for getting the most current Policy
def getCurrentPolicy()

// COMMAND ----------

// get current policy active
var currentPolicy = histTemp.select("POLICY_YEAR_SK","FIRM_SK","POLICY_COVERAGE_END")
currentPolicy = currentPolicy.withColumn("idx", monotonically_increasing_id())
//currentPolicy = currentPolicy.groupBy("FIRM_SK")
display(currentPolicy)

// COMMAND ----------

// important join
var aggHistoryDF = historyAggPrem.join(histTemp, Seq("POLICY_YEAR_SK","FIRM_SK"),"inner")
printCounts(aggHistoryDF, "POLICY_YEAR_SK")

// COMMAND ----------

// issue * aggHistoryDF = aggHistoryDF.join(historyAgg.select("POLICY_YEAR_SK","LOB_SK"), Seq("POLICY_YEAR_SK"), "left")
display(aggHistoryDF)

// COMMAND ----------

// join policy with policyHistory to retain columns for start/end policy

val tempPolicy = dfPolicyYear.select("POLICY_YEAR_SK","POLICY_PERIOD_STARTS","POLICY_PERIOD_END")
var baseTableDF = aggHistoryDF.join(tempPolicy, Seq("POLICY_YEAR_SK"), "inner")
display(baseTableDF)

// COMMAND ----------

printCounts(baseTableDF, "POLICY_YEAR_SK")

// COMMAND ----------

// join baseTable with GROSS_RESERVE_DOLLARS (RI+RD) from financialDF
baseTableDF = baseTableDF.join(billing, Seq("POLICY_YEAR_SK"), "left")
baseTableDF = baseTableDF.withColumnRenamed("WIR", "RETENTION_AMOUNT_SPENT")
display(baseTableDF)

// COMMAND ----------

printCounts(baseTableDF, "POLICY_YEAR_SK")

// COMMAND ----------

baseTableDF = baseTableDF.join(countsDF, Seq("POLICY_YEAR_SK"),"left")
display(baseTableDF)

// COMMAND ----------

// test
display(historyAgg.where($"POLICY_VERSION_STATUS_CODE" === "P")) // P=88 A=12786 R=1519

// COMMAND ----------

display(historyAgg.where($"POLICY_VERSION_SK" === 10590))

// COMMAND ----------

//val currentPolicy = baseTableDF.select("POLICY_YEAR_SK","FIRM_SK",)

// COMMAND ----------

// fill nulls in RETENTION_AMOUNT, COINS_PAID, REMAINING_MATTERS
var saveDF = baseTableDF.drop("max(POLICY_COVERAGE_END_DATE_SK)")
saveDF = saveDF.na.fill(0, Seq("RETENTION_AMOUNT_SPENT"))
saveDF = saveDF.na.fill(0, Seq("COINS_PAID_BASE"))
saveDF = saveDF.na.fill(0, Seq("REMAINING_MATTERS"))
display(saveDF)

// COMMAND ----------

 // add IS_CURRENT column to indicate most recent policyYearSK

// COMMAND ----------

// for reference 
val columns = List[String]("POLICY_FINANCIAL_ACTIVITY_SK",
"POLICY_YEAR_SK",
"LINE_OF_BUSINESS_SK",
"FIRM_SK",
"ACTIVITY_AMOUNT",
"RETENTION_AMOUNT_REMAINING",
"ACTUAL_RETENTION_AGGREGATE_AMOUNT",
"LIMIT_AGGREGATE_AMOUNT",
"RETENTION_AMOUNT_REMAINING",
"REMAINING_POLICY_LIMITS",
"GROSS_RESERVE_DOLLARS",
"GROSS_PREMIUM_DOLLARS",
"TOTAL_PREMIUM",
"TOTAL_ACTIVITY_AMOUNT",
"MATTERS_OPEN",
"REMAINING_COINS_AGGR_LIMIT",
"POLICY_PERIOD_STARTS",
"POLICY_PERIOD_END",
"EFFECTIVE_ON_DATE",
"EXPIRED_ON_DATE",
"CREATED_ON",
"MODIFIED")

// COMMAND ----------

// use to fail/succeed table update
var failed = false

// COMMAND ----------

//save curate version of table
if (!failed){
  val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  val tableName = "factPolicyFinancialActivity"
  saveDF.write.format("delta").save(curatedDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

