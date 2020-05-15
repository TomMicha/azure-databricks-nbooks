// Databricks notebook source
// MAGIC %md PULL IN AS_OF and AGGREGATE and COUNTS for MATTER PAID BASE

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// pull in helper/partial dfs
val billingHeaderDF = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/measureMattersHelper_bh")
val financialDF = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/measureMattersHelper_fa")
val matterCountDF = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/measureMattersHelper_cnt")

// add needed policyYear
var fMD = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterDetail")


// COMMAND ----------

// func to help print distinct vs total counts
def printCounts(df: DataFrame, field: String): Unit = {
  println("total: "+df.count)
  println(s"total $field: "+df.select(field).count)
  println(s"total distinct: "+df.select(field).distinct.count)
}

// COMMAND ----------

// add policyYearSk to Matter Counts
val factMD = fMD.select("MATTER_SK", "POLICY_YEAR_SK","AOP_SK","PRIMARY_CAUSE_OF_LOSS_SK","FIRM_SK")

// COMMAND ----------

printCounts(matterCountDF, "DATE_KEY")

// COMMAND ----------

// join Matter Counts with PolicyYear
val countsJoin = matterCountDF.join(factMD, Seq("MATTER_SK"), "left")
display(countsJoin)

// COMMAND ----------

// MAGIC %md USE BELOW to aggregate based on what you need. Not aggregating here for now

// COMMAND ----------

// try aggregating counts by MONTH
//var countsDF = countsJoin.groupBy("POLICY_YEAR_SK", "DATE_KEY").agg(countDistinct("MATTER_SK"))
//countsDF = countsDF.withColumnRenamed("count(DISTINCT MATTER_SK)", "REPORTED_MATTERS")
//display(countsDF)

// COMMAND ----------

//display(countsDF.where($"POLICY_YEAR_SK" === 64))

// COMMAND ----------

//println(countsDF.where($"POLICY_YEAR_SK" === 64).count)
println(fMD.where($"POLICY_YEAR_SK" === 64).count) //14

// COMMAND ----------

// aggregate financial sums (FinActivity)
display(financialDF)

// COMMAND ----------

// groupBy to get different groupings
var PDdf = financialDF.groupBy("MATTER_SK","DATE_KEY","ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "PD")
PDdf = PDdf.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "ALAS_DEFENSE_PAID")
PDdf = PDdf.drop("ACTIVITY_TYPE")

var PIdf = financialDF.groupBy("MATTER_SK","DATE_KEY","ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "PI")
PIdf = PIdf.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "ALAS_INDEMNITY_PAID")
PIdf = PIdf.drop("ACTIVITY_TYPE")


var RDdf = financialDF.groupBy("MATTER_SK","DATE_KEY","ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "RD")
RDdf = RDdf.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "RESERVE_DEFENSE")
RDdf = RDdf.drop("ACTIVITY_TYPE")

var RIdf = financialDF.groupBy("MATTER_SK","DATE_KEY","ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "RI")
RIdf = RIdf.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "RESERVE_INDEMNITY")
RIdf = RIdf.drop("ACTIVITY_TYPE")
display(PIdf)

// COMMAND ----------

printCounts(PIdf, "MATTER_SK")
printCounts(RIdf, "MATTER_SK")
printCounts(PDdf, "MATTER_SK")
printCounts(RDdf, "MATTER_SK")


// COMMAND ----------

// join all Finacial Activity types (RI,RD,PI,PD)
var joinDF = PIdf.join(PDdf, Seq("MATTER_SK","DATE_KEY"),"outer")

display(joinDF) 

// COMMAND ----------

display(joinDF.where($"MATTER_SK" === 122))
//joinDF.select("DATE_KEY").distinct.count //243
//joinDF.where($"MATTER_SK" === 122).count //39

// COMMAND ----------

joinDF = joinDF.join(RIdf, Seq("MATTER_SK","DATE_KEY"),"outer")
display(joinDF.filter($"MATTER_SK" === 122))
println(joinDF.where($"MATTER_SK" === 122).count)

// COMMAND ----------

joinDF = joinDF.join(RDdf, Seq("MATTER_SK","DATE_KEY"),"outer")
print(joinDF.filter($"MATTER_SK"===122).count)

// COMMAND ----------

// can decide to join on BASE, or keep as counts are correct

// COMMAND ----------

// fill NA values with 0 if needed
joinDF = joinDF.na.fill(0, Seq("RESERVE_INDEMNITY"))
joinDF = joinDF.na.fill(0, Seq("RESERVE_DEFENSE"))
joinDF = joinDF.na.fill(0, Seq("ALAS_INDEMNITY_PAID"))
joinDF = joinDF.na.fill(0, Seq("ALAS_DEFENSE_PAID"))
display(joinDF)

// COMMAND ----------

// create additional PAID MEASURES
var paidBase = joinDF.withColumn("ALAS_PAID", $"ALAS_DEFENSE_PAID"+$"ALAS_INDEMNITY_PAID")
paidBase = paidBase.withColumn("ALAS_RESERVE", $"RESERVE_INDEMNITY"+$"RESERVE_DEFENSE")
display(paidBase)

// COMMAND ----------

//keep this for now
// drop some of the lower level columns
//paidBase = paidBase.drop("ALAS_INDEMNITY_PAID", "ALAS_DEFENSE_PAID","RESERVE_INDEMNITY","RESERVE_DEFENSE")
//display(paidBase)

// COMMAND ----------

// MAGIC %md billing Header Creation

// COMMAND ----------

var billingDF = billingHeaderDF.groupBy("DATE_KEY","MATTER_SK").agg(sum("CUST_FIRM_COINS_PAYMENT_AMOUNT"), sum("TOTAL_NET_AMOUNT"))
display(billingDF)

// COMMAND ----------

// CREATE agg BillingHeader Table
val renameCols = Seq("DATE_KEY","MATTER_SK", "COINS_PAID_BASE", "WIR")
val billingDF1 = billingDF.toDF(renameCols: _*)
var billingSumsDF = billingDF1.withColumn("FIRM_PAID", $"COINS_PAID_BASE"+$"WIR")
billingSumsDF = billingSumsDF.drop("COINS_PAID_BASE","WIR")
display(billingSumsDF)

// COMMAND ----------

printCounts(billingSumsDF, "MATTER_SK")


// COMMAND ----------

println(billingSumsDF.where($"MATTER_SK"===122).distinct.count)
val billingSK = billingSumsDF.where($"MATTER_SK"===122).distinct.collect().map(_(0)).toSet
println(billingSK.size)

// COMMAND ----------

println(paidBase.where($"MATTER_SK"===122).distinct.count)
val financeSK = paidBase.where($"MATTER_SK"===122).distinct.collect().map(_(1)).toSet
println(financeSK.size)

// COMMAND ----------

// check if sets disjoint
var x = billingSK.forall(financeSK.contains)

// COMMAND ----------

// begin joining BillingHeader and FinancialActivity Aggregations
var finalBase = paidBase.join(billingSumsDF, Seq("DATE_KEY","MATTER_SK"), "outer")
display(finalBase)

// COMMAND ----------

display(finalBase.where($"MATTER_SK"=== 122))
println(finalBase.where($"MATTER_SK"=== 122).count) //56

// COMMAND ----------

printCounts(finalBase, "MATTER_SK")

// COMMAND ----------

// zero out nulls in FIRM_PAID, ALAS_PAID, ALAS_RESERVE, so we can aggregate values in ext cell
finalBase = finalBase.na.fill(0, Seq("ALAS_RESERVE"))
finalBase = finalBase.na.fill(0, Seq("ALAS_PAID"))
finalBase = finalBase.na.fill(0, Seq("FIRM_PAID"))

// COMMAND ----------

finalBase = finalBase.withColumn("TOTAL_GROUND_UP", $"FIRM_PAID"+$"ALAS_PAID")
finalBase = finalBase.withColumn("TOTAL_INCURRED", $"ALAS_PAID"+$"ALAS_RESERVE")
display(finalBase)

// COMMAND ----------

// add PolicyYearSK and firmSK for easier access
val extraDF = fMD.select("MATTER_SK","POLICY_YEAR_SK","AOP_SK","PRIMARY_CAUSE_OF_LOSS_SK","FIRM_SK")
val saveDF = finalBase.join(extraDF, Seq("MATTER_SK"), "left")
display(saveDF)

// COMMAND ----------

//display(saveDF.where($"MATTER_SK" === 122))
println(saveDF.where($"MATTER_SK" === 122).count) //56

// COMMAND ----------

println(saveDF.count)
println(saveDF.select("MATTER_SK").distinct.count)
println(saveDF.select("DATE_KEY").distinct.count)
println(saveDF.select("AOP_SK").distinct.count)
println(saveDF.select("PRIMARY_CAUSE_OF_LOSS_SK").distinct.count)
println(saveDF.select("FIRM_SK").distinct.count)

// COMMAND ----------

saveDF.filter($"PRIMARY_CAUSE_OF_LOSS_SK" === null).count

// COMMAND ----------

//var counts = saveDF.groupBy("DATE_KEY", "MATTER_SK").agg(count($"MATTER_SK").alias("COUNTS"))
//display(counts.where($"COUNTS">1))

// COMMAND ----------

var failed = false

// COMMAND ----------

//save curate version of table
if (!failed){
  val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  val tableName = "summaryMatterMeasure_financial"
  saveDF.write.format("delta").save(curatedDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

//save curate version of table
if (!failed){
  val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  val tableName = "summaryMatterMeasure_counts"
  countsJoin.write.format("delta").save(curatedDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

// MAGIC %md TESTING BELOW

// COMMAND ----------

display(saveDF.filter($"MATTER_SK" === 6848))

// COMMAND ----------

display(saveDF.filter($"MATTER_SK" === 122))


// COMMAND ----------

// check values, ex 122 MATTER_Sk seems to repeat 3x values

// COMMAND ----------

display(countsJoin)

// COMMAND ----------

