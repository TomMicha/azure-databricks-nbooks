// Databricks notebook source
// MAGIC %md 
// MAGIC ### Building Curated measureMatterPaidBase (part of summary/measures)
// MAGIC Dependent Tables:
// MAGIC - pull persisted/XDW/dimMatter
// MAGIC - pull persisted/XDW/dimMatterActivity
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

// pull necessary tables from persisted delta tables
val dfPolicyYear = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyYear")
val dfMatter = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimMatter")
val dfFinancialActivity = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterFinancialActivity")
val dfBillingHeader = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factBillingHeader")
val dfPolicyHistory = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factPolicyHistory")

// COMMAND ----------

//Notes:
//SUM RD+RI === 0 theoretically + means that the reserve wasn't spent which doesn't (never) happens
// PI && PD needs to be separate
// SUM (PI) is what ALAS paid on indemnity
// SUM(PD) you get ALAS paid for defense
// ALAS reserve && ALAS paid are the same? on a matter level? or is reserve just zero 
// WIR Base 
//PAID COINS BASE


//Below just looking at a Matter's financials
//display(dfFinancialActivity.where($"MATTER_SK" === 4823))

// COMMAND ----------

//NOTES: Just testing specific MATTER's Financials
display(dfBillingHeader.select("MATTER_SK", "CUST_ALAS_GROSS_PAYMENT_AMOUNT", "CUST_ALAS_NET_PAYMENT_AMOUNT", "CUST_FIRM_COINS_PAYMENT_AMOUNT", "CUST_FIRM_IN_RETENTION_SPEND_AM","TOTAL_GROSS_AMOUNT", "TOTAL_NET_AMOUNT").where($"MATTER_SK" === 16831))

//how do we distinguish which is firm paid vs total net amount
//all these will be billed to firm?

// COMMAND ----------

// create partial dfs selecting needed columns out of tables
val financialActivityDF = dfFinancialActivity.select("MATTER_SK","ACTIVITY_AMOUNT","ACTIVITY_TYPE","COINS_NET_ACTIVITY_AMOUNT","MATTER_FINANCIAL_HISTORY_PROCESS_DATE")

val billingHeaderDF = dfBillingHeader.select("POLICY_YEAR_SK","MATTER_SK","CUST_ALAS_NET_PAYMENT_AMOUNT","CUST_FIRM_COINS_PAYMENT_AMOUNT","TOTAL_NET_AMOUNT","BILLING_APPROVAL_DATE")

val policyDF = dfPolicyYear.select("POLICY_YEAR_SK")

// COMMAND ----------

//Creating the financial table from FinancialActivity
val financeDF = financialActivityDF.groupBy("MATTER_SK", "ACTIVITY_TYPE").agg(sum("ACTIVITY_AMOUNT"), sum("COINS_NET_ACTIVITY_AMOUNT"))
display(financeDF)

// COMMAND ----------

//Creating the financial sums from BillingHeader
val sums = billingHeaderDF.groupBy("MATTER_SK").agg(sum("CUST_FIRM_COINS_PAYMENT_AMOUNT"),sum("CUST_ALAS_NET_PAYMENT_AMOUNT"),sum("TOTAL_NET_AMOUNT") )
display(sums)

// COMMAND ----------

//Renaming the sum columns in sums (billingHeaderFinancials)
val renameCols = Seq("MATTER_SK","COINS_PAID_BASE", "ALAS_PAID_MAYBE", "WIR")
val matterDFtemp = sums.select("MATTER_SK","sum(CUST_FIRM_COINS_PAYMENT_AMOUNT)","sum(CUST_ALAS_NET_PAYMENT_AMOUNT)", "sum(TOTAL_NET_AMOUNT)")
val measurePaidBase = matterDFtemp.toDF(renameCols: _*)

display(measurePaidBase)

// COMMAND ----------

//Just checking to make sure that coins_paid_base isn't zero for all 
measurePaidBase.where($"COINS_PAID_BASE" > 0).show

// COMMAND ----------

//Adding FIRM_PAID to above table 
//FIRM_PAID === COINS_PAID_BASE + WIR_BASE
val mpbFirm = measurePaidBase.withColumn("FIRM_PAID", $"COINS_PAID_BASE"+$"WIR")
display(mpbFirm)

// COMMAND ----------

//Getting counts of the two tables - just for stats of which might have more distinct matters
println(billingHeaderDF.distinct.count) //160473
println(financialActivityDF.distinct.count) //97881


// COMMAND ----------

//Doing PD type sums
val sumsPD = financialActivityDF.groupBy("MATTER_SK", "ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "PD")
val alasDefensePaid = sumsPD.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "ALAS_DEFENSE_PAID")
display(alasDefensePaid)

// COMMAND ----------

//Doing PI type sums
val sumsPI = financialActivityDF.groupBy("MATTER_SK", "ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "PI")
val alasIndemnityPaid = sumsPI.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "ALAS_INDEMNITY_PAID")
display(alasIndemnityPaid)

// COMMAND ----------

//PAID BASE -- this is just these two amounts added to each other.. yes? 
val sumPB = alasDefensePaid.drop($"ACTIVITY_TYPE").join(alasIndemnityPaid.drop($"ACTIVITY_TYPE"), Seq("MATTER_SK"), "left")
display(sumPB)

// COMMAND ----------

//putting them together - ALAS_DEFENSE_PAID, ALAS_INDEMNITY_PAID, ALAS_PAID(sum of the two previous)
val paidBase1 = sumPB.na.fill(0, Seq("ALAS_DEFENSE_PAID"))
val paidBase2 = paidBase1.na.fill(0, Seq("ALAS_INDEMNITY_PAID"))
val paidBase = paidBase2.withColumn("ALAS_PAID", $"ALAS_DEFENSE_PAID"+$"ALAS_INDEMNITY_PAID")

display(paidBase)

// COMMAND ----------

//Reserve Defense Sums
val sumsRD = financialActivityDF.groupBy("MATTER_SK", "ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "RD")
val rdSum = sumsRD.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "RESERVE_DEFENSE")
val reserveDefense = rdSum.na.fill(0, Seq("RESERVE_DEFENSE"))
display(reserveDefense)


// COMMAND ----------

//Reserves Indemnity Sum
val sumsRI = financialActivityDF.groupBy("MATTER_SK", "ACTIVITY_TYPE").agg(sum("COINS_NET_ACTIVITY_AMOUNT")).where($"ACTIVITY_TYPE" === "RI")
val riSum = sumsRI.withColumnRenamed("sum(COINS_NET_ACTIVITY_AMOUNT)", "RESERVE_INDEMNITY")
val reserveIndemnity = riSum.na.fill(0, Seq("RESERVE_INDEMNITY"))
display(reserveIndemnity)

// COMMAND ----------

//Counts to show how much of each we have 
println(reserveIndemnity.select("MATTER_SK").count) //2108
println(reserveDefense.select("MATTER_SK").count) //1921

// COMMAND ----------

//ALAS RI and RD
val alasReserve = reserveIndemnity.drop($"ACTIVITY_TYPE").join(reserveDefense.drop($"ACTIVITY_TYPE"), Seq("MATTER_SK"), "outer")
display(alasReserve)

// COMMAND ----------

//Adding ALAS_RESERVE -- sum of RI +RD
var totalReserve = alasReserve.na.fill(0, Seq("RESERVE_INDEMNITY"))
totalReserve = totalReserve.na.fill(0,Seq("RESERVE_DEFENSE"))
totalReserve = totalReserve.withColumn("ALAS_RESERVE", $"RESERVE_INDEMNITY"+$"RESERVE_DEFENSE")
display(totalReserve)

// COMMAND ----------

//Seeing counts to see just how many nulls we'll have when we join these two tables
println(totalReserve.count) //2386
println(paidBase.count) //1627

// COMMAND ----------

//Creating baseTable with WIR , paid coins, alas reserve, alas paid
var baseTable = totalReserve.drop("RESERVE_INDEMNITY", "RESERVE_DEFENSE").join(paidBase.drop("ALAS_DEFENSE_PAID", "ALAS_INDEMNITY_PAID"), Seq("MATTER_SK"), "outer")
baseTable = baseTable.na.fill(0, Seq("ALAS_PAID"))

baseTable = baseTable.join(mpbFirm, Seq("MATTER_SK"), "outer") // changed to mpbFirm to include FIRM_PAID
//baseTable = baseTable.na.fill(0, Seq("ALAS_PAID_MAYBE"))
//baseTable = baseTable.na.fill(0, Seq("COINS_PAID_BASE"))
display(baseTable)
//println(totalReserve.count) //2386
//println(paidBase.count) //1627

// COMMAND ----------

// zero out nulls in FIRM_PAID, ALAS_PAID, ALAS_RESERVE so can get the aggregate values in next cell
baseTable = baseTable.na.fill(0, Seq("ALAS_RESERVE"))
baseTable = baseTable.na.fill(0, Seq("ALAS_PAID"))
baseTable = baseTable.na.fill(0, Seq("FIRM_PAID"))
display(baseTable)

// COMMAND ----------

// add ALAS_INCURRED_AMOUNT and ALAS_GROUND_UP
var finalPaidBase = baseTable.withColumn("TOTAL_GROUND_UP", $"FIRM_PAID"+$"ALAS_PAID")
finalPaidBase = finalPaidBase.withColumn("TOTAL_INCURRED", $"ALAS_PAID"+$"ALAS_RESERVE")
display(finalPaidBase)

// COMMAND ----------

//Checking the table 
var testing = finalPaidBase.where($"MATTER_SK" === 10792)
display(testing)

// COMMAND ----------

//check these should be the same number - 1627
println(financialActivityDF.select("MATTER_SK").where($"ACTIVITY_TYPE"==="PD").distinct.count)
println(sumsPD.select("MATTER_SK").distinct.count)

//check these should be the same number - 1404
println(financialActivityDF.select("MATTER_SK").where($"ACTIVITY_TYPE"==="PI").distinct.count)
println(sumsPI.select("MATTER_SK").distinct.count)

// COMMAND ----------

// for reference 
val columns = List[String]("MATTER_KEY",
"POLICY_YEAR_KEY",
"ALAS_DEFENSE_PAID",
"ALAS_INDEMNITY_PAID",
"ALAS_PAID",
"FIRM_PAID",
"DEFENSE_COINS_BASE",
"INDEMNITY_COINS_BASE",
"PAID_COINSURANCE_BASE",
"FIRM_PAID",
"TOTAL_INCURRED",
"AS_OF_MONTH")

// COMMAND ----------

// use to fail/succeed table update
var failed = false

// COMMAND ----------

// save curated version of table -- finalPaidBase is the final dataFrame
if (!failed){
  val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  val tableName = "measureMatterPaidBase_helper"
  finalPaidBase.write.format("delta").save(curatedDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

