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
val dfMatterDetail = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterDetail")
val dfFinancialActivity = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterFinancialActivity")
val dfBillingHeader = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factBillingHeader")
val dfPolicyHistory = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factPolicyHistory")

// COMMAND ----------

//asof month
//policyYear - cause subtype cat code grouping
//each of these need to be populated with value for every firm 
//mix with dimDate
display(dfFinancialActivity)

// COMMAND ----------

billingHeaderDF.where($"MATTER_SK" === 3).show()

// COMMAND ----------

display(dfBillingHeader.select("MATTER_SK", "CUST_ALAS_GROSS_PAYMENT_AMOUNT", "CUST_ALAS_NET_PAYMENT_AMOUNT", "CUST_FIRM_COINS_PAYMENT_AMOUNT", "CUST_FIRM_IN_RETENTION_SPEND_AM","TOTAL_GROSS_AMOUNT", "TOTAL_NET_AMOUNT").where($"MATTER_SK" === 16831))

//how do we distinguish which is firm paid vs total net amount
//all these will be billed to firm?

// COMMAND ----------

// create partial dfs selecting needed columns out of tables

val financialActivityDF = dfFinancialActivity.select("POLICY_YEAR_SK","MATTER_SK","MATTER_FINANCIAL_ACTIVITY_SK","ACTIVITY_AMOUNT","ACTIVITY_TYPE","COINS_NET_ACTIVITY_AMOUNT","MATTER_FINANCIAL_HISTORY_DATE")

val billingHeaderDF = dfBillingHeader.select("POLICY_YEAR_SK","MATTER_SK","CUST_FIRM_COINS_PAYMENT_AMOUNT","TOTAL_NET_AMOUNT","BILLING_APPROVAL_DATE")
// unneeded?
val matterDetailDF = dfMatterDetail.select("MATTER_OPEN_DATE", "MATTER_SK")

// COMMAND ----------

// check if MATTER_SK in bh is subset of MATTER_SKs in FAtable
val fasks = financialActivityDF.select("MATTER_SK").distinct.collect().map(_(0)).toSet
println(fasks)
val bhsks = billingHeaderDF.select("MATTER_SK").distinct.collect().map(_(0)).toSet
println(bhsks)

bhsks.getClass
fasks.getClass
//xdf.filter(($"cnt" > 1) || ($"name" isin ("A","B"))).show()

// COMMAND ----------

var x = bhsks.forall(fasks.contains)

//val disjoint = bhsks.filter(($"MATTER_SK" isin fasks)).show()

// COMMAND ----------

println(billingHeaderDF.count)
println(billingHeaderDF.select("MATTER_SK").distinct.count)
println(billingHeaderDF.select("POLICY_YEAR_SK").distinct.count)
println(financialActivityDF.count)
println(financialActivityDF.select("MATTER_SK").distinct.count)
println(financialActivityDF.select("POLICY_YEAR_SK").distinct.count)
println(financialActivityDF.select("MATTER_FINANCIAL_ACTIVITY_SK").count)
println(financialActivityDF.select("MATTER_FINANCIAL_ACTIVITY_SK").distinct.count)
println(financialActivityDF.where($"MATTER_FINANCIAL_ACTIVITY_SK" < 0).count)


// COMMAND ----------

// need to only select matters with OPEN DATE > 2000, so create subset of matterSKs with this property
var mattersOver2000 = matterDetailDF.filter($"MATTER_OPEN_DATE" > "2000-01-01") // review start date, may need to use policy period
mattersOver2000 = mattersOver2000.drop("MATTER_OPEN_DATE")
display(mattersOver2000)

// COMMAND ----------

printCounts(mattersOver2000, "MATTER_SK")

// COMMAND ----------

// create MatterDetail with Matter OPEN date for counts
var matterDates = matterDetailDF.withColumn("YEAR", year(col("MATTER_OPEN_DATE"))).withColumn("MONTH", month(col("MATTER_OPEN_DATE")))
matterDates = matterDates.withColumn("DATE_KEY", concat($"YEAR", $"MONTH"))
display(matterDates)

// COMMAND ----------

// join on subset of matters > 2000 and check counts
matterDates = matterDates.join(mattersOver2000, Seq("MATTER_SK"), "inner")

// COMMAND ----------

printCounts(matterDates, "MATTER_SK")

// COMMAND ----------

// func to help print distinct vs total counts
def printCounts(df: DataFrame, field: String): Unit = {
  println("total: "+df.count)
  println(s"total $field: "+df.select(field).count)
  println(s"total distinct: "+df.select(field).distinct.count)
}

// COMMAND ----------

printCounts(matterDates, "MATTER_SK")

// COMMAND ----------

// FinancialActivity needs the +5 days for Effective Date

// policy level reporting starts in 2000, all other matters under those policies are not necessary

// COMMAND ----------

// try modifying internal function to if statemnt to get MONTH/YEAR (this one need)

// COMMAND ----------

// add dates to BillingHeaderDF
var datesBHDF = billingHeaderDF.select("MATTER_SK","CUST_FIRM_COINS_PAYMENT_AMOUNT","TOTAL_NET_AMOUNT","BILLING_APPROVAL_DATE")
datesBHDF = datesBHDF.withColumn("YEAR", year(col("BILLING_APPROVAL_DATE"))).withColumn("MONTH", month(col("BILLING_APPROVAL_DATE")))
datesBHDF = datesBHDF.withColumn("DATE_KEY", concat($"YEAR", $"MONTH"))
display(datesBHDF)

// COMMAND ----------

printCounts(datesBHDF, "MATTER_SK")

// COMMAND ----------

// join datesBHDF on the subset of matters > 2000
datesBHDF = datesBHDF.join(mattersOver2000, Seq("MATTER_SK"), "inner")

// COMMAND ----------

printCounts(datesBHDF, "MATTER_SK")

// COMMAND ----------

import java.sql.Date
import java.time.{Duration, LocalDate}
import java.sql.Timestamp

// COMMAND ----------

// define UDF that checks if a date falls within a certain date range, return Month it should be categorized (date + 5 trailing days)
def fiveTrailingMonth(datetime: Timestamp): Int = {
  val date = datetime.toLocalDateTime()
  var month = date.getMonthValue()
  var day = date.getDayOfMonth()
  //println(s"Month:$month D:$day")
  if (day < 6){
    if (month == 1){
      month = 12
    }
    else {
      month = month-1 
    }
   // println(s"Month:$month D:$day")
    return month
  }
  return month
}
val fiveDaysMonthUDF = udf(fiveTrailingMonth _)

// COMMAND ----------

// need a UDF for YEAR column as well 
def fiveTrailingYear(datetime: Timestamp): Int = {
  val date = datetime.toLocalDateTime()
  var month = date.getMonthValue()
  var day = date.getDayOfMonth()
  var year = date.getYear()
  if (month == 1 && day < 6){
    year = year-1
  }
  return year
}
val fiveDaysYearUDF = udf(fiveTrailingYear _)

// COMMAND ----------

// add dates to the FinActivity DFs
var datesFADF = financialActivityDF.select("MATTER_SK","MATTER_FINANCIAL_ACTIVITY_SK","MATTER_FINANCIAL_HISTORY_DATE","COINS_NET_ACTIVITY_AMOUNT","ACTIVITY_AMOUNT","ACTIVITY_TYPE")


// COMMAND ----------

// try the UDF to create new column for month
datesFADF = datesFADF.withColumn("MONTH", fiveDaysMonthUDF(col("MATTER_FINANCIAL_HISTORY_DATE")))
datesFADF = datesFADF.withColumn("YEAR", fiveDaysYearUDF(col("MATTER_FINANCIAL_HISTORY_DATE")))
display(datesFADF)

// COMMAND ----------

// add DATE_KEY
datesFADF = datesFADF.withColumn("DATE_KEY", concat($"YEAR",$"MONTH"))

// COMMAND ----------

printCounts(datesFADF, "MATTER_SK")

// COMMAND ----------

// join datesFADF on subset of matters over 2000
datesFADF = datesFADF.join(mattersOver2000, Seq("MATTER_SK"), "inner")

// COMMAND ----------

printCounts(datesFADF, "MATTER_SK")

// COMMAND ----------

// create date table for all time windows 
import java.time.LocalDate

val numMonths = 12 * 20 + 3
val now = LocalDate.now()
val startTime = now.minusMonths(numMonths)

lazy val dateStream: Stream[LocalDate] = startTime #:: dateStream.map(_.plusMonths(1))
val dates = dateStream.take(numMonths+1).toSeq.map(t => (t.getYear(), t.getMonth().getValue()))

// COMMAND ----------

// create BASE date table back to 1995
var dfdates = dates.toDF("YEAR","MONTH")
dfdates = dfdates.withColumn("DATE_KEY", concat($"YEAR",$"MONTH"))
dfdates = dfdates.withColumn("IDX", monotonically_increasing_id())
display(dfdates)

// COMMAND ----------

// try joining financialActivity with date df
datesFADF = datesFADF.drop("YEAR","MONTH")
val fa_joindates = dfdates.join(datesFADF, Seq("DATE_KEY"), "left")
display(fa_joindates)

// COMMAND ----------

// try joining billingHeader with date df
datesBHDF = datesBHDF.drop("YEAR","MONTH")
val bh_joindates = dfdates.join(datesBHDF, Seq("DATE_KEY"), "left")
display(bh_joindates)

// COMMAND ----------

// join dates on Matter Detail to get counts of matters when opened
matterDates = matterDates.drop("YEAR","MONTH")
val matters_joindates = dfdates.join(matterDates, Seq("DATE_KEY"), "left")
display(matters_joindates)

// COMMAND ----------

printCounts(matters_joindates, "MATTER_SK") // these counts are # of matters, # of billing for matters, # of FA for matters => after 2000
printCounts(fa_joindates, "MATTER_SK")
printCounts(bh_joindates, "MATTER_SK")

// COMMAND ----------

//CHECKS for counts
//println(matters_joindates.select("DATE_KEY").count) // this used to be 21K but now 17K which makes sense if we fixed dupes in FMD..right?
//println(matters_joindates.select("MATTER_SK").distinct.count)//but why is this number like 3 off? does that make sense
//nvm i figured it out. the three off are most likely in the same month so dupe dateKeys but not matter_SKs
//println(matters_joindates.count)
//println(matters_joindates.select("MATTER_SK").count)

// COMMAND ----------

//checking dupe MatterSk Vadym mentioned and dupe Tom found 25808 && 122

//display(matters_joindates.where($"MATTER_SK" === 25808)) //NO dupe
//display(fa_joindates.where($"MATTER_SK" === 25808)) //Doesn't exist
//display(bh_joindates.where($"MATTER_SK" === 25808)) //Doesn't exist

//display(matters_joindates.where($"MATTER_SK" === 122)) //NO dupe

//FinActivity
//display(fa_joindates.where($"MATTER_SK" === 122)) // this makes sense if there are at least two for each matterSK
//fa_joindates.where($"MATTER_SK" === 122).count //89
//fa_joindates.select("DATE_KEY").where($"MATTER_SK" === 122).distinct.count //42


//BillingHeader 
//display(bh_joindates.where($"MATTER_SK" === 122)) // different totalNetAmounts which should make sense
//bh_joindates.where($"MATTER_SK" === 122).count //57
//bh_joindates.select("DATE_KEY").where($"MATTER_SK" === 122).distinct.count  //53

// COMMAND ----------

//note.. val x = 7418205 / 164054  i get 45x more rows after joining on MatterSK to append MATTER_FINANCIAL_ACTIVITY_SK
//this should apply anymore

// COMMAND ----------

// save temporary tables
val curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
val tableFA = "measureMattersHelper_fa"
val tableBH = "measureMattersHelper_bh"
val tableMatters = "measureMattersHelper_cnt"
fa_joindates.write.format("delta").save(curatedDeltaPath+tableFA)
bh_joindates.write.format("delta").save(curatedDeltaPath+tableBH)
matters_joindates.write.format("delta").save(curatedDeltaPath+tableMatters)

// COMMAND ----------

// MAGIC %md Starting DateTableHelper

// COMMAND ----------

// use to fail/succeed table update
var failed = false

// COMMAND ----------

