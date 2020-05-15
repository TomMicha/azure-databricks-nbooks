// Databricks notebook source
// MAGIC %md 
// MAGIC ### Building Curated factMatterReverseHistory (part of summary/measures)
// MAGIC Dependent Tables:
// MAGIC - pull persisted/XDW/dimMatter
// MAGIC - pull persisted/XDW/dimMatterActivity
// MAGIC - pull persisted/XDW/ ** some Billing tables
// MAGIC > Actions: group by matter key, get reserve history

// COMMAND ----------

// pull necessary tables from persisted delta tables
val dfMatter = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/")
val dfMatterActivity = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/")
val dfBillingHeader = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factBillingHeader")

// COMMAND ----------



// COMMAND ----------

// for reference 
val columns = List[String]("MATTER_KEY",
"MATTER_NUMBER",
"FINANCIAL_ACTIVITY_TYPE",
"SIGNED_ACTIVITY_AMOUNT",
"RESERVE_DEFENSE_AS_OF",
"RESERVE_INDEMNITY_AS_OF",
"FINANCIAL_ACTIVITY_RECORD_DATE")

// COMMAND ----------



// COMMAND ----------

// save curate version of table