# Databricks notebook source
import os
import sys
import json
import pandas as pd
import numpy as np
import re

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# run this notebook to connect to data lake, get your service principal etc.
servicePrincipalID = dbutils.secrets.get(scope = "aldssecretscope", key = "aldsprincipal")
servicePrincipalKey = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssecret")
directoryID = dbutils.secrets.get(scope = "aldssecretscope", key = "aldstenant")
directory = "https://login.microsoftonline.com/{}/oauth2/token".format(directoryID)
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": servicePrincipalID,
          "fs.azure.account.oauth2.client.secret": servicePrincipalKey,
          "fs.azure.account.oauth2.client.endpoint": directory}

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# JDBC Connection (NOT RAN)
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
val jdbcHostname = "<hostname>"
val jdbcPort = 1433
val jdbcDatabase = "<database>"
val username = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqluser")
val password = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqlpass")

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

# COMMAND ----------

# spark SQL Connector to tyrion DB

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._

# COMMAND ----------

# MAGIC %scala
# MAGIC val username = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqluser")
# MAGIC val password = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqlpass")
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"            -> "tryrion-sqlsrv.database.windows.net",
# MAGIC   "databaseName"   -> "tyrion-dw",
# MAGIC   "dbTable"        -> "SalesLT.ProductDescription",
# MAGIC   "user"           -> username,
# MAGIC   "password"       -> password,
# MAGIC   "connectTimeout" -> "5", //seconds
# MAGIC   "queryTimeout"   -> "5"  //seconds
# MAGIC ))
# MAGIC 
# MAGIC val collection = sqlContext.read.sqlDB(config)
# MAGIC collection.show()

# COMMAND ----------



# COMMAND ----------

# SQL Spark Connector to thanos(my db)

# COMMAND ----------

# MAGIC %scala
# MAGIC val password = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqlthanos")
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"            -> "testx-sqltom.database.windows.net",
# MAGIC   "databaseName"   -> "testx-passportdata",
# MAGIC //  "dbTable"        -> "SalesLT.ProductDescription",
# MAGIC   "user"           -> "thanos",
# MAGIC   "password"       -> password,
# MAGIC   "connectTimeout" -> "5", //seconds
# MAGIC   "queryTimeout"   -> "5"  //seconds
# MAGIC ))
# MAGIC 
# MAGIC val collection = sqlContext.read.sqlDB(config)
# MAGIC collection.show()

# COMMAND ----------

# first CosmosDB connection (removed)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.joda.time._
# MAGIC import org.joda.time.format._
# MAGIC 
# MAGIC import com.microsoft.azure.cosmosdb.spark.schema._
# MAGIC import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
# MAGIC import com.microsoft.azure.cosmosdb.spark.config.Config
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._

# COMMAND ----------

# MAGIC %scala
# MAGIC // configure the connection to cosmos
# MAGIC val key = dbutils.secrets.get(scope = "aldssecretscope", key = "aldscosmos")
# MAGIC val configMap = Map(
# MAGIC   "Endpoint" -> "https://testx-passportdata.table.cosmos.azure.com:443/",
# MAGIC   "Masterkey" -> key,
# MAGIC   "Database" -> "passportSrc",
# MAGIC  // "Collection" -> "passportRaw",
# MAGIC   "preferredRegions" -> "East US")
# MAGIC val config = Config(configMap)

# COMMAND ----------

dbutils.library.installPyPI('azure-cosmosdb-table')

# COMMAND ----------

# MAGIC %scala
# MAGIC // create sample data and write to cosmos to ensure connection
# MAGIC val df = spark.range(5).select(col("id").cast("string").as("value"))
# MAGIC CosmosDBSpark.save(df, config)

# COMMAND ----------

# mount the datalake on DBFS at /mnt/Lake location
dbutils.fs.mount(
  source = "abfss://passport-raw-src@testxdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/Pass",
  extra_configs= configs)
dbutils.fs.ls("/")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Pass"))


# COMMAND ----------

# needed foir excel parsing
dbutils.library.installPyPI("xlrd")

# COMMAND ----------

# sample script (duplicate) to get all workbooks out of LOOK tables file and separate for dataframe/csv creation for each
excel_file_lookups = pd.ExcelFile("/dbfs/mnt/Pass/Passport Lookup Tables.xlsx")
worksheets = excel_file_lookups.sheet_names
print("Names of workbooks ",worksheets)
# parse if needed here, otherwise drop 1st worksheet since it is index
# data_sheets = [ x for x in worksheets if re.search('DATA',x,re.IGNORECASE)]
each_lookup_worksheet = {}

# COMMAND ----------

# loop through lookup workbooks
for wb in worksheets:
  each_lookup_worksheet[wb] = pd.read_excel("/dbfs/mnt/Pass/Passport Lookup Tables.xlsx",
                                                     headers=None,
                                                     sheet_name=wb)

# COMMAND ----------

# save all dataframes in csv format
for table in each_lookup_worksheet.keys():
  filepath = "/dbfs/mnt/Pass/lookupassport/{}.csv".format(table)
  df = each_lookup_worksheet[table]
  df.to_csv(filepath, index=False)

# COMMAND ----------

# sample script to get all workbooks out of file and separate dataframe creation

excel_file_src = pd.ExcelFile("/dbfs/mnt/Cake/sampledata/passport_data_masking.xlsx")
worksheets = excel_file_src.sheet_names
print("Names of workbooks ",worksheets)
# parse if needed here, otherwise drop 1st worksheet since it is index
# data_sheets = [ x for x in worksheets if re.search('DATA',x,re.IGNORECASE)]
each_data_worksheet = {}
all_datasheets_df = pd.DataFrame()


# COMMAND ----------

# loop through 
for wb in worksheets:
  each_data_worksheet[wb] = pd.read_excel("/dbfs/mnt/Cake/sampledata/passport_data_masking.xlsx",
                                                     headers=None,
                                                     sheet_name=wb)

# COMMAND ----------

dimmatter_df = each_data_worksheet['DIMMATTER']
dimmatter_df.dtypes

# COMMAND ----------

# rawDataDF.write .mode("overwrite") .format("delta") .saveAsTable("customer_data", path=customerTablePath)

# COMMAND ----------

# save all raw Python passport dataframes just in case

for table in each_data_worksheet.keys():
  filepath = "/dbfs/mnt/Pass/sampledata/{}.csv".format(table)
  df = each_data_worksheet[table]
  df.to_csv(filepath, index=False)

# COMMAND ----------

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "false")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

sdf = spark.createDataFrame(each_data_worksheet["DIMMATTER"])


# COMMAND ----------

# convert all into SPARK DATAFRAMES
each_spark_dataframe = {}
for t in each_data_worksheet.keys():
  pdf = each_data_worksheet[t] # nth pyDF
  sdf = spark.createDataFrame(pdf)
  each_spark_dataframe[t] = sdf

# COMMAND ----------



# COMMAND ----------

# STILL CREATE ALL TABLES FROM DFs or CSVs