# Databricks notebook source
# MAGIC %run ./preRun

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# mount a drive to get my csv files for passport data (already mounted)
dbutils.fs.mount(
  source = "abfss://passport-raw-src@testxdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/Pass",
  extra_configs= configs)
dbutils.fs.ls("/")

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

# read csvs in my directory


# COMMAND ----------

# MAGIC %scala
# MAGIC // establish connection jdbc
# MAGIC val jdbcHostname = "testx-sqltom.database.windows.net"
# MAGIC val jdbcPort = 3306
# MAGIC val jdbcDatabase = "testx-passportdata"
# MAGIC val username = "thanos"
# MAGIC val password = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqlthanos")
# MAGIC 
# MAGIC // create jdbc url
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"${username}")
# MAGIC connectionProperties.put("password", s"${password}")

# COMMAND ----------

#python jdbc
jdbcHostname = "testx-sqltom.database.windows.net"
jdbcPort = 3306
jdbcDatabase = "testx-passportdata"
username = "thanos"
password = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqlthanos")

jdbcUrl = "jdbc:sqlserver://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.sqlserver.jdbc.Driver"
}

# COMMAND ----------

# MAGIC %scala
# MAGIC // check connectivity 
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

dbutils.fs.ls('/mnt/Pass')

# COMMAND ----------

# MAGIC %scala
# MAGIC // read in csv file
# MAGIC val df_single = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
# MAGIC   .csv("/mnt/Pass/lookupassport/LKPCOUNTRY.csv")
# MAGIC 
# MAGIC val df_multi = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
# MAGIC   .csv("/mnt/Pass/lookupassport")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df_single)

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala 
# MAGIC spark.table("LKPCOUNTRY")
# MAGIC      .write
# MAGIC      .jdbc(jdbcUrl, df_single, connectionProperties)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // this should create a table 
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC val user = "thanos"
# MAGIC val password = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssqlthanos")
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url" ->"testx-sqltom.database.windows.net",
# MAGIC   "databaseName"->"testx-passportdata",
# MAGIC   "dbTable" -> "ref.LKPCOUNTRY",
# MAGIC   "user" ->user, 
# MAGIC   "password"->password
# MAGIC ))
# MAGIC 
# MAGIC val datawarehouse = sqlContext.read.sqlDB(config)
# MAGIC println("Total row:" + datawarehouse.count)
# MAGIC datawarehouse.show()

# COMMAND ----------

# create taables and database in SPARK SQL

# COMMAND ----------

# Should be abel to save csv files into the DBFS instead of mounted Data Lake, and then create tables from source files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS TEST_DB;

# COMMAND ----------

# MAGIC %sql
# MAGIC use TEST_DB;

# COMMAND ----------

dbutils.fs.unmount("/mnt/Pass")

# COMMAND ----------

Array((UPDATED_FLAG,StringType), (ACTIVE,IntegerType), (CODE,StringType), (CREATED_AT,TimestampType), (CREATED_BY,StringType), (DISPLAY_NAME,StringType), (ID,IntegerType), (UPDATED_AT,TimestampType), (UPDATED_BY,StringType), (DTYPE,StringType))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS TEST_DB.LKPCOUNTRY(
# MAGIC         UPDATED_FLAG STRING, 
# MAGIC         ACTIVE INT, 
# MAGIC         CODE STRING, 
# MAGIC         CREATED_AT Timestamp, 
# MAGIC         CREATED_BY STRING, 
# MAGIC         DISPLAY_NAME STRING, 
# MAGIC         ID INT, 
# MAGIC         UPDATED_AT Timestamp, 
# MAGIC         UPDATED_BY STRING, 
# MAGIC         DTYPE STRING)
# MAGIC     USING CSV  
# MAGIC     OPTIONS ( header='false',
# MAGIC               nullvalue='NA',
# MAGIC               timestampFormat="yyyy-MM-dd'T'HH:mm:ss",
# MAGIC               path='/mnt/Pass/lookupassport/LKPCOUNTRY.csv'); 

# COMMAND ----------

