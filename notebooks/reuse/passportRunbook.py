# Databricks notebook source
import os
import sys
import json
import pandas as pd
import numpy as np
import re

# COMMAND ----------

dbutils.secrets.help()
#dbutils.secrets.get(scope = "aldssecretscope", key = "aldsprincipal")

# COMMAND ----------

# MAGIC %sh pwd

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

# use this command to run this notebook in others - https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils#dbutils-secrets
# %run /path/to/InstallDependencies  
%run /reuse/preRunbook

# COMMAND ----------

        ####################### ABOVE SAVE IN NOTEBOOK FOR PRE RUN ##########################

# COMMAND ----------

# mount the datalake on DBFS at /mnt/Lake location
dbutils.fs.mount(
  source = "abfss://passport-raw-src@testxdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/Cake",
  extra_configs= configs)
dbutils.fs.ls("/")

# COMMAND ----------

display(dbutils.fs.mounts())
display(dbutils.fs.ls("/mnt/Cake/sampledata"))

# COMMAND ----------

# needed foir excel parsing
dbutils.library.installPyPI("xlrd")

# COMMAND ----------

# MAGIC %sh ls

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

print(len(worksheets))

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

# MAGIC %sh ls

# COMMAND ----------

# save all raw Python passport dataframes just in case
filepath = "/dbfs/mnt/Cake/sampledata/"
for table in each_data_worksheet.keys():
  filepath = "/dbfs/mnt/Cake/sampledata/{}.csv".format(table)
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