# Databricks notebook source
servicePrincipalID = "a6b5bdef-6233-4115-b503-12f2ab45addf"
servicePrincipalKey = "?UOHlwRhlfKH@tAV9k_[cCgdFC3jPl24"
directoryID = "efee43ad-8ca6-4bbd-b90e-634e62d519f4"
directory = "https://login.microsoftonline.com/{}/oauth2/token".format(directoryID)
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": servicePrincipalID,
          "fs.azure.account.oauth2.client.secret": servicePrincipalKey,
          "fs.azure.account.oauth2.client.endpoint": directory}

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.testxdatalake.dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type.testxdatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.testxdatalake.dfs.core.windows.net", dbutils.secrets.get(scope = "aldssecretscope", key = "aldsprincipal"))
spark.conf.set("fs.azure.account.oauth2.client.secret.testxdatalake.dfs.core.windows.net", dbutils.secrets.get(scope = "aldssecretscope", key = "aldssecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.testxdatalake.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get(scope = "aldssecretscope", key = "aldstenant")))

# COMMAND ----------

# test taking my csv
df = spark.read.csv("abfss://passport-raw-src@testxdatalake.dfs.core.windows.net/sampledata/passport1table.csv")
af = spark.read.format('csv').options(header='true', inferSchema='true').load('abfss://passport-raw-src@testxdatalake.dfs.core.windows.net/sampledata/passport1table.csv')

# COMMAND ----------

display(af)

# COMMAND ----------

# should refactor to use dbutils.secrets to store these credentials
servicePrincipalID = "a6b5bdef-6233-4115-b503-12f2ab45addf"
servicePrincipalKey = "?UOHlwRhlfKH@tAV9k_[cCgdFC3jPl24"
directoryID = "efee43ad-8ca6-4bbd-b90e-634e62d519f4"
directory = "https://login.microsoftonline.com/{}/oauth2/token".format(directoryID)
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": servicePrincipalID,
          "fs.azure.account.oauth2.client.secret": servicePrincipalKey,
          "fs.azure.account.oauth2.client.endpoint": directory}

# COMMAND ----------

# mount the datalake on DBFS at /mnt/Lake location
dbutils.fs.mount(
  source = "abfss://elect-data@testxdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/Lake",
  extra_configs= configs)

# COMMAND ----------

# what is available in this mount
dbutils.fs.ls("mnt/Lake")

# COMMAND ----------

# create spark DF using DBFS path /mnt/Lake
df = (spark
     .read
     .option("header","true")
     .option("inferSchema","true")
     .csv("/mnt/Lake/passport1table.csv")
     )
# inspect data
display(df)

# COMMAND ----------

sample_df = df.drop('UPDATED_FLAG','C_TOT_F_IN_RTN_SP_DEF_BAS_AM')
display(sample_df)

# COMMAND ----------

# save back sample df to data lake - worked
sample_df.write.csv(header="true", path="abfss://elect-data@testxdatalake.dfs.core.windows.net/passportTest.csv")

# COMMAND ----------

# retrieve written file // worked and able to read
df_sample_check = (spark
     .read
     .option("header","true")
     .option("inferSchema","true")
     .csv("/mnt/Lake/passportTest.csv")
     )
# inspect data
display(df_sample_check)

# COMMAND ----------

# dbutils.fs file system commands for DBFS root
%fs ls

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# see my curent file directeory
display(dbutils.fs.ls("/dbfs"))

# COMMAND ----------

# create pandas DF and save as csv
import pandas as pd
df_read = pd.read_csv("/dbfs/mnt/Lake/passport1table.csv")


# able to read in csv file from dbfs storage

# COMMAND ----------

df_read.head()

# COMMAND ----------

# save python dataframe to mounted drive
df_read.to_csv("/dbfs/mnt/Lake/passport_python_read.csv", header="true")

# COMMAND ----------

# where am i in mystic  file hierarchy?
%sh cd ..

ls

# COMMAND ----------

dbutils.fs.ls("/foobar")

# COMMAND ----------



# COMMAND ----------

# play around with file storing
dbutils.fs.put("/foobar/baz.txt", "Hello, World!")
#Out[88]: [FileInfo(path='dbfs:/foobar/baz.txt', name='baz.txt', size=13)]

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/foobar"))

# COMMAND ----------

# create in memory data frame

import numpy as np
np.random.seed(1)
data_df = pd.DataFrame({"A" : np.random.randint(low=1, high=100, size=30),
                     "B"  : np.random.normal(0.0, 1.0, size=30),
                     "C"  : np.random.randint(low=1, high=1000, size=30)
                     })
data_df.size

# COMMAND ----------

# save data_df into internal memory storage in python
# data_df.to_csv("rando.csv",header="true")
data_df.to_csv("/dbfs/foobar/rando.csv",header="true")
# my file initially gets saved in /databricks/driver

# COMMAND ----------

# where is my file
display(dbutils.fs.ls("/mnt/Lake"))

# COMMAND ----------

# my current files here
%sh ls
# my file is

# COMMAND ----------

# save my rando csv file dataframe to mounted storage
#dbutils.fs.mkdirs("/dbfs/mnt/Lake/TEST")
data_df.to_csv("/dbfs/mnt/Lake/rando.csv",header="true")

# COMMAND ----------

# now read the same pandas dataframe (says its in application/octet format)
mount_df = pd.read_csv("/dbfs/mnt/Lake/rando.csv", header="infer", index_col=None)
mount_df


# COMMAND ----------

# now retrieve random csv file with spark and dbutils
df_final_rando = (spark
     .read
     .option("header","true")
     .option("inferSchema","true")
     .csv("/mnt/Lake/rando.csv")
     )
display(df_final_rando)

# COMMAND ----------

dbutils.fs.unmount("/mnt/Lake")

# COMMAND ----------

