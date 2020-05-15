# Databricks notebook source
#still working on this
#need to figure out what .format(jdbc) does to a delta file 
#test if the data is queriable by just saving it this way. -- looks to be true
#any information that is added can we use delta time travel? -- no 
#...so what type of file is saved to the database then...?

# COMMAND ----------

# MAGIC %md ### Connection to warehouse jbdc

# COMMAND ----------

#OUR DB
adminUser = "thanos"
pw = dbutils.secrets.get(scope = "alas-key-vault-secrets", key = "sqlAdminPass")
jdbcHost = "rawpassport-sqlserver-dev.database.windows.net"
jdbcPort = 1433
jdbcDB = "ALAS-dw-dev"
jbdcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHost, jdbcPort, jdbcDB)

connectionProperties = {
  "user" : adminUser,
  "password" : pw,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
print("You are now connected to "+adminUser, jdbcDB)

# COMMAND ----------

#ALAS DB connection
adminUser = "megatron"
pw = dbutils.secrets.get(scope = "alas-key-vault-secrets", key = "sqlStagingPass")
jdbcHost = "alas-stagingdw-dev.database.windows.net"
jdbcPort = 1433
jdbcDB = "ALAS-dw-dev_Copy"
jbdcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHost, jdbcPort, jdbcDB)

connectionProperties = {
  "user" : adminUser,
  "password" : pw,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
print("You are now connected to "+adminUser, jdbcDB)

# COMMAND ----------

#list of tables to write to the warehouse
#tables = ["AOP_Mapping","dimAOP","dimCauseOfLoss","dimCensusSize","dimCoverageYear","dimFirm","dimDate","dimLineOfBusiness", "dimMatter", "dimMatter_History","dimMatterActivity","dimMatterCategory", "dimMatterDefendant", "dimPolicyVersion", "dimPolicyYear","factBillingHeader","factBillingDetail","factCensusUW","factMatterFinancialActivity","factPolicyHistory","factPolicyProrataCensus","factMatterCategoryHistory",  "factMatterDetail"]

len(tables)

# COMMAND ----------

#Added measure tables - 04/15/20
#measureTables = ["measureMatterPaidBase_helper", "summaryMatterCauseOfLoss", "summaryMatterTypeCount"]
#adding WIP measureTAbles 4/22
#measureTables = ["measureMatterTypeCount", "summaryMatterCauseOfLoss"]

#Adding new tables 4/29
#in persisted
tables = ["dimPerson", "dimPersonRole", "factMatterPersonInvolved"]
#in curated
measureTables = ["summaryAOPtable","summaryMatterTypeCount","summaryMatterCauseOfLoss", "summaryMatterMeasure_counts", "summaryMatterMeasure_financial"]

# COMMAND ----------

#adding Tables 4/22
#sumTables = ["summaryMatterMeasure_counts", "summaryMatterMeasure_financial"]
#others = ["dimAOP", "factMatterDetail"]

# COMMAND ----------

#read in dataframes to write
def toWriteTables(tablesList, container):
  for t in tablesList:
    print("Writing " + t + " To "+jdbcDB+ " From "+ container)
    print()
    df = spark.read.format("delta").load('/mnt/alasdeltalake/'+container+'/XDW/'+t)
    #write the dataframe into a sql table
   # spark.sql("DROP IF EXISTS "+ t)
    df.write.mode("overwrite").format("jdbc").jdbc(url=jbdcUrl, table = 'XDW.'+t, properties=connectionProperties)
    print("Wrote " + t + " To Warehouse!")
    print()
  

# COMMAND ----------

#run to write XDW tables to alas db march 31st
toWriteTables(tables)

# COMMAND ----------

# MAGIC %md Run to add select measure tables

# COMMAND ----------

#Cell to save single table
t = "factMatterPersonInvolved"
container = 'persisted'
print("Writing " + t + " To "+jdbcDB+ " From "+ container)
df = spark.read.format("delta").load('/mnt/alasdeltalake/'+container+'/XDW/'+t)
df.write.mode("overwrite").format("jdbc").jdbc(url=jbdcUrl, table = 'XDW.'+t, properties=connectionProperties)
print("Wrote " + t + " To "+jdbcDB+ " Warehouse!")


# COMMAND ----------

#Writing multiple table
toWriteTables(measureTables, "curated")

# COMMAND ----------

#ignore
#overwriting factMatterDetail in New EDW and diaBackup
'''df = spark.read.format("delta").load('/mnt/alasdeltalake/persisted/XDW/factMatterDetail')
df.write.mode("overwrite").format("jdbc").jdbc(url=jbdcUrl, table = 'XDWX.factMatterDetail', properties=connectionProperties)
'''

# COMMAND ----------

#Just verifying that it worked
#dwdia = spark.read.jdbc(url=jbdcUrl, table = 'XDWX.factMatterDetail', properties=connectionProperties)
#display(dwdia)

# COMMAND ----------

#dwdf = spark.read.jdbc(url=jbdcUrl, table = 'XDW.factMatterDetail', properties=connectionProperties)
#display(dwdf)

# COMMAND ----------

#dwdf.select("MATTER_SK").distinct().where(dwdf.MATTER_NUMBER.isNull()).show()

# COMMAND ----------

# MAGIC %md Extras

# COMMAND ----------

'''df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database};") \
    .option("dbtable", "XDW."+t) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()
    '''

# COMMAND ----------

df = spark.read.format("delta").load('/mnt/alasdeltalake/persisted/XDW/dimAOP')

# COMMAND ----------

df.write.mode("overwrite").format("jdbc").jdbc(url=jbdcUrl, table = 'XDW.dimDate', properties=connectionProperties)

# COMMAND ----------

df = spark.read.format("delta").load('/mnt/alasdeltalake/persisted/XDW/dimDate')

# COMMAND ----------

 #write the dataframe into a sql table
df.write.mode("overwrite").format("jdbc").jdbc(url=jbdcUrl, table = 'test.dimDate', properties=connectionProperties)
##this apparently works.. the schema needs to be there before we can "overwrite" it but the table is create and we can query the information 
#what is the Format.(jdbc) do exactly ? does it convert back to parquet?

# COMMAND ----------

dwdf = spark.read.jdbc(url=jbdcUrl, table = 'test.dimDate', properties=connectionProperties)
display(dwdf)

# COMMAND ----------

dfAOP = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimAOP")

# COMMAND ----------

display(dfAOP.where(dfAOP.CREATED > '2019-06-21T10:35:18.160+0000'))

# COMMAND ----------

