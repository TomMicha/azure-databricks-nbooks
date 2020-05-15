# Databricks notebook source
# MAGIC %md 
# MAGIC ### Building Persisted AOPMapping
# MAGIC Dependent Tables:
# MAGIC - pull transient/XDW/AOPMapping
# MAGIC > Actions: No null checks for this table

# COMMAND ----------

dfAOPMapping = spark.read.format("delta").load("/mnt/alasdeltalake/transient/XDW/AOPMapping")

# COMMAND ----------

display(dfAOPMapping)

# COMMAND ----------

# MAGIC %md Writing to DeltaLake Persisted Zone

# COMMAND ----------

#save dim table in Persisted path
persistedDeltaPath = "/mnt/alasdeltalake/persisted/XDW/"
tableName = "AOP_Mapping"
dfAOPMapping.write.format("delta").save(persistedDeltaPath+tableName)

# COMMAND ----------

