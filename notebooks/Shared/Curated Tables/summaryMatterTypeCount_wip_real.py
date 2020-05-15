# Databricks notebook source
# MAGIC %md 
# MAGIC ### Building Curated summaryMatterTypeCount (part of summary/measures)
# MAGIC Dependent Tables:
# MAGIC  - pull persisted/XDW/dimPolicyVersion
# MAGIC  - pull persisted/XDW/factMatterDetail
# MAGIC  - pull persisted/XDW/dimMatter
# MAGIC > Actions: matter and claim counts by type - summary view
# MAGIC >
# MAGIC ```
# MAGIC POLICY_VERSION_SK ,FIRM_SK, LOB_SK, POLICY_COVERAGE_START, POLICY_COVERAGE_END, MATTER_CATEGORY_CODE, MATTERS_OPEN, MATTERS_TOTAL
# MAGIC Calc = FIRM_PAID(WIR BASE), ALAS_PAID, ALAS_INCURRED_AMOUNT (sum(ALAS_PAID, ALAS_RESERVE))
# MAGIC ```

# COMMAND ----------

# MAGIC %fs ls '/mnt/alasdeltalake/persisted/XDW/'

# COMMAND ----------

import datetime 
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC For Reference
# MAGIC > columns = ["POLICY_VERSION_SK",
# MAGIC >"FIRM SK","LOB SK" "POLICY_COVERAGE_START",
# MAGIC  >"POLICY_COVERAGE_END",
# MAGIC > "MATTER_CATEGORY_CODE",
# MAGIC > "MATTERS_OPEN",
# MAGIC >"MATTERS_TOTAL"]
# MAGIC 
# MAGIC > - Calc Fields - Count of Claims per Cat Code 
# MAGIC > - Gross Paid Base 
# MAGIC > - Firm Paid 
# MAGIC > - ALAS incurred Amount - paid + Reserve Dollars (net of coins)

# COMMAND ----------

# MAGIC %md Going to refactor this portion up to financials - our base table will be the financial table so this can be read now, but won't be used till much later

# COMMAND ----------

#GREEN
##pull necessary tables from persisted delta tables
dfPolicyYear = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyYear") #base table
dfFMatterDetail = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterDetail") #use primarily for Matter_Number
dfFMatCatHistory = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterCategoryHistory") #for Category_Code (CL-01etc)
dfMatter = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimMatter") #Matter_Status_Code


# COMMAND ----------

#GREEN
#getting the columns we want from the tables - note:Haven't gotten dimMatter yet - we'll do that later to build a secondary table
dfPY = dfPolicyYear.select("POLICY_YEAR_SK","POLICY_YEAR_KEY","LOB", "POLICY_PERIOD_STARTS", "POLICY_PERIOD_END", "CYCLE_KEY")
dfMatCat = dfFMatCatHistory.select("POLICY_YEAR_SK","MATTER_SK", "MATTER_CATEGORY_CODE", "MATTER_CATEGORY", "MATTER_TYPE","CLAIM_INDICATOR", "MATTER_CATEGORY_EFFECTIVE_ON")
dfFMD = dfFMatterDetail.select("FIRM_SK", "LOB_SK", "MATTER_SK", "MATTER_OPEN_DATE", "MATTER_NUMBER", "POLICY_YEAR_SK") 
dfMatters = dfMatter.select("NAME", "MATTER_STATUS_CODE", "MATTER_SK")

# COMMAND ----------

# MAGIC %md CREATION OF BASE SK TABLE

# COMMAND ----------

base = dfFMD.join(dfMatters, "MATTER_SK", "left")
base = base.join(dfPY, "POLICY_YEAR_SK", "left")

# COMMAND ----------

#TEST
#display(base.where(F.col('MATTER_SK') == 6872))

# COMMAND ----------

#GREEN
#add counts to this table

cntTable =spark.read.format('delta').load('/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_counts')

# COMMAND ----------

#GREEN
datesBase = cntTable.join(base, ["MATTER_SK", "POLICY_YEAR_SK", "FIRM_SK", "MATTER_OPEN_DATE"],"left" )

# COMMAND ----------

#TEST
#display(cntTable.where(F.col('MATTER_SK') == 6872))

# COMMAND ----------

# MAGIC %md Adding Financials

# COMMAND ----------

#GREEN
financialTable =spark.read.format('delta').load('/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_financial')
finTable = financialTable.select('MATTER_SK', 'POLICY_YEAR_SK','DATE_KEY', 'ALAS_PAID', 'FIRM_PAID', 'TOTAL_INCURRED')

# COMMAND ----------

#GREEN
#lets try taking only dateKeys to see if we can just get all date keys aligned
datesFin = finTable.select("MATTER_SK", "DATE_KEY", "POLICY_YEAR_SK")
baseD = datesBase.drop('DATE_KEY')
datesFins = datesFin.join(baseD, ['MATTER_SK', 'POLICY_YEAR_SK'],'left')

# COMMAND ----------

#display(datesFins.where(F.col('MATTER_SK')==6872).orderBy('DATE_KEY'))
print(datesFins.where(F.col('MATTER_SK')==6872).count()) #should be 73

# COMMAND ----------

#TEST
#display(finTable.where(finTable.MATTER_SK ==6872).orderBy('DATE_KEY'))

# COMMAND ----------

#GREEN
#try joining our base table here -
joinedBaseFin = finTable.join(datesFins, ["MATTER_SK", "POLICY_YEAR_SK", "DATE_KEY"], "left")

# COMMAND ----------

#TEST
#display(joinedBaseFin.where(F.col('MATTER_SK')==6872).orderBy("DATE_KEY"))

# COMMAND ----------

#TEST
print(joinedBaseFin.where(F.col('MATTER_SK')==6872).count()) #should be 73

# COMMAND ----------

# MAGIC %md Create mat cat table with date key for mat cat category 
# MAGIC >We'll use this matcat_date_key to insert the matcat whenever the key is the same year month and or datekey 

# COMMAND ----------

#dfMatCat.printSchema()

# COMMAND ----------

#here we create a base table with a MAT_CAT_YEAR, MAT_CAT_MONTH, MAT_CAT_DATE_KEY
matCatBase = dfMatCat.select("POLICY_YEAR_SK","MATTER_SK","MATTER_TYPE","MATTER_CATEGORY", "CLAIM_INDICATOR", F.year("MATTER_CATEGORY_EFFECTIVE_ON").alias('MAT_CAT_YEAR'), F.month("MATTER_CATEGORY_EFFECTIVE_ON").alias("MAT_CAT_MONTH"), "MATTER_CATEGORY_EFFECTIVE_ON")
matCatBase = matCatBase.withColumn("MAT_CAT_DATE_KEY", F.concat("MAT_CAT_YEAR", "MAT_CAT_MONTH"))

#get matter status code dfMatters and factMatterDetail to dwindle down matterSKs 
fmd = dfFMD.select("MATTER_SK", "POLICY_YEAR_SK")
mats = dfMatters.select("MATTER_STATUS_CODE", "MATTER_SK")
fmdMats = fmd.join(mats, "MATTER_SK", "left")
matCatBase = fmdMats.join(matCatBase, ["MATTER_SK", "POLICY_YEAR_SK"], "left")

display(matCatBase)


# COMMAND ----------

#remember we have to dwindle this down to only matters after 2000
#we'll be joining this table to the financial table but only by the dateKey of the specific category 

# COMMAND ----------

#getting only Open Matters
openMatters = matCatBase.where(F.col('MATTER_STATUS_CODE')=='Open')
print(openMatters.select("MATTER_SK").distinct().count()) #1918
openMatterSub = openMatters.select("MATTER_SK", "POLICY_YEAR_SK", "MATTER_CATEGORY", "MAT_CAT_DATE_KEY")

#getting total Claims ??

# COMMAND ----------

#Count of open Matters per policy Year
groupOpenMatters = openMatterSub.select('MATTER_SK', 'POLICY_YEAR_SK', 'MAT_CAT_DATE_KEY', 'MATTER_CATEGORY').groupBy('POLICY_YEAR_SK', 'MATTER_CATEGORY', 'MAT_CAT_DATE_KEY').agg({'MATTER_SK':'count'})
groupOpenMatters = groupOpenMatters.withColumnRenamed('count(MATTER_SK)', 'OPEN_MATTERS_PER_CAT')

# COMMAND ----------

#TEST
#display(groupOpenMatters)

# COMMAND ----------

#TEST
#display(groupOpenMatters.where(F.col('POLICY_YEAR_SK')==10329).orderBy('MAT_CAT_DATE_KEY'))

# COMMAND ----------

#TEST - make sure the information is consistent 
#display(matCatBase.where(F.col('POLICY_YEAR_SK') == 10329).orderBy('MAT_CAT_DATE_KEY', 'MATTER_SK'))
#display(openMatters.where(F.col('POLICY_YEAR_SK') == 10329).orderBy('MAT_CAT_DATE_KEY', 'MATTER_SK'))

# COMMAND ----------

#Test this matterSk is in openMatters and base table but not in financial table - doesn't look like there are financials on this
#display(base.where(F.col("MATTER_SK") == 28780))

# COMMAND ----------

#Test - does not exist because no financials on this matter
#display(financialTable.where(F.col("MATTER_SK") == 28780))

# COMMAND ----------

#TEST 
#print(openMatters.where(F.col('POLICY_YEAR_SK') == 10329).count()) #69

# COMMAND ----------

#TOTAL MATTERS COUNT
groupMatCat = matCatBase.select("MATTER_SK", "POLICY_YEAR_SK","MATTER_CATEGORY", "MAT_CAT_DATE_KEY").groupBy("POLICY_YEAR_SK", "MATTER_CATEGORY", "MAT_CAT_DATE_KEY").agg({'MATTER_SK':'count'})
groupMatCat = groupMatCat.withColumnRenamed('count(MATTER_SK)', 'MATTERS_PER_CAT')

# COMMAND ----------

#TEST
#display(groupMatCat.orderBy("POLICY_YEAR_SK"))

# COMMAND ----------

#TEST - making sure the counts make sense 
#display(matCatBase.where(F.col('POLICY_YEAR_SK') == 102).orderBy('MAT_CAT_DATE_KEY'))
#display(joinedBaseFin.where(F.col('POLICY_YEAR_SK') == 102).orderBy('DATE_KEY'))
#display(finTable.where(F.col('POLICY_YEAR_SK') == 102).orderBy('DATE_KEY'))
#display(base.where(F.col('POLICY_YEAR_SK') == 102))

# COMMAND ----------

#Lets do a first pass at running a join for mat cats on matCatDateKey == date key
#renamedMatCat = matCatBase.withColumnRenamed('MAT_CAT_YEAR', 'YEAR')
#renamedMatCat = renamedMatCat.withColumnRenamed('MAT_CAT_MONTH', 'MONTH')
#renamedMatCat = matCatBase.withColumnRenamed('MAT_CAT_DATE_KEY', 'DATE_KEY')
#renamedMatcat = renamedMatCat.drop('MATTER_OPEN_DATE')
joinedBF = joinedBaseFin.select("FIRM_SK", "POLICY_YEAR_SK", "LOB_SK", "LOB","DATE_KEY", "ALAS_PAID", "FIRM_PAID", "TOTAL_INCURRED", "POLICY_PERIOD_STARTS", "POLICY_PERIOD_END", "MATTER_OPEN_DATE")
groupingMC = groupMatCat.withColumnRenamed('MAT_CAT_DATE_KEY', 'DATE_KEY')
finCatBase = joinedBF.join(groupingMC, ['POLICY_YEAR_SK','DATE_KEY'],'left') 

# COMMAND ----------

#display(finCatBase.orderBy('DATE_KEY','POLICY_YEAR_SK'))

# COMMAND ----------


#display(finCatBase.where((F.col('MATTER_SK')==6872) & (F.col('YEAR') == 2017)).orderBy('DATE_KEY'))
#display(finCatBase.where(F.col('POLICY_YEAR_SK')==10329).orderBy('DATE_KEY'))
#display(finCatBase.where(F.col('POLICY_YEAR_SK')== 102).orderBy('DATE_KEY'))

# COMMAND ----------

#TEST - should be 7 
#print(finCatBase.where(F.col('POLICY_YEAR_SK')==10329).count())

# COMMAND ----------

#add Open matters count 
groupingOM = groupOpenMatters.withColumnRenamed('MAT_CAT_DATE_KEY', 'DATE_KEY')
#groupingOM = groupingOM.select('POLICY_YEAR_SK','DATE_KEY', 'OPEN_MATTERS_PER_CAT')
semiFinalFC = finCatBase.join(groupingOM, ['POLICY_YEAR_SK', 'DATE_KEY', 'MATTER_CATEGORY'], 'left')

# COMMAND ----------

#display(semiFinalFC.orderBy('DATE_KEY','POLICY_YEAR_SK'))

# COMMAND ----------

finalFC = semiFinalFC.select('DATE_KEY','POLICY_YEAR_SK', 'FIRM_SK','LOB_SK','LOB', 'ALAS_PAID','FIRM_PAID', 'TOTAL_INCURRED', 'POLICY_PERIOD_STARTS', 'POLICY_PERIOD_END', 'MATTER_OPEN_DATE', 'MATTER_CATEGORY','OPEN_MATTERS_PER_CAT','MATTERS_PER_CAT')
finalFC = finalFC.withColumnRenamed('MATTERS_PER_CAT', 'TOTAL_MATTERS_PER_CAT')
finalFC = finalFC.fillna({'OPEN_MATTERS_PER_CAT':0, 'TOTAL_MATTERS_PER_CAT':0, 'MATTER_CATEGORY':-1})
finalFC = finalFC.where(F.col('DATE_KEY')!=20001)

# COMMAND ----------

#display(finalFC.orderBy('DATE_KEY', 'POLICY_YEAR_SK'))

# COMMAND ----------

# MAGIC %md Tests

# COMMAND ----------

#display(finalFC.where(F.col('POLICY_YEAR_SK')== 5895).orderBy('DATE_KEY'))

# COMMAND ----------

#display(base.where((F.col('POLICY_YEAR_SK')== 5895)))
#display(finTable.where(F.col('POLICY_YEAR_SK')== 5895).orderBy('DATE_KEY'))
#display(matCatBase.where(F.col('POLICY_YEAR_SK')== 5895).orderBy('MAT_CAT_DATE_KEY'))

# COMMAND ----------

#three rows and 4 CR03 2 CR03 2 CL03 they should all be open
#display(semiFinalFC.where((F.col('POLICY_YEAR_SK')==10329) & (F.col('DATE_KEY')==20201)))

# COMMAND ----------

#8 rows and there should be 4 CR03 - 2 CR06 and 2CL03 -- all different MatterSKs all Open as of march 30 2020
#display(matCatBase.where((F.col('POLICY_YEAR_SK')== 10329) & (F.col('MAT_CAT_DATE_KEY') == 20201)).orderBy("MATTER_SK"))

# COMMAND ----------

#print(finCatBase.where(F.col('MATTER_SK')==6872).count())

# COMMAND ----------

#Add checks and verify table is done correctly 
failed = False

# COMMAND ----------

#save curate version of table
if (not failed):
  curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  tableName = "summaryMatterTypeCount"
  finalFC.write.format("delta").save(curatedDeltaPath+tableName)
else: 
  print("Validation Logic Failed: Please check output for issues")


# COMMAND ----------

