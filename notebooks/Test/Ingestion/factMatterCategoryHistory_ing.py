# Databricks notebook source
# MAGIC %run "/Test/Utility/Python/_pyUtil"

# COMMAND ----------

# Widgets save values here
dbutils.widgets.text("DATE", "", "DateForPull")

# COMMAND ----------

DATE = dbutils.widgets.get("DATE") 
if DATE == "":
  dbutils.notebook.exit("Error: Require DATE param passed to notebook for daily update")
DATE = datetime.fromisoformat(DATE)
print(DATE)

# COMMAND ----------

#setUp
target = "factMatterCategoryHistory"
entity = "MatterHistory"
primaryKey = "MATTER_CATEGORY_HISTORY_SK"

errors = []
clear = True
savePath = transientPath+target

# COMMAND ----------

# get Passed Date Parameter or hardcode for now
# ---- get from caller notebook // condition otherwise get today
todayDate = datetime.now()
startDate = DATE
endDate = DATE + timedelta(days=1) 
print(startDate)
print(endDate)

# COMMAND ----------

# MAGIC %md Ingestion Notebook for factMatterCategoryHistory - update this table

# COMMAND ----------

#Read in newMatterHistory Records - make sure we only get Matter Type
newMH =  dailyRead("updatedAt",startDate, endDate, truncatedPath+entity).where(F.col('attributeName') == 'Matter Type')
#display(newMH)

#exit If we don't have any records
exitIfNoRecords(newMH, 'MATTER_HISTORY_ID', target, entity)

# COMMAND ----------

# read in existing factMatterCategoryHistory
factMatterCatHistory = spark.read.format("delta").load(transientPath+target)
startingFMCHCount = factMatterCatHistory.count()

# COMMAND ----------

#printNullCount(factMatterCatHistory, factMatterCatHistory.columns)

# COMMAND ----------

# Add Null Check for any SKs in this table
#display(factMatterCatHistory.where(F.col("FIRM_SK").isNull()))
display(factMatterCatHistory.orderBy(F.desc('MODIFIED')))

# COMMAND ----------

print('Starting Count: {}'.format(startingFMCHCount))
print('New Records: {}'.format(newMH.count()))

# COMMAND ----------

# read in existing dimMatterCat
dimMatterCategory = spark.read.format("delta").load(transientPath+"dimMatterCategory")

# COMMAND ----------

#need to join on updated DSM 
flexDSM = getFlexDSM('MATTER_SK', 'MATTER_ID')
print(flexDSM.count())

# COMMAND ----------

#match matter with matterHistory base on the matter_id
ingestedSK = newMH.join(flexDSM, 'MATTER_ID', 'left').select('MATTER_SK', *newMH.columns)
#rename attributeNewValue to  matterCategory to ingested to be able to match
renamed = ingestedSK.withColumnRenamed('attributeNewValue', 'MATTER_CATEGORY').where(F.col('MATTER_SK').isNotNull())
print(renamed.count())

# COMMAND ----------

#what is this table made up of? 
#MatterHistory matched with Matters - so if new Matter 
#new matter add its corresponding category 
# updates to matter - match to matter category and then see if there's an update to it's category
#expired = updatedAt new update 
#new updated row
#effectiveOn = customHistoryDateTime new update

#create window with ranking to be able to do this -- #do a window to rank them and then do a udf that basically puts 1's expiredOn == 9999 for 2's put expiredOn == 1's customHistoryDateTime == updatedAt == effectiveOn

# COMMAND ----------

# MAGIC %md Adding Sks and additional Columns

# COMMAND ----------

Sks = ['MATTER_SK','POLICY_YEAR_SK', 'POLICY_VERSION_SK', 'MATTER_SK', 'FIRM_SK', 'AOP_SK', 'FIRM_KEY']

# COMMAND ----------

#Add firmKey using dimFirm - getting SKs from FMD and only matters that are OPEN or closed from dimMatter
dimFirm = optionOpenDF(transientPath+'dimFirm').select('FIRM_SK', 'FIRM_KEY')
fmd = spark.read.format('delta').load(transientPath+'factMatterDetail').join(dimFirm, 'FIRM_SK', 'left')
nonPendingDimMatter = spark.read.format('delta').load(transientPath+'dimMatter').where(F.col("MATTER_STATUS_CODE").isin('Open', 'Closed')).select("MATTER_SK")

# COMMAND ----------

# renamed, exclude matters that are pending from list - category isn't filled in until matter is open but just in case
renamedWithoutPending = renamed.join(nonPendingDimMatter, "MATTER_SK", "inner")
#display(renamedWithoutPending)

# COMMAND ----------

#Join with the fmd that has the Sks and firmKey
mattersWithSK = renamedWithoutPending.join(fmd.select('POLICY_YEAR_SK', 'POLICY_VERSION_SK', 'MATTER_SK', 'FIRM_SK', 'AOP_SK', 'FIRM_KEY').distinct(), 'MATTER_SK', 'left')
print(mattersWithSK.count())

# COMMAND ----------

#Null check - might want to take this out or add different SKs as well. 
if nullCheck(mattersWithSK, Sks):
  error = 'ERROR - There are matters that have nulls in one or more of the following {}'.format(Sks)
  print(error)
  errors.append(error)
  clear = False
else: 
  #clear = True
  print('PROCEED - obtained SKs for matters')

# COMMAND ----------

#appending rows - adding the columsn necessary 
addedCols = mattersWithSK.join(dimMatterCategory, 'MATTER_CATEGORY', 'left')

# COMMAND ----------

# MAGIC %md Create The Rest of the Table - Transformations

# COMMAND ----------

selectCols = ['POLICY_YEAR_SK', 'POLICY_VERSION_SK', 'MATTER_SK', 'FIRM_SK','AOP_SK', 'FIRM_KEY', 'MATTER_CATEGORY_SK','MATTER_CATEGORY_EFFECTIVE_ON', 'MATTER_TYPE', 'MATTER_CATEGORY_CODE', 'MATTER_CATEGORY', 'CLAIM_INDICATOR', 'CIRCUMSTANCE_INDICATOR', 'FREQUENCY_INDICATOR', 'WITHOUT_MERIT_INDICATOR', 'CASE_RESERVE_INDICATOR', 'PRE_CASE_INDICATOR', 'EFFECTIVE_ON_DATE', 'EXPIRED_ON_DATE', 'CREATED', 'MODIFIED']

# COMMAND ----------

#Create the columns for this table based on some transformations
semiFinal = (addedCols
             .withColumn('CLAIM_INDICATOR', F.when(F.col('MATTER_TYPE') == 'CL', 1).otherwise(0))
             .withColumn('CIRCUMSTANCE_INDICATOR', F.when(F.col('MATTER_TYPE') == 'CR', 1).otherwise(0))
             .withColumn('FREQUENCY_INDICATOR', F.when(F.col('MATTER_CATEGORY').isin(['CL01', 'CL03', 'CL04', 'CL05', 'CL06']), 1).otherwise(0))
             .withColumn('WITHOUT_MERIT_INDICATOR', F.when(F.col('MATTER_CATEGORY') == 'CL02', 1).otherwise(0))
             .withColumn('CASE_RESERVE_INDICATOR', F.when(F.col('MATTER_CATEGORY') == 'CL01', 1).otherwise(0))
             .withColumn('PRE_CASE_INDICATOR', F.when(F.col('MATTER_CATEGORY') == 'CL06', 1).otherwise(0))
             .withColumn('MATTER_CATEGORY_EFFECTIVE_ON', F.col('customHistoryDateTime'))
             .withColumn('EFFECTIVE_ON_DATE', F.col('customHistoryDateTime'))
             .withColumn('CREATED', F.col('createdAt'))
             .withColumn('MODIFIED', F.col('updatedAt'))
             .withColumn('EXPIRED_ON_DATE', F.lit('9999-12-31').cast('timestamp'))
             .select(*selectCols)
            )
#join with the exising factMatCat to get the appropriate SKs for exisiting matters - also have to determine the last category stage and expire that row for the corresponding row (right?)
print(semiFinal.count())

# COMMAND ----------

# MAGIC %md Add the SK 

# COMMAND ----------

proceed, finalWithSK = getNewSKDataFrame(factMatterCatHistory, semiFinal, 'MATTER_CATEGORY_HISTORY_SK')
if not proceed:
  clear = False
  error = 'there was an issue with creating the New Sks'
  errors.append(error)
  print(error)

# COMMAND ----------

#printNullCount(finalWithSK, ['FIRM_SK', 'AOP_SK' ,'FIRM_KEY', 'MATTER_CATEGORY', 'EFFECTIVE_ON_DATE', 'POLICY_YEAR_SK', 'POLICY_VERSION_SK'])

# COMMAND ----------

#union with old table 
finalTable = factMatterCatHistory.unionByName(finalWithSK)
newRecordsCOUNT = finalWithSK.count()
newTableCOUNT = finalTable.count()
if newTableCOUNT != startingFMCHCount+newRecordsCOUNT:
  print('THERE IS AN ISSUE WITH THE UNION')
  print('Original count: {}'.format(orgCOUNT))
  print('newRecords to be added: {}'.format(newRecordsCOUNT))
else: 
  print('We can move on to saving new factMatCatHistory')
  print('Original count: {}'.format(startingFMCHCount))
  print('newRecords to be added: {}'.format(newRecordsCOUNT))
  print('New count: {}'.format(newTableCOUNT))

# COMMAND ----------

#Final Check - NUll MatterSK
if nullCheck(finalTable, finalTable.columns):
  error = 'We have NUlls in the final table columns '
  errors.append(error)
  print(error)
  clear = False
else:
  print('We are good to overwrite! -- CLEAR: {}'.format(clear))

# COMMAND ----------

#printNullCount(finalTable, ['FIRM_SK', 'AOP_SK' ,'FIRM_KEY', 'MATTER_SK','MATTER_CATEGORY', 'EFFECTIVE_ON_DATE', 'POLICY_YEAR_SK', 'POLICY_VERSION_SK'])

# COMMAND ----------

#Distinct Check 
#printCounts(finalTable, 'MATTER_SK')
#printCounts(finalTable, 'MATTER_CATEGORY_HISTORY_SK') #Do supplementals have categories?

if not checkDistinct(finalTable, 'MATTER_CATEGORY_HISTORY_SK'):
  error =  'We don\'t have distinct counts of {} '.format(primaryKey)
  errors.append(error)
  print(error)

# COMMAND ----------

jobID, runID = getNotebookInfo()
status = "PASS" if clear else "FAIL"
exit_dict = {"entity": target, "jobID": str(jobID), "runID": str(runID), "zone": "transient", "status": status, "date": DATE, "before": str(startingFMCHCount), "after": str(newTableCOUNT), "updated":0,  "created": str(newRecordsCOUNT)}



# COMMAND ----------

# add logging next gen
logDF = createLoggingEntry(exit_dict)
saveLoggingRecord(logDF)

# COMMAND ----------



# COMMAND ----------

if clear:
  print('WRITING TO TRANSIENT')
  #save dataframe with updated
  saveTransient(finalTable, target, clear, errors, exit_results)

# COMMAND ----------

