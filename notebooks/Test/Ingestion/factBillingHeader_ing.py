# Databricks notebook source
# MAGIC %md YOU ARE IN A TEST NOTEBOOK

# COMMAND ----------

# MAGIC %run "/Test/Utility/Python/_pyUtil"

# COMMAND ----------

jobID, runID = getNotebookInfo()

# COMMAND ----------

def displayFBH(df, matterIDSK, invoiceHeaderKey, invoiceNumber, SK = False):
  if SK: 
    display(df.where((F.col('MATTER_SK').isin(matterIDSK)) & (F.col('INVOICE_HEADER_KEY').isin(invoiceHeaderKey)) & (F.col('INVOICE_NUMBER').isin( invoiceNumber))))
  else: 
    display(df.where((F.col('MATTER_ID').isin(matterIDSK)) & (F.col('INVOICE_HEADER_KEY').isin(invoiceHeaderKey)) & (F.col('INVOICE_NUMBER').isin( invoiceNumber))))
    

# COMMAND ----------

def checkDistinct_FBH(df: DataFrame)->bool:
  if df.groupBy('MATTER_SK', 'INVOICE_HEADER_KEY', 'INVOICE_NUMBER').count().where(F.col('count')>1).count() > 0:
    print('We do not have distinct Counts for combo MATTER_SK, INVOICE_HEADER_KEY, INVOICE_NUMBER -- we should')
    return False
  else:
    print('We have distinct counts!')
    return True

# COMMAND ----------

# MAGIC %md Ingestion Notebook for FactBillingHeader - update this table

# COMMAND ----------

target = "factBillingHeader"
entity = "InvoiceHeader"
primaryKey = 'BILLING_HEADER_SK'
listOfSks = ['FIRM_SK', 'AOP_SK', 'POLICY_COVERAGE_START_DATE_SK', 'POLICY_COVERAGE_END_DATE_SK', 'POLICY_VERSION_SK', 'POLICY_YEAR_SK', 'LOB_SK', 'MATTER_SK', 'PRIMARY_CAUSE_OF_LOSS_SK', 'CENSUSUW_SK', 'CENSUS_SIZE_SK']

startingCount = finalCOUNT = newCount = realUpdatesCount = 0
errors = []
clear = True
savePath = transientPath+target

# COMMAND ----------

# Widgets save values here
dbutils.widgets.text("DATE", "", "DateForPull")
DATE = startingDate = dbutils.widgets.get("DATE") 
if DATE == "":
  dbutils.notebook.exit("Error: Require DATE param passed to notebook for daily update")
DATE = datetime.fromisoformat(DATE)
print(DATE)

# COMMAND ----------

#start and endDate initiation
todayDate = datetime.now()
startDate = DATE    #datetime.datetime(2020, 5, 12)
endDate = DATE + timedelta(days=1) #datetime.datetime(2020, 7, 8)
print(startDate)
print(endDate)

# COMMAND ----------

# MAGIC %md STARTING INGESTION PROCESSING

# COMMAND ----------

newrecords = dailyRead('updatedAt', startDate, endDate, truncatedPath+entity)
newRecCount = newrecords.count()

# COMMAND ----------

#EXIT IF NO NEW RECORDS 
if newRecCount == 0:
  exit_results = getZoneLogStats({"entity" :target, "jobID" : jobID, "runID" :runID, "status" :"SKIP", "date" : startingDate})
  dbutils.notebook.exit(exit_results)
  print(exit_results)

# COMMAND ----------

if checkIfDateUpdated(transientPath+target, DATE):
  dbutils.notebook.exit('EXIT: {} has already ran for {}'.format(target, startingDate))

# COMMAND ----------

# read in existing factBillingHeader
#version 60 is the refresh after issue found on 9/17
factBillingHeader = spark.read.format("delta").load(transientPath+target) 
startingCount = factBillingHeader.count()

# COMMAND ----------

printNullCount(factBillingHeader, [ "MATTER_SK",'ORG_SK', "ORGANIZATION"])

# COMMAND ----------

print('Starting Count: {}'.format(startingCount))
print('Ingested Records Count: {}'.format(newRecCount))
#Assure that we have distinct counts for the starting factBillingHeader
checkDistinct_FBH(factBillingHeader)

# COMMAND ----------

#bringing in factMatterDetail in order to make sure we update for the MatterSK in this table
fmd = optionOpenDF(transientPath+'factMatterDetail').select(listOfSks)

# COMMAND ----------

#Grab DSM 
flexDSM = getFlexDSM('MATTER_SK', 'MATTER_ID')

#Add MatterID to FMD  
fmd_ID = flexDSM.join(fmd, 'MATTER_SK', 'right')

#add MatterSk to newrecords 
newRec_MatSK = newrecords.join(fmd_ID, 'MATTER_ID', 'left')

# COMMAND ----------

#CHECK - Null Check for fmd_ID
if nullCheck(fmd_ID, ['MATTER_ID', 'MATTER_SK']):
  error = 'There were null matterSKs in the fmd to Matter_ID join'
  errors.append(error)
  clear = False
else: 
  print('PROCEED -- all matters accounted for')

# COMMAND ----------

#Null Check for newRec_MatSK join
if nullCheck(newRec_MatSK, ['MATTER_SK']):
  error = 'ERROR: we have null Matter SK with new rows! - check newRec_MatSK'
  print(error) #if this is true that means there's something wrong and we didn't put all newMatters in  from factMatterDetail
  errors.append(error)
  clear = False
  nullMatterSK_IH = newRec_MatSK.where(F.col('MATTER_SK').isNull())
else: 
  print('We have rows to append - no Null Matter SKs - GOOD!')

# COMMAND ----------

# MAGIC %md Grab the latest most updated row - based on InvoiceHeaderKey and InvoiceNumber

# COMMAND ----------

#grab the lateset rows for each matterSk. InvoicHeader
latestIngestedRecords = getLatestRows(newRec_MatSK, ['MATTER_SK', 'INVOICE_HEADER_KEY', 'INVOICE_NUMBER'], 'updatedAt')
print(latestIngestedRecords.count())

# COMMAND ----------

#CHECK = make sure we only have distinct matter-invoiceheader-Invoicenumber combo
if not checkDistinct_FBH(latestIngestedRecords):
  error = 'We have more than one row for matter, invoice header key, and invoice number combo - should be distinct'
  errors.append(error)
  print(error)
  clear = False
else: 
  print('We have distinct Matter-InvoiceHeaderKey-InvoiceNumber relation')

# COMMAND ----------

#add dimOrgSK and full name
flexORG_DSM = getFlexDSM('ORG_SK', 'ORG_ID')
dimOrg = optionOpenDF(transientPath+'dimOrganization').select('ORG_SK',F.col('NAME').alias('ORGANIZATION')).join(flexORG_DSM, 'ORG_SK','left')

latestIngestedRecords_org = latestIngestedRecords.join(dimOrg, 'ORG_ID', 'left')

# COMMAND ----------

#so now we have: 
#NEW: only rows/invoices to append both for new MatterSKs and for existing Matters in factBillingHeader
#UPDATES: if there is a change in the real Update column list 

distinctCombo = ['MATTER_SK', 'INVOICE_HEADER_KEY', 'INVOICE_NUMBER']
realUpdatesCond = ['MATTER_SK','ORG_SK', 'INVOICE_HEADER_KEY', 'INVOICE_NUMBER', 'INVOICE_TYPE_NAME', 'CUSTOM_PAYMENT_NUMBER', 'INVOICE_DISPOSITION_CODE', 'ORGANIZATION']

#Calculations
#totalNetAmount = totalAmount
#FeeTotal_net_amount = totalGrossFeeAmount + totalFeeDiscount + totalFeeAdjustment
#exp_total_net_amount = totalGrossExpense + totalAdjustmentExpense + totalExpenseDiscount

# COMMAND ----------

# MAGIC %md Determine updates and new Rows 
# MAGIC > will be discarding updates if they don't change anything in our env

# COMMAND ----------

#Decide between NEW and real UPDATES
haveUpdates = haveNew = False

#NEW
new = latestIngestedRecords_org.join(factBillingHeader.select(*distinctCombo), distinctCombo, 'left_anti')
newCount = new.count()
#UPDATES
possibleUpdates = latestIngestedRecords_org.join(factBillingHeader.select(primaryKey, *distinctCombo), distinctCombo, 'inner')
realUpdates = possibleUpdates.join(factBillingHeader, realUpdatesCond, 'left_anti')
realUpdatesCount = realUpdates.count()

if realUpdatesCount >0:
  print('WE HAVE REAL UPDATES')
  print('UPDATES: {}'.format(realUpdatesCount))
  haveUpdates = True
else: 
  print('NO REAL UPDATES')
if newCount >0: 
  print('WE HAVE NEW RECORDS')
  print('NEW COUNT:{}'.format(newCount))
  haveNew = True
else:
  print('NO NEW RECORDS')

# COMMAND ----------

#display(new) #REJECTED STATUS -- matterSK = 25946, matterID = 25964, invoiceHeader = 204709 invoiceNumber = 22807 - trace this through truncated

# COMMAND ----------

if not haveUpdates and not haveNew:
  print('We\'re going to exit the notebook - No new records or updates required today- no further action is required')
  exit_results = getZoneLogStats({"entity" :target, "jobID" : jobID, "runID" :runID, "status" :"SKIP", "date" : startingDate})
  dbutils.notebook.exit(exit_results)

# COMMAND ----------

# what is this? factBillingHeaders real row is the matter, invoice header key and invoice number, this allows me to display that without all the ".where" for each of the columns above
#displayFBH(factBillingHeader, matterSks, invoiceHeaders, invoiceNumber, SK=True)

# COMMAND ----------

#clean up updates 
if haveUpdates: 
  cleanedUpdates = (realUpdates
                    .withColumnRenamed('updatedAt', 'MODIFIED')
                    .withColumn('EFFECTIVE_ON_DATE', F.col('MODIFIED'))
                    .withColumn('EXPIRED_ON_DATE', F.lit('9999-12-31').cast('timestamp'))
                    .join(factBillingHeader.select('MATTER_SK', 'MATTER_KEY').distinct(), 
                   'MATTER_SK', 'left')
                   ).select(*factBillingHeader.columns)
  print(cleanedUpdates.count())

# COMMAND ----------

if haveUpdates: 
  #createSubset and union with the starting table subset
  subsetToAddUpdates = factBillingHeader.join(cleanedUpdates, distinctCombo, 'left_anti')
  subsetCount = subsetToAddUpdates.count()
  if subsetCount != (startingCount - realUpdatesCount):
    error = 'Our updates were not subtracted from our starting table correctly'
    print(error)
    errors.append(error)
  else: 
    print('unioning subsetwithout updates with Updates')
    updatedTable = subsetToAddUpdates.unionByName(cleanedUpdates)
    updatedFactTableCount = updatedTable.count()

# COMMAND ----------

#CHECK for updates - make sure we have the correct count before and after update has been unioned
if haveUpdates:
  if not assertCorrectSubset(startingCount, subsetCount, realUpdatesCount):
    error = 'Our dim with new Updates does not match our counts'
    errors.append(error)
    print(error)
    clear = False
  else: 
    print('Updated Fact Table complete - Proceed')
    print(updatedFactTableCount)

# COMMAND ----------

# MAGIC %md Add BILLING_HEADER_SK to new rows

# COMMAND ----------

if haveNew: 
  proceed, recordsToAppend = getNewSKDataFrame(factBillingHeader, new, primaryKey)
  if not proceed:
    error = 'There was an error creating {} for {}'.format(primaryKey, target)
    errors.append(error)
    print(error)
  else:
    print('We have fresh NEW Records with Sks!')

# COMMAND ----------

# MAGIC %md Exisintg FactBillingHeader Has MatterKey - adding 

# COMMAND ----------

if haveNew: 
  semiFinal = (recordsToAppend
             .withColumn('EFFECTIVE_ON_DATE', F.col('CREATED'))
             .withColumn('MODIFIED', F.col('updatedAt'))
             .withColumn('EXPIRED_ON_DATE', F.lit('9999-12-31').cast('timestamp'))
             .join(factBillingHeader.select('MATTER_SK', 'MATTER_KEY').distinct(), 
                   'MATTER_SK', 'left')
            )

  finalRecordsToAppend = semiFinal.select(*factBillingHeader.columns)

# COMMAND ----------

# MAGIC %md [Section 3]
# MAGIC > Save the final DataFrame after some Checks

# COMMAND ----------

def runNew(brandNewRecords, startingDF):
  print("Only have new records to append")
  if not checkEqualColumnSize(len(brandNewRecords.columns), len(startingDF.columns)):
    error = 'We don\'t have have equal column sizes'
    errors.append(error)
    print('returning BrandNewRecords')
    return brandNewRecords, False
  else: 
    print('UNIONING brandNewRecords with starting DF')
    unioned = startingDF.unionByName(brandNewRecords)
    return unioned, True

# COMMAND ----------

def runBoth(updatedTable, brandNewRecords):
  print('Have both New and Updated')
  if not checkEqualColumnSize(len(updatedTable.columns), len(brandNewRecords.columns)):
    error = 'We don\'t have have equal column sizes'
    errors.append(error)
    print('returning BrandNewRecords')
    return brandNewRecords, False
  else: 
    print('UNIONING brandNewRecords with updated Table')
    unioned = updatedTable.unionByName(brandNewRecords)
    return unioned, True

# COMMAND ----------

justUpdates = justNew = haveBoth = False

# COMMAND ----------

if haveUpdates and not haveNew: 
  justUpdates = True
  #We already joined the updates with the subset of the non updated records - so just saving into new df to keep naming consistent
  if clear:
    # here we take out any non approved rows - see if we want to push this to persisted
    finalFact = updatedTable.orderBy(F.desc('MODIFIED'))
    
elif not haveUpdates and haveNew:
  #Only have new records - need to union finalRecordsToAppend with starting Dim Table
  finalFact, proceed = runNew(finalRecordsToAppend, factBillingHeader)
  if proceed: 
    justNew = True
    
elif haveUpdates and haveNew:
  #union the updatedTable and finalRecordsToAppend
  finalFact, proceed = runBoth(updatedTable, finalRecordsToAppend)
  if proceed: 
    haveBoth = True
      
else:
  error = 'something went awry with trying to decide updates, new, or both - see section 3'
  errors.append(error)
  print(error)
  clear = False

# COMMAND ----------

#CHECK - distinct counts and NUll MATTERSK  
if not checkDistinct_FBH(finalFact):
  clear = False
  error = 'We do not have distinct counts - we should - getting latest rows of full unioned dataframe'
  errors.append(error)
if nullCheck(finalFact, ['MATTER_SK']): 
  error = 'We have null Matters SKs -- cannot happen'
  print(error)
  errors.append(error)
  clear = False

# COMMAND ----------

if clear:
  finalCOUNT = finalFact.count()
  if justUpdates: 
    # we already joined with dim so if just Updates - just save and exit
    print('WE are clear to overwrite - CLEAR for UPDATES ONLY ')
    
  if justNew: 
    print('WE ARE GOOD TO OVERWRITE WITH NEW RECORDS')
    
  if haveBoth:
    if finalCOUNT != (startingCount+newCount): 
      error = 'our final count NEW+STARTING do not match'
      errors.append(error)
      print(error)
      clear = False
    else: 
      print('WE ARE GOOD TO OVERWRITE')

# COMMAND ----------

#printNullCount(finalFact, ['BILLING_HEADER_SK', 'FIRM_SK', 'POLICY_YEAR_SK', 'POLICY_VERSION_SK', 'MATTER_SK', 'LOB_SK', 'CENSUSUW_SK', 'CENSUS_SIZE_SK', 'BILLING_INVOICE_DATE_SK','BILLING_RECEIVED_DATE_SK', 'BILLING_APPROVAL_DATE_SK', 'INVOICE_HEADER_KEY'])

# COMMAND ----------

status = "PASS" if clear else "FAIL"
exit_dict  = {"entity": target,"jobID": str(jobID), "runID": str(runID), "zone": "transient", "status" : status,  "date" : startingDate, "created" : str(newCount), "before" : str(startingCount), "updated" : str(realUpdatesCount), "after" : str(finalCOUNT)}
logDF = createLoggingEntry(exit_dict)
saveLoggingRecord(logDF)

# COMMAND ----------

def getExitResults(passFlag:bool):
  status = "PASS" if passFlag else "FAIL"
  exit_dict = {"entity": target, "jobID": jobID, "runID": runID, "status": status, "date": startingDate, "before": startingCount, "after": finalCOUNT, "updated": realUpdatesCount, "created": newCount}
  return getZoneLogStats(exit_dict)

# COMMAND ----------

saveTransient(finalFact, target, clear, errors, getExitResults(clear))

# COMMAND ----------

# MAGIC %md END