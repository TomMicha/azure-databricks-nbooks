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

#for reference 
columns = ["POLICY_VERSION_SK","FIRM SK","LOB SK"
"POLICY_COVERAGE_START",
"POLICY_COVERAGE_END",
"MATTER_CATEGORY_CODE",
"MATTERS_OPEN",
"MATTERS_TOTAL"]
#Calc Fields - Count of Claims per Cat Code 
#Gross Paid Base 
#Firm Paid 
#ALAS incurred Amount - paid + Reserve Dollars (net of coins)

# COMMAND ----------

#GREEN
##pull necessary tables from persisted delta tables
dfPolicyYear = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimPolicyYear") #base table
dfFMatterDetail = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterDetail") #use primarily for Matter_Number
dfFMatCatHistory = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterCategoryHistory") #for Category_Code (CL-01etc)
dfMatter = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimMatter") #Matter_Status_Code


# COMMAND ----------

display(dfFMatCatHistory.where(dfFMatCatHistory.MATTER_SK == 6872))

# COMMAND ----------

display(dfFMatterDetail) #we'll get Firm_SK, LOB_SK from this table - we'll join with PolicyYearSK

# COMMAND ----------

#GREEN
#getting the columns we want from the tables - note:Haven't gotten dimMatter yet - we'll do that later to build a secondary table
dfPY = dfPolicyYear.select("POLICY_YEAR_SK","POLICY_YEAR_KEY", "LOB", "POLICY_PERIOD_STARTS", "POLICY_PERIOD_END", "CYCLE_KEY")
dfMatCat = dfFMatCatHistory.select("POLICY_YEAR_SK","MATTER_SK", "MATTER_CATEGORY_CODE", "MATTER_CATEGORY", "MATTER_TYPE","CLAIM_INDICATOR", "MATTER_CATEGORY_EFFECTIVE_ON")
dfFMD = dfFMatterDetail.select("FIRM_SK", "LOB_SK", "MATTER_SK", "MATTER_OPEN_DATE","CLAIM_MADE_DATE", "MATTER_NUMBER") 
dfMatters = dfMatter.select("NAME", "MATTER_STATUS_CODE", "MATTER_SK")

# COMMAND ----------

#starting with Matters first this time around
print(dfMatters.select("MATTER_SK").distinct().count()) #28946
print(dfFMD.select("MATTER_SK").distinct().count()) #28947
print(dfMatCat.select("MATTER_SK").distinct().count()) #28941
print(dfPY.select("POLICY_YEAR_SK").distinct().count()) #11975
print(dfMatCat.select("POLICY_YEAR_SK").distinct().count()) #8236


# COMMAND ----------

#GREEN
joinedMatter = dfFMD.join(dfMatters, "MATTER_SK", "left")
joinedMatters = joinedMatter.join(dfMatCat, "MATTER_SK", "left")
display(joinedMatters)

# COMMAND ----------

display(joinedMatters.where(joinedMatters.MATTER_SK == 6872))

# COMMAND ----------

#joinedMatters.select("MATTER_SK").distinct().count() #28947

# COMMAND ----------

#GREEN
joinedPY = dfPY.join(joinedMatters, "POLICY_YEAR_SK", "left")
joinedPY = joinedPY.drop("POLICY_YEAR_KEY")
display(joinedPY)

# COMMAND ----------

#joinedPY.select("MATTER_SK").distinct().count() #should be 28942
#joinedPY.select("POLICY_YEAR_SK").distinct().count() #11975

# COMMAND ----------

#separate Claims and Circumstance - 
claims = joinedPY.where(joinedPY.CLAIM_INDICATOR == 1)
openClaims = claims.where(claims.MATTER_STATUS_CODE == 'Open')
openMatters = joinedPY.where(joinedPY.MATTER_STATUS_CODE == 'Open')

# COMMAND ----------

#claims.select("MATTER_SK").distinct().count() #17606
#openClaims.select("MATTER_SK").distinct().count() #1133
#openMatters.select("MATTER_SK").distinct().count() #1918

# COMMAND ----------

#YELLOW - im adding MatterCategory effective on to see if we can dwindle this table down to only show the most up to date mat cat code per matterSK

groupMatCat = joinedPY.select("POLICY_YEAR_SK", "MATTER_CATEGORY", "MATTER_SK", "MATTER_CATEGORY_EFFECTIVE_ON").groupBy("POLICY_YEAR_SK","MATTER_SK","MATTER_CATEGORY", "MATTER_CATEGORY_EFFECTIVE_ON").agg({'MATTER_SK':'count'})
groupMatCat = groupMatCat.withColumnRenamed('count(MATTER_SK)', 'TOTAL_MATTERS_PER_CAT')
display(groupMatCat.orderBy("POLICY_YEAR_SK", "MATTER_SK", "MATTER_CATEGORY_EFFECTIVE_ON"))

# COMMAND ----------

#display(joinedPY.where(joinedPY.MATTER_SK == 2752)) #has both CR and CL counted in table above
#display(joinedPY.where(joinedPY.MATTER_SK == 6138))
display(joinedPY.where(joinedPY.MATTER_SK == 6872))

# COMMAND ----------

#Testing 5 rows matcat is 13,3,4, 7,4 they won't be distinct yet - three different MATTER_SKs 5025,4735, 4914
display(joinedPY.where(joinedPY.POLICY_YEAR_SK == 474))

# COMMAND ----------

#GREEN
#Open Matters only
mattersOpen = openMatters.select("MATTER_SK", "POLICY_YEAR_SK", "MATTER_CATEGORY_CODE")
openMats = mattersOpen.select("POLICY_YEAR_SK", "MATTER_SK","MATTER_CATEGORY_CODE").groupBy("POLICY_YEAR_SK", "MATTER_CATEGORY_CODE").agg({'MATTER_SK':'count'})
openMats = openMats.withColumnRenamed('count(MATTER_SK)', 'OPEN_MATTERS_PER_CAT')
display(openMats.orderBy("POLICY_YEAR_SK"))

# COMMAND ----------

display(openMatters.where(openMatters.POLICY_YEAR_SK == 10181).orderBy("MATTER_SK"))

# COMMAND ----------

#Testing
#display(openMats.where(openMats.POLICY_YEAR_SK == 474)) #should have no matters since all are closed
display(openMats.where(openMats.POLICY_YEAR_SK == 11625)) #should give four rows, matcats 7,4,13,10 all open total of 5 matters 2 in matcat7 -- this was when we have MATCAT_CODE where it was the SK - this is correct now 
#new as of 423 two cats -06-03 with 3 and 2 respectively 

# COMMAND ----------

#GREEN
joinedMats = groupMatCat.join(openMats, ["POLICY_YEAR_SK","MATTER_CATEGORY_CODE"], "left")
joinedMats = joinedMats.fillna({'TOTAL_MATTERS_PER_CAT': 0})
joinedMats = joinedMats.fillna({'OPEN_MATTERS_PER_CAT': 0})
display(joinedMats)

# COMMAND ----------

mattersOver2000 = joinedPY.where(joinedPY.CYCLE_KEY > 1999)
#display(mattersOver2000.orderBy("POLICY_YEAR_SK"))
groupedMatCatOver2000 = groupMatCat.where(groupMatCat.)

# COMMAND ----------

import datetime
from pyspark.sql.functions import year, month, dayofmonth
#Get date keys for the matterEffectiveon
# we can then create the "LATEST" cat using a UDF where it takes into account the same MatterSK and if the Cats are different look at the matcateffective on - choose which ever is the latest for the YEAR 
def latestCat(matterSK):
 # month = month(categoryEffectiveDate)
  #year = year(categoryEffectiveDate)
  #can i just do a greater than or equal to? >2000 means after 20000
  catsForMatter = joinedPY.select("MATTER_SK", "MATTER_CATEGORY_CODE", "MATTER_CATEGORY", "MATTER_CATEGORY_EFFECTIVE_ON").where(joinedPY.MATTER_SK == matterSK)
  catsArray = [int(row['MATTER_CATEGORY_EFFECTIVE_ON']) for row in catsForMatter.collect()]
  catsArray.sort()
  printCatsArray
  effectiveDate = catsArray[0]
  print(effectiveDate)
  '''
 
  for i in range(1, len(catsArray)):
    date = catsArray[i]
    if year(date) == year(effectiveDate):
        if month(date) > month(effectiveDate):
          effectiveDate = date
          latestDatePerYear = date
      else: 
        effectiveDate = date
        latestDate = date 

        
       '''

# COMMAND ----------

latestCat(6827)

# COMMAND ----------

#testing for no dupe cats in joined table
#should give four rows, matcats 7,4,13,10 all open total of 5 matters 2 in matcat7
display(joinedMats.where(joinedMats.POLICY_YEAR_SK == 11625))

# COMMAND ----------

#For every Policy Year SK we want to know how many Open matters, Total, Matters, and spread them out per type 
#this is just a test
test = matterCat.groupBy("MATTER_SK", "MATTER_CATEGORY_CODE").agg({'MATTER_NUMBER': 'count'})
test = test.withColumnRenamed('count(MATTER_NUMBER)', 'TOTAL_MATTERS')
display(test)

# COMMAND ----------

#This is ugly -- are we keeping it like this or? only claims? so no 7-13?
reshapedTest = test.groupBy("MATTER_SK").pivot("MATTER_CATEGORY_CODE").max("TOTAL_MATTERS").fillna(0)
display(reshapedTest)

# COMMAND ----------

test.where(test.MATTER_SK== 1162).show()
#fine reshapedTest it is -- this is ugly
reshapedTest.where(reshapedTest.MATTER_SK == 1162).show()

# COMMAND ----------

#Just making sure it all is correct
openMatters.where(openMatters.MATTER_SK == 1162).show()

# COMMAND ----------

#we're going to only join the distinct policYearSK or else we'll get dupes of MatCat COde and mess up the table
print(joinedPY.select("POLICY_YEAR_SK").count()) #43222
print(joinedMats.select("POLICY_YEAR_SK").count()) #24882
print(joinedPY.select("POLICY_YEAR_SK").distinct().count()) #11975
print(joinedMats.select("POLICY_YEAR_SK").distinct().count()) #11975

# COMMAND ----------

#GREEN
#Adding SKs
addSKs =joinedPY.select("POLICY_YEAR_SK", "FIRM_SK", "LOB_SK", "LOB","POLICY_PERIOD_STARTS","POLICY_PERIOD_END", "CYCLE_KEY").distinct()
joinedBase = joinedMats.join(addSKs, "POLICY_YEAR_SK","left")
joinedBase = joinedBase.select("POLICY_YEAR_SK", "FIRM_SK", "LOB_SK","LOB", "POLICY_PERIOD_STARTS","POLICY_PERIOD_END","CYCLE_KEY", "MATTER_CATEGORY_CODE", "OPEN_MATTERS_PER_CAT","TOTAL_MATTERS_PER_CAT")
display(joinedBase)

# COMMAND ----------

#Making sure this PolicyYear with only Open Matters is done correctly
display(joinedBase.where(joinedBase.POLICY_YEAR_SK == 11625))

# COMMAND ----------

# MAGIC %md Adding Financials

# COMMAND ----------

#GREEN
#Adding financials 
dfFinCounts = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_counts")
dfFinfin = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_financial")
display(dfFinCounts)
display(dfFinfin)

# COMMAND ----------

# MAGIC %md do not look below - it's a mess 

# COMMAND ----------

#YELLOW this join gives nulls in year and month because dfCounts doesn't have some of the MatterSKs found in DfFinfin
#see cmd 28
dfCounts = dfFinCounts.select("DATE_KEY", "YEAR", "MONTH", "MATTER_SK")
measureDF = dfFinfin.join(dfCounts, ["DATE_KEY", "MATTER_SK"], "left")
display(measureDF)

# COMMAND ----------

#display(dfFinfin.where(dfFinfin.POLICY_YEAR_SK == 1401))
display(dfFinfin.where(dfFinfin.MATTER_SK == 6872))

# COMMAND ----------

display(joinedPY.where(joinedPY.POLICY_YEAR_SK ==1401))
#display(joinedPY.where(joinedPY.MATTER_SK == 10574))

# COMMAND ----------

#TEST
display(measureDF.where(measureDF.MATTER_SK == 11607).orderBy("DATE_KEY"))
print(measureDF.where(measureDF.MATTER_SK == 11607).count()) #58

# COMMAND ----------

#Okay I'm joining on the measureTables with PolicySK outer or left?? if I do outer -then I won't have date keys for some policy Years? but maybe we don't want because some of those PolicySKs are before that 2000s cut off 
#LEFT joining on measure tables - starting with dfFinfin
dfFinFin = dfFinfin.drop("FIRM_SK")
financials = dfFinFin.join(joinedBase, "POLICY_YEAR_SK", "left")
display(financials)

# COMMAND ----------

display(joinedPY)

# COMMAND ----------

#TEST MATTERSK = policy start and matter open dates don't match the datekey 
print(financialMatCat.where(financialMatCat.MATTER_SK == 6872).count()) #219

display(financialMatCat.select("MATTER_SK", "POLICY_YEAR_SK", "DATE_KEY", "POLICY_PERIOD_STARTS", "POLICY_PERIOD_END", "CYCLE_KEY", "MATTER_OPEN_DATE", "MATTER_NUMBER", "MATTER_CATEGORY").where(financialMatCat.MATTER_SK == 6872))

# COMMAND ----------

#saving for now 
curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
tableName = "measureMatterTypeCount"
financialMatCat.write.format("delta").save(curatedDeltaPath+tableName)


# COMMAND ----------

openClaims = financialMatCat.where((financialMatCat.CLAIM_INDICATOR == 1) & (financialMatCat.MATTER_STATUS_CODE == 'Open'))
claims = financialMatCat.where(financialMatCat.CLAIM_INDICATOR == 1)
openMatters = financialMatCat.where(financialMatCat.MATTER_STATUS_CODE == 'Open')

openClaims = openClaims.drop("CLAIM_INDICATOR", )


#openMatters.select("POLICY_YEAR_SK", "MATTER_SK","MATTER_CATEGORY_CODE").groupBy("POLICY_YEAR_SK", "MATTER_CATEGORY_CODE").agg({'MATTER_SK':'count'})
#openMats = openMats.withColumnRenamed('count(MATTER_SK)', 'OPEN_MATTERS_PER_CAT')


# COMMAND ----------

#GREEN
#since MeasurePaidBase only has Matter_SK we'll join it with our previous joinedPY Table but only selecting MATTER_SK and POLICY_YEAR_SK
mattersToJoin = joinedPY.select("MATTER_SK", "POLICY_YEAR_SK", "MATTER_CATEGORY_CODE")
print(mattersToJoin.select("MATTER_SK").count()) #43222
print(mattersToJoin.select("MATTER_SK").distinct().count()) #28942
print(mattersToJoin.select("POLICY_YEAR_SK").distinct().count()) #11975

dfMeasurePaidBase.count() #7813
dfMeasurePaidBase.select("MATTER_SK").distinct().count()#7813

# COMMAND ----------

#GREEN
#left joining on financials since they have less Matter_SK 
#financials = dfMeasurePaidBase.join(mattersToJoin, "MATTER_SK", "left")
#display(financials)
#uncomment above if the below is not what we want to do

#or maybe we don't want to do that? We lose a couple of cases - see cmd29 with policy year sk = 11625 - should have five matters but since measurePaidBase only has three we won't get stats for the other two.. 
#I'll do it below here 
financials = mattersToJoin.join(dfMeasurePaidBase, "MATTER_SK", "left")
display(financials)

# COMMAND ----------

display(financials.where(financials.POLICY_YEAR_SK == 11625)) 
#see how now with option B we get all five of the matters in this policy year - just don't get any info finances for two of them 

# COMMAND ----------

financials.select("MATTER_SK").count()#43222 with optionB
financials.select("POLICY_YEAR_SK").distinct().count() #11975

# COMMAND ----------

#Testing - could probably delete baseB table since its a dupe of the count table done above
baseB = financials.drop("MATTER_SK") #just keep this
#baseB = baseB.groupBy("POLICY_YEAR_SK", "MATTER_CATEGORY_CODE").count()
#display(baseB)

# COMMAND ----------

#GREEN
import pyspark.sql.functions as fn
finance = financials.groupBy("POLICY_YEAR_SK", "MATTER_CATEGORY_CODE").agg( fn.sum("ALAS_PAID").alias("ALAS_PAID"), fn.sum("FIRM_PAID").alias("FIRM_PAID"), fn.sum("TOTAL_GROUND_UP").alias("TOTAL_GROUND_UP"), fn.sum("TOTAL_INCURRED").alias("TOTAL_INCURRED"))

display(finance)

# COMMAND ----------

#stats first 
print(joinedBase.select("POLICY_YEAR_SK").count()) #24882
print(joinedBase.select("POLICY_YEAR_SK").distinct().count()) #11975
print(finance.select("POLICY_YEAR_SK").count())
print(finance.select("POLICY_YEAR_SK").distinct().count())
#same for these two tables, great

print(finance.where(finance.ALAS_PAID.isNull()).count()) #14943
print(finance.where(finance.TOTAL_GROUND_UP.isNull()).count()) #same as above

# COMMAND ----------

#GREEN
#join with the previous count table joinedBase - joining on both PolicyYearSK and MATTERCATCODE
final = joinedBase.join(finance, ["POLICY_YEAR_SK","MATTER_CATEGORY_CODE"], "left")
display(final)

# COMMAND ----------

display(final.where(final.POLICY_YEAR_SK == 11625)) #four rows all open matters 5 total matters GREAT

# COMMAND ----------

#GREEN
#adding the table SK 
#finance.withColumn #probably shouldn't use this
final = final.withColumn("MATTER_TYPE_COUNT_SK", fn.monotonically_increasing_id())
final = final.select("MATTER_TYPE_COUNT_SK","POLICY_YEAR_SK", "FIRM_SK", "LINE_OF_BUSINESS_SK", "CYCLE_KEY","POLICY_PERIOD_STARTS","POLICY_PERIOD_END", "MATTER_CATEGORY_CODE", "OPEN_MATTERS_PER_CAT", "TOTAL_MATTERS_PER_CAT","ALAS_PAID", "FIRM_PAID", "TOTAL_GROUND_UP", "TOTAL_INCURRED" )
display(final)

# COMMAND ----------

#Verifying 4 rows - 5 matters, all open, matcats are 7,4,13,10 - one null row for finances
display(final.where(final.POLICY_YEAR_SK == 11625))

# COMMAND ----------

# MAGIC %md Extras - clean up 

# COMMAND ----------

#some stats
#dfPY.count() #11,975
#dfPY.select("POLICY_YEAR_SK").distinct().count() #11,975 
#dfPY.select("POLICY_YEAR_KEY").distinct().count() #11,968 - looks like ther might be dupe keys

#dfMatCat.count() #39,483
#dfMatCat.select("POLICY_YEAR_SK").distinct().count() #8,236
#dfMatCat.select("MATTER_SK").distinct().count() #28,941
#dfMatCat.select("MATTER_SK").distinct().where(dfMatCat.CLAIM_INDICATOR == 1).count() #11,498 Claims only

# COMMAND ----------

#stats on table 
#joinedMatCat.where(joinedMatCat.MATTER_SK.isNull()).count() #3739 - vs 3738 from factMatterDetail join
#joinedMatCat.count() #43222
#joinedMatCat.where(joinedMatCat.MATTER_SK.isNotNull()).count() #39483 vs 28947 from factMatterDetail join

#We only want Claims to taking out all the ones where Claim indicator is not 1
joinedMatCatJustClaims = joinedMatCat.where(joinedMatCat.CLAIM_INDICATOR == 1)
#joinedMatCatJustClaims.count() #24544
#joinedMatCatJustClaims.where(joinedMatCat.MATTER_SK.isNull()).count() #zero this makes sense
#joinedMatCatJustClaims.where(joinedMatCat.MATTER_SK.isNotNull()).count() #24544

#So taking all the matters and grouping them by policy year 
groupedPYMatCat = joinedMatCatJustClaims.groupBy("POLICY_YEAR_SK").agg({'MATTER_SK': 'count'})
groupedPYMatCat = groupedPYMatCat.withColumnRenamed("count(MATTER_SK)","TOTAL_MATTERS_MatCat")
display(groupedPYMatCat)

# COMMAND ----------

#Add checks and verify table is done correctly 
failed = False

# COMMAND ----------

#save curate version of table
if (not failed):
  curatedDeltaPath = "/mnt/alasdeltalake/curated/XDW/"
  tableName = "summaryMatterTypeCount"
  final.write.format("delta").save(curatedDeltaPath+tableName)
else: 
  print("Validation Logic Failed: Please check output for issues")


# COMMAND ----------

