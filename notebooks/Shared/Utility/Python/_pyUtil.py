# Databricks notebook source
# imports
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql import functions as F
#import koalas as ks
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.types import *
from typing import *
import json
#allows me to print out python method definitions since it doesn't do it like scala imports do
import inspect
from delta.tables import *

# COMMAND ----------

#commonly used file paths //add final
rawPath = "/mnt/alasdeltalake/raw/Passport/API_json/"
fullPath = "/mnt/alasdeltalake/raw/Passport/Full/"
truncatedPath = "/mnt/alasdeltalake/raw/Passport/Truncated/"
transientPath = "/mnt/alasdeltalake/transient/XDW/"
persistedPath = "/mnt/alasdeltalake/persisted/XDW/"
curatedPath = "/mnt/alasdeltalake/curated/XDW/"
refPath = "/mnt/passport-tables/REF/"
passLogsPath = '/mnt/alasdeltalake/raw/logs/Passport/'
origamiRawPath = '/mnt/alasdeltalake/raw/Origami/rawCSV/'
origamiDeltaPath = '/mnt/alasdeltalake/raw/Origami/delta/'
origamiLogsPath = '/mnt/alasdeltalake/raw/logs/Origami/'
leopardProcPath = '/mnt/alasdeltalake/processed-data/Leopard/'
leopardDeltaPath = '/mnt/alasdeltalake/processed-data/Leopard_delta/'
leopardLogsPath = '/mnt/alasdeltalake/raw/logs/Leopard/'
ceProcPath = '/mnt/alasdeltalake/raw/CE/'
ceDeltaPath = '/mnt/alasdeltalake/raw/CE/'
ceLogsPath = '/mnt/alasdeltalake/raw/logs/CE/'
print('rawPath: {}'.format(rawPath))
print('fullPath: {}'.format(fullPath))
print('truncatedPath: {}'.format(truncatedPath))
print('transientPath: {}'.format(transientPath))
print('persistedPath: {}'.format(persistedPath))
print('curatedPath: {}'.format(curatedPath))
print('refPath: {}'.format(refPath))

print('passLogPath: {}'.format(passLogsPath))
print('Origami Raw Path: {}'.format(origamiRawPath))
print('Origami Log Path: {}'.format(origamiLogsPath))
print('Leopard Proc Path: {}'.format(leopardProcPath))
print('Leopard Delta Path: {}'.format(leopardDeltaPath))
print('CE Proc Path: {}'.format(ceProcPath))
print('CE Delta Path: {}'.format(ceDeltaPath))
print('CE Logs Path: {}'.format(ceLogsPath))

# COMMAND ----------

# to enable mdm functions - > %run "/Shared/Utility/Python/_mdmUtil"

# COMMAND ----------

#import DSM 
print('Imported DSM from {}'.format(transientPath+'DataSourceMapping'))
DSM = (spark.read.format('delta').load(transientPath+"DataSourceMapping"))

#Mapping File for Processing
mappingFilePath = '/mnt/passport-tables/REF/ingestionRefFiles/PassportTypeMapping'
print('Mapping file: {}'.format(mappingFilePath))

# COMMAND ----------

# MAGIC %md Auxilirary Functions

# COMMAND ----------

#Changes Data type given a dataframe column name and the cast type
def changeDataType(df: DataFrame, columnName: str, castType: str)-> DataFrame:
  return df.withColumn(columnName, F.col(columnName).cast(castType))

#Gets the dataframe for the subtraction of the leftDF - rightDF 
def getDifferences(leftDF: DataFrame, rightDF: DataFrame, joinColumns: list)-> DataFrame:
  return leftDF.join(rightDF, joinColumns, 'left_anti')

#Get the real updates when the columns you have has a data column that will need to be casted - anti join 
def getRealUpdatesForDate(possibleUpdates: DataFrame, baseTable: DataFrame, joinColumns: list, dateColumn: str, key: str, castType = 'date')-> DataFrame:
  changedBase = changeDataType(baseTable, dateColumn, castType)
  changedDF = changeDataType(possibleUpdates, dateColumn, castType)
  realUpdates = getDifferences(changedDF, changedBase, joinColumns)
  return possibleUpdates.join(realUpdates.select(key).distinct(), key, 'right')

# COMMAND ----------

# replace checkIfUpdated below :: Retrieve date of last table 'update' run to see if persisted should run (should use mainly in persisted zone or where applicable)
def dailyZoneUpdateCheck(tablePath: str) -> bool:
  deltaTable = DeltaTable.forPath(spark, "/mnt/alasdeltalake/transient/XDW/dimOrganization")
  hist = deltaTable.history(1)
  lastOperation = hist.select("timestamp").collect()[0][0].date()
  currentDate = datetime.now().date()
  if currentDate == lastOperation:
    return True
  else:
    return False

# COMMAND ----------

#Print the counts and distinct counts of a dataframe
def printCounts(df: DataFrame, entityKey: str):
  print('Total Count: {}'.format(df.count()))
  print('Distinct counts of {} : {}'.format(entityKey, df.select(entityKey).distinct().count()))

# COMMAND ----------

#Methods to do -1 checks - prints them out only
def printNoneValCount(df: DataFrame, columns: List[str]):
  print('Number of -1:')
  for x in columns:
    print('-1 Value for {}: {}'.format(x, df.where(F.col(x) == -1).count()))

# COMMAND ----------

#Methods to do null checks - prints them out only
def printNullCount(df: DataFrame, columns: List[str]):
  print('Number of Nulls:')
  for x in columns:
    print('Nulls for {}: {}'.format(x, df.where(F.col(x).isNull()).count()))

# COMMAND ----------

#null check - will return true as soon as any of the columns has nulls 
def nullCheck(df: DataFrame, columns: List[str]) -> bool:
  failed = False
  for c in columns: 
    nulls = df.where(F.col(c).isNull()).count()
    if nulls > 0: 
      print('{} failed null check'.format(c))
      failed = True
  print('Did We Fail Null Check? {}'.format(failed))
  return failed

# COMMAND ----------

#Grab DSM for specific xdwKey and sourceID
def getDSMMapping(entityID: str, xdwSK: str) -> DataFrame:
  flexDSM = (DSM
             .where(F.col('DATA_SOURCE_KEY') == entityID)
             .where(F.col('XDW_KEY') == xdwSK)
             .withColumnRenamed('XDW_VALUE', xdwSK)
             .withColumnRenamed('DATA_SOURCE_VALUE', entityID)
             .select(entityID, xdwSK)
            )
  return flexDSM

# COMMAND ----------

#Grab DSM for specific xdwKey and sourceID
def getFlexDSM(xdw_key: str, p_id: str) -> DataFrame:
  flexDSM = (DSM
             .where(F.col('DATA_SOURCE_KEY') == p_id)
             .withColumnRenamed('XDW_VALUE', xdw_key)
             .withColumnRenamed('DATA_SOURCE_VALUE', p_id)
             .select(xdw_key, p_id)
            )
  return flexDSM

# COMMAND ----------

#Grab DSM for specific xdwKey and sourceID - boolean flag that tells it whether the key to filter by is XDW or from source
def getFlexDSM_choice(xdw_key: str, p_id: str, source=False) -> DataFrame:
  if source: 
    flexDSM = (DSM
               .where(F.col('DATA_SOURCE_KEY') == p_id)
               .withColumnRenamed('XDW_VALUE', xdw_key)
               .withColumnRenamed('DATA_SOURCE_VALUE', p_id)
               .select(xdw_key, p_id)
              )
  
  else:
    flexDSM = (DSM
               .where(F.col('XDW_KEY') == xdw_key)
               .withColumnRenamed('XDW_VALUE', xdw_key)
               .withColumnRenamed('DATA_SOURCE_VALUE', p_id)
               .select(xdw_key, p_id)
              )
  return flexDSM

# COMMAND ----------

#Filter the mapping file to give you the correct columns for the Entity 
def getMappingFileForEntity(entity): 
  df = spark.read.format('parquet').load(mappingFilePath).where(F.col('P_ENTITY') == entity)
  print('Number of Columns for {}: {}'.format(entity, df.count()))
  return df

# COMMAND ----------

#Get the values in each row for a given column
def getRowValuesForColumn(columnName, df):
  print('Returning all values in column:{} for given DataFrame'.format(columnName))
  return [row[columnName] for row in df.collect()]

# COMMAND ----------

#General reaction of a dictionary mapping
def getColMappingDict(keyColumn, valColumn, df):
  keyColumns = getRowValuesForColumn(keyColumn, df)
  valColumns = getRowValuesForColumn(valColumn, df)
  dictMapping = dict(zip(keyColumns, valColumns))
  return dictMapping

# COMMAND ----------

#Specific to passport mapping - for typing
def getTypeMappingDict_Passport(df:DataFrame):
  pFullColumns = [row.P_COLUMN_NAME for row in df.collect()]
  pFullTypes = [row.P_COLUMN_TYPE for row in df.collect()]
  p_typeMapping = dict(zip(pFullColumns, pFullTypes))
  return p_typeMapping

# COMMAND ----------

#Specific for passports mapping - get the truncated dataframe 
def getTruncatedMapping_Passport(df):
  truncated = df.select('P_ENTITY', 'P_COLUMN_NAME', 'P_COLUMN_TYPE', "FOR_TRUNCATED", F.explode(df.XDW_DESTINATION).alias("XDW_DESTINATIONS")).select('P_ENTITY', 'P_COLUMN_NAME', 'P_COLUMN_TYPE', "FOR_TRUNCATED","XDW_DESTINATIONS.*")
  
  return truncated.select('P_ENTITY', 'P_COLUMN_NAME', 'P_COLUMN_TYPE', 'FOR_TRUNCATED',  'XTABLE_NAME', 'XCOLUMN_NAME')

# COMMAND ----------

#exit notebook if we don't have any records 
def exitIfNoRecords(recs_df: DataFrame, rec_id: str, target: str, entity: str) -> None:
  rowsCount = recs_df.where(F.col(rec_id).isNotNull()).count()
  if rowsCount == 0:
    dbutils.notebook.exit('Status: SKIP - no new {} records for Table {} Entity {} found to be ingested'.format(rec_id, target, entity))
  else:
    #log stdout?
    print('{} count: {} OK! Proceed with updates'.format(rec_id, rowsCount))

# COMMAND ----------

def getZoneStats(results_dict: dict)-> str:
  # extract from map given the correct keys
  returnString = ""
  for k in results_dict.keys():
    string = '{} : {} '.format(k,results_dict.get(k))
    returnString+=string
  return returnString

# COMMAND ----------

def getNotebookInfo():
  tags = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  if 'runId' in tags.get('tags').keys():
    runID = str(tags["tags"]["runId"])
  if 'jobId' in tags.get('tags').keys():
    jobID = str(tags["tags"]["jobId"])
  else:
    runID = jobID = "0000"
  return jobID, runID

# COMMAND ----------

print('Print the counts and distinct counts of a dataframe')
print(inspect.getsourcelines(printCounts)[0][0])
print()
print('Method to do null checks - prints them out only')
print(inspect.getsourcelines(printNullCount)[0][0])
print()
print('Null check - will return true as soon as any of the columns has nulls ')
print(inspect.getsourcelines(nullCheck)[0][0])
print()
print('Grab DSM for specific sourceID')
print(inspect.getsourcelines(getFlexDSM)[0][0])
print()
print('Grab DSM for either xdw or sourceID - boolFlag =False for sourceID')
print(inspect.getsourcelines(getFlexDSM_choice)[0][0])
print()
print('Exit notebook if no records')
print(inspect.getsourcelines(exitIfNoRecords)[0][0])
print()
print('Prints results from the desired Zone')
print(inspect.getsourcelines(getZoneStats)[0][0])
print()
print('Prints the count of -1 values in a given list of columns')
print(inspect.getsourcelines(printNoneValCount)[0][0])
print()
print('Gets Mapping file for specific Entity given')
print(inspect.getsourcelines(getMappingFileForEntity)[0][0])
print('Changes Data type given a dataframe column name and the cast type')
print(inspect.getsourcelines(changeDataType)[0][0])
print()

print('Returns the dataframe for the subtraction of the leftDF - rightDF')
print(inspect.getsourcelines(getDifferences)[0][0])
print()

print('Get the real updates when the columns you have a column that will need to be casted - anti join ')
print(inspect.getsourcelines(getRealUpdatesForDate)[0][0])
print()

# COMMAND ----------

# MAGIC %md Saving Path functions

# COMMAND ----------

#function to check current day's update for transient/persisted/curated areas if Date has already been added, override available
# @param: targetPath path to table
# @param: runDate - date to check if data updated
# @param: force : default(false) else can force to rerun for day
def checkIfDateUpdated(targetPath: str, runDate: str, force: bool = False) ->bool:
  if force: return False
  lastDateDF = (optionOpenDF(targetPath)
                .select("MODIFIED")
                .withColumn("MODIFIEDX", F.to_date(F.col("MODIFIED")))
                .drop("MODIFIED")
                .filter(F.col("MODIFIEDX") == runDate)
               )

  if len(lastDateDF.head(1)) == 0:
    return False
  
  lastUpdate = lastDateDF.first()[0]
  #lastUpdate = LocalDate.parse(lastUpdate.toString)
  print('extracted date {}'.format(lastUpdate))
  if lastUpdate == runDate:
    print('This {} date has already been last updated on {}'.format(runDate, lastUpdate))
    return True
  else: return False 

# COMMAND ----------

#Function to check current day's update for transient/persisted/curated areas if Date has already been added, override available
#Return False if the path table has already been updated on given date - able to override
#@param: targetPath path to table
#@param: runDate - date to check if data updated
#@param: forceRun - default:false change if force to rerun current day

def checkIfDateUpdated(targetPath: str, runDate: datetime)-> bool:
  #get the most updated row for table
  lastUpdated = optionOpenDF(targetPath).select('MODIFIED').orderBy(F.desc('MODIFIED')).first()[0]
  lastUpdatedDate = lastUpdated.date()
  #compare the dates
  if lastUpdatedDate == runDate:
    print('This {} date has already been run. Last Updated on {}'.format(runDate, lastUpdated))
    return True
  else:
    return False

# COMMAND ----------

#general save method - see chat with Tom
''' Function to write dataframes as different file path
* @param: df - DataFrame
* @param: path = path to save given df as file with default configuration applied or modified
* @param: fileType = default:(delta) - cal also be parquet, csv, json etc.
* @param: modeType = default:(overwrite) - can also be append
'''
def optionSaveDataFrame(df:DataFrame, path: str, fileType = "delta", modeType ="overwrite")->DataFrame:
  df.write.mode(modeType).format(fileType).save(path)

# COMMAND ----------

#same method but add a commit message - userMetadata - second index writes the message
def optionSaveDataFrame_commitMessage(path:str, commitMsg: str, fileType = "delta") -> DataFrame:
   df = spark.read.option('userMetadata', commitMsg).format(fileType).load(path)                             

# COMMAND ----------

#open dataframe can modify delta and give an option parameter 
def optionOpenDF(path:str, fileType = "delta", option = ["inferSchema", "true"]) ->DataFrame:
  df = spark.read.option(option[0], option[1]).format(fileType).load(path)
  return df

# COMMAND ----------

#read for entity making sure we have distinct counts
def dailyRead(filterCol: str, startDate: datetime, endDate: datetime, path:str) -> DataFrame:
  df = optionOpenDF(path,'parquet')
  filteredDF = df.filter(F.col(filterCol).between(startDate, endDate)).distinct()
  return filteredDF

# COMMAND ----------

#helper function to take df, fileteron a date and return the distinct df
def helperFilterDateDF(df: DataFrame, targetCol: str, startDate: datetime, endDate:datetime)-> DataFrame: 
  returnDF = df.filter(F.col(targetCol).between(startDate, endDate)).distinct()
  return returnDF

# COMMAND ----------

#Return dataframe for given raw path and return the createdAt updatedAt records for given date 
def dailyCoreEntityRead(targetPath: str, pullDate: str, targetCols= ['CREATED', 'updatedAt'])-> DataFrame:
  #dateTemp = LocalDate.parse(pullDate)
  startDate = datetime.fromisoformat(pullDate)
  endDate = startDate+ timedelta(days=1)
  #can refactor to get other datatype besides parquet 
  df = optionOpenDF(targetPath, 'parquet')
  createdDF = helperFilterDateDF(df, targetCols[0], startDate ,endDate)
  updatedDF = helperFilterDateDF(df, targetCols[1], startDate, endDate)
  #Merge the dataframes so we get the full records that were updated AND created records for a give date
  returnDF = createdDF.unionByName(updatedDF).distinct()
  return returnDF

# COMMAND ----------

def rollBackOneVersion(tableName: str, path:str, fileType = 'delta') -> DataFrame:
  query = 'SELECT max(version) FROM (DESCRIBE HISTORY delta. `/mnt/alasdeltalake/transient/XDW/{}`)'.format(tableName)
  latest_version = spark.sql(query).collect()
  rollBackOne = (latest_version[0][0]) - 1
  
  df =  spark.read.option('versionAsOf', rollBackOne).format(fileType).load(path)
  return df

# COMMAND ----------

'''/* function to drop all records past a certain point and resave - to run and drop duplicates from table zones
* @param filePath - full path to file/table
* @param fileType - file type - parquet, delta, csv
* @param targetCol - String for target col to pull date field
* @param afterDate - datetime for date in time to remove all date on /after this date
*/
'''
def dropAfterDate(filePath: str, fileType: str, targetCol: str, afterDate: datetime):
  openDF = (optionOpenDF(filePath, fileType)
            .filter(col(targetCol) < afterDate)
            .distinct()
           )
   #save over file dropping
  optionSaveDataFrame(openDF, fileType)

# COMMAND ----------

#save paths Transient with notebook message 
def saveTransient(finalDF: DataFrame, tableName: str, clear: bool, errors: list, returnResults: str):
  if clear: 
    print('Saving Transient Table: {} !'.format(tableName))
    savePath = transientPath+tableName
    print(savePath)
    optionSaveDataFrame(finalDF, savePath)
    dbutils.notebook.exit('PASS, {}'.format(returnResults))
  else:
    print('Not saving.. check table: {} see errors {}'.format(table, errors))
    dbutils.notebook.exit('FAILED, {} -- See errors: {}'.format(errors, returnResults))

# COMMAND ----------

def savePersisted(finalDF: DataFrame, tableName: str, clear: bool, dfCount: int):
  if clear: 
    print('Saving Persisted Table: {} !'.format(tableName))
    finalDF.write.mode('overwrite').format('delta').save(persistedPath+tableName)
    dbutils.notebook.exit('Success! {} was updated successfully with {} as the final count'.format(tableName, dfCount))
  else:
    dbutils.notebook.exit('FAILED! {} was not updated. See errors'.format(tableName))

# COMMAND ----------

def saveCurated(finalDF: DataFrame, tableName: str, clear: bool, dfCount: int):
  if clear: 
    print('Saving Curated Table: {} !'.format(tableName))
    finalDF.write.mode('overwrite').format('delta').save(curatedPath+tableName)
    dbutils.notebook.exit('Success! {} was updated successfully with {} as the final count'.format(tableName, dfCount))
  else:
    dbutils.notebook.exit('FAILED! {} was not updated. See errors'.format(tableName))

# COMMAND ----------

print('Save to Transient Zone Method, given a boolean flag')
print(inspect.getsourcelines(saveTransient)[0][0])
print()
print('Save to Persisted Zone Method, given a boolean flag')
print(inspect.getsourcelines(savePersisted)[0][0])
print()
print('Save to Curated Zone Method, given a boolean flag')
print(inspect.getsourcelines(saveCurated)[0][0])
print()
print('Read for distinct for specified path on a start or endDate')
print(inspect.getsourcelines(dailyRead)[0][0])
print()
print('Open dataframe for path default delta')
print(inspect.getsourcelines(optionOpenDF)[0][0])
print()
print('Function to write dataframes as different file path')
print(inspect.getsourcelines(optionSaveDataFrame)[0][0])
print()
print('Function to write dataframes as different file path with commit message')
print(inspect.getsourcelines(optionSaveDataFrame_commitMessage)[0][0])
print()
print('RollBack a delta table one version')
print(inspect.getsourcelines(rollBackOneVersion)[0][0])
print()
print('Function that returns True if the date given has already been run')
print(inspect.getsourcelines(checkIfDateUpdated)[0][0])
print()
print('Helps to read a dataframe by a given filtered date')
print(inspect.getsourcelines(helperFilterDateDF)[0][0])
print()#helperFilterDateDF #dailyCoreEntityRead
print('Returns df with updatedAt/createdAt records for day of given raw parquet path')
print(inspect.getsourcelines(dailyCoreEntityRead)[0][0])
print()#helperFilterDateDF #dailyCoreEntityRead

# COMMAND ----------

# MAGIC %md Functions for use for transformations in "_ing" Notebooks

# COMMAND ----------

#Determines the last SK and and if we don't have that SK in the old table - will return True and the SK
def getLatestSK(oldTable: DataFrame, tableSKName: str):
  clear = True
  lastSK = oldTable.select(tableSKName).orderBy(F.desc(tableSKName)).first()[0] #is this the highest _SK
  lastSK+=1
  print(lastSK)
  #test to determine we have the right SK
  if oldTable.where(F.col(tableSKName) == lastSK).count() != 0:
    print('CREATION OF NEW '+tableSKName+' FAILED')
    clear = False
  else:
    print('new records starting at {}'.format(lastSK))
    return clear, lastSK

# COMMAND ----------

#Adding NewSK to Table
def addNewSK(newTable: DataFrame, tableSKName: str, startingSK: int)-> DataFrame:
  from pyspark.sql import Row
  from pyspark.sql.types import StructType, StructField, LongType
  
  new_schema = StructType(newTable.schema.fields[:] + [StructField("index", LongType(), False)])
  zipped_rdd = newTable.rdd.zipWithIndex()
  
  skDF = (zipped_rdd.map(lambda ri: Row(*list(ri[0]) + [ri[1] + startingSK])).toDF(new_schema))
  #rename index to SK_NAME
  brandNewRecords = skDF.withColumnRenamed("index", tableSKName)

  return brandNewRecords

# COMMAND ----------

def getNewSKDataFrame(oldTable: DataFrame, newTable: DataFrame, tableSKName: str):
  proceed, lastSK = getLatestSK(oldTable, tableSKName)
  if proceed:
    print('Proceeding to add new SK\'s to table {}'.format(tableSKName))
    brandNewSKDF = addNewSK(newTable, tableSKName, lastSK)
    return proceed, brandNewSKDF
  else:
    print('We were not able to create the SKs')
    return proceed, oldTable

# COMMAND ----------

def getLatestRows(df: DataFrame, partitionColumns: list, orderByColumn: str) -> DataFrame:
  w = Window.partitionBy(partitionColumns).orderBy(F.desc(orderByColumn))
  latestRows = df.withColumn('LATEST', F.dense_rank().over(w))

  latest = latestRows.filter(latestRows.LATEST ==1).drop(latestRows.LATEST)
  return latest

# COMMAND ----------

#Creation of Updated Dataframe::
#essentially an addition of a subtracted part == updated Dataframe
def unionUpdates(startingDF: DataFrame, updatesToInsert: DataFrame, joinKey: str) -> DataFrame:
  #make the dataframes have the same number of columns
  updatesToInsert_selected = updatesToInsert.select(*startingDF.columns)
  #take out the chunk of rows that we'll be replacing from the starting datagrame
  dfWithoutUpdates = startingDF.join(updatesToInsert, joinKey, 'left_anti')
  #union the dataframe above with updates to create the updated Dataframe 
  unionedUpdate = dfWithoutUpdates.unionByName(updatesToInsert)
  
  return unionedUpdate

# COMMAND ----------

print('Get Latest Rows of dataframe based on column list')
print(inspect.getsourcelines(getLatestRows)[0][0])
print()
print('Add updates to the starting Dataframe')
print(inspect.getsourcelines(unionUpdates)[0][0])
print()
print('ADD new PrimaryKeys to Table Starting at the end of the last PK')
print(inspect.getsourcelines(getNewSKDataFrame)[0][0])
print()
print('Determines the last SK - if we don\'t have that SK in the old table - will return True and the SK')
print(inspect.getsourcelines(getLatestSK)[0][0])

# COMMAND ----------

# MAGIC %md Test Functions

# COMMAND ----------

#Test to make sure we have distinct columns based on a given column
def checkDistinct(df, distinctColumn):
  if df.select(distinctColumn).count() != df.select(distinctColumn).distinct().count():
    print("We don't have distinct: ", distinctColumn)
    return False
  print("WE HAVE DISTINCT ", distinctColumn)
  return True

# COMMAND ----------

#test to make sure we did updates subset correctly 
def assertCorrectSubset(startingDFCount: int, updatesCount: int, subsetCount: int) -> bool:
  if (startingDFCount - updatesCount) != subsetCount:
    print('There was an error with taking the updates out')
    return False
  else: 
    print('PROCEED - Moving on to union and overwrite transient table')
  return True

# COMMAND ----------

#TEST to decide if we have same length of columns
def checkEqualColumnSize(lengthA: int, lengthB: int) -> bool:
  canProceed = True
  if lengthA != lengthB:
    canProceed = False
    print('Our column size does not match - cannot union')
  else: 
    print('PROCEED - columns are same size')
  return canProceed

# COMMAND ----------

print('For Updates: Assert the subset equals startDF - updates')
print(inspect.getsourcelines(assertCorrectSubset)[0][0])
print()
print('TEST - make sure columns are of equal length')
print(inspect.getsourcelines(checkEqualColumnSize)[0][0])
print()
print('TEST - Check to make sure a dataframe has distinct of column given ')
print(inspect.getsourcelines(checkDistinct)[0][0])
print()

# COMMAND ----------

# MAGIC %md Functions for Curated/Summary Tables

# COMMAND ----------

#Add a datekey to dataframe
def createDateKeyColumn(df: DataFrame, dateTarget: str)-> DataFrame:
  dfWithDateKey = (df
                   .withColumn('YEAR', F.year(F.col(dateTarget)))
                   .withColumn('MONTH', F.month(F.col(dateTarget)))
                   .withColumn('DATE_KEY', concat(F.col('YEAR'), F.col('MONTH')))
                   .drop('YEAR', 'MONTH')
                  )
  return dfWithDateKey

# COMMAND ----------

#Creates a full date key:: 2020/11/29 ==  20201129 key
def createFullDateKeyColumn(df: DataFrame, dateTarget: str) -> DataFrame:
  dfWithDateKey = (df
                   .withColumn('YEAR', F.year(F.col(dateTarget)))
                   .withColumn('MONTH', F.month(F.col(dateTarget)))
                   .withColumn('DAY', F.dayofmonth(F.col(dateTarget)))
                   .withColumn('DATE_KEY', F.concat(F.col('YEAR'), F.col('MONTH'), F.col('DAY')))
                   .drop('YEAR', 'MONTH', 'DAY')
                  )
  return dfWithDateKey

# COMMAND ----------

print('Add YEARMONTH date key Column')
print(inspect.getsourcelines(createDateKeyColumn)[0][0])
print()
print('Add YEARMONTHDAY date key Column')
print(inspect.getsourcelines(createFullDateKeyColumn)[0][0])

# COMMAND ----------

# MAGIC %md Log File Write Methods

# COMMAND ----------

#write to a certain file (in process)
def writeSummaryLogs(zone: str, filePath: str, data: list):
  fileName = filePath+zone+".txt"
  return fileName

# COMMAND ----------

#formats stats for a specific zone when passed a dict of values
def getZoneStats(results_dict: dict)-> list:
  # extract from map given the correct keys
  returnList =[]
  for k in results_dict.keys():
    string = '{} : {}'.format(k,str(results_dict.get(k)))
    returnList.append(string)
  return returnList

# COMMAND ----------

# helper func to search thru map with string keys and check if kv pair exists, if so return stringified value, else empty string
def mapExtractLogsHelper(searchKey: str, map: dict) -> str: 
  if not map:
    return ""
  #mapContainsFlag = map.contains(searchKey)
  if searchKey in map:
    mapValue = map[searchKey]
    return str(mapValue)
  
  else:
    return ""
  

# New  function to save a schema for csv to file // get map of values and return comma delimited text string
# @param  runResults = Map[String] - schema defined by k,v passed by zone
def getZoneLogStats(runResults: dict)-> str: 
  # schema strings
  status = mapExtractLogsHelper("status",runResults)
  entity = mapExtractLogsHelper("entity",runResults)
  jobID = mapExtractLogsHelper("jobID",runResults)
  runID = mapExtractLogsHelper("runID",runResults)
  date = mapExtractLogsHelper("date",runResults)
  created = mapExtractLogsHelper("created",runResults)
  updated = mapExtractLogsHelper("updated",runResults)
  before = mapExtractLogsHelper("before",runResults)
  after = mapExtractLogsHelper("after",runResults)
  
  returningStr = "{0},{1},{2},{3},{4},{5},{6},{7},{8}".format(status,entity,jobID,runID,date,created,updated,before,after)
  return returningStr

# COMMAND ----------

#print('Add YEARMONTH date key Column')
#print(inspect.getsourcelines(writeSummaryLogs)[0][0])
#print()
print('Formats stats for a specific zone when passed a dict of values')
print(inspect.getsourcelines(getZoneStats)[0][0])
print('Formats stats for a specific zone when passed a dict of values')
print(inspect.getsourcelines(getZoneLogStats)[0][0])

# COMMAND ----------

