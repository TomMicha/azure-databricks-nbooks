// Databricks notebook source
// MAGIC %md  Initial create DataFrame for ReferentialMapping from Source to Destination(XDW)
// MAGIC >Golden Record of all Entity Objects from Source to Destination XDW Model
// MAGIC >DATA_SOURCE_MAPPING  :  
// MAGIC   -DSM_SK	DATA_SOURCE	 DATA_DESTINATION	DATA_SOURCE_KEY	 DATA_SOURCE_VALUE	XDW_KEY  XDW_VALUE

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// func to help print distinct vs total counts
def printCounts(df: DataFrame, field: String): Unit = {
  println("total: "+df.count)
  println(s"total $field distinct: "+df.select(field).distinct.count)
}

// COMMAND ----------

// Reference Columns list for updating
val columns = List[String]("DSM_SK","DATA_SOURCE","DATA_DESTINATION","DATA_SOURCE_KEY","DATA_SOURCE_VALUE","XDW_KEY","XDW_VALUE")

// COMMAND ----------

// dimFirm for FirmKey : FirmSK mapping (firmKey comes from Member data)
val dimFirm = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimFirm")
var firmMap = dimFirm.select("FIRM_SK","FIRM_KEY")
  .withColumn("DATA_SOURCE",lit("MEMBER"))
  .withColumn("DATA_DESTINATION", lit("XDW"))
  .withColumn("DATA_SOURCE_KEY", lit("FIRM_KEY"))
  .withColumn("XDW_KEY", lit("FIRM_SK"))
  .withColumnRenamed("FIRM_SK", "XDW_VALUE")
  .withColumnRenamed("FIRM_KEY", "DATA_SOURCE_VALUE")
firmMap = firmMap.select("DATA_SOURCE","DATA_DESTINATION","DATA_SOURCE_KEY","DATA_SOURCE_VALUE","XDW_KEY","XDW_VALUE")

// COMMAND ----------

// fBH for MatterKey : MatterSK mapping (matterKey comes from Passport Data)
var btMatter = spark.read.format("parquet").load("/mnt/RAWDUMPMAY/Passport.BTMATTERPERSON")
btMatter = btMatter.select("MATTER_KEY","MATTER_ID","MATTER_NUMBER")
var dimMatter = spark.read.format("parquet").load("/mnt/RAWDUMPMAY/EDW.dimMatter")
dimMatter = dimMatter.select("MatterSK","matter_number").withColumnRenamed("matter_number","MATTER_NUMBER").withColumnRenamed("MatterSK","MATTER_SK")


// COMMAND ----------

// Join to get MATTER_ID : MATTER_SK mapping
var matterMap = btMatter.join(dimMatter, Seq("MATTER_NUMBER"), "left").drop("MATTER_KEY","MATTER_NUMBER")
printCounts(matterMap, "MATTER_ID")

matterMap = matterMap.select("MATTER_SK","MATTER_ID").distinct
printCounts(matterMap, "MATTER_SK")

// COMMAND ----------

// create dataframe for MATTER ID : MATTER SK

matterMap = matterMap.withColumn("DATA_SOURCE",lit("PASSPORT"))
  .withColumn("DATA_DESTINATION", lit("XDW"))
  .withColumn("DATA_SOURCE_KEY", lit("MATTER_ID"))
  .withColumn("XDW_KEY", lit("MATTER_SK"))
  .withColumnRenamed("MATTER_ID", "XDW_VALUE")
  .withColumnRenamed("MATTER_SK", "DATA_SOURCE_VALUE")
matterMap = matterMap.select("DATA_SOURCE","DATA_DESTINATION","DATA_SOURCE_KEY","DATA_SOURCE_VALUE","XDW_KEY","XDW_VALUE")

// COMMAND ----------

// dimPerson for PersonKey : PersonSK mapping
var btPerson = spark.read.format("parquet").load("/mnt/RAWDUMPMAY/Passport.BTMATTERPERSON")
btPerson = btPerson.select("PERSON_KEY").distinct.orderBy("PERSON_KEY")
printCounts(btPerson, "PERSON_KEY")


// COMMAND ----------

// add index / PK for person
val schema = btPerson.schema
val rows = btPerson.rdd.zipWithIndex.map{
   case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
// get DF back with PKs
val personWithPK = sqlContext.createDataFrame(
  rows, StructType(StructField("id", LongType, false) +: schema.fields)).withColumnRenamed("id","PERSON_SK")
display(personWithPK)

// COMMAND ----------

printCounts(personWithPK, "PERSON_SK")

// COMMAND ----------

// create personKey : PersonSK dataframe with all columns
var personMap = personWithPK.withColumn("DATA_SOURCE",lit("PASSPORT"))
  .withColumn("DATA_DESTINATION", lit("XDW"))
  .withColumn("DATA_SOURCE_KEY", lit("PERSON_KEY"))
  .withColumn("XDW_KEY", lit("PERSON_SK"))
  .withColumnRenamed("PERSON_SK", "XDW_VALUE")
  .withColumnRenamed("PERSON_KEY", "DATA_SOURCE_VALUE")
personMap = personMap.select("DATA_SOURCE","DATA_DESTINATION","DATA_SOURCE_KEY","DATA_SOURCE_VALUE","XDW_KEY","XDW_VALUE")

// COMMAND ----------

// check CoL mapping ID
var xdwCoL = spark.read.format("delta").load("/mnt/alasdeltalake/transient/XDW/dimCauseOfLoss")
xdwCoL = xdwCoL.select("PASSPORT_ID","PRIMARY_CAUSE_OF_LOSS_SK")
printCounts(xdwCoL, "PASSPORT_ID")
printCounts(xdwCoL, "PRIMARY_CAUSE_OF_LOSS_SK")

// COMMAND ----------

// Cause OF Loss PK : Passport Cause ID DataFrame
var CoLMap = xdwCoL.withColumn("DATA_SOURCE",lit("PASSPORT"))
  .withColumn("DATA_DESTINATION", lit("XDW"))
  .withColumn("DATA_SOURCE_KEY", lit("PASSPORT_CAUSE_ID"))
  .withColumn("XDW_KEY", lit("PRIMARY_CAUSE_OF_LOSS_SK"))
  .withColumnRenamed("PRIMARY_CAUSE_OF_LOSS_SK", "XDW_VALUE")
  .withColumnRenamed("PASSPORT_ID", "DATA_SOURCE_VALUE")
CoLMap = CoLMap.select("DATA_SOURCE","DATA_DESTINATION","DATA_SOURCE_KEY","DATA_SOURCE_VALUE","XDW_KEY","XDW_VALUE")

// COMMAND ----------

printCounts(matterMap, "XDW_VALUE")
printCounts(personMap, "XDW_VALUE")
printCounts(firmMap, "XDW_VALUE")
printCounts(CoLMap, "XDW_VALUE")

// COMMAND ----------

// MAGIC %md UNION above dataframe together

// COMMAND ----------

// Union multiple dataframe 
var saveDF = matterMap.union(personMap)
printCounts(saveDF, "DATA_SOURCE_VALUE")
saveDF = saveDF.union(firmMap)
printCounts(saveDF, "DATA_SOURCE_VALUE")
saveDF = saveDF.union(CoLMap)
printCounts(saveDF, "DATA_SOURCE_VALUE")

// COMMAND ----------

// add index / PK
val schema = saveDF.schema
val rows = saveDF.rdd.zipWithIndex.map{
   case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
// get DF back with PKs
val MappingTable = sqlContext.createDataFrame(
  rows, StructType(StructField("id", LongType, false) +: schema.fields)).withColumnRenamed("id","DSM_SK")
display(MappingTable)

// COMMAND ----------

var failed = false

// COMMAND ----------

//save curate version of table
if (!failed){
  val transientDeltaPath = "/mnt/alasdeltalake/transient/XDW/"
  val tableName = "DataSourceMapping"
  MappingTable.write.format("delta").save(transientDeltaPath+tableName)
} else {
  println("Validation Logic Failed: Please check output for issues")
}

// COMMAND ----------

