# Databricks notebook source
# DBTITLE 1,Curated measureClaimFrequencyWindows (part of summary/measures)
# MAGIC %md 
# MAGIC [PLM] Sorry I have to convert Scala --> Python. Good thing: it can be undone, and I haven't deleted the existing scala cmds.
# MAGIC 
# MAGIC ### Dependent Tables:
# MAGIC   - `persisted/XDW/dimDate` (probably): std month dates, although there is SQL function `EOMONTH`, Python fn `monthofrange()` ...
# MAGIC   - `persisted/XDW/factPolicyHistory`: source of UNADJUSTED_CENSUS, link to POLICY_YEAR_SK, FIRM_SK
# MAGIC   - `persisted/XDW/factMatterCategoryHistory`: all matters that are Claims (CLAIM_INDICATOR=1), link to both FIRMs and POLICY_YEARs
# MAGIC   - `persisted/XDW/dimFirm`: possibly, but I think it can be ignored if we just use FIRM_SK refs from other tables
# MAGIC   - `curated/XDW/summaryMatterMeasure_Count`: supplies curated key info from factMatterDetail
# MAGIC 
# MAGIC > Actions: add rolling window views of dates
# MAGIC 
# MAGIC why firm_key here?

# COMMAND ----------

dft = spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_counts")

# COMMAND ----------

display(dft)

# COMMAND ----------

# MAGIC %scala
# MAGIC // IGNORE (py)
# MAGIC // pull necessary tables from persisted delta tables
# MAGIC val dfDate = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimDate")

# COMMAND ----------

# MAGIC %scala
# MAGIC // IGNORE (py)
# MAGIC // for reference 
# MAGIC val columns = List[String]("FIRM_KEY",
# MAGIC "AS_OF_MONTH",
# MAGIC "TIME_PERIOD_YEARS_NO",
# MAGIC "TIME_PERIOD_BEGIN_DATE",
# MAGIC "TIME_PERIOD_END_DATE",
# MAGIC "CLAIM_FREQUENCY_OVER_PERIOD",
# MAGIC "FIRM-CENSUS_OVER-PERIOD")

# COMMAND ----------

### we make a TEMP month-last-date table to get the AS_OF keys
dfDate = spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimDate")

# COMMAND ----------

from pyspark.sql import functions as F
dfASOF = dfDate.groupBy('MONTH').agg(F.max('DATE').alias('LAST_DAY_MO')).filter("LAST_DAY_MO > '1999-12-31'")
# for example
display(dfASOF.filter("LAST_DAY_MO > '2018-01-30'"))

# COMMAND ----------

# now build the claim count part
dfASOF.createOrReplaceTempView("asofdates")
xx = spark.sql("SELECT * FROM asofdates WHERE LAST_DAY_MO > '2018-01-15'")
display(xx)

# COMMAND ----------

from pyspark.sql.functions import add_months, date_sub

# apparently these can sql-ized in one shot
spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/dimFirm").createOrReplaceTempView("dfirms")
# CHECK display(spark.sql("SELECT * FROM dfirms LIMIT 10"))
spark.read.format("delta").load("/mnt/alasdeltalake/curated/XDW/summaryMatterMeasure_counts").createOrReplaceTempView("dfSMM")
spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factMatterCategoryHistory").createOrReplaceTempView("dfMCH")
spark.read.format("delta").load("/mnt/alasdeltalake/persisted/XDW/factPolicyHistory").createOrReplaceTempView("dfPH")

# lets do the count for the 7, 12 years separately (20 yrs todo)
# NB: uses matter_open_date as it seems more reliable actually

cfCount7 = spark.sql("SELECT asof.LAST_DAY_MO AS AS_OF_MONTH, \
                             df.FIRM_SK, \
                             7 AS TIME_PERIOD_YEARS_NUM, \
                             add_months(asof.LAST_DAY_MO, -(7*12)) AS TIME_PERIOD_BEGINS_DATE, \
                             asof.LAST_DAY_MO AS TIME_PERIOD_ENDS_DATE, \
                             count(smd.MATTER_SK) AS CLAIM_FREQUENCY_OVER_PERIOD, \
                             sum(fph.UNADJUSTED_CENSUS)/1000 AS FIRM_CENSUS_OVER_PERIOD \
                      FROM dfSMM smd JOIN dfMCH fch \
                      ON smd.MATTER_SK = fch.MATTER_SK AND fch.CLAIM_INDICATOR=1 AND fch.WITHOUT_MERIT_INDICATOR=0 \
                      JOIN dfPH fph ON smd.FIRM_SK = fph.FIRM_SK AND smd.POLICY_YEAR_SK = fph.POLICY_YEAR_SK \
                      JOIN asofdates asof ON smd.MATTER_OPEN_DATE BETWEEN add_months(asof.LAST_DAY_MO, -(7*12)) and asof.LAST_DAY_MO \
                      JOIN dfirms df ON df.FIRM_SK = smd.FIRM_SK \
                      WHERE NOT EXISTS (select 1 from dfMCH fch2 \
                                        where fch2.MATTER_SK = smd.MATTER_SK \
                                        AND fch2.CLAIM_INDICATOR=1 AND fch2.WITHOUT_MERIT_INDICATOR=0 \
                                        AND fch2.MATTER_CATEGORY_EFFECTIVE_ON > fch.MATTER_CATEGORY_EFFECTIVE_ON) \
                      GROUP BY asof.LAST_DAY_MO, df.FIRM_SK, fph.CENSUSUW_SK")


cfCount12 = spark.sql("SELECT asof.LAST_DAY_MO AS AS_OF_MONTH, \
                             df.FIRM_SK, \
                             12 AS TIME_PERIOD_YEARS_NUM, \
                             add_months(asof.LAST_DAY_MO, -(12*12)) AS TIME_PERIOD_BEGINS_DATE, \
                             asof.LAST_DAY_MO AS TIME_PERIOD_ENDS_DATE, \
                             count(smd.MATTER_SK) AS CLAIM_FREQUENCY_OVER_PERIOD, \
                             sum(fph.UNADJUSTED_CENSUS)/1000 AS FIRM_CENSUS_OVER_PERIOD \
                      FROM dfSMM smd JOIN dfMCH fch \
                      ON smd.MATTER_SK = fch.MATTER_SK AND fch.CLAIM_INDICATOR=1 AND fch.WITHOUT_MERIT_INDICATOR=0 \
                      JOIN dfPH fph ON smd.FIRM_SK = fph.FIRM_SK AND smd.POLICY_YEAR_SK = fph.POLICY_YEAR_SK \
                      JOIN asofdates asof ON smd.MATTER_OPEN_DATE BETWEEN add_months(asof.LAST_DAY_MO, -(12*12)) and asof.LAST_DAY_MO \
                      JOIN dfirms df ON df.FIRM_SK = smd.FIRM_SK \
                      WHERE NOT EXISTS (select 1 from dfMCH fch2 \
                                        where fch2.MATTER_SK = smd.MATTER_SK \
                                        AND fch2.CLAIM_INDICATOR=1 AND fch2.WITHOUT_MERIT_INDICATOR=0 \
                                        AND fch2.MATTER_CATEGORY_EFFECTIVE_ON > fch.MATTER_CATEGORY_EFFECTIVE_ON) \
                      GROUP BY asof.LAST_DAY_MO, df.FIRM_SK, fph.CENSUSUW_SK")


cfCount20 = spark.sql("SELECT asof.LAST_DAY_MO AS AS_OF_MONTH, \
                             df.FIRM_SK, \
                             20 AS TIME_PERIOD_YEARS_NUM, \
                             add_months(asof.LAST_DAY_MO, -(20*12)) AS TIME_PERIOD_BEGINS_DATE, \
                             asof.LAST_DAY_MO AS TIME_PERIOD_ENDS_DATE, \
                             count(smd.MATTER_SK) AS CLAIM_FREQUENCY_OVER_PERIOD, \
                             sum(fph.UNADJUSTED_CENSUS)/1000 AS FIRM_CENSUS_OVER_PERIOD \
                      FROM dfSMM smd JOIN dfMCH fch \
                      ON smd.MATTER_SK = fch.MATTER_SK AND fch.CLAIM_INDICATOR=1 AND fch.WITHOUT_MERIT_INDICATOR=0 \
                      JOIN dfPH fph ON smd.FIRM_SK = fph.FIRM_SK AND smd.POLICY_YEAR_SK = fph.POLICY_YEAR_SK \
                      JOIN asofdates asof ON smd.MATTER_OPEN_DATE BETWEEN add_months(asof.LAST_DAY_MO, -(20*12)) and asof.LAST_DAY_MO \
                      JOIN dfirms df ON df.FIRM_SK = smd.FIRM_SK \
                      WHERE NOT EXISTS (select 1 from dfMCH fch2 \
                                        where fch2.MATTER_SK = smd.MATTER_SK \
                                        AND fch2.CLAIM_INDICATOR=1 AND fch2.WITHOUT_MERIT_INDICATOR=0 \
                                        AND fch2.MATTER_CATEGORY_EFFECTIVE_ON > fch.MATTER_CATEGORY_EFFECTIVE_ON) \
                      GROUP BY asof.LAST_DAY_MO, df.FIRM_SK, fph.CENSUSUW_SK")


cfTot = cfCount7.union(cfCount12.union(cfCount20))




# COMMAND ----------

display(cfTot.where("TIME_PERIOD_YEARS_NUM = 12").sort('AS_OF_MONTH', ascending=False))


# COMMAND ----------

## save curated version of table