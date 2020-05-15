# Databricks notebook source
# MAGIC %md Function to Mount deltalake and its containers

# COMMAND ----------

dbutils.widgets.text("secretScope", "KeyVault", "Secrets Scope")
dbutils.widgets.text("spId", "service principal", "SP")
dbutils.widgets.text("spKey", "service principal key", "SPKey")
dbutils.widgets.text("lakeKey", "***", "Storage Key")
dbutils.widgets.text("TID", "tenant", "Tenant ID")

# COMMAND ----------

# MAGIC %md Getting Secrets for the configuration of lake

# COMMAND ----------

secretScope = dbutils.widgets.get("secretScope")
storageKey = dbutils.widgets.get("lakeKey")
secretServicePrincipalId = dbutils.widgets.get("spId")
secretServicePrincipalKey = dbutils.widgets.get("spKey")
secretTenantId = dbutils.widgets.get("TID")

storageAccountAccessKey = dbutils.secrets.get(scope = secretScope, key = storageKey)
servicePrincipalId=dbutils.secrets.get(scope=secretScope, key =secretServicePrincipalId)
servicePrincipalKey=dbutils.secrets.get(scope=secretScope, key = secretServicePrincipalKey)
tenantId =dbutils.secrets.get(scope=secretScope, key = secretTenantId)

# COMMAND ----------

#will need to change these values
deltalakeName = "alasdeltalake"
storageAccount = "alasdatalakepnh4fdev"

# COMMAND ----------

adlsConfigs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": servicePrincipalId,
  "fs.azure.account.oauth2.client.secret": servicePrincipalKey,
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenantId+"/oauth2/token",
  "fs.azure.createRemoteFileSystemDuringInitialization":"true"
}

# COMMAND ----------

def mountStorageContainer(storageAccount, storageAccountKey, storageContainer, lakeMountPoint):
    print("Mounting {0} to {1}:".format(storageContainer, lakeMountPoint))
    try:
    # Unmount the storage container if already mounted
      dbutils.fs.unmount(lakeMountPoint)
    except Exception as e:
    # If this errors, safe to assume that the container is not mounted
      print("....Container is not mounted; Attempting mounting now..")
    
      # Mount the storage container
      mountStatus = dbutils.fs.mount(
                  source = "abfss://{0}@{1}.dfs.core.windows.net/".format(storageContainer, storageAccount),
                  mount_point = lakeMountPoint,
                  extra_configs = adlsConfigs
      )

      print("....Status of mount is: " + str(mountStatus))
      print() # Provide a blank line between mounts

# COMMAND ----------

# Mount the various storage containers created
#change to raw, bronze, silver, gold?
mountStorageContainer(storageAccount,storageAccountAccessKey,"transient","/mnt/"+deltalakeName+"/transient")
mountStorageContainer(storageAccount,storageAccountAccessKey,"persisted","/mnt/"+deltalakeName+"/persisted")
mountStorageContainer(storageAccount,storageAccountAccessKey,"curated","/mnt/"+deltalakeName+"/curated")
mountStorageContainer(storageAccount,storageAccountAccessKey,"experiments","/mnt/"+deltalakeName+"/experiments")

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

mountStorageContainer(storageAccount,storageAccountAccessKey,"raw","/mnt/passport-tables/RAW")
mountStorageContainer(storageAccount,storageAccountAccessKey,"ref","/mnt/passport-tables/REF")

# COMMAND ----------

# MAGIC %fs ls '/mnt/passport-tables/RAW/btmatter-header'

# COMMAND ----------

data = spark.read.format('parquet').options(
    header='true', inferschema='true').load("/mnt/passport-tables/RAW/btmatter-header/*.parquet")

# COMMAND ----------

dataNoHeader =  spark.read.format('parquet').load("/mnt/passport-tables/RAW/btmatter-noheader/*.parquet")

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

