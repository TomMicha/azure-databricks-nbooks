# Databricks notebook source
import os
import sys
import json
import pandas as pd
import numpy as np
import re

# COMMAND ----------

dbutils.secrets.help()
#dbutils.secrets.get(scope = "aldssecretscope", key = "aldsprincipal")

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# run this notebook to connect to data lake, get your service principal etc.
servicePrincipalID = dbutils.secrets.get(scope = "aldssecretscope", key = "aldsprincipal")
servicePrincipalKey = dbutils.secrets.get(scope = "aldssecretscope", key = "aldssecret")
directoryID = dbutils.secrets.get(scope = "aldssecretscope", key = "aldstenant")
directory = "https://login.microsoftonline.com/{}/oauth2/token".format(directoryID)
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": servicePrincipalID,
          "fs.azure.account.oauth2.client.secret": servicePrincipalKey,
          "fs.azure.account.oauth2.client.endpoint": directory}

# COMMAND ----------

# use this command to run this notebook in others - https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils#dbutils-secrets
# %run /path/to/InstallDependencies  
#n%run /reuse/preRunbook

# COMMAND ----------



# COMMAND ----------

        ####################### ABOVE SAVE IN NOTEBOOK FOR PRE RUN ##########################