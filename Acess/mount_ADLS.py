# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "091e16d3-f951-4b4b-8f0e-c5cd0c12d205", 
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="key-vault-secrets",key="DlkDtkKey"), 
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/cef04b19-7776-4a94-b89b-375c77a8f936/oauth2/token"} 

# COMMAND ----------

dbutils.fs.mount( 
  source = "abfss://sap-gb-dlk-de-ascptl@spgbddlkascptl001.dfs.core.windows.net/", 
  mount_point = "/mnt", 
  extra_configs = configs)

# COMMAND ----------

