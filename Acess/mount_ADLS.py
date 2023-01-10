# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "",
           "fs.azure.account.oauth2.client.id": "", 
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="key-vault-secrets",key="cptdaaaa"), 
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/abcd-defg-hijk/oauth2/token"} 

# COMMAND ----------

dbutils.fs.mount( 
  source = "abfss://XPTO.dfs.core.windows.net/", 
  mount_point = "/mnt", 
  extra_configs = configs)

# COMMAND ----------

