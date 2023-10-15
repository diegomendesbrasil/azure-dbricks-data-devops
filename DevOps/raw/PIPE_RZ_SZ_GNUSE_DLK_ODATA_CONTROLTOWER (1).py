# Databricks notebook source
pip install fastparquet

# COMMAND ----------


import base64
import json
import requests
import time

from pyspark.sql.functions import regexp_replace, date_format
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import initcap, lit
from pandas import DataFrame
from re import findall
from fastparquet import write 
import pandas as pd
from pyspark.sql.types import StringType



# COMMAND ----------

# MAGIC %md
# MAGIC ##### CONEXÃO COM O BANCO DE DADOS

# COMMAND ----------

#CONEXÃO COM O BANCO DE DADOS
StartProcess = time.time()

host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DatabaseName")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DBPass")

url = f'jdbc:sqlserver://{host_name}:{port};databaseName={database};user={user};password={password}' 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Busca Dados da API

# COMMAND ----------

Lista = ['WorkItems']

# COMMAND ----------

start = time.time()
dfOdata = pd.DataFrame([])
DFWorkItem = pd.DataFrame([])
lst = []
EXECUCAO = 0
EXECUCAOFOR = 0
EXECUCAOWHILE = 0 
for line in Lista:
  EXECUCAOFOR= EXECUCAOFOR +1
  print('EXECUCAOFOR ='+str(EXECUCAOFOR))
  CONTINUATIONTOKEN = 0
  LASTCONTINUATIONTOKEN = 0
  PERSONAL_AUTHENTICATION_TOKEN = dbutils.secrets.get(scope = "key-vault-secrets", key = "OdtSrvDevOps")
  #PERSONAL_AUTHENTICATION_TOKEN = "ufmw7ygitxqf2n3lp4cornkxod5ayruwtp3atwfkqgtdagpvjqoq"
  USERNAME = "ab-inbev"
  USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
  B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()
  #ORGANIZATION_URL = f'https://dev.azure.com/'
  HEADERS = {
	    'Authorization': 'Basic %s' % B64USERPASS,
        'Accept': 'application/json'
	} 

  while True:
    try:
        hourI = time.strftime("%H:%M:%S", time.gmtime(time.time()))
        EXECUCAO= EXECUCAO +1
        REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?&$skiptoken='+str(CONTINUATIONTOKEN)
        #REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?&$filter=ChangedDate ge 2022-01-10&$skiptoken='+str(CONTINUATIONTOKEN)
        ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
        if ADO_RESPONSE.status_code == 200:
          df = json.loads(ADO_RESPONSE.content) 
          dfP2 = pd.json_normalize(df['value'])
          dfOdata = pd.concat([dfOdata,dfP2])
          #print(REQUEST_URL)
          if df.get("@odata.nextLink"):
            print('Entrou no if @odata.nextLink')
            print(REQUEST_URL)
            if LASTCONTINUATIONTOKEN == int(df.get("@odata.nextLink").split('skiptoken=')[1]):
              print('parou aqui 1'+str(LASTCONTINUATIONTOKEN))
              break
            else:
              #print('o que tinha antes no continuation token? : '+str(CONTINUATIONTOKEN + int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])))
              #CONTINUATIONTOKEN = CONTINUATIONTOKEN + int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])
              CONTINUATIONTOKEN = int(df.get("@odata.nextLink").split('skiptoken=')[1])
              LASTCONTINUATIONTOKEN = CONTINUATIONTOKEN
              print('parou aqui 2 - Setado Token '+str(CONTINUATIONTOKEN))
              EXECUCAOWHILE = EXECUCAOWHILE +1
              print('EXECUCAOFWHILE ='+str(EXECUCAOWHILE)+'Tempo '+str(hourI))
          else:
            print('parou aqui 3')
            break
        else:
            print('ERROR - {}'.format(ADO_RESPONSE.content))
            print('parou aqui 4')
            break
    except Exception as e:
      print('EXCEPTION - {}'.format(e))
      print('parou aqui 5')
      break
end = time.time()
print('Tempo de execução {} minutos'.format(((end-start)/60)))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando Schema para WorkItem

# COMMAND ----------

df.get("@odata.nextLink").split('skiptoken=')[1]

# COMMAND ----------

sinkPath = '/mnt/raw/DevOps/Aurora/Brazil/WorkItems'
DFWorkItem.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM
# MAGIC DimWorkItemTemp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC A EXTRAÇÃO DO DEVOPS É BASEADA NA LISTA ABAIXO
# MAGIC 
# MAGIC Areas
# MAGIC BoardLocations
# MAGIC Iterations
# MAGIC Processes
# MAGIC Projects
# MAGIC Teams
# MAGIC Users
# MAGIC WorkItemRevisions
# MAGIC WorkItemTypeFields
# MAGIC WorkItemLinks
# MAGIC WorkItems
# MAGIC 
# MAGIC Então criaremos uma carga para cada um