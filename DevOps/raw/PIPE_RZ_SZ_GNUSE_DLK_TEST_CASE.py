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


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Abre conexão com o banco de Dados

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
# MAGIC ##### Busca a lista dos teste Suites que farão parte da extração

# COMMAND ----------

#BUSCA AS PLANS NO BANCO DE DADOS PARA TRAZER AS SUITES RELACIONADAS
qry_Suite = \
"""
select  
	tp.WorkItemSystemId as idTestPlan,
	ts2.WorkItemSystemId as idTestSuite
from
dimworkitem tp
join DimWorkItem ts1
on tp.WorkItemSystemId = ts1.ParentWorkItemId	
join DimWorkItem ts2
on ts1.WorkItemSystemId = ts2.ParentWorkItemId
where tp.Test_Suite_Phase = 'System Integration Testing 2' 
and tp.workitemtype = 'Test Plan'
"""


TestCase = spark.read\
.format('jdbc')\
.option('url', url)\
.option('query', qry_Suite)\
.load().toPandas()


# COMMAND ----------

#TRANSFORMA EM LISTA O RESULTADO DA CONSULTA ANTERIOR
Lista = TestCase.values.tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Processo de Loop e Busca da API

# COMMAND ----------

start = time.time()
dfPoint = pd.DataFrame([])
EXECUCAO = 0
EXECUCAOFOR = 0
for line in Lista:
  EXECUCAOFOR= EXECUCAOFOR +1
  #print('EXECUCAOFOR ='+str(EXECUCAOFOR))
  CONTINUATIONTOKEN = 0
  LASTCONTINUATIONTOKEN = 0
  PERSONAL_AUTHENTICATION_TOKEN = dbutils.secrets.get(scope = "key-vault-secrets", key = "OdtSrvDevOps")
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
        REQUEST_URL = 'https://ab-inbev.visualstudio.com/Aurora_Program/_apis/testplan/Plans/'+str(line[0])+'/Suites/'+str(line[1])+'/TestPoint?api-version=6.0&continuationToken=' + str(CONTINUATIONTOKEN)
        ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
        if ADO_RESPONSE.status_code == 200:
          jsPoint = json.loads(ADO_RESPONSE.content) 
          dfP = pd.DataFrame(jsPoint)
          dfP2 = pd.json_normalize(dfP['value'])
          dfPoint = pd.concat([dfPoint,dfP2])
          #print(REQUEST_URL)
          if 'x-ms-continuationToken' in ADO_RESPONSE.headers.keys():
            #print('LASTCONTINUATIONTOKEN='+str(LASTCONTINUATIONTOKEN)+' - continuationtoken='+ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])
            if LASTCONTINUATIONTOKEN == int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0]):
              break
            else:
              #print('o que tinha antes no continuation token? : '+str(CONTINUATIONTOKEN + int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])))
              #CONTINUATIONTOKEN = CONTINUATIONTOKEN + int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])
              CONTINUATIONTOKEN = int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])
              LASTCONTINUATIONTOKEN = CONTINUATIONTOKEN

          else:
            break
        else:
            print('ERROR - {}'.format(ADO_RESPONSE.content))
            break
    except Exception as e:
      print('EXCEPTION - {}'.format(e))
      break
end = time.time()
print('Tempo de execução {} minutos'.format(((end-start)/60)))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Gravando dados na RAW

# COMMAND ----------

sparkDF=spark.createDataFrame(dfPoint) 
sinkPath = '/mnt/raw/DevOps/Aurora/Brazil/TestSuite'
sparkDF.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

#write("/dbfs/FileStore/df/PointExport.parquet", dfPoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Gravando dados no banco de dados

# COMMAND ----------

parquetFile = spark.read.format('parquet').load(sinkPath)

# COMMAND ----------

urlgrava = f'jdbc:sqlserver://{host_name}:{port};databaseName={database}'

sparkDF.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", urlgrava)\
    .option("dbtable", "dbo.TabPointExport")\
    .option("user", user)\
    .option("password", password)\
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC #####Execução de Procedure

# COMMAND ----------

exec_procedure = "EXEC dbo.SPDimPointExport"
driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
connection = driver_manager.getConnection(urlgrava, user, password)
connection.prepareCall(exec_procedure).execute()
connection.close()