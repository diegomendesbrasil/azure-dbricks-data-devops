# Databricks notebook source
import base64
import json
import requests
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pandas as pd
import pyspark.sql
import sys

# COMMAND ----------

#CONEXÃO COM O BANCO DE DADOS
StartProcess = time.time()

#CONEXÃO COM O BANCO DE DADOS
host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DatabaseName")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DBPass")

url = f'jdbc:sqlserver://{host_name}:{port};databaseName={database};user={user};password={password}' 

# COMMAND ----------

#BUSCA AS PLANS NO BANCO DE DADOS PARA TRAZER AS SUITES RELACIONADAS
qry_Suite = \
"""
SELECT 
WorkItemSystemId as WorkItemIdRisk
FROM DimWorkItem DW
where WorkItemType = 'Risk'
"""


RiskMitigation = spark.read\
.format('jdbc')\
.option('url', url)\
.option('query', qry_Suite)\
.load().toPandas()


# COMMAND ----------

#TRANSFORMA EM LISTA O RESULTADO DA CONSULTA ANTERIOR
Lista = RiskMitigation.values.tolist()

# COMMAND ----------

start = time.time()
DfRiskMitigation = pd.DataFrame([])
EXECUCAO = 0
EXECUCAOFOR = 0
lst = []
for line in Lista:
  EXECUCAOFOR= EXECUCAOFOR +1
  print('EXECUCAOFOR ='+str(EXECUCAOFOR))
  CONTINUATIONTOKEN = 0
  LASTCONTINUATIONTOKEN = 0
  PERSONAL_AUTHENTICATION_TOKEN = "7dyojugghu2nkgwbbvsaigylr3tvos22bw5k4gq4cyp4tcfw2taa"#"pdojz5ezqbvvnr6acgvpaynayuxa6gd7ctrnzwver52guyolphha"
  #RESOURCE_PATH = 'dmbData/Projeto_DevOps/_apis/testplan/Plans/'+str(line[0])+'/suites/'+str(line[1])+'?$continuationToken='+str(CONTINUATIONTOKEN)+'&api-version=6.0'
  USERNAME = "ab-inbev"
  USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
  B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()
  #ORGANIZATION_URL = f'https://dev.azure.com/'
  HEADERS = {
	    'Authorization': 'Basic %s' % B64USERPASS,
        'Accept': 'application/json'
	} 

  #565308
  #' + str(line[0]) + '+'&includePointDetails=false'
  while True:
    try:
        hourI = time.strftime("%H:%M:%S", time.gmtime(time.time()))
        EXECUCAO= EXECUCAO +1
        REQUEST_URL = 'https://dev.azure.com/dmbData/Projeto_DevOps/_apis/wit/workitems/'+str(line[0])+'?api-version=5.0'
        ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
        if ADO_RESPONSE.status_code == 200:
          jsRiskMitigation = json.loads(ADO_RESPONSE.content) 
          id = jsRiskMitigation['id']
          RiskLevel = jsRiskMitigation['fields']['Custom.RiskLevel']
          Description = jsRiskMitigation['fields']['System.Description']
          RiskMitigation = jsRiskMitigation['fields']['Custom.RiskMitigation']
          lst.append([id,RiskLevel,Description,RiskMitigation])
          
          if 'x-ms-continuationToken' in ADO_RESPONSE.headers.keys():
            #print('LASTCONTINUATIONTOKEN='+str(LASTCONTINUATIONTOKEN)+' - continuationtoken='+ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])
            if LASTCONTINUATIONTOKEN == int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0]):
              break
            else:
              CONTINUATIONTOKEN = CONTINUATIONTOKEN + int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])
              LASTCONTINUATIONTOKEN = CONTINUATIONTOKEN
              #print('LASTCONTINUATIONTOKEN='+str(LASTCONTINUATIONTOKEN))
              #print('CONTINUATIONTOKEN='+str(CONTINUATIONTOKEN))
              
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

schema = StructType([ 
    StructField("id",IntegerType(),True), 
    StructField("RiskLevel",StringType(),True), 
    StructField("Description",StringType(),True), 
    StructField("RiskMitigation", StringType(), True)
  ])

DFRiskMitigation = sqlContext.createDataFrame(lst,schema=schema)

# COMMAND ----------

sinkPath = '/mnt/raw/DevOps/Aurora/Brazil/RiskMitigation'
DFRiskMitigation.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

urlgrava = f'jdbc:sqlserver://{host_name}:{port};databaseName={database}'

DFRiskMitigation.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", urlgrava)\
    .option("dbtable", "dbo.WorkItemRiskDesc")\
    .option("user", user)\
    .option("password", password)\
    .save()


# COMMAND ----------

exec_procedure = "EXEC dbo.SPDimWorkRiskDescription"
driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
connection = driver_manager.getConnection(urlgrava, user, password)
connection.prepareCall(exec_procedure).execute()
connection.close()

# COMMAND ----------

endProcess = time.time()
print('Tempo de execução {} minutos'.format(((end-StartProcess)/60)))