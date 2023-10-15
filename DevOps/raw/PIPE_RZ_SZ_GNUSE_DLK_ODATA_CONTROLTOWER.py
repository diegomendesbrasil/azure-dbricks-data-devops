# Databricks notebook source
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
# MAGIC ##### COPIA DE DADOS DO WORKITEM PARA O DATALAKE

# COMMAND ----------

Lista = ['WorkItems']

# COMMAND ----------

start = time.time()
dfPoint = pd.DataFrame([])
EXECUCAO = 0
EXECUCAOFOR = 0
for line in Lista:
  EXECUCAOFOR= EXECUCAOFOR +1
  print('EXECUCAOFOR ='+str(EXECUCAOFOR))
  CONTINUATIONTOKEN = 0
  LASTCONTINUATIONTOKEN = 0
  PERSONAL_AUTHENTICATION_TOKEN = dbutils.secrets.get(scope = "key-vault-secrets", key = "OdtSrvDevOps")
  USERNAME = "ab-inbev"
  USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
  B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()
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
        REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/'+str(line[0])+'?$skiptoken=' + str(CONTINUATIONTOKEN)
        ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
        if ADO_RESPONSE.status_code == 200:
          df = json.loads(ADO_RESPONSE.content) 
          
          if "@odata.nextLink" in df.get():
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

start = time.time()
dfPoint = pd.DataFrame([])
EXECUCAO = 0
EXECUCAOFOR = 0
for line in Lista:
  EXECUCAOFOR= EXECUCAOFOR +1
  print('EXECUCAOFOR ='+str(EXECUCAOFOR))
  CONTINUATIONTOKEN = 0
  LASTCONTINUATIONTOKEN = 0
  PERSONAL_AUTHENTICATION_TOKEN = dbutils.secrets.get(scope = "key-vault-secrets", key = "OdtSrvDevOps")
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
        REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/'+str(line[0])+'?api-version=6.0&continuationToken=' + str(CONTINUATIONTOKEN)
        ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
        if ADO_RESPONSE.status_code == 200:
          df = json.loads(ADO_RESPONSE.content) 
          
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

PERSONAL_AUTHENTICATION_TOKEN = "7dyojugghu2nkgwbbvsaigylr3tvos22bw5k4gq4cyp4tcfw2taa"
RESOURCE_PATH = 'dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?api-version=6.0&continuationToken=0'
USERNAME = "ab-inbev"
USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()
ORGANIZATION_URL = f'https://analytics.dev.azure.com/'

HEADERS = {
   'Authorization': 'Basic %s' % B64USERPASS
	}
try:
   ADO_RESPONSE = requests.get(ORGANIZATION_URL + RESOURCE_PATH, headers=HEADERS)
    js = json.loads(ADO_RESPONSE.content)
    
except requests.exceptions.HTTPError as err:
   print(err)
    
      


# COMMAND ----------

PERSONAL_AUTHENTICATION_TOKEN = "7dyojugghu2nkgwbbvsaigylr3tvos22bw5k4gq4cyp4tcfw2taa"
RESOURCE_PATH = 'dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?api-version=6.0&continuationToken=0'
USERNAME = "ab-inbev"
USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()
ORGANIZATION_URL = f'https://analytics.dev.azure.com/'

HEADERS = {
   'Authorization': 'Basic %s' % B64USERPASS
	}
ADO_RESPONSE = requests.get(ORGANIZATION_URL + RESOURCE_PATH, headers=HEADERS)
if ADO_RESPONSE.status_code == 200:
  df = json.loads(ADO_RESPONSE.content)
  dfP  = pd.json_normalize(js['value'])
else:
  print(ADO_RESPONSE.status_code)
    
    
      


# COMMAND ----------

dfP.

# COMMAND ----------

js = json.loads(ADO_RESPONSE.content)

# COMMAND ----------

pip install vsts.vss_connection
pip install msrest.authentication
pip install vsts.work_item_tracking.v4_1.models.wiql

# COMMAND ----------



from vsts.vss_connection import VssConnection
from msrest.authentication import BasicAuthentication
import json
from vsts.work_item_tracking.v4_1.models.wiql import Wiql

# COMMAND ----------

from vsts.vss_connection import VssConnection
from msrest.authentication import BasicAuthentication
import json
from vsts.work_item_tracking.v4_1.models.wiql import Wiql

def emit(msg, *args):
print(msg % args)

def print_work_item(work_item):
    emit(
        "{0} {1}: {2}".format(
            work_item.fields["System.WorkItemType"],
            work_item.id,
            work_item.fields["System.Title"],
        )
    )

personal_access_token = 'YourPATToken'
organization_url = 'https://dev.azure.com/YourorgName'
# Create a connection to the org
credentials = BasicAuthentication('', personal_access_token)
connection = VssConnection(base_url=organization_url, creds=credentials)
wiql = Wiql(
        query="""select [System.Id] From WorkItems """
    )

wit_client = connection.get_client('vsts.work_item_tracking.v4_1.work_item_tracking_client.WorkItemTrackingClient')
wiql_results = wit_client.query_by_wiql(wiql).work_items
if wiql_results:
        # WIQL query gives a WorkItemReference with ID only
        # => we get the corresponding WorkItem from id
        work_items = (
            wit_client.get_work_item(int(res.id)) for res in wiql_results
        )
        for work_item in work_items:
            print_work_item(work_item)

# COMMAND ----------

