# Databricks notebook source
##### Database DW Connection 

# COMMAND ----------

#CONEXÃO COM O BANCO DE DADOS
host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "Database")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "Pass")

url = f'jdbc:sqlserver://{host_name}:{port};databaseName={database};user={user};password={password}' 

# COMMAND ----------

##### Token Connection

# COMMAND ----------

PERSONAL_AUTHENTICATION_TOKEN = dbutils.secrets.get(scope = "key-vault-secrets", key = "DevOpsToken")

# COMMAND ----------

##### DataLake Paths

# COMMAND ----------

raw_folder = '/mnt/raw/DevOps/DmBData/'
standardized_folder = '/mnt/standardized/DevOps/DmBData/'
consume_folder = '/mnt/Consume/DevOps/DmBData/'

# COMMAND ----------

#### Função para buscar os dados do DevOps

# COMMAND ----------

def getDadosDevOpsAPI(source, colunas=[]):
  
  dfOdata = pd.DataFrame([])
  
  BASE_URL = 'https://analytics.dev.azure.com/{Company}/{Project}/_odata/v3.0/'

  EXECUCAO = 0
  EXECUCAOFOR = 0
  EXECUCAOWHILE = 0 

  EXECUCAOFOR= EXECUCAOFOR +1

  CONTINUATIONTOKEN = 0

  LASTCONTINUATIONTOKEN = 0

  USERNAME = "UserDiego"
  USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
  B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()

  HEADERS = {
    'Authorization': 'Basic %s' % B64USERPASS,
    'Accept': 'application/json'
  } 
  
  while True:
    try:
      hourI = time.strftime("%H:%M:%S", time.gmtime(time.time()))
      EXECUCAO= EXECUCAO +1
      if len(colunas) > 0:
        REQUEST_URL = BASE_URL + source + '?&$select=' + ','.join(colunas) + '&$skiptoken=' + str(CONTINUATIONTOKEN)
      else:
        REQUEST_URL = BASE_URL + source + '?&$skiptoken='+str(CONTINUATIONTOKEN)
      ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
      if ADO_RESPONSE.status_code == 200:
        df = json.loads(ADO_RESPONSE.content) 
        dfP2 = pd.json_normalize(df['value'])
        dfOdata = pd.concat([dfOdata,dfP2])
      
        if df.get("@odata.nextLink"):
        
          if LASTCONTINUATIONTOKEN == int(df.get("@odata.nextLink").split('skiptoken=')[1]):
            break
          
          else:
            CONTINUATIONTOKEN = int(df.get("@odata.nextLink").split('skiptoken=')[1])
            LASTCONTINUATIONTOKEN = CONTINUATIONTOKEN
            EXECUCAOWHILE = EXECUCAOWHILE +1
            qtd_linhas = len(dfOdata.index)
            print(f'[{str(hourI)}] {str(EXECUCAOWHILE)}º Paginação, continuando execução... | Qtd Linhas: {str(qtd_linhas)}')
        else:
          print(f'Processamento completo. Qtd Linhas {str(len(dfOdata.index))}')
          break
      else:
        print('ERROR - {}'.format(ADO_RESPONSE.content))
        print('Execução cancelada. Código: ', ADO_RESPONSE.status_code)
        return 
    except Exception as e:
      print('EXCEPTION - {}'.format(e))
      break
  return dfOdata

# COMMAND ----------

def getDadosDiarioDevOpsAPI(source, data_corte):
    
  dfOdata = pd.DataFrame([])
  
  BASE_URL = 'https://analytics.dev.azure.com/{Company}/{Project}/_odata/v3.0/'
  
  if source == 'WorkItems' or source == 'WorkItemRevisions':
    COLUNA_LOOKUP = 'ChangedDate'
  else:
    COLUNA_LOOKUP = 'AnalyticsUpdatedDate'

  EXECUCAO = 0
  EXECUCAOFOR = 0
  EXECUCAOWHILE = 0 

  EXECUCAOFOR= EXECUCAOFOR +1

  CONTINUATIONTOKEN = 0

  LASTCONTINUATIONTOKEN = 0

  USERNAME = "UserDiego"
  USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
  B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()

  HEADERS = {
    'Authorization': 'Basic %s' % B64USERPASS,
    'Accept': 'application/json'
  } 
  
  while True:
    try:
      hourI = time.strftime("%H:%M:%S", time.gmtime(time.time()))
      EXECUCAO= EXECUCAO +1
      REQUEST_URL = BASE_URL + source + '?&$skiptoken='+str(CONTINUATIONTOKEN) + '&$filter=' + COLUNA_LOOKUP + ' ge ' + data_corte
      ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
      if ADO_RESPONSE.status_code == 200:
        df = json.loads(ADO_RESPONSE.content) 
        dfP2 = pd.json_normalize(df['value'])
        dfOdata = pd.concat([dfOdata,dfP2])
      
        if df.get("@odata.nextLink"):
        
          if LASTCONTINUATIONTOKEN == int(df.get("@odata.nextLink").split('skiptoken=')[1]):
            break
          
          else:
            CONTINUATIONTOKEN = int(df.get("@odata.nextLink").split('skiptoken=')[1])
            LASTCONTINUATIONTOKEN = CONTINUATIONTOKEN
            EXECUCAOWHILE = EXECUCAOWHILE +1
            qtd_linhas = len(dfOdata.index)
            print(f'[{str(hourI)}] {str(EXECUCAOWHILE)}º Paginação, continuando execução... | Qtd Linhas: {str(qtd_linhas)}')
        else:
          print(f'Processamento completo. Qtd Linhas {str(len(dfOdata.index))}')
          break
      else:
        print('ERROR - {}'.format(ADO_RESPONSE.content))
        print('Execução cancelada por erro. Código: ', ADO_RESPONSE.status_code)
        break
    except Exception as e:
      print('EXCEPTION - {}'.format(e))
      break
  return dfOdata

# COMMAND ----------

# NÃO ESTÁ SENDO UTILIZADA

def UpdateDataLoad(hora_atualizacao, subject_area, table_name, zone_name):
  
  df_ControlDatasetLoad = (spark
  .read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", 'dbo.ControlDatasetLoad_Temp')
  .option("user", user)
  .option("password", password)
  .load()
)
  
  df_DataAtualizada = df_ControlDatasetLoad.withColumn('LastDataLoad', 
                                            when((col('SubjectArea') == subject_area) & 
                                                 (col('TableName') == table_name) & 
                                                 (col('Zone') == zone_name) & 
                                                 (col('DataSource') == 'DevOps'), hora_atualizacao).otherwise(df_ControlDatasetLoad.LastDataLoad))
  
  df_DataAtualizada.write.mode('overwrite') \
  .format("jdbc") \
  .option("url", url) \
  .option("dbtable", 'dbo.ControlDatasetLoad_Temp') \
  .option("user", user) \
  .option("password", password) \
  .save()
  
  print(f'Data de atualização de {zone_name}/{subject_area}/{table_name} na tabela ControlDatasetLoad foi alterada para {hora_atualizacao}')
  
  return

# COMMAND ----------

# NÃO ESTÁ SENDO UTILIZADA

def update_log(assunto, origem, destino, tempo, qtd_linhas, ordem):
  
  colunas = ['Assunto', 'Origem', 'Destino', 'Tempo', 'Linhas', 'Ordem', 'DataCarregamento']
  linhas = [(assunto, origem, destino, tempo, qtd_linhas, ordem, (datetime.now() - pd.DateOffset(hours=3)).replace(microsecond=0).isoformat())]
  
  df_schema = StructType([       
    StructField('Assunto', StringType(), True),
    StructField('Origem', StringType(), True),
    StructField('Destino', StringType(), True),
    StructField('Tempo', StringType(), True),
    StructField('Linhas', IntegerType(), True),
    StructField('Ordem', IntegerType(), True),
    StructField('DataCarregamento', StringType(), True)
  ])
  
  
  df = spark.createDataFrame(linhas, df_schema)
  
  df.write.mode('append') \
  .format("jdbc") \
  .option("url", url) \
  .option("dbtable", 'dbo.UpdateLog') \
  .option("user", user) \
  .option("password", password) \
  .save()
  
  print(f'{qtd_linhas} linhas do assunto {assunto} foram gravadas na {destino} zone')
  
  return

# COMMAND ----------


