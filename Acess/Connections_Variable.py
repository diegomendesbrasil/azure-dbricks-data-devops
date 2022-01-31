# Databricks notebook source
##### Database DW Connection 

# COMMAND ----------

#CONEXÃO COM O BANCO DE DADOS
host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DatabaseName")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DBPass")

url = f'jdbc:sqlserver://{host_name}:{port};databaseName={database};user={user};password={password}' 

# COMMAND ----------

##### Token Connection

# COMMAND ----------

PERSONAL_AUTHENTICATION_TOKEN = dbutils.secrets.get(scope = "key-vault-secrets", key = "OdtSrvDevOps")

# COMMAND ----------

##### DataLake Paths

# COMMAND ----------

aurora_raw_folder = '/mnt/raw/DevOps/Aurora/'
aurora_standardized_folder = '/mnt/standardized/DevOps/Aurora/'
aurora_consume_folder = '/mnt/Consume/DevOps/Aurora/'

# COMMAND ----------

#### Função para buscar os dados do DevOps

# COMMAND ----------

def getDadosAuroraAPI(source, colunas=[]):
  
  dfOdata = pd.DataFrame([])
  
  BASE_URL = 'https://analytics.dev.azure.com/ab-inbev/Aurora_Program/_odata/v3.0/'

  EXECUCAO = 0
  EXECUCAOFOR = 0
  EXECUCAOWHILE = 0 

  EXECUCAOFOR= EXECUCAOFOR +1

  CONTINUATIONTOKEN = 0

  LASTCONTINUATIONTOKEN = 0

  USERNAME = "ab-inbev"
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
        print('Execução cancelada por erro')
        break
    except Exception as e:
      print('EXCEPTION - {}'.format(e))
      break
  return dfOdata

# COMMAND ----------

