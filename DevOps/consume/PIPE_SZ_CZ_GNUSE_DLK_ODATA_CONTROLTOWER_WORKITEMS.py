# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notebook de configurações e funções

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Assunto a ser buscado no Datalake e criação do path a ser utilizado para ler os arquivos

sourceFile = 'WorkItems'
sourcePath = aurora_standardized_folder + sourceFile + '/'

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Cria o path onde será salvo o arquivo. Padrão: zona do datalake / assunto do notebook

sinkPath = aurora_consume_folder + sourceFile
print(sinkPath)

# COMMAND ----------

# Lê o arquivo avro do path e salva em um spark dataframe

df = spark.read.parquet(sourcePath)

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gravação no Banco de Dados para atividade de contigência
# MAGIC 1. Busca dados gravados na consume zone
# MAGIC 2. Cria um DataFrame com esses dados
# MAGIC 3. Grava os dados na primeira tabela do processo de carga de workItems

# COMMAND ----------

host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DatabaseName")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DBPass")

# COMMAND ----------

DimWorkItem = spark.read.parquet(sinkPath)

# COMMAND ----------

urlgrava = f'jdbc:sqlserver://{host_name}:{port};databaseName={database}'

DimWorkItem.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", urlgrava)\
    .option("dbtable", "dbo.DimWorkItemTemp")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

df.count()

# COMMAND ----------

# Fim carga Consume WorkItems
