# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Processes'
sourcePath = aurora_standardized_folder + sourceFile + '/'

# COMMAND ----------

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

sinkPath = aurora_consume_folder + sourceFile

# COMMAND ----------

df = spark.read.parquet(sourcePath)

# COMMAND ----------

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

DimProcesses = spark.read.parquet(sinkPath)

# COMMAND ----------

urlgrava = f'jdbc:sqlserver://{host_name}:{port};databaseName={database}'

DimProcesses.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", urlgrava)\
    .option("dbtable", "dbo.DimProcesses")\
    .option("user", user)\
    .option("password", password)\
    .save()