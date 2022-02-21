# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd

# Hora de início do processamento do notebook
start_time = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notebook de configurações e funções

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Assunto a ser buscado no Datalake e criação do path a ser utilizado para ler os arquivos

sourceFile = 'Users'
sourcePath = aurora_raw_folder + sourceFile + '/'

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data and '.parquet' not in i.name:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Cria o path onde será salvo o arquivo. Padrão: zona do datalake /assunto do notebook / yyyy-mm-dd_hh_mm_ss

sinkPath = aurora_standardized_folder + sourceFile + '/' + max_data

# COMMAND ----------

# Lê o arquivo avro do path e salva em um spark dataframe

df = spark.read.format('avro').load(sourcePath)

# COMMAND ----------

# Substitui campos None para None do Python

df = df.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

# Mantém apenas linhas distintas no Dataframe

df = df.distinct()

# COMMAND ----------

# Tipagem das colunas do Dataframe (Schema)

df = df \
.withColumn('UserSK', df.UserSK.cast(StringType()))\
.withColumn('UserId', df.UserId.cast(StringType()))\
.withColumn('UserName', df.UserName.cast(StringType()))\
.withColumn('UserType', df.UserType.cast(StringType()))\
.withColumn('UserEmail', df.UserEmail.cast(StringType()))\
.withColumn('GitHubUserId', df.GitHubUserId.cast(StringType()))\
.withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))\
.withColumn('DataCarregamento', df.DataCarregamento.cast(TimestampType()))

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

# Fim carga Stand Users
