# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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

sourceFile = 'WorkItemLinks'
sourcePath = aurora_standardized_folder + sourceFile + '/'
print(sourcePath)

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data and '.parquet' not in i.name:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Assunto a ser buscado no Datalake (apenas dados da carga diária) e criação do path a ser utilizado para ler os arquivos

sourcePath_diario = aurora_raw_folder + sourceFile + '/'

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath_diario):
  if i.name > max_data:
    max_data = i.name
    
sourcePath_diario = sourcePath_diario + max_data
print(sourcePath_diario)

# COMMAND ----------

# Lê o arquivo parquet do path (carga full) e salva em um spark dataframe

df_full = spark.read.parquet(sourcePath)
