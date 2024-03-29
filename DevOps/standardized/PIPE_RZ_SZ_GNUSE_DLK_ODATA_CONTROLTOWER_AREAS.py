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

sourceFile = 'Areas'
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
.withColumn('ProjectSK', df.ProjectSK.cast(StringType()))\
.withColumn('AreaSK', df.AreaSK.cast(StringType()))\
.withColumn('AreaId', df.AreaId.cast(StringType()))\
.withColumn('AreaName', df.AreaName.cast(StringType()))\
.withColumn('Number', df.Number.cast(IntegerType()))\
.withColumn('AreaPath', df.AreaPath.cast(StringType()))\
.withColumn('AreaLevel1', df.AreaLevel1.cast(StringType()))\
.withColumn('AreaLevel2', df.AreaLevel2.cast(StringType()))\
.withColumn('AreaLevel3', df.AreaLevel3.cast(StringType()))\
.withColumn('AreaLevel4', df.AreaLevel4.cast(StringType()))\
.withColumn('AreaLevel5', df.AreaLevel5.cast(StringType()))\
.withColumn('AreaLevel6', df.AreaLevel6.cast(StringType()))\
.withColumn('AreaLevel7', df.AreaLevel7.cast(StringType()))\
.withColumn('AreaLevel8', df.AreaLevel8.cast(StringType()))\
.withColumn('AreaLevel9', df.AreaLevel9.cast(StringType()))\
.withColumn('AreaLevel10', df.AreaLevel10.cast(StringType()))\
.withColumn('AreaLevel11', df.AreaLevel11.cast(StringType()))\
.withColumn('AreaLevel12', df.AreaLevel12.cast(StringType()))\
.withColumn('AreaLevel13', df.AreaLevel13.cast(StringType()))\
.withColumn('AreaLevel14', df.AreaLevel14.cast(StringType()))\
.withColumn('Depth', df.Depth.cast(IntegerType()))\
.withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

# Fim carga Stand Areas
