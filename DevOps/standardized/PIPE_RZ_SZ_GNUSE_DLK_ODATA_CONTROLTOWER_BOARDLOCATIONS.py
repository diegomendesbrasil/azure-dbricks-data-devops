# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

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

sourceFile = 'BoardLocations'
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
.withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))\
.withColumn('BacklogType', df. BacklogType.cast(StringType()))\
.withColumn('BoardCategoryReferenceName', df. BoardCategoryReferenceName.cast(StringType()))\
.withColumn('BoardId', df. BoardId.cast(StringType()))\
.withColumn('BoardLevel', df. BoardLevel.cast(IntegerType()))\
.withColumn('BoardLocationSK', df. BoardLocationSK.cast(IntegerType()))\
.withColumn('BoardName', df. BoardName.cast(StringType()))\
.withColumn('ChangedDate', df. ChangedDate.cast(TimestampType()))\
.withColumn('ColumnId', df. ColumnId.cast(StringType()))\
.withColumn('ColumnItemLimit', df. ColumnItemLimit.cast(IntegerType()))\
.withColumn('ColumnName', df. ColumnName.cast(StringType()))\
.withColumn('ColumnOrder', df. ColumnOrder.cast(IntegerType()))\
.withColumn('DataCarregamento', df. DataCarregamento.cast(TimestampType()))\
.withColumn('Done', df. Done.cast(StringType()))\
.withColumn('IsBoardVisible', df. IsBoardVisible.cast(StringType()))\
.withColumn('IsColumnSplit', df. IsColumnSplit.cast(StringType()))\
.withColumn('IsCurrent', df. IsCurrent.cast(StringType()))\
.withColumn('IsDefaultLane', df. IsDefaultLane.cast(StringType()))\
.withColumn('IsDone', df. IsDone.cast(StringType()))\
.withColumn('LaneId', df. LaneId.cast(StringType()))\
.withColumn('LaneName', df. LaneName.cast(StringType()))\
.withColumn('LaneOrder', df. LaneOrder.cast(IntegerType()))\
.withColumn('ProjectSK', df. ProjectSK.cast(StringType()))\
.withColumn('RevisedDate', df. RevisedDate.cast(TimestampType()))\
.withColumn('TeamSK', df. TeamSK.cast(StringType()))

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

# Fim carga Stand BoardLocations
