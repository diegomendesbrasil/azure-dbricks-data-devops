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

sourcePath_diario = aurora_raw_folder + sourceFile + '_diario' + '/'

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

# COMMAND ----------

# Número de linhas e de colunas do dataframe com a carga full

num_linhas_full = df_full.count()

print(f'Número de colunas no df full: {len(df_full.columns)}')
print(f'Número de linhas no df full: {num_linhas_full}')

# COMMAND ----------

# Lê o arquivo parquet do path (carga full) e salva em um spark dataframe

df_diario = spark.read.format('avro').load(sourcePath_diario)

# COMMAND ----------

# Substitui campos None para None do Python

df_diario = df_diario.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

# Tipagem das colunas do Dataframe (Schema)

df_diario = df_diario\
  .withColumn('WorkItemLinkSK', df_diario.WorkItemLinkSK.cast(IntegerType()))\
  .withColumn('SourceWorkItemId', df_diario.SourceWorkItemId.cast(IntegerType()))\
  .withColumn('TargetWorkItemId', df_diario.TargetWorkItemId.cast(IntegerType()))\
  .withColumn('CreatedDate', df_diario.CreatedDate.cast(TimestampType()))\
  .withColumn('DeletedDate', df_diario.DeletedDate.cast(TimestampType()))\
  .withColumn('Comment', df_diario.Comment.cast(StringType()))\
  .withColumn('LinkTypeId', df_diario.LinkTypeId.cast(IntegerType()))\
  .withColumn('LinkTypeReferenceName', df_diario.LinkTypeReferenceName.cast(StringType()))\
  .withColumn('LinkTypeName', df_diario.LinkTypeName.cast(StringType()))\
  .withColumn('LinkTypeIsAcyclic', df_diario.LinkTypeIsAcyclic.cast(BooleanType()))\
  .withColumn('LinkTypeIsDirectional', df_diario.LinkTypeIsDirectional.cast(BooleanType()))\
  .withColumn('ProjectSK', df_diario.ProjectSK.cast(StringType()))\
  .withColumn('AnalyticsUpdatedDate', df_diario.AnalyticsUpdatedDate.cast(TimestampType()))

# COMMAND ----------

# Número de linhas e de colunas do dataframe com a carga diária

num_linhas_diario = df_diario.count()

print(f'Número de colunas no df diario: {len(df_diario.columns)}')
print(f'Número de linhas no df diario: {num_linhas_diario}')

# COMMAND ----------

# Colocando as colunas em ordem alfabética

df_full = df_full.select(
col('AnalyticsUpdatedDate'),
col('Comment'),
col('CreatedDate'),
col('DataCarregamento'),
col('DeletedDate'),
col('LinkTypeId'),
col('LinkTypeIsAcyclic'),
col('LinkTypeIsDirectional'),
col('LinkTypeName'),
col('LinkTypeReferenceName'),
col('ProjectSK'),
col('SourceWorkItemId'),
col('TargetWorkItemId'),
col('WorkItemLinkSK')
)

# COMMAND ----------

# Colocando as colunas em ordem alfabética

df_diario = df_diario.select(
col('AnalyticsUpdatedDate'),
col('Comment'),
col('CreatedDate'),
col('DataCarregamento'),
col('DeletedDate'),
col('LinkTypeId'),
col('LinkTypeIsAcyclic'),
col('LinkTypeIsDirectional'),
col('LinkTypeName'),
col('LinkTypeReferenceName'),
col('ProjectSK'),
col('SourceWorkItemId'),
col('TargetWorkItemId'),
col('WorkItemLinkSK')
)

# COMMAND ----------

# Merge dos dois dataframes

df_merge = df_full.union(df_diario)

# COMMAND ----------

# Criando uma coluna de nome rank, ranqueando os WorkItemId pela maior data em ChangedDateSK, ou seja, o mesmo WorkItemId terá o rank maior para aquele com a data de atualização mais recente

df_merge = df_merge.withColumn(
  'rank', dense_rank().over(Window.partitionBy('WorkItemLinkSK').orderBy(desc('AnalyticsUpdatedDate'), desc('DataCarregamento')))
)

# COMMAND ----------

print(f'{(df_merge.filter(df_merge.rank == 2)).count()} linhas serão excluidas, pois os seus WorkItemLinkSK correspondentes possuem atualizações')

# COMMAND ----------

# Criando o Dataframe final, filtrando apenas as linhas que resultaram em rank = 1 e retirando as colunas de controle

df = df_merge.filter(df_merge.rank == 1).drop('rank')
print(f'Qtd de colunas final: {len(df.columns)}')

num_linhas_final = df.count()

print(f'Qtd de linhas final: {num_linhas_final}')

print(f'Originalmente havia {num_linhas_full} linhas na tabela full e {num_linhas_diario} linhas na carga diaria. {num_linhas_final - num_linhas_full} linhas foram adicionadas.')

# COMMAND ----------

print('QTD LINHAS ANTES DO DISTINCT: ', df.count())
df = df.distinct()
print('QTD LINHAS DEPOIS DO DISTINCT: ', df.count())

# COMMAND ----------

# Salva a tabela de volta em modo parquet no caminho especificado

sinkPath = aurora_standardized_folder + sourceFile + '/' + max_data
print(sinkPath)
