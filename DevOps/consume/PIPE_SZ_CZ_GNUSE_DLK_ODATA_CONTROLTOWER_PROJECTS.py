# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notebook de configurações e funções

# COMMAND ----------

# MAGIC 
# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Assunto a ser buscado no Datalake e criação do path a ser utilizado para ler os arquivos

sourceFile = 'Projects'
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

# Lê o arquivo em um novo Dataframe

DimProjectTemp = spark.read.parquet(sinkPath)

# COMMAND ----------

# Escreve o Dataframe no banco de dados

DimProjectTemp.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", url)\
    .option("dbtable", "dbo.DimProjectTemp")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

# Fim carga Consume Project
