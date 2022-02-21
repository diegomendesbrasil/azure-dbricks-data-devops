# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd

# Hora de início do processamento do notebook
start_time = datetime.now()

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

stgProcesses = spark.read.parquet(sinkPath)

# COMMAND ----------

# Escreve o Dataframe no banco de dados

#stgProcesses.write\
    #.format("jdbc")\
    #.mode("overwrite")\
    #.option("url", url)\
    #.option("dbtable", "dbo.stgProcesses")\
    #.option("user", user)\
    #.option("password", password)\
    #.save()

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

# Fim carga Consume Processes
