# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Areas'
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

#urlLeitura = f'jdbc:sqlserver://{host_name}:{port};databaseName={database}'

#dfDimWorkstream = (spark
  #.read
  #.format("jdbc")
  #.option("url", urlLeitura)
  #.option("dbtable", 'dbo.DimWorkstream')
  #.option("user", user)
  #.option("password", password)
  #.load()
#)

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

