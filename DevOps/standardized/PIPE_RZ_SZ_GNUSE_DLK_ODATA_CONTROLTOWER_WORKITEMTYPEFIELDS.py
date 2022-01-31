# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'WorkItemTypeFields'
sourcePath = aurora_raw_folder + sourceFile + '/'

# COMMAND ----------

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

sinkPath = aurora_standardized_folder + sourceFile + '/' + max_data

# COMMAND ----------

df = spark.read.json(sourcePath)

# COMMAND ----------

df = df.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

df = df.select(
  'ProjectSK',
  'FieldName',
  'FieldReferenceName',
  'FieldType',
  'WorkItemType',
  'DataCarregamento'
)

# COMMAND ----------

df = df\
  .withColumn('FieldName', df.FieldName.cast(StringType()))\
  .withColumn('FieldReferenceName', df.FieldReferenceName.cast(StringType()))\
  .withColumn('FieldType', df.FieldType.cast(StringType()))\
  .withColumn('ProjectSK', df.ProjectSK.cast(StringType()))\
  .withColumn('WorkItemType', df.WorkItemType.cast(StringType()))\
  .withColumn('DataCarregamento', df.DataCarregamento.cast(TimestampType()))

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

