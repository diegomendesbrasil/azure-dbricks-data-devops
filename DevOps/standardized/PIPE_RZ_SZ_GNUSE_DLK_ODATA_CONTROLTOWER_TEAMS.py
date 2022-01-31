# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Teams'
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

df = df.distinct()

# COMMAND ----------

df = df.select(
  'ProjectSK',
  'TeamId',
  'TeamSK',
  'TeamName',
  'AnalyticsUpdatedDate',
  'DataCarregamento'
)

# COMMAND ----------

df = df \
.withColumn('ProjectSK', df.ProjectSK.cast(StringType()))\
.withColumn('TeamId', df.TeamId.cast(StringType()))\
.withColumn('TeamName', df.TeamName.cast(StringType()))\
.withColumn('TeamSK', df.TeamSK.cast(StringType()))\
.withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))\
.withColumn('DataCarregamento', df.DataCarregamento.cast(TimestampType()))

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

