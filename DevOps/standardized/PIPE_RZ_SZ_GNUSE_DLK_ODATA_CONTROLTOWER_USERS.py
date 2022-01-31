# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Users'
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
  'UserSK',
  'UserId',
  'UserName',
  'UserType',
  'UserEmail',
  'GitHubUserId',
  'AnalyticsUpdatedDate',
  'DataCarregamento'
)

# COMMAND ----------

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

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

