# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Areas'
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

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

