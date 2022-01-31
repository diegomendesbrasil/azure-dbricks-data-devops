# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Iterations'
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
.withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))\
.withColumn('DataCarregamento', df.DataCarregamento.cast(TimestampType()))\
.withColumn('Depth', df.Depth.cast(IntegerType()))\
.withColumn('EndDate', df.EndDate.cast(TimestampType()))\
.withColumn('IsEnded', df.IsEnded.cast(StringType()))\
.withColumn('IterationId', df.IterationId.cast(StringType()))\
.withColumn('IterationLevel1', df.IterationLevel1.cast(StringType()))\
.withColumn('IterationLevel10', df.IterationLevel10.cast(StringType()))\
.withColumn('IterationLevel11', df.IterationLevel11.cast(StringType()))\
.withColumn('IterationLevel12', df.IterationLevel12.cast(StringType()))\
.withColumn('IterationLevel13', df.IterationLevel13.cast(StringType()))\
.withColumn('IterationLevel14', df.IterationLevel14.cast(StringType()))\
.withColumn('IterationLevel2', df.IterationLevel2.cast(StringType()))\
.withColumn('IterationLevel3', df.IterationLevel3.cast(StringType()))\
.withColumn('IterationLevel4', df.IterationLevel4.cast(StringType()))\
.withColumn('IterationLevel5', df.IterationLevel5.cast(StringType()))\
.withColumn('IterationLevel6', df.IterationLevel6.cast(StringType()))\
.withColumn('IterationLevel7', df.IterationLevel7.cast(StringType()))\
.withColumn('IterationLevel8', df.IterationLevel8.cast(StringType()))\
.withColumn('IterationLevel9', df.IterationLevel9.cast(StringType()))\
.withColumn('IterationName', df.IterationName.cast(StringType()))\
.withColumn('IterationPath', df.IterationPath.cast(StringType()))\
.withColumn('IterationSK', df.IterationSK.cast(StringType()))\
.withColumn('Number', df.Number.cast(IntegerType()))\
.withColumn('ProjectSK', df.ProjectSK.cast(StringType()))\
.withColumn('StartDate', df.StartDate.cast(TimestampType()))

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

