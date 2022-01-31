# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Processes'
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

df = df\
.withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))\
.withColumn('BacklogCategoryReferenceName', df.BacklogCategoryReferenceName.cast(StringType()))\
.withColumn('BacklogLevel', df.BacklogLevel.cast(IntegerType()))\
.withColumn('BacklogName', df.BacklogName.cast(StringType()))\
.withColumn('BacklogType', df.BacklogType.cast(StringType()))\
.withColumn('DataCarregamento', df.DataCarregamento.cast(TimestampType()))\
.withColumn('HasBacklog', df.HasBacklog.cast(StringType()))\
.withColumn('IsBugType', df.IsBugType.cast(StringType()))\
.withColumn('IsDeleted', df.IsDeleted.cast(StringType()))\
.withColumn('IsHiddenType', df.IsHiddenType.cast(StringType()))\
.withColumn('ProcessSK', df.ProcessSK.cast(IntegerType()))\
.withColumn('ProjectSK', df.ProjectSK.cast(StringType()))\
.withColumn('TeamSK', df.TeamSK.cast(StringType()))\
.withColumn('WorkItemType', df.WorkItemType.cast(StringType()))

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

