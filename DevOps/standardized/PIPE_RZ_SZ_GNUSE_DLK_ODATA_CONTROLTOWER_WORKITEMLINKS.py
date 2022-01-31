# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'WorkItemLinks'
sourcePath = aurora_raw_folder + sourceFile + '/'

# COMMAND ----------

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data:
    max_data = i.name
    
sourcePath = sourcePath + max_data

# COMMAND ----------

sinkPath = aurora_standardized_folder + sourceFile + '/' + max_data

# COMMAND ----------

df = spark.read.json(sourcePath)

# COMMAND ----------

df = df.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

df = df\
  .withColumn('WorkItemLinkSK', df.WorkItemLinkSK.cast(IntegerType()))\
  .withColumn('SourceWorkItemId', df.SourceWorkItemId.cast(IntegerType()))\
  .withColumn('TargetWorkItemId', df.TargetWorkItemId.cast(IntegerType()))\
  .withColumn('CreatedDate', df.CreatedDate.cast(TimestampType()))\
  .withColumn('DeletedDate', df.DeletedDate.cast(TimestampType()))\
  .withColumn('Comment', df.Comment.cast(StringType()))\
  .withColumn('LinkTypeId', df.LinkTypeId.cast(IntegerType()))\
  .withColumn('LinkTypeReferenceName', df.LinkTypeReferenceName.cast(StringType()))\
  .withColumn('LinkTypeName', df.LinkTypeName.cast(StringType()))\
  .withColumn('LinkTypeIsAcyclic', df.LinkTypeIsAcyclic.cast(BooleanType()))\
  .withColumn('LinkTypeIsDirectional', df.LinkTypeIsDirectional.cast(BooleanType()))\
  .withColumn('ProjectSK', df.ProjectSK.cast(StringType()))\
  .withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))

# COMMAND ----------

df = df.distinct()

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

