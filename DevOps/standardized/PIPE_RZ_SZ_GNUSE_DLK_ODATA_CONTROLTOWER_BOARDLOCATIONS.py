# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'BoardLocations'
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
.withColumn('BacklogType', df. BacklogType.cast(StringType()))\
.withColumn('BoardCategoryReferenceName', df. BoardCategoryReferenceName.cast(StringType()))\
.withColumn('BoardId', df. BoardId.cast(StringType()))\
.withColumn('BoardLevel', df. BoardLevel.cast(IntegerType()))\
.withColumn('BoardLocationSK', df. BoardLocationSK.cast(IntegerType()))\
.withColumn('BoardName', df. BoardName.cast(StringType()))\
.withColumn('ChangedDate', df. ChangedDate.cast(TimestampType()))\
.withColumn('ColumnId', df. ColumnId.cast(StringType()))\
.withColumn('ColumnItemLimit', df. ColumnItemLimit.cast(IntegerType()))\
.withColumn('ColumnName', df. ColumnName.cast(StringType()))\
.withColumn('ColumnOrder', df. ColumnOrder.cast(IntegerType()))\
.withColumn('DataCarregamento', df. DataCarregamento.cast(TimestampType()))\
.withColumn('Done', df. Done.cast(StringType()))\
.withColumn('IsBoardVisible', df. IsBoardVisible.cast(StringType()))\
.withColumn('IsColumnSplit', df. IsColumnSplit.cast(StringType()))\
.withColumn('IsCurrent', df. IsCurrent.cast(StringType()))\
.withColumn('IsDefaultLane', df. IsDefaultLane.cast(StringType()))\
.withColumn('IsDone', df. IsDone.cast(StringType()))\
.withColumn('LaneId', df. LaneId.cast(StringType()))\
.withColumn('LaneName', df. LaneName.cast(StringType()))\
.withColumn('LaneOrder', df. LaneOrder.cast(IntegerType()))\
.withColumn('ProjectSK', df. ProjectSK.cast(StringType()))\
.withColumn('RevisedDate', df. RevisedDate.cast(TimestampType()))\
.withColumn('TeamSK', df. TeamSK.cast(StringType()))

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

