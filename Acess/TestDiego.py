# Databricks notebook source
import base64
import json
import requests
import time

from pyspark.sql.functions import regexp_replace, date_format
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import initcap, lit
from pandas import DataFrame
from re import findall



# COMMAND ----------

display(dbutils.fs.ls("mnt/sit/output/ListaDeSuitesIDs.parquet/"))

# COMMAND ----------

df = dbutils.fs.head("/mnt/sit/input/ListaDeSuitesIDs.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC var DFSuites = spark
# MAGIC   .read
# MAGIC   .option("header","true")
# MAGIC   .csv("/mnt/sit/input/ListaDeSuitesIDs.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(DFSuites)

# COMMAND ----------

# MAGIC %scala
# MAGIC DFSuites
# MAGIC   .write
# MAGIC   .option("header","true")
# MAGIC   .option("dateFormat","yyyy-MM-dd")
# MAGIC   .mode(SaveMode.Overwrite)
# MAGIC   .parquet("mnt/sit/output/ListaDeSuitesIDs.parquet")