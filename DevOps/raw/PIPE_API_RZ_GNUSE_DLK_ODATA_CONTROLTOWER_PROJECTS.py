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
#from fastparquet import write 
import pandas as pd
from pyspark.sql.types import StringType

from datetime import datetime


# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

sourceFile = 'Projects'

# COMMAND ----------

dfOdata = getDadosAuroraAPI(sourceFile)

# COMMAND ----------

horaAtual = (datetime.now() - pd.DateOffset(hours=3)).strftime("%Y-%m-%d_%H_%M_%S")
dfOdata['DataCarregamento'] = horaAtual

# COMMAND ----------

sinkPath = aurora_raw_folder + sourceFile + '/' + horaAtual

# COMMAND ----------

df = spark.createDataFrame(dfOdata.astype(str))

# COMMAND ----------

df.write.mode('overwrite').format('json').save(sinkPath)