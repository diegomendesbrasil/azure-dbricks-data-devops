# Databricks notebook source
# Importanto bibliotecas

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

from datetime import datetime, timedelta

# Hora de início do processamento do notebook
start_time = datetime.now()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notebook de configurações e funções

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Assunto a ser buscado na API

sourceFile = 'WorkItemLinks'

# COMMAND ----------

# Cria string contendo a data de hoje e a usa como filtro na coluna ChangedDate

#hoje = datetime.now().strftime("%Y-%m-%d") EXEMPLO DE COMO PEGAR O 'HOJE'
reprocessamento = ''
ontem = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d") 

if reprocessamento != '':
  data_corte = reprocessamento
else:
  data_corte = ontem

print(f'A data a ser utilizada no filtro é: {data_corte}')

# COMMAND ----------

# Busca os dados na API filtrando o ChangedDate por data_corte e retorna um pandas dataframe. Ao final do processamento é exibido quantas linhas o dataframe possui

dfOdata = getDadosDiarioAuroraAPI(sourceFile, data_corte)

# COMMAND ----------

# Captura a data/hora atual e insere como nova coluna no dataframe

horaAtual = (datetime.now() - pd.DateOffset(hours=3)).strftime("%Y-%m-%d_%H_%M_%S")
dfOdata['DataCarregamento'] = horaAtual

# COMMAND ----------

# Cria o path onde será salvo o arquivo. Padrão: zona do datalake /assunto do notebook / yyyy-mm-dd_hh_mm_ss

sinkPath = aurora_raw_folder + sourceFile + '_diario/' + horaAtual

# COMMAND ----------

# Transforma o pandas dataframe em spark dataframe

df = spark.createDataFrame(dfOdata.astype(str))

# COMMAND ----------

# Salva a tabela em modo avro no caminho especificado

df.write.mode('overwrite').format('avro').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

# Fim carga Raw WorkItemLinks Diario
