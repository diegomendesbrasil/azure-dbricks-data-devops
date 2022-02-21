# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd

# Hora de início do processamento do notebook
start_time = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notebook de configurações e funções

# COMMAND ----------

# MAGIC 
# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Assunto a ser buscado no Datalake e criação do path a ser utilizado para ler os arquivos

sourceFile = 'Projects'
sourcePath = aurora_standardized_folder + sourceFile + '/'

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Cria o path onde será salvo o arquivo. Padrão: zona do datalake / assunto do notebook

sinkPath = aurora_consume_folder + sourceFile
print(sinkPath)

# COMMAND ----------

# Lê o arquivo avro do path e salva em um spark dataframe

df = spark.read.parquet(sourcePath)

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

# Lê o arquivo em um novo Dataframe

stgProject = spark.read.parquet(sinkPath)

# COMMAND ----------

# Escreve o Dataframe no banco de dados

stgProject.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", url)\
    .option("dbtable", "dbo.stgProject")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

script = """
MERGE [dbo].[DimProject] AS DIM  
USING  (SELECT ProjectSK, ProjectName FROM stgProject WHERE ProjectName IS NOT NULL GROUP BY ProjectName, ProjectSK) AS SOURCE  
 ON (SOURCE.ProjectSK = DIM.ProjectSK)  
WHEN MATCHED AND DIM.ProjectName <> SOURCE.ProjectName  
 THEN UPDATE SET DIM.ProjectName = SOURCE.ProjectName   
WHEN NOT MATCHED   
 THEN INSERT (ProjectSK, ProjectName)   
 VALUES (SOURCE.ProjectSK,SOURCE.ProjectName);
"""

# COMMAND ----------

driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
connection = driver_manager.getConnection(url, user, password)
connection.prepareCall(script).execute()
connection.close()

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

update_log(sourceFile, 'STANDARDIZED', 'CONSUME', duracao_notebook, df.count(), 3)

# COMMAND ----------

# Fim carga Consume Project
