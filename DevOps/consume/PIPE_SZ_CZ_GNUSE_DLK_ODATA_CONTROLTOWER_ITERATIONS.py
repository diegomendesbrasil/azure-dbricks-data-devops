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

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Assunto a ser buscado no Datalake e criação do path a ser utilizado para ler os arquivos

sourceFile = 'Iterations'
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

stgIterationSprint = spark.read.parquet(sinkPath)

# COMMAND ----------

# Escreve o Dataframe no banco de dados

stgIterationSprint.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", url)\
    .option("dbtable", "dbo.stgIterationSprint")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

script = """
MERGE DimIterationSprint AS DIM  
USING  (SELECT  IterationSprintStartDate, IterationSprintEndDate, IterationSprintSK, IterationSprintPath, IsSprintEnded,   
    IdProject, IterationSprintName  
  FROM (  
  SELECT  COALESCE(DATE1.DateKey, -1) AS IterationSprintStartDate,  
  COALESCE(DATE2.DateKey, -1) AS IterationSprintEndDate,  
  IterationSK AS IterationSprintSK,   
  IterationName AS IterationSprintName,   
  IterationPath AS IterationSprintPath,   
  IsEnded AS IsSprintEnded,   
  COALESCE(PROJ.IDPROJECT, -1) AS IdProject  
  from  stgIterationSprint TMP  
  LEFT JOIN [dbo].[DimProject] PROJ ON TMP.ProjectSK = PROJ.ProjectSK  
  LEFT JOIN DimDate DATE1 ON Convert(INT, CONVERT(varchar(10),TMP.StartDate, 112)) = DATE1.DateKey  
  LEFT JOIN DimDate DATE2 ON Convert(INT, CONVERT(varchar(10), DATEADD(day, -1, TMP.EndDate), 112)) = DATE2.DateKey  
  GROUP BY IterationSK, DATE2.DateKey, DATE1.DateKey, IterationName,   
  IterationPath, IsEnded, IdProject)    
  stgIterationSprint  
  ) AS SOURCE  
 ON (SOURCE.IterationSprintSK = DIM.IterationSprintSK)  
WHEN MATCHED AND DIM.IterationSprintStartDate <> SOURCE.IterationSprintStartDate or DIM.IterationSprintEndDate <> SOURCE.IterationSprintEndDate  
    or DIM.IterationSprintPath <> SOURCE.IterationSprintPath or DIM.IsSprintEnded <> SOURCE.IsSprintEnded   
    or DIM.IdProject <> SOURCE.IdProject or DIM.IterationSprintName <> SOURCE.IterationSprintName  
 THEN UPDATE SET DIM.IterationSprintStartDate = SOURCE.IterationSprintStartDate, DIM.IterationSprintEndDate = SOURCE.IterationSprintEndDate  
   , DIM.IterationSprintPath = SOURCE.IterationSprintPath, DIM.IsSprintEnded = SOURCE.IsSprintEnded   
   , DIM.IdProject = SOURCE.IdProject, DIM.IterationSprintName = SOURCE.IterationSprintName  
WHEN NOT MATCHED   
 THEN INSERT (IterationSprintStartDate, IterationSprintEndDate, IterationSprintSK, IterationSprintPath, IsSprintEnded,   
    IdProject, IterationSprintName)   
 VALUES (SOURCE.IterationSprintStartDate, SOURCE.IterationSprintEndDate, SOURCE.IterationSprintSK,   
   SOURCE.IterationSprintPath, SOURCE.IsSprintEnded, SOURCE.IdProject, SOURCE.IterationSprintName);
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

# Fim carga Consume Iterations
