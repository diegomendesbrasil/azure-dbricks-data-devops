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

sourceFile = 'WorkItemLinks'
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

# COMMAND ----------

# Lê o arquivo avro do path e salva em um spark dataframe

df = spark.read.parquet(sourcePath)

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

# Lê o arquivo em um novo Dataframe

DimWorkItemParentTemp = spark.read.parquet(sinkPath)

# COMMAND ----------

# Escreve o Dataframe no banco de dados

DimWorkItemParentTemp.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", url)\
    .option("dbtable", "dbo.WorkItemLinks")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

script = """
MERGE DimWorkItemParent AS DIM  
USING  (SELECT  WorkItemParentSourceId, WorkItemParentTargetId,   
    ParentTypeId, ParentTypeReferenceName, ParentTypeName, Comment, CreatedDate, ParentTypeIsAcyclic,   
    ParentTypeIsDirectional, AnalyticsUpdatedDate, IdProject  
  FROM (  
  SELECT  COALESCE(DATE1.DateKey, -1) AS CreatedDate,  
  COALESCE(DATE2.DateKey, -1) AS AnalyticsUpdatedDate,  
  SourceWorkItemId AS WorkItemParentSourceId,   
  TargetWorkItemId as WorkItemParentTargetId,   
  Comment,   
  COALESCE(PROJ.IDPROJECT, -1) AS IdProject,  
  LinkTypeId as ParentTypeId, LinkTypeReferenceName as ParentTypeReferenceName, LinkTypeName as ParentTypeName  
  , LinkTypeIsAcyclic as ParentTypeIsAcyclic, LinkTypeIsDirectional as ParentTypeIsDirectional  
  from WorkItemLinks TMP  
  --from DimWorkItemParentTemp TMP  
  LEFT JOIN [dbo].[DimProject] PROJ ON TMP.ProjectSK = PROJ.ProjectSK  
  LEFT JOIN DimDate DATE1 ON Convert(INT, CONVERT(varchar(10),TMP.CreatedDate, 112)) = DATE1.DateKey  
  LEFT JOIN DimDate DATE2 ON Convert(INT, CONVERT(varchar(10),TMP.AnalyticsUpdatedDate, 112)) = DATE2.DateKey  
  GROUP BY SourceWorkItemId, DATE2.DateKey, DATE1.DateKey, IdProject, Comment, TargetWorkItemId,   
  LinkTypeId, LinkTypeReferenceName, LinkTypeName, LinkTypeIsAcyclic, LinkTypeIsDirectional)    
  --DimWorkItemParentTemp  
  WorkItemLinks  
  ) AS SOURCE  
 ON (SOURCE.WorkItemParentSourceId = DIM.WorkItemParentSourceId and   
  SOURCE.WorkItemParentTargetId = DIM.WorkItemParentTargetId and  
  SOURCE.ParentTypeId = DIM.ParentTypeId and   
  SOURCE.IdProject = DIM.IdProject)  
WHEN NOT MATCHED   
 THEN INSERT (WorkItemParentSourceId, WorkItemParentTargetId, ParentTypeId, ParentTypeReferenceName, ParentTypeName,   
    Comment, CreatedDate, ParentTypeIsAcyclic, ParentTypeIsDirectional, AnalyticsUpdatedDate, IdProject)   
 VALUES (SOURCE.WorkItemParentSourceId, SOURCE.WorkItemParentTargetId,  
   SOURCE.ParentTypeId, SOURCE.ParentTypeReferenceName  
   , SOURCE.ParentTypeName, SOURCE.Comment, SOURCE.CreatedDate, SOURCE.ParentTypeIsAcyclic, SOURCE.ParentTypeIsDirectional  
   , SOURCE.AnalyticsUpdatedDate, SOURCE.IdProject)  
WHEN NOT MATCHED BY SOURCE  
THEN DELETE;
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

# Fim carga Consume WorkItemLinks
