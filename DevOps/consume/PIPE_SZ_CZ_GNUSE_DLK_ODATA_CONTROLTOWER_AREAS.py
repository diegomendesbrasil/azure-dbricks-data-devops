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

sourceFile = 'Areas'
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

# Lê o arquivo parquet do path e salva em um spark dataframe

df = spark.read.parquet(sourcePath)

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

# Lê o arquivo em um novo Dataframe

stgAreas = spark.read.parquet(sinkPath)

# COMMAND ----------

# Escreve o Dataframe no banco de dados


# stgAreas

stgAreas.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", url)\
    .option("dbtable", "dbo.stgAreas")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

script = """
MERGE [dbo].[DimWorkstream] AS DIM  
USING  (SELECT Workstream FROM   
  (SELECT AreaLevel2 AS Workstream   
  FROM stgAreas  
  WHERE AreaLevel2 IS NOT NULL  
  GROUP BY AreaLevel2  
  ) DimWorkstream) AS SOURCE  
 ON (SOURCE.Workstream = DIM.Workstream)   
WHEN NOT MATCHED   
 THEN INSERT (Workstream)   
 VALUES (SOURCE.Workstream);
"""

# COMMAND ----------

driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
connection = driver_manager.getConnection(url, user, password)
connection.prepareCall(script).execute()
connection.close()

# COMMAND ----------

script = """
MERGE [dbo].[DimArea] AS DIM  
USING  (SELECT AreaSK, AreaPath, IdTeam, IdProject, IdWorkstream,ScrumTeams  FROM   
   (SELECT AreaSK,   
     AreaPath,   
     COALESCE(TEAM.IDTEAM, -1) AS IdTeam,  
     COALESCE(PROJ.IDPROJECT, -1) AS IdProject,  
     COALESCE(WORK.IDWORKSTREAM, -1) as IdWorkstream,   
     AreaName as ScrumTeams  
   FROM stgAreas TMP  
   LEFT JOIN [dbo].[DimTeam] TEAM ON   
   CASE    
    WHEN TMP.AreaLevel3 IS NOT NULL  
    THEN   
    CASE  
     WHEN TMP.AreaLevel2 = 'Arch and Infra' THEN CONCAT('AI-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Analytics' THEN CONCAT('ANL-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Central Finance' THEN CONCAT('CFI-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Data' THEN CONCAT('DAT-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Logistics' THEN CONCAT('LOG-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Non SAP Integration (Leg)' THEN CONCAT('NSI-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Supply' THEN CONCAT('SUP-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Technical' THEN CONCAT('TEC-', TMP.AreaLevel3)  
     WHEN TMP.AreaLevel2 = 'Product Cycle Test' THEN CONCAT('PCT-', TMP.AreaLevel3)  
     ELSE CONCAT(TMP.AreaLevel2, '-', TMP.AreaLevel3)  
    END  
    WHEN TMP.AreaPath = 'Aurora_Program' THEN CONCAT('Aurora Program Team', TMP.AreaLevel2)  
   ELSE TMP.AreaLevel2 END = TEAM.TEAMNAME  
   LEFT JOIN [dbo].[DimProject] PROJ ON TMP.PROJECTSK = PROJ.PROJECTSK  
   LEFT JOIN [dbo].[DimWorkstream] WORK ON WORK.Workstream = TMP.AreaLevel2  
   GROUP BY AreaSK, AreaName, AreaPath, TEAM.IDTEAM, WORK.IDWORKSTREAM, AreaLevel3,AreaLevel2, PROJ.IDPROJECT )  
  DimArea) AS SOURCE  
 ON (SOURCE.AreaSK = DIM.AreaSK)  
WHEN MATCHED AND DIM.IdWorkstream <> SOURCE.IdWorkstream or DIM.AreaPath <> SOURCE.AreaPath or DIM.IdTeam <> SOURCE.IdTeam  
or DIM.IdProject <> SOURCE.IdProject or DIM.ScrumTeams <> SOURCE.ScrumTeams  
 THEN UPDATE SET DIM.IdWorkstream = SOURCE.IdWorkstream, DIM.AreaPath = SOURCE.AreaPath, DIM.IdTeam = SOURCE.IdTeam,  
 DIM.IdProject = SOURCE.IdProject , DIM.ScrumTeams = SOURCE.ScrumTeams   
WHEN NOT MATCHED   
 THEN INSERT (AreaSK, AreaPath, IdTeam, IdProject, IdWorkstream, ScrumTeams)   
 VALUES (SOURCE.AreaSK,SOURCE.AreaPath,SOURCE.IdTeam,SOURCE.IdProject,SOURCE.IdWorkstream, SOURCE.ScrumTeams);"""

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

# Fim carga Consume Areas
