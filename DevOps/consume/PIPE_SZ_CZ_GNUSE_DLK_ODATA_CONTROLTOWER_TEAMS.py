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

sourceFile = 'Teams'
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

stgTeam = spark.read.parquet(sinkPath)

# COMMAND ----------

# Escreve o Dataframe no banco de dados

stgTeam.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", url)\
    .option("dbtable", "dbo.stgTeam")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

script = """
MERGE [dbo].[DimTeam] AS DIM  
USING  (SELECT S.TeamSK,   
  CASE  
   WHEN S.TeamName = 'GRC-Business Controls' THEN 'GRC-Business Control'  
   WHEN S.TeamName = 'GRC-IT Controls' THEN 'GRC-IT Control'  
   WHEN S.TeamName = 'GRC-UAM Access Mapping' THEN 'GRC-UAM.Access Mapping'  
   WHEN S.TeamName = 'GRC-UAM SAP Security' THEN 'GRC-UAM.Sap Security'  
   WHEN S.TeamName = 'OTC-Finance' THEN 'OTC-Finance.Assets Lending'  
   WHEN S.TeamName = 'OTC-Sales' THEN 'OTC-Sales Order.Billing'  
   WHEN S.TeamName = 'PCT - Test 1' THEN 'Product Cycle Test-Test 1'  
   WHEN S.TeamName = 'TAX-SAP' THEN 'TAX-Tax SAP'  
   WHEN S.TeamName = 'TAX-Taxweb' THEN 'TAX-Tax Web'  
   WHEN S.TeamName = 'Archived-LOG-Charge Management' THEN 'LOG-Charge Management'  
   WHEN S.TeamName = 'Archived-LOG-Empties' THEN 'LOG-Empties'  
   WHEN S.TeamName = 'Archived-LOG-Maintenance' THEN 'LOG-Maintenance'  
   WHEN S.TeamName = 'Archived-LOG-Planning DP.SNP' THEN 'LOG-Planning DP.SNP'  
   WHEN S.TeamName = 'Archived-LOG-Planning MRP' THEN 'LOG-Planning MRP'  
   WHEN S.TeamName = 'Archived-LOG-Quality' THEN 'LOG-Quality'  
   WHEN S.TeamName = 'Archived-LOG-Transportation T2' THEN 'LOG-Transportation T2'  
   WHEN S.TeamName = 'Archived-LOG-WM T1' THEN 'LOG-WM T1'  
  ELSE S.TeamName  
  END TeamName  
  FROM  
   (SELECT TeamSK, REPLACE(TeamName, ' - ', '-') as TeamName  
    FROM stgTeam WHERE TeamName IS NOT NULL GROUP BY TeamName, TeamSK) AS S)   
  AS SOURCE  
  ON (SOURCE.TeamSK = DIM.TeamSK)  
WHEN MATCHED AND DIM.TeamName <> SOURCE.TeamName  
 THEN UPDATE SET DIM.TeamName = SOURCE.TeamName   
WHEN NOT MATCHED   
 THEN INSERT (TeamSK, TeamName)   
 VALUES (SOURCE.TeamSK,SOURCE.TeamName);
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

# Fim carga Consume Teams
