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

from datetime import datetime

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

sourceFile = 'WorkItems'

# COMMAND ----------

# Lista de colunas a serem buscadas na API

colunas_escolhidas = ['ABIProprietaryMethodology_IssueCreatedDate','ABIProprietaryMethodology_IssueOwnerSK','ActivatedByUserSK','ActivatedDateSK','Activity','AreaSK','AssignedToUserSK','AutomatedTestId','AutomatedTestName',
'AutomatedTestStorage','AutomatedTestType','AutomationStatus','BacklogPriority','Blocked','BusinessValue','ChangedByUserSK','ClosedByUserSK','ClosedDateSK','CommentCount','CreatedByUserSK','CreatedDateSK','Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866','Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422','Custom_AccordingwithSOXcontrols','Custom_AssociatedProject','Custom_Aurora_Category','Custom_Automation','Custom_BlockedJustification','Custom_BuC_Approved_Test_Result','Custom_BuC_Approved_Test_Script','Custom_BuC_Impacted','Custom_BuildType','Custom_BusinessProcessImpact','Custom_BusinessRolesPersona','Custom_Centralization','Custom_Complexity','Custom_Contingency','Custom_Control_Name','Custom_Criticality','Custom_DateIdentified','Custom_Deadline','Custom_Dependency','Custom_DependencyConsumer','Custom_DependencyProvider','Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8','Custom_E2E_Name','Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e','Custom_Environment_Field_TEC','Custom_EscalationNeeded','Custom_Executed_Today','Custom_FeatureCategory','Custom_FeatureType','Custom_FioriApp_System','Custom_FIT_GAP','Custom_FS_Submitted_for_CPO_Approval','Custom_FS_Submitted_for_GRC_M_Approval','Custom_FS_Submitted_for_GRC_SOX_Approval','Custom_FS_Submitted_for_GRC_Testing_Approval','Custom_FS_Submitted_for_PO_Approval','Custom_FS_Submitted_for_QA_Approval','Custom_FS_Submitted_for_TI_Approval','Custom_FSID','Custom_FutureRelease','Custom_GAP_Reprioritization','Custom_GAPAPPID','Custom_GAPCategory','Custom_GAPEstimation','Custom_GAPNumber','Custom_Glassif','Custom_Global_Local_Escope','Custom_GRC_BUC_APPROVED','Custom_GRC_TESTING_APPROVED','Custom_GRC_UAM_APPROVED','Custom_GUID','Custom_IndexProcess','Custom_IsReportRequired','Custom_IssuePriority','Custom_L2','Custom_L2Product','Custom_L3','Custom_L3Product','Custom_L4Product','Custom_L4WorkShopDescription','Custom_LE_December','Custom_LE_January','Custom_LE_November','Custom_LE_October','Custom_LeadershipApproval','Custom_Legacies_Integration','Custom_LegaciesDescription','Custom_Main_Blocker','Custom_MDG_Relevant','Custom_MoscowPriority','Custom_Name_Of_Environment','Custom_NonSAP','Custom_NoSAPApprovalJustification','Custom_NotAllowedtothisState','Custom_OD_LE_Dec','Custom_OD_LE_Nov','Custom_OD_LE_Oct','Custom_OD_Submitted_for_Consultant_Approval','Custom_OD_Submitted_for_CPO_Approval','Custom_OD_Submitted_for_DA_Approval_CCPO','WorkItemId','ChangedDateSK']

# COMMAND ----------

# Busca os dados na API filtrando as colunas por colunas_escolhidas e retorna um pandas dataframe. Ao final do processamento é exibido quantas linhas o dataframe possui

dfOdata = getDadosAuroraAPI(sourceFile, colunas = colunas_escolhidas)

# COMMAND ----------

# Captura a data/hora atual e insere como nova coluna no dataframe

horaAtual = (datetime.now() - pd.DateOffset(hours=3)).strftime("%Y-%m-%d_%H_%M_%S")
dfOdata['DataCarregamento'] = horaAtual

# COMMAND ----------

# Cria o path onde será salvo o arquivo. Padrão: zona do datalake /assunto do notebook / yyyy-mm-dd_hh_mm_ss

sinkPath = aurora_raw_folder + sourceFile + '_P1/' + horaAtual

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

# Fim carga Raw WorkItems Parte 1
