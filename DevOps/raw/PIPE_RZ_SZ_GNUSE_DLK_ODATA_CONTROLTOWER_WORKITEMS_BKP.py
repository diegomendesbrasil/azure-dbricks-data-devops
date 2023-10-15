# Databricks notebook source
#pip install fastparquet

# COMMAND ----------


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

# MAGIC %md
# MAGIC ##### CONEXÃO COM O BANCO DE DADOS

# COMMAND ----------

#CONEXÃO COM O BANCO DE DADOS
StartProcess = time.time()

host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DatabaseName")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DBPass")

url = f'jdbc:sqlserver://{host_name}:{port};databaseName={database};user={user};password={password}' 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Busca Dados da API

# COMMAND ----------

Lista = ['WorkItems']

# COMMAND ----------

colunas_escolhidas = [
  'ABIProprietaryMethodology_IssueCreatedDate',
  'ABIProprietaryMethodology_IssueOwnerSK',
  'ActivatedByUserSK',
  'ActivatedDateSK',
  'Activity',
  'AreaSK',
  'AssignedToUserSK',
  'AutomatedTestId',
  'AutomatedTestName',
  'AutomatedTestStorage',
  'AutomatedTestType',
  'AutomationStatus',
  'BacklogPriority',
  'Blocked',
  'BusinessValue',
  'ChangedByUserSK',
  'ChangedDateSK',
  'ClosedByUserSK',
  'ClosedDateSK',
  'CommentCount',
  'CreatedByUserSK',
  'CreatedDateSK',
  'Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866',
  'Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422',
  'Custom_AccordingwithSOXcontrols',
  'Custom_AssociatedProject',
  'Custom_Aurora_Category',
  'Custom_Automation',
  'Custom_BlockedJustification',
  'Custom_BuC_Approved_Test_Result',
  'Custom_BuC_Approved_Test_Script',
  'Custom_BuC_Impacted',
  'Custom_BuildType',
  'Custom_BusinessProcessImpact',
  'Custom_BusinessRolesPersona',
  'Custom_Centralization',
  'Custom_Complexity',
  'Custom_Contingency',
  'Custom_Control_Name',
  'Custom_Criticality',
  'Custom_DateIdentified',
  'Custom_Deadline',
  'Custom_Dependency',
  'Custom_DependencyConsumer',
  'Custom_DependencyProvider',
  'Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8',
  'Custom_E2E_Name',
  'Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e',
  'Custom_Environment_Field_TEC',
  'Custom_EscalationNeeded',
  'Custom_Executed_Today',
  'Custom_FeatureCategory',
  'Custom_FeatureType',
  'Custom_FioriApp_System',
  'Custom_FIT_GAP',
  'Custom_FS_Submitted_for_CPO_Approval',
  'Custom_FS_Submitted_for_GRC_M_Approval',
  'Custom_FS_Submitted_for_GRC_SOX_Approval',
  'Custom_FS_Submitted_for_GRC_Testing_Approval',
  'Custom_FS_Submitted_for_PO_Approval',
  'Custom_FS_Submitted_for_QA_Approval',
  'Custom_FS_Submitted_for_TI_Approval',
  'Custom_FSID',
  'Custom_FutureRelease',
  'Custom_GAP_Reprioritization',
  'Custom_GAPAPPID',
  'Custom_GAPCategory',
  'Custom_GAPEstimation',
  'Custom_GAPNumber',
  'Custom_Glassif',
  'Custom_Global_Local_Escope',
  'Custom_GRC_BUC_APPROVED',
  'Custom_GRC_TESTING_APPROVED',
  'Custom_GRC_UAM_APPROVED',
  'Custom_GUID',
  'Custom_IndexProcess',
  'Custom_IsReportRequired',
  'Custom_IssuePriority',
  'Custom_L2',
  'Custom_L2Product',
  'Custom_L3',
  'Custom_L3Product',
  'Custom_L4Product',
  'Custom_L4WorkShopDescription',
  'Custom_LE_December',
  'Custom_LE_January',
  'Custom_LE_November',
  'Custom_LE_October',
  'Custom_LeadershipApproval',
  'Custom_Legacies_Integration',
  'Custom_LegaciesDescription',
  'Custom_Main_Blocker',
  'Custom_MDG_Relevant',
  'Custom_MoscowPriority',
  'Custom_Name_Of_Environment',
  'Custom_NonSAP',
  'Custom_NoSAPApprovalJustification',
  'Custom_NotAllowedtothisState',
  'Custom_OD_LE_Dec',
  'Custom_OD_LE_Nov',
  'Custom_OD_LE_Oct',
  'Custom_OD_Submitted_for_Consultant_Approval',
  'Custom_OD_Submitted_for_CPO_Approval',
  'Custom_OD_Submitted_for_DA_Approval_CCPO',
  'Custom_OD_Submitted_for_PO_Approval',
  'Custom_OD_Submitted_for_TI_Approval',
  'Custom_OD_Target_Dec',
  'Custom_OD_Target_Nov',
  'Custom_OD_Target_Oct',
  'Custom_OD_YTD_Dec',
  'Custom_OD_YTD_Nov',
  'Custom_OD_YTD_Oct',
  'Custom_OpenDesignID',
  'Custom_OpenDesignType',
  'Custom_Out_Scope',
  'Custom_OutsideAuroraStartDate',
  'Custom_OverviewNonSAP',
  'Custom_PartnerResponsible',
  'Custom_PartnerStatus',
  'Custom_PlannedDate',
  'Custom_PotentialImpact',
  'Custom_PriorityforRelease',
  'Custom_PriorityRank',
  'Custom_ProbabilityofOccurrence',
  'Custom_Product_Field_TEC',
  'Custom_ReasonforFutureRelease',
  'Custom_ResolvedTargetDate',
  'Custom_RiskLevel',
  'Custom_RiskorIssuesCategory',
  'Custom_RiskorIssuesEscalationLevel',
  # -------------------------- LIMITE DE 3.000 CARACTERES -------------------------------
  'Custom_SAPAdherence',
  'Custom_SAPRole',
  'Custom_Scenario',
  'Custom_Scenario_Name',
  'Custom_Scenario_Order',
  'Custom_ScopeItem',
  'Custom_SolutionType',
  'Custom_Source',
  'Custom_StoryType',
  'Custom_SystemName',
  'Custom_Target_Dec',
  'Custom_Target_Jan',
  'Custom_Target_Nov',
  'Custom_Target_Oct',
  'Custom_TargetDateFuncSpec',
  'Custom_TC_Automation_status',
  'Custom_Test_Case_ExecutionValidation',
  'Custom_Test_Case_Phase',
  'Custom_Test_Suite_Phase',
  'Custom_TestScenario',
  'Custom_UC_Submitted_for_CPO_Approval',
  'Custom_UC_Submitted_for_DA_Approval_CCPO',
  'Custom_UC_Submitted_for_GRC_Validation_PO',
  'Custom_UC_Submitted_for_PO_approval',
  'Custom_UC_Submitted_for_SAP_Approval',
  'Custom_UC_Submitted_for_TI_Approval',
  'Custom_US_Priority',
  'Custom_UserStoryAPPID',
  'Custom_UserStoryType',
  'Custom_Work_Team',
  'Custom_Workshop_ID',
  'Custom_Workshoptobepresented',
  'Custom_WRICEF',
  'Custom_YTD_December',
  'Custom_YTD_Jan',
  'Custom_YTD_November',
  'Custom_YTD_Oct',
  'Custom_Zone_Field_TEC',
  'DueDate',
  'Effort',
  'FoundIn',
  'InProgressDateSK',
  'IntegrationBuild',
  'IterationSK',
  'Microsoft_VSTS_TCM_TestSuiteType',
  'Microsoft_VSTS_TCM_TestSuiteTypeId',
  'ParentWorkItemId',
  'Priority',
  'Reason',
  'RemainingWork',
  'ResolvedByUserSK',
  'ResolvedDateSK',
  'Revision',
  'Severity',
  'StartDate',
  'State',
  'StateCategory',
  'StateChangeDateSK',
  'TagNames',
  'TargetDate',
  'TimeCriticality',
  'Title',
  'ValueArea',
  'Watermark',
  'WorkItemId',
  'WorkItemRevisionSK',
  'WorkItemType'  
]

# COMMAND ----------

#CONTINUATIONTOKEN = 0
#url_base = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?$select='
#query = ','.join(colunas_escolhidas)
#token = '&$skiptoken=' + str(CONTINUATIONTOKEN)
#url = url_base + query + token
#print(url)

#REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?$select=WorkItemId,TagNames,Title,WorkItemType,AreaSK,IterationSK,State,StateCategory,StateChangeDateSK,Reason,ParentWorkItemId,WorkItemRevisionSK,Revision,CommentCount,Watermark,CreatedByUserSK,CreatedDateSK,InProgressDateSK,ActivatedByUserSK,ActivatedDateSK,AssignedToUserSK,ChangedByUserSK,ChangedDateSK,ClosedByUserSK,ClosedDateSK,Effort,Priority,BacklogPriority,Custom_SolutionType,Custom_IndexProcess,Custom_E2E_Name,Custom_L2,Custom_L3,Custom_TestScenario,Custom_BusinessRolesPersona,Custom_PlannedDate,TargetDate,Custom_Dependency,Custom_Test_Suite_Phase,DueDate,Custom_Environment_Field_TEC,Custom_Product_Field_TEC,Custom_Zone_Field_TEC,Custom_Work_Team,Custom_Name_Of_Environment,Custom_Executed_Today,Custom_FioriApp_System,Custom_TC_Automation_status,Custom_Criticality,Custom_Main_Blocker,Custom_Test_Case_Phase,Custom_Test_Case_ExecutionValidation,Custom_Executed_Today,Custom_RiskorIssuesCategory,Custom_IssuePriority,Custom_Contingency,Custom_EscalationNeeded,Custom_Deadline,Custom_AssociatedProject,Custom_RiskorIssuesEscalationLevel,Custom_DateIdentified,Custom_MDG_Relevant&$skiptoken='+str(CONTINUATIONTOKEN)

# COMMAND ----------

start = time.time()
dfOdata = pd.DataFrame([])
DFWorkItem = pd.DataFrame([])
lst = []
EXECUCAO = 0
EXECUCAOFOR = 0
EXECUCAOWHILE = 0 
for line in Lista:
  EXECUCAOFOR= EXECUCAOFOR +1
  print('EXECUCAOFOR ='+str(EXECUCAOFOR))
  CONTINUATIONTOKEN = 0
  LASTCONTINUATIONTOKEN = 0
  #PERSONAL_AUTHENTICATION_TOKEN = dbutils.secrets.get(scope = "key-vault-secrets", key = "OdtSrvDevOps")
  PERSONAL_AUTHENTICATION_TOKEN = "4ark3uh444c65uxtjfyzvrxndtjdhgad2amubesbszvqt7s4w5mq"
  USERNAME = "ab-inbev"
  USER_PASS = USERNAME + ":" + PERSONAL_AUTHENTICATION_TOKEN
  B64USERPASS = base64.b64encode(USER_PASS.encode()).decode()
  #ORGANIZATION_URL = f'https://dev.azure.com/'
  HEADERS = {
	    'Authorization': 'Basic %s' % B64USERPASS,
        'Accept': 'application/json'
	} 

  while True:
    try:
        hourI = time.strftime("%H:%M:%S", time.gmtime(time.time()))
        EXECUCAO= EXECUCAO +1
        #REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/Areas?&$skiptoken='+str(CONTINUATIONTOKEN)
        #REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?&$skiptoken='+str(CONTINUATIONTOKEN)
        REQUEST_URL = 'https://analytics.dev.azure.com/dmbData/Projeto_DevOps/_odata/v3.0/WorkItems?&$filter=ChangedDate ge 2022-01-17&$skiptoken='+str(CONTINUATIONTOKEN)
        ADO_RESPONSE = requests.get(REQUEST_URL, headers=HEADERS)
        if ADO_RESPONSE.status_code == 200:
          df = json.loads(ADO_RESPONSE.content)
          dfP2 = pd.json_normalize(df['value'])
          dfOdata = pd.concat([dfOdata,dfP2])
          #print(REQUEST_URL)
          if df.get("@odata.nextLink"):
            print('Entrou no if @odata.nextLink')
            #print(REQUEST_URL)
            if LASTCONTINUATIONTOKEN == int(df.get("@odata.nextLink").split('skiptoken=')[1]):
              print('parou aqui 1'+str(LASTCONTINUATIONTOKEN))
              break
            else:
              #print('o que tinha antes no continuation token? : '+str(CONTINUATIONTOKEN + int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])))
              #CONTINUATIONTOKEN = CONTINUATIONTOKEN + int(ADO_RESPONSE.headers['x-ms-continuationtoken'].split(';')[0])
              CONTINUATIONTOKEN = int(df.get("@odata.nextLink").split('skiptoken=')[1])
              LASTCONTINUATIONTOKEN = CONTINUATIONTOKEN
              print('parou aqui 2 - Setado Token '+str(CONTINUATIONTOKEN))
              EXECUCAOWHILE = EXECUCAOWHILE +1
              print('EXECUCAOFWHILE ='+str(EXECUCAOWHILE)+'Tempo '+str(hourI))
          else:
            print('parou aqui 3')
            break
        else:
            print('ERROR - {}'.format(ADO_RESPONSE.content))
            print('parou aqui 4')
            break
    except Exception as e:
      print('EXCEPTION - {}'.format(e))
      print('parou aqui 5')
      break
end = time.time()
print('Tempo de execução {} minutos'.format(((end-start)/60)))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando Schema para WorkItem

# COMMAND ----------

horaAtual = (datetime.now() - pd.DateOffset(hours=3)).replace(microsecond=0).isoformat()
dfOdata['DataCarregamento'] = horaAtual

# COMMAND ----------

dfOdata['WorkItemId'].count()

# COMMAND ----------

sinkPath = '/mnt/raw/DevOps/Aurora/Brazil/' + Lista[0] + '/' + horaAtual

print(sinkPath)

# COMMAND ----------

df = spark.createDataFrame(dfOdata.astype(str))

# COMMAND ----------

df.write.mode('overwrite').format('json').save(sinkPath)

# COMMAND ----------

#dfOdata['WorkItemId'].count()
dftest = spark.read.json(sinkPath)
dftest.count()

# COMMAND ----------

