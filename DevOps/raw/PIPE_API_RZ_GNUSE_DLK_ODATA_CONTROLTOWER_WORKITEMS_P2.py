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

sourceFile = 'WorkItems'

# COMMAND ----------

colunas_escolhidas = ['Custom_OD_Submitted_for_PO_Approval','Custom_OD_Submitted_for_TI_Approval','Custom_OD_Target_Dec','Custom_OD_Target_Nov','Custom_OD_Target_Oct','Custom_OD_YTD_Dec','Custom_OD_YTD_Nov','Custom_OD_YTD_Oct','Custom_OpenDesignID','Custom_OpenDesignType','Custom_Out_Scope','Custom_OutsideAuroraStartDate','Custom_OverviewNonSAP','Custom_PartnerResponsible','Custom_PartnerStatus','Custom_PlannedDate','Custom_PotentialImpact','Custom_PriorityforRelease','Custom_PriorityRank','Custom_ProbabilityofOccurrence','Custom_Product_Field_TEC','Custom_ReasonforFutureRelease','Custom_ResolvedTargetDate','Custom_RiskLevel','Custom_RiskorIssuesCategory','Custom_RiskorIssuesEscalationLevel','Custom_SAPAdherence','Custom_SAPRole','Custom_Scenario','Custom_Scenario_Name','Custom_Scenario_Order','Custom_ScopeItem','Custom_SolutionType','Custom_Source','Custom_StoryType','Custom_SystemName','Custom_Target_Dec','Custom_Target_Jan','Custom_Target_Nov','Custom_Target_Oct','Custom_TargetDateFuncSpec','Custom_TC_Automation_status','Custom_Test_Case_ExecutionValidation','Custom_Test_Case_Phase','Custom_Test_Suite_Phase','Custom_TestScenario','Custom_UC_Submitted_for_CPO_Approval','Custom_UC_Submitted_for_DA_Approval_CCPO','Custom_UC_Submitted_for_GRC_Validation_PO','Custom_UC_Submitted_for_PO_approval','Custom_UC_Submitted_for_SAP_Approval','Custom_UC_Submitted_for_TI_Approval','Custom_US_Priority','Custom_UserStoryAPPID','Custom_UserStoryType','Custom_Work_Team','Custom_Workshop_ID','Custom_Workshoptobepresented','Custom_WRICEF','Custom_YTD_December','Custom_YTD_Jan','Custom_YTD_November','Custom_YTD_Oct','Custom_Zone_Field_TEC','DueDate','Effort','FoundIn','InProgressDateSK','IntegrationBuild','IterationSK','Microsoft_VSTS_TCM_TestSuiteType','Microsoft_VSTS_TCM_TestSuiteTypeId','ParentWorkItemId','Priority','Reason','RemainingWork','ResolvedByUserSK','ResolvedDateSK','Revision','Severity','StartDate','State','StateCategory','StateChangeDateSK','TagNames','TargetDate','TimeCriticality','Title','ValueArea','Watermark','WorkItemRevisionSK','WorkItemType','WorkItemId','ChangedDateSK','Custom_IndexE2E','Custom_TargetDateGAP','Custom_Classification']

# COMMAND ----------

dfOdata = getDadosAuroraAPI(sourceFile, colunas = colunas_escolhidas)

# COMMAND ----------

horaAtual = (datetime.now() - pd.DateOffset(hours=3)).strftime("%Y-%m-%d_%H_%M_%S")
dfOdata['DataCarregamento'] = horaAtual

# COMMAND ----------

df = spark.createDataFrame(dfOdata.astype(str))

# COMMAND ----------

dfOdata['WorkItemId'].count()

# COMMAND ----------

sinkPath = aurora_raw_folder + sourceFile + '_P2/' + horaAtual

# COMMAND ----------

df.write.mode('overwrite').format('json').save(sinkPath)