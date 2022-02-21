# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
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

sourceFile = 'WorkItemRevisions'
sourcePath = aurora_standardized_folder + sourceFile + '/'
print(sourcePath)

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data and '.parquet' not in i.name:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Assunto a ser buscado no Datalake (apenas dados da carga diária) e criação do path a ser utilizado para ler os arquivos

sourcePath_diario = aurora_raw_folder + sourceFile + '/'

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath_diario):
  if i.name > max_data:
    max_data = i.name
    
sourcePath_diario = sourcePath_diario + max_data
print(sourcePath_diario)

# COMMAND ----------

# Lê o arquivo parquet do path (carga full) e salva em um spark dataframe

df_full = spark.read.parquet(sourcePath)

# COMMAND ----------

# Número de linhas e de colunas do dataframe com a carga full

num_linhas_full = df_full.count()

print(f'Número de colunas no df full: {len(df_full.columns)}')
print(f'Número de linhas no df full: {num_linhas_full}')

# COMMAND ----------

# Lê o arquivo parquet do path (carga diario) e salva em um spark dataframe

df_diario = spark.read.format('avro').load(sourcePath_diario)

# COMMAND ----------

# Número de linhas e de colunas do dataframe com a carga diária

num_linhas_diario = df_diario.count()

print(f'Número de colunas no df diario: {len(df_diario.columns)}')
print(f'Número de linhas no df diario: {num_linhas_diario}')

# COMMAND ----------

# Substitui campos None para None do Python

df_diario = df_diario.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

# Substitui campos None para None do Python

df_full = df_full.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

df_full = df_full \
.withColumn('WorkItemId', df_full.WorkItemId.cast(IntegerType())) \
.withColumn('Revision', df_full.Revision.cast(IntegerType())) \
.withColumn('RevisedDate', df_full.RevisedDate.cast(TimestampType())) \
.withColumn('RevisedDateSK', df_full.RevisedDateSK.cast(IntegerType())) \
.withColumn('DateSK', df_full.DateSK.cast(IntegerType())) \
.withColumn('IsCurrent', df_full.IsCurrent.cast(StringType())) \
.withColumn('IsLastRevisionOfDay', df_full.IsLastRevisionOfDay.cast(StringType())) \
.withColumn('AnalyticsUpdatedDate', df_full.AnalyticsUpdatedDate.cast(TimestampType())) \
.withColumn('ProjectSK', df_full.ProjectSK.cast(StringType())) \
.withColumn('WorkItemRevisionSK', df_full.WorkItemRevisionSK.cast(IntegerType())) \
.withColumn('AreaSK', df_full.AreaSK.cast(StringType())) \
.withColumn('IterationSK', df_full.IterationSK.cast(StringType())) \
.withColumn('AssignedToUserSK', df_full.AssignedToUserSK.cast(StringType())) \
.withColumn('ChangedByUserSK', df_full.ChangedByUserSK.cast(StringType())) \
.withColumn('CreatedByUserSK', df_full.CreatedByUserSK.cast(StringType())) \
.withColumn('ActivatedByUserSK', df_full.ActivatedByUserSK.cast(StringType())) \
.withColumn('ClosedByUserSK', df_full.ClosedByUserSK.cast(StringType())) \
.withColumn('ResolvedByUserSK', df_full.ResolvedByUserSK.cast(StringType())) \
.withColumn('ActivatedDateSK', df_full.ActivatedDateSK.cast(IntegerType())) \
.withColumn('ChangedDateSK', df_full.ChangedDateSK.cast(IntegerType())) \
.withColumn('ClosedDateSK', df_full.ClosedDateSK.cast(IntegerType())) \
.withColumn('CreatedDateSK', df_full.CreatedDateSK.cast(IntegerType())) \
.withColumn('ResolvedDateSK', df_full.ResolvedDateSK.cast(IntegerType())) \
.withColumn('StateChangeDateSK', df_full.StateChangeDateSK.cast(IntegerType())) \
.withColumn('InProgressDateSK', df_full.InProgressDateSK.cast(IntegerType())) \
.withColumn('CompletedDateSK', df_full.CompletedDateSK.cast(IntegerType())) \
.withColumn('Watermark', df_full.Watermark.cast(IntegerType())) \
.withColumn('Title', df_full.Title.cast(StringType())) \
.withColumn('WorkItemType', df_full.WorkItemType.cast(StringType())) \
.withColumn('ChangedDate', df_full.ChangedDate.cast(TimestampType())) \
.withColumn('CreatedDate', df_full.CreatedDate.cast(TimestampType())) \
.withColumn('State', df_full.State.cast(StringType())) \
.withColumn('Reason', df_full.Reason.cast(StringType())) \
.withColumn('FoundIn', df_full.FoundIn.cast(StringType())) \
.withColumn('IntegrationBuild', df_full.IntegrationBuild.cast(StringType())) \
.withColumn('ActivatedDate', df_full.ActivatedDate.cast(TimestampType())) \
.withColumn('Activity', df_full.Activity.cast(StringType())) \
.withColumn('BacklogPriority', df_full.BacklogPriority.cast(FloatType())) \
.withColumn('BusinessValue', df_full.BusinessValue.cast(IntegerType())) \
.withColumn('ClosedDate', df_full.ClosedDate.cast(TimestampType())) \
.withColumn('Issue', df_full.Issue.cast(StringType())) \
.withColumn('Priority', df_full.Priority.cast(IntegerType())) \
.withColumn('Rating', df_full.Rating.cast(StringType())) \
.withColumn('ResolvedDate', df_full.ResolvedDate.cast(TimestampType())) \
.withColumn('Severity', df_full.Severity.cast(StringType())) \
.withColumn('TimeCriticality', df_full.TimeCriticality.cast(FloatType())) \
.withColumn('ValueArea', df_full.ValueArea.cast(StringType())) \
.withColumn('DueDate', df_full.DueDate.cast(TimestampType())) \
.withColumn('Effort', df_full.Effort.cast(FloatType())) \
.withColumn('FinishDate', df_full.FinishDate.cast(TimestampType())) \
.withColumn('RemainingWork', df_full.RemainingWork.cast(FloatType())) \
.withColumn('StartDate', df_full.StartDate.cast(TimestampType())) \
.withColumn('TargetDate', df_full.TargetDate.cast(TimestampType())) \
.withColumn('Blocked', df_full.Blocked.cast(StringType())) \
.withColumn('ParentWorkItemId', df_full.ParentWorkItemId.cast(IntegerType())) \
.withColumn('TagNames', df_full.TagNames.cast(StringType())) \
.withColumn('StateCategory', df_full.StateCategory.cast(StringType())) \
.withColumn('InProgressDate', df_full.InProgressDate.cast(TimestampType())) \
.withColumn('CompletedDate', df_full.CompletedDate.cast(TimestampType())) \
.withColumn('LeadTimeDays', df_full.LeadTimeDays.cast(FloatType())) \
.withColumn('CycleTimeDays', df_full.CycleTimeDays.cast(FloatType())) \
.withColumn('AutomatedTestId', df_full.AutomatedTestId.cast(StringType())) \
.withColumn('AutomatedTestName', df_full.AutomatedTestName.cast(StringType())) \
.withColumn('AutomatedTestStorage', df_full.AutomatedTestStorage.cast(StringType())) \
.withColumn('AutomatedTestType', df_full.AutomatedTestType.cast(StringType())) \
.withColumn('AutomationStatus', df_full.AutomationStatus.cast(StringType())) \
.withColumn('StateChangeDate', df_full.StateChangeDate.cast(TimestampType())) \
.withColumn('Count', df_full.Count.cast(LongType())) \
.withColumn('CommentCount', df_full.CommentCount.cast(IntegerType())) \
.withColumn('ABIProprietaryMethodology_IssueCreatedDate', df_full.ABIProprietaryMethodology_IssueCreatedDate.cast(TimestampType())) \
.withColumn('ABIProprietaryMethodology_IssueOwnerSK', df_full.ABIProprietaryMethodology_IssueOwnerSK.cast(StringType())) \
.withColumn('Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866', df_full.Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866.cast(StringType())) \
.withColumn('Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17', df_full.Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17.cast(StringType())) \
.withColumn('Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422', df_full.Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422.cast(StringType())) \
.withColumn('Custom_AccordingwithSOXcontrols', df_full.Custom_AccordingwithSOXcontrols.cast(StringType())) \
.withColumn('Custom_ActualWork', df_full.Custom_ActualWork.cast(FloatType())) \
.withColumn('Custom_Add_Remove_Fields', df_full.Custom_Add_Remove_Fields.cast(StringType())) \
.withColumn('Custom_Affected_APPs_Transactions', df_full.Custom_Affected_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_Affected_Workstream', df_full.Custom_Affected_Workstream.cast(StringType())) \
.withColumn('Custom_Analytics_Relevant', df_full.Custom_Analytics_Relevant.cast(StringType())) \
.withColumn('Custom_Analytics_Transport_Request', df_full.Custom_Analytics_Transport_Request.cast(StringType())) \
.withColumn('Custom_APIs_Description', df_full.Custom_APIs_Description.cast(StringType())) \
.withColumn('Custom_Applicable_Contingencies_Workarounds', df_full.Custom_Applicable_Contingencies_Workarounds.cast(StringType())) \
.withColumn('Custom_APPs_Transactions', df_full.Custom_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_AssociatedProject', df_full.Custom_AssociatedProject.cast(StringType())) \
.withColumn('Custom_Aurora_Category', df_full.Custom_Aurora_Category.cast(StringType())) \
.withColumn('Custom_Automation', df_full.Custom_Automation.cast(StringType())) \
.withColumn('Custom_AutomationBlankField', df_full.Custom_AutomationBlankField.cast(StringType())) \
.withColumn('Custom_AutomationNeeded', df_full.Custom_AutomationNeeded.cast(StringType())) \
.withColumn('Custom_AutomationNeeded2', df_full.Custom_AutomationNeeded2.cast(StringType())) \
.withColumn('Custom_Block_Workstream_Requester', df_full.Custom_Block_Workstream_Requester.cast(StringType())) \
.withColumn('Custom_Blocked_TestCase', df_full.Custom_Blocked_TestCase.cast(StringType())) \
.withColumn('Custom_BlockedJustification', df_full.Custom_BlockedJustification.cast(StringType())) \
.withColumn('Custom_BuC_Approved_Test_Result', df_full.Custom_BuC_Approved_Test_Result.cast(StringType())) \
.withColumn('Custom_BuC_Approved_Test_Script', df_full.Custom_BuC_Approved_Test_Script.cast(StringType())) \
.withColumn('Custom_BuC_Impacted', df_full.Custom_BuC_Impacted.cast(StringType())) \
.withColumn('Custom_Bug_Phase', df_full.Custom_Bug_Phase.cast(StringType())) \
.withColumn('Custom_Bug_Root_Cause', df_full.Custom_Bug_Root_Cause.cast(StringType())) \
.withColumn('Custom_Bug_Workstream_Requester', df_full.Custom_Bug_Workstream_Requester.cast(StringType())) \
.withColumn('Custom_BuildType', df_full.Custom_BuildType.cast(StringType())) \
.withColumn('Custom_BusinessProcessImpact', df_full.Custom_BusinessProcessImpact.cast(StringType())) \
.withColumn('Custom_BusinessRolesPersona', df_full.Custom_BusinessRolesPersona.cast(StringType())) \
.withColumn('Custom_Centralization', df_full.Custom_Centralization.cast(StringType())) \
.withColumn('Custom_CID', df_full.Custom_CID.cast(StringType())) \
.withColumn('Custom_Classification', df_full.Custom_Classification.cast(StringType())) \
.withColumn('Custom_Closed_Date_Transport_Request', df_full.Custom_Closed_Date_Transport_Request.cast(TimestampType())) \
.withColumn('Custom_Complexity', df_full.Custom_Complexity.cast(StringType())) \
.withColumn('Custom_ConditionName', df_full.Custom_ConditionName.cast(StringType())) \
.withColumn('Custom_Contingency', df_full.Custom_Contingency.cast(StringType())) \
.withColumn('Custom_Control_Name', df_full.Custom_Control_Name.cast(StringType())) \
.withColumn('Custom_Criticality', df_full.Custom_Criticality.cast(StringType())) \
.withColumn('Custom_Custom2', df_full.Custom_Custom2.cast(StringType())) \
.withColumn('Custom_DARelated', df_full.Custom_DARelated.cast(StringType())) \
.withColumn('Custom_DateIdentified', df_full.Custom_DateIdentified.cast(TimestampType())) \
.withColumn('Custom_Deadline', df_full.Custom_Deadline.cast(TimestampType())) \
.withColumn('Custom_Dependency', df_full.Custom_Dependency.cast(StringType())) \
.withColumn('Custom_DependencyConsumer', df_full.Custom_DependencyConsumer.cast(StringType())) \
.withColumn('Custom_DependencyProvider', df_full.Custom_DependencyProvider.cast(StringType())) \
.withColumn('Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8', df_full.Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8.cast(StringType())) \
.withColumn('Custom_E2E_Impacted', df_full.Custom_E2E_Impacted.cast(StringType())) \
.withColumn('Custom_E2E_Name', df_full.Custom_E2E_Name.cast(StringType())) \
.withColumn('Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e', df_full.Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e.cast(StringType())) \
.withColumn('Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e', df_full.Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e.cast(TimestampType())) \
.withColumn('Custom_Effort_Aurora', df_full.Custom_Effort_Aurora.cast(LongType())) \
.withColumn('Custom_email', df_full.Custom_email.cast(StringType())) \
.withColumn('Custom_EnhancementGAPID', df_full.Custom_EnhancementGAPID.cast(StringType())) \
.withColumn('Custom_Environment_Field_TEC', df_full.Custom_Environment_Field_TEC.cast(StringType())) \
.withColumn('Custom_ER_Affected_APPs_Transactions', df_full.Custom_ER_Affected_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_ER_Affected_Workstream', df_full.Custom_ER_Affected_Workstream.cast(StringType())) \
.withColumn('Custom_ER_Analytics_Transport_Request', df_full.Custom_ER_Analytics_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_Applicable_Contingencies_Workarounds', df_full.Custom_ER_Applicable_Contingencies_Workarounds.cast(StringType())) \
.withColumn('Custom_ER_APPs_Transactions', df_full.Custom_ER_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_ER_Blocked_TestCase', df_full.Custom_ER_Blocked_TestCase.cast(StringType())) \
.withColumn('Custom_ER_E2E_Impacted', df_full.Custom_ER_E2E_Impacted.cast(StringType())) \
.withColumn('Custom_ER_GAP_APP_ID', df_full.Custom_ER_GAP_APP_ID.cast(StringType())) \
.withColumn('Custom_ER_Impact_Information', df_full.Custom_ER_Impact_Information.cast(StringType())) \
.withColumn('Custom_ER_Logistics_Transport_Request', df_full.Custom_ER_Logistics_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_MDG_Transport_Request', df_full.Custom_ER_MDG_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_OTC_Transport_Request', df_full.Custom_ER_OTC_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_P2P_Transport_Request', df_full.Custom_ER_P2P_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_RTR_Transport_Request', df_full.Custom_ER_RTR_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_SAP_Change', df_full.Custom_ER_SAP_Change.cast(StringType())) \
.withColumn('Custom_ER_Security_Authorizations_Impacts', df_full.Custom_ER_Security_Authorizations_Impacts.cast(StringType())) \
.withColumn('Custom_ER_SIT_Committee_Approval', df_full.Custom_ER_SIT_Committee_Approval.cast(StringType())) \
.withColumn('Custom_ER_SIT_Committee_Comments', df_full.Custom_ER_SIT_Committee_Comments.cast(StringType())) \
.withColumn('Custom_ER_Supply_Transport_Request', df_full.Custom_ER_Supply_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_TEC_Transport_Request', df_full.Custom_ER_TEC_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_Transport_List', df_full.Custom_ER_Transport_List.cast(StringType())) \
.withColumn('Custom_ER_Transport_Status', df_full.Custom_ER_Transport_Status.cast(StringType())) \
.withColumn('Custom_ER_WorkItem_Charm_Number', df_full.Custom_ER_WorkItem_Charm_Number.cast(StringType())) \
.withColumn('Custom_EscalationDate', df_full.Custom_EscalationDate.cast(TimestampType())) \
.withColumn('Custom_EscalationNeeded', df_full.Custom_EscalationNeeded.cast(StringType())) \
.withColumn('Custom_Executed_Today', df_full.Custom_Executed_Today.cast(FloatType())) \
.withColumn('Custom_FeatureCategory', df_full.Custom_FeatureCategory.cast(StringType())) \
.withColumn('Custom_FeatureType', df_full.Custom_FeatureType.cast(StringType())) \
.withColumn('Custom_FioriApp_System', df_full.Custom_FioriApp_System.cast(StringType())) \
.withColumn('Custom_FirstDueDate', df_full.Custom_FirstDueDate.cast(TimestampType())) \
.withColumn('Custom_FIT_GAP', df_full.Custom_FIT_GAP.cast(StringType())) \
.withColumn('Custom_FIT_ID', df_full.Custom_FIT_ID.cast(StringType())) \
.withColumn('Custom_Flow_FSEC_1', df_full.Custom_Flow_FSEC_1.cast(StringType())) \
.withColumn('Custom_Flow_FSEC_2', df_full.Custom_Flow_FSEC_2.cast(StringType())) \
.withColumn('Custom_Flow_FSEC_3', df_full.Custom_Flow_FSEC_3.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_CPO_Approval', df_full.Custom_FS_Submitted_for_CPO_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_GRC_M_Approval', df_full.Custom_FS_Submitted_for_GRC_M_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_GRC_SOX_Approval', df_full.Custom_FS_Submitted_for_GRC_SOX_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_GRC_Testing_Approval', df_full.Custom_FS_Submitted_for_GRC_Testing_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_PO_Approval', df_full.Custom_FS_Submitted_for_PO_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_QA_Approval', df_full.Custom_FS_Submitted_for_QA_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_TI_Approval', df_full.Custom_FS_Submitted_for_TI_Approval.cast(StringType())) \
.withColumn('Custom_FSID', df_full.Custom_FSID.cast(StringType())) \
.withColumn('Custom_FutureRelease', df_full.Custom_FutureRelease.cast(StringType())) \
.withColumn('Custom_GAP_APP_ID_Related', df_full.Custom_GAP_APP_ID_Related.cast(StringType())) \
.withColumn('Custom_GAP_Reprioritization', df_full.Custom_GAP_Reprioritization.cast(StringType())) \
.withColumn('Custom_GAPAPPID', df_full.Custom_GAPAPPID.cast(StringType())) \
.withColumn('Custom_GAPCategory', df_full.Custom_GAPCategory.cast(StringType())) \
.withColumn('Custom_GAPEnhancement', df_full.Custom_GAPEnhancement.cast(StringType())) \
.withColumn('Custom_GAPEstimation', df_full.Custom_GAPEstimation.cast(StringType())) \
.withColumn('Custom_GAPNumber', df_full.Custom_GAPNumber.cast(StringType())) \
.withColumn('Custom_Glassif', df_full.Custom_Glassif.cast(StringType())) \
.withColumn('Custom_Global_Local_Escope', df_full.Custom_Global_Local_Escope.cast(StringType())) \
.withColumn('Custom_GRC_BUC_APPROVED', df_full.Custom_GRC_BUC_APPROVED.cast(StringType())) \
.withColumn('Custom_GRC_TESTING_APPROVED', df_full.Custom_GRC_TESTING_APPROVED.cast(StringType())) \
.withColumn('Custom_GRC_UAM_APPROVED', df_full.Custom_GRC_UAM_APPROVED.cast(StringType())) \
.withColumn('Custom_GUID', df_full.Custom_GUID.cast(StringType())) \
.withColumn('Custom_Impact', df_full.Custom_Impact.cast(StringType())) \
.withColumn('Custom_Impact_Information', df_full.Custom_Impact_Information.cast(StringType())) \
.withColumn('Custom_IndexE2E', df_full.Custom_IndexE2E.cast(StringType())) \
.withColumn('Custom_IndexProcess', df_full.Custom_IndexProcess.cast(StringType())) \
.withColumn('Custom_Interim_Solution', df_full.Custom_Interim_Solution.cast(StringType())) \
.withColumn('Custom_IsGlobalSteerCoRequired', df_full.Custom_IsGlobalSteerCoRequired.cast(StringType())) \
.withColumn('Custom_IsReportRequired', df_full.Custom_IsReportRequired.cast(StringType())) \
.withColumn('Custom_IssuePriority', df_full.Custom_IssuePriority.cast(StringType())) \
.withColumn('Custom_L2', df_full.Custom_L2.cast(StringType())) \
.withColumn('Custom_L2Product', df_full.Custom_L2Product.cast(StringType())) \
.withColumn('Custom_L3', df_full.Custom_L3.cast(StringType())) \
.withColumn('Custom_L3Product', df_full.Custom_L3Product.cast(StringType())) \
.withColumn('Custom_L4Product', df_full.Custom_L4Product.cast(StringType())) \
.withColumn('Custom_L4WorkShopDescription', df_full.Custom_L4WorkShopDescription.cast(StringType())) \
.withColumn('Custom_LE_December', df_full.Custom_LE_December.cast(StringType())) \
.withColumn('Custom_LE_January', df_full.Custom_LE_January.cast(StringType())) \
.withColumn('Custom_LE_November', df_full.Custom_LE_November.cast(StringType())) \
.withColumn('Custom_LE_October', df_full.Custom_LE_October.cast(StringType())) \
.withColumn('Custom_LeadershipApproval', df_full.Custom_LeadershipApproval.cast(StringType())) \
.withColumn('Custom_Legacies_Integration', df_full.Custom_Legacies_Integration.cast(StringType())) \
.withColumn('Custom_LegaciesDescription', df_full.Custom_LegaciesDescription.cast(StringType())) \
.withColumn('Custom_Logistics_Transport_Request', df_full.Custom_Logistics_Transport_Request.cast(StringType())) \
.withColumn('Custom_Main_Blocker', df_full.Custom_Main_Blocker.cast(StringType())) \
.withColumn('Custom_MDG_Relevant', df_full.Custom_MDG_Relevant.cast(StringType())) \
.withColumn('Custom_MDG_Transport_Request', df_full.Custom_MDG_Transport_Request.cast(StringType())) \
.withColumn('Custom_Microservices_Change', df_full.Custom_Microservices_Change.cast(StringType())) \
.withColumn('Custom_Microservices_Description', df_full.Custom_Microservices_Description.cast(StringType())) \
.withColumn('Custom_MoscowPriority', df_full.Custom_MoscowPriority.cast(StringType())) \
.withColumn('Custom_Name_Of_Environment', df_full.Custom_Name_Of_Environment.cast(StringType())) \
.withColumn('Custom_NonSAP', df_full.Custom_NonSAP.cast(StringType())) \
.withColumn('Custom_NoSAPApprovalJustification', df_full.Custom_NoSAPApprovalJustification.cast(StringType())) \
.withColumn('Custom_NotAllowedForLocalTeam', df_full.Custom_NotAllowedForLocalTeam.cast(StringType())) \
.withColumn('Custom_NotAllowedtothisState', df_full.Custom_NotAllowedtothisState.cast(StringType())) \
.withColumn('Custom_OD_LE_Dec', df_full.Custom_OD_LE_Dec.cast(StringType())) \
.withColumn('Custom_OD_LE_Nov', df_full.Custom_OD_LE_Nov.cast(StringType())) \
.withColumn('Custom_OD_LE_Oct', df_full.Custom_OD_LE_Oct.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_Consultant_Approval', df_full.Custom_OD_Submitted_for_Consultant_Approval.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_CPO_Approval', df_full.Custom_OD_Submitted_for_CPO_Approval.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_DA_Approval_CCPO', df_full.Custom_OD_Submitted_for_DA_Approval_CCPO.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_PO_Approval', df_full.Custom_OD_Submitted_for_PO_Approval.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_TI_Approval', df_full.Custom_OD_Submitted_for_TI_Approval.cast(StringType())) \
.withColumn('Custom_OD_Target_Dec', df_full.Custom_OD_Target_Dec.cast(StringType())) \
.withColumn('Custom_OD_Target_Nov', df_full.Custom_OD_Target_Nov.cast(StringType())) \
.withColumn('Custom_OD_Target_Oct', df_full.Custom_OD_Target_Oct.cast(StringType())) \
.withColumn('Custom_OD_YTD_Dec', df_full.Custom_OD_YTD_Dec.cast(StringType())) \
.withColumn('Custom_OD_YTD_Nov', df_full.Custom_OD_YTD_Nov.cast(StringType())) \
.withColumn('Custom_OD_YTD_Oct', df_full.Custom_OD_YTD_Oct.cast(StringType())) \
.withColumn('Custom_OpenDesignID', df_full.Custom_OpenDesignID.cast(StringType())) \
.withColumn('Custom_OpenDesignType', df_full.Custom_OpenDesignType.cast(StringType())) \
.withColumn('Custom_OTC_Transport_Request', df_full.Custom_OTC_Transport_Request.cast(StringType())) \
.withColumn('Custom_Out_Scope', df_full.Custom_Out_Scope.cast(StringType())) \
.withColumn('Custom_OutsideAuroraRecoveryDate', df_full.Custom_OutsideAuroraRecoveryDate.cast(TimestampType())) \
.withColumn('Custom_OutsideAuroraStartDate', df_full.Custom_OutsideAuroraStartDate.cast(TimestampType())) \
.withColumn('Custom_OverviewNonSAP', df_full.Custom_OverviewNonSAP.cast(StringType())) \
.withColumn('Custom_P2P_Transport_Request', df_full.Custom_P2P_Transport_Request.cast(StringType())) \
.withColumn('Custom_PartnerAnalysisTargetDate', df_full.Custom_PartnerAnalysisTargetDate.cast(TimestampType())) \
.withColumn('Custom_PartnerReleaseTargetDate', df_full.Custom_PartnerReleaseTargetDate.cast(TimestampType())) \
.withColumn('Custom_PartnerResponsible', df_full.Custom_PartnerResponsible.cast(StringType())) \
.withColumn('Custom_PartnerStatus', df_full.Custom_PartnerStatus.cast(StringType())) \
.withColumn('Custom_PlannedDate', df_full.Custom_PlannedDate.cast(TimestampType())) \
.withColumn('Custom_PlannedStartDatetoBuild', df_full.Custom_PlannedStartDatetoBuild.cast(TimestampType())) \
.withColumn('Custom_PlannedWeeklySprint', df_full.Custom_PlannedWeeklySprint.cast(TimestampType())) \
.withColumn('Custom_PotentialImpact', df_full.Custom_PotentialImpact.cast(StringType())) \
.withColumn('Custom_PriorityforRelease', df_full.Custom_PriorityforRelease.cast(LongType())) \
.withColumn('Custom_PriorityRank', df_full.Custom_PriorityRank.cast(LongType())) \
.withColumn('Custom_ProbabilityofOccurrence', df_full.Custom_ProbabilityofOccurrence.cast(StringType())) \
.withColumn('Custom_Product_Field_TEC', df_full.Custom_Product_Field_TEC.cast(StringType())) \
.withColumn('Custom_ProfileAccessInformation', df_full.Custom_ProfileAccessInformation.cast(StringType())) \
.withColumn('Custom_RAIDPriority', df_full.Custom_RAIDPriority.cast(StringType())) \
.withColumn('Custom_ReasonforFutureRelease', df_full.Custom_ReasonforFutureRelease.cast(StringType())) \
.withColumn('Custom_Related_Interface', df_full.Custom_Related_Interface.cast(StringType())) \
.withColumn('Custom_Related_Interface_Description', df_full.Custom_Related_Interface_Description.cast(StringType())) \
.withColumn('Custom_Related_Interface_Systems', df_full.Custom_Related_Interface_Systems.cast(FloatType())) \
.withColumn('Custom_Related_LIB', df_full.Custom_Related_LIB.cast(StringType())) \
.withColumn('Custom_RelatedWorkStream', df_full.Custom_RelatedWorkStream.cast(StringType())) \
.withColumn('Custom_RelativeMassValuation', df_full.Custom_RelativeMassValuation.cast(LongType())) \
.withColumn('Custom_ResolvedTargetDate', df_full.Custom_ResolvedTargetDate.cast(TimestampType())) \
.withColumn('Custom_RiskLevel', df_full.Custom_RiskLevel.cast(StringType())) \
.withColumn('Custom_RiskorIssuesCategory', df_full.Custom_RiskorIssuesCategory.cast(StringType())) \
.withColumn('Custom_RiskorIssuesEscalationLevel', df_full.Custom_RiskorIssuesEscalationLevel.cast(StringType())) \
.withColumn('Custom_RTR_Transport_Request', df_full.Custom_RTR_Transport_Request.cast(StringType())) \
.withColumn('Custom_SAP_Change', df_full.Custom_SAP_Change.cast(StringType())) \
.withColumn('Custom_SAPAdherence', df_full.Custom_SAPAdherence.cast(StringType())) \
.withColumn('Custom_SAPRole', df_full.Custom_SAPRole.cast(StringType())) \
.withColumn('Custom_Scenario', df_full.Custom_Scenario.cast(StringType())) \
.withColumn('Custom_Scenario_Name', df_full.Custom_Scenario_Name.cast(StringType())) \
.withColumn('Custom_Scenario_Order', df_full.Custom_Scenario_Order.cast(FloatType())) \
.withColumn('Custom_ScopeItem', df_full.Custom_ScopeItem.cast(StringType())) \
.withColumn('Custom_Security_Authorizations_Impacts', df_full.Custom_Security_Authorizations_Impacts.cast(StringType())) \
.withColumn('Custom_Servicenow_Close_Notes', df_full.Custom_Servicenow_Close_Notes.cast(StringType())) \
.withColumn('Custom_Servicenow_ticket_ID', df_full.Custom_Servicenow_ticket_ID.cast(StringType())) \
.withColumn('Custom_SIT_Committee_Approval', df_full.Custom_SIT_Committee_Approval.cast(StringType())) \
.withColumn('Custom_SIT_Committee_Comments', df_full.Custom_SIT_Committee_Comments.cast(StringType())) \
.withColumn('Custom_SolutionName', df_full.Custom_SolutionName.cast(StringType())) \
.withColumn('Custom_SolutionType', df_full.Custom_SolutionType.cast(StringType())) \
.withColumn('Custom_Source', df_full.Custom_Source.cast(StringType())) \
.withColumn('Custom_StoryType', df_full.Custom_StoryType.cast(StringType())) \
.withColumn('Custom_SubmittedforIntegrationTeam', df_full.Custom_SubmittedforIntegrationTeam.cast(StringType())) \
.withColumn('Custom_SuccessfullyTestedinQAEnvironment', df_full.Custom_SuccessfullyTestedinQAEnvironment.cast(StringType())) \
.withColumn('Custom_Supply_Transport_Request', df_full.Custom_Supply_Transport_Request.cast(StringType())) \
.withColumn('Custom_System', df_full.Custom_System.cast(StringType())) \
.withColumn('Custom_SystemName', df_full.Custom_SystemName.cast(StringType())) \
.withColumn('Custom_Target_Dec', df_full.Custom_Target_Dec.cast(StringType())) \
.withColumn('Custom_Target_Jan', df_full.Custom_Target_Jan.cast(StringType())) \
.withColumn('Custom_Target_Nov', df_full.Custom_Target_Nov.cast(StringType())) \
.withColumn('Custom_Target_Oct', df_full.Custom_Target_Oct.cast(StringType())) \
.withColumn('Custom_TargetDateFuncSpec', df_full.Custom_TargetDateFuncSpec.cast(TimestampType())) \
.withColumn('Custom_TargetDateGAP', df_full.Custom_TargetDateGAP.cast(TimestampType())) \
.withColumn('Custom_TC_Automation_status', df_full.Custom_TC_Automation_status.cast(StringType())) \
.withColumn('Custom_TEC_Transport_Request', df_full.Custom_TEC_Transport_Request.cast(StringType())) \
.withColumn('Custom_Test_Case_ExecutionValidation', df_full.Custom_Test_Case_ExecutionValidation.cast(StringType())) \
.withColumn('Custom_Test_Case_Phase', df_full.Custom_Test_Case_Phase.cast(StringType())) \
.withColumn('Custom_Test_FSEC_1', df_full.Custom_Test_FSEC_1.cast(StringType())) \
.withColumn('Custom_Test_FSEC_2', df_full.Custom_Test_FSEC_2.cast(StringType())) \
.withColumn('Custom_Test_FSEC_3', df_full.Custom_Test_FSEC_3.cast(StringType())) \
.withColumn('Custom_Test_FSEC_4', df_full.Custom_Test_FSEC_4.cast(StringType())) \
.withColumn('Custom_Test_FSEC_6', df_full.Custom_Test_FSEC_6.cast(LongType())) \
.withColumn('Custom_Test_Scenario_Description', df_full.Custom_Test_Scenario_Description.cast(StringType())) \
.withColumn('Custom_Test_Suite_Phase', df_full.Custom_Test_Suite_Phase.cast(StringType())) \
.withColumn('Custom_TestScenario', df_full.Custom_TestScenario.cast(StringType())) \
.withColumn('Custom_Transport_List', df_full.Custom_Transport_List.cast(StringType())) \
.withColumn('Custom_Transport_Request_Type', df_full.Custom_Transport_Request_Type.cast(StringType())) \
.withColumn('Custom_Transport_Status', df_full.Custom_Transport_Status.cast(StringType())) \
.withColumn('Custom_TransportNeeded', df_full.Custom_TransportNeeded.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_CPO_Approval', df_full.Custom_UC_Submitted_for_CPO_Approval.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_DA_Approval_CCPO', df_full.Custom_UC_Submitted_for_DA_Approval_CCPO.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_GRC_Validation_PO', df_full.Custom_UC_Submitted_for_GRC_Validation_PO.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_PO_approval', df_full.Custom_UC_Submitted_for_PO_approval.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_SAP_Approval', df_full.Custom_UC_Submitted_for_SAP_Approval.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_TI_Approval', df_full.Custom_UC_Submitted_for_TI_Approval.cast(StringType())) \
.withColumn('Custom_US_Priority', df_full.Custom_US_Priority.cast(StringType())) \
.withColumn('Custom_User_Exist_BADIs', df_full.Custom_User_Exist_BADIs.cast(StringType())) \
.withColumn('Custom_User_Exist_BADIs_Description', df_full.Custom_User_Exist_BADIs_Description.cast(StringType())) \
.withColumn('Custom_UserStoryAPPID', df_full.Custom_UserStoryAPPID.cast(StringType())) \
.withColumn('Custom_UserStoryType', df_full.Custom_UserStoryType.cast(StringType())) \
.withColumn('Custom_WaitingBuCApproval', df_full.Custom_WaitingBuCApproval.cast(StringType())) \
.withColumn('Custom_Work_Team', df_full.Custom_Work_Team.cast(StringType())) \
.withColumn('Custom_Workaround', df_full.Custom_Workaround.cast(StringType())) \
.withColumn('Custom_WorkItem_Charm_Number', df_full.Custom_WorkItem_Charm_Number.cast(StringType())) \
.withColumn('Custom_workitem_ID', df_full.Custom_workitem_ID.cast(StringType())) \
.withColumn('Custom_workpackage_ID', df_full.Custom_workpackage_ID.cast(StringType())) \
.withColumn('Custom_Workshop_ID', df_full.Custom_Workshop_ID.cast(StringType())) \
.withColumn('Custom_Workshoptobepresented', df_full.Custom_Workshoptobepresented.cast(StringType())) \
.withColumn('Custom_WRICEF', df_full.Custom_WRICEF.cast(StringType())) \
.withColumn('Custom_YTD_December', df_full.Custom_YTD_December.cast(StringType())) \
.withColumn('Custom_YTD_Jan', df_full.Custom_YTD_Jan.cast(StringType())) \
.withColumn('Custom_YTD_November', df_full.Custom_YTD_November.cast(StringType())) \
.withColumn('Custom_YTD_Oct', df_full.Custom_YTD_Oct.cast(StringType())) \
.withColumn('Custom_Zone_Field_TEC', df_full.Custom_Zone_Field_TEC.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_AcceptedBySK', df_full.Microsoft_VSTS_CodeReview_AcceptedBySK.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_AcceptedDate', df_full.Microsoft_VSTS_CodeReview_AcceptedDate.cast(TimestampType())) \
.withColumn('Microsoft_VSTS_CodeReview_ClosedStatus', df_full.Microsoft_VSTS_CodeReview_ClosedStatus.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_ClosedStatusCode', df_full.Microsoft_VSTS_CodeReview_ClosedStatusCode.cast(LongType())) \
.withColumn('Microsoft_VSTS_CodeReview_ClosingComment', df_full.Microsoft_VSTS_CodeReview_ClosingComment.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_Context', df_full.Microsoft_VSTS_CodeReview_Context.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_ContextCode', df_full.Microsoft_VSTS_CodeReview_ContextCode.cast(LongType())) \
.withColumn('Microsoft_VSTS_CodeReview_ContextOwner', df_full.Microsoft_VSTS_CodeReview_ContextOwner.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_ContextType', df_full.Microsoft_VSTS_CodeReview_ContextType.cast(StringType())) \
.withColumn('Microsoft_VSTS_Common_ReviewedBySK', df_full.Microsoft_VSTS_Common_ReviewedBySK.cast(StringType())) \
.withColumn('Microsoft_VSTS_Common_StateCode', df_full.Microsoft_VSTS_Common_StateCode.cast(LongType())) \
.withColumn('Microsoft_VSTS_Feedback_ApplicationType', df_full.Microsoft_VSTS_Feedback_ApplicationType.cast(StringType())) \
.withColumn('Microsoft_VSTS_TCM_TestSuiteType', df_full.Microsoft_VSTS_TCM_TestSuiteType.cast(StringType())) \
.withColumn('Microsoft_VSTS_TCM_TestSuiteTypeId', df_full.Microsoft_VSTS_TCM_TestSuiteTypeId.cast(LongType()))

# COMMAND ----------

df_diario = df_diario \
.withColumn('WorkItemId', df_diario.WorkItemId.cast(IntegerType())) \
.withColumn('Revision', df_diario.Revision.cast(IntegerType())) \
.withColumn('RevisedDate', df_diario.RevisedDate.cast(TimestampType())) \
.withColumn('RevisedDateSK', df_diario.RevisedDateSK.cast(IntegerType())) \
.withColumn('DateSK', df_diario.DateSK.cast(IntegerType())) \
.withColumn('IsCurrent', df_diario.IsCurrent.cast(StringType())) \
.withColumn('IsLastRevisionOfDay', df_diario.IsLastRevisionOfDay.cast(StringType())) \
.withColumn('AnalyticsUpdatedDate', df_diario.AnalyticsUpdatedDate.cast(TimestampType())) \
.withColumn('ProjectSK', df_diario.ProjectSK.cast(StringType())) \
.withColumn('WorkItemRevisionSK', df_diario.WorkItemRevisionSK.cast(IntegerType())) \
.withColumn('AreaSK', df_diario.AreaSK.cast(StringType())) \
.withColumn('IterationSK', df_diario.IterationSK.cast(StringType())) \
.withColumn('AssignedToUserSK', df_diario.AssignedToUserSK.cast(StringType())) \
.withColumn('ChangedByUserSK', df_diario.ChangedByUserSK.cast(StringType())) \
.withColumn('CreatedByUserSK', df_diario.CreatedByUserSK.cast(StringType())) \
.withColumn('ActivatedByUserSK', df_diario.ActivatedByUserSK.cast(StringType())) \
.withColumn('ClosedByUserSK', df_diario.ClosedByUserSK.cast(StringType())) \
.withColumn('ResolvedByUserSK', df_diario.ResolvedByUserSK.cast(StringType())) \
.withColumn('ActivatedDateSK', df_diario.ActivatedDateSK.cast(IntegerType())) \
.withColumn('ChangedDateSK', df_diario.ChangedDateSK.cast(IntegerType())) \
.withColumn('ClosedDateSK', df_diario.ClosedDateSK.cast(IntegerType())) \
.withColumn('CreatedDateSK', df_diario.CreatedDateSK.cast(IntegerType())) \
.withColumn('ResolvedDateSK', df_diario.ResolvedDateSK.cast(IntegerType())) \
.withColumn('StateChangeDateSK', df_diario.StateChangeDateSK.cast(IntegerType())) \
.withColumn('InProgressDateSK', df_diario.InProgressDateSK.cast(IntegerType())) \
.withColumn('CompletedDateSK', df_diario.CompletedDateSK.cast(IntegerType())) \
.withColumn('Watermark', df_diario.Watermark.cast(IntegerType())) \
.withColumn('Title', df_diario.Title.cast(StringType())) \
.withColumn('WorkItemType', df_diario.WorkItemType.cast(StringType())) \
.withColumn('ChangedDate', df_diario.ChangedDate.cast(TimestampType())) \
.withColumn('CreatedDate', df_diario.CreatedDate.cast(TimestampType())) \
.withColumn('State', df_diario.State.cast(StringType())) \
.withColumn('Reason', df_diario.Reason.cast(StringType())) \
.withColumn('FoundIn', df_diario.FoundIn.cast(StringType())) \
.withColumn('IntegrationBuild', df_diario.IntegrationBuild.cast(StringType())) \
.withColumn('ActivatedDate', df_diario.ActivatedDate.cast(TimestampType())) \
.withColumn('Activity', df_diario.Activity.cast(StringType())) \
.withColumn('BacklogPriority', df_diario.BacklogPriority.cast(FloatType())) \
.withColumn('BusinessValue', df_diario.BusinessValue.cast(IntegerType())) \
.withColumn('ClosedDate', df_diario.ClosedDate.cast(TimestampType())) \
.withColumn('Issue', df_diario.Issue.cast(StringType())) \
.withColumn('Priority', df_diario.Priority.cast(IntegerType())) \
.withColumn('Rating', df_diario.Rating.cast(StringType())) \
.withColumn('ResolvedDate', df_diario.ResolvedDate.cast(TimestampType())) \
.withColumn('Severity', df_diario.Severity.cast(StringType())) \
.withColumn('TimeCriticality', df_diario.TimeCriticality.cast(FloatType())) \
.withColumn('ValueArea', df_diario.ValueArea.cast(StringType())) \
.withColumn('DueDate', df_diario.DueDate.cast(TimestampType())) \
.withColumn('Effort', df_diario.Effort.cast(FloatType())) \
.withColumn('FinishDate', df_diario.FinishDate.cast(TimestampType())) \
.withColumn('RemainingWork', df_diario.RemainingWork.cast(FloatType())) \
.withColumn('StartDate', df_diario.StartDate.cast(TimestampType())) \
.withColumn('TargetDate', df_diario.TargetDate.cast(TimestampType())) \
.withColumn('Blocked', df_diario.Blocked.cast(StringType())) \
.withColumn('ParentWorkItemId', df_diario.ParentWorkItemId.cast(IntegerType())) \
.withColumn('TagNames', df_diario.TagNames.cast(StringType())) \
.withColumn('StateCategory', df_diario.StateCategory.cast(StringType())) \
.withColumn('InProgressDate', df_diario.InProgressDate.cast(TimestampType())) \
.withColumn('CompletedDate', df_diario.CompletedDate.cast(TimestampType())) \
.withColumn('LeadTimeDays', df_diario.LeadTimeDays.cast(FloatType())) \
.withColumn('CycleTimeDays', df_diario.CycleTimeDays.cast(FloatType())) \
.withColumn('AutomatedTestId', df_diario.AutomatedTestId.cast(StringType())) \
.withColumn('AutomatedTestName', df_diario.AutomatedTestName.cast(StringType())) \
.withColumn('AutomatedTestStorage', df_diario.AutomatedTestStorage.cast(StringType())) \
.withColumn('AutomatedTestType', df_diario.AutomatedTestType.cast(StringType())) \
.withColumn('AutomationStatus', df_diario.AutomationStatus.cast(StringType())) \
.withColumn('StateChangeDate', df_diario.StateChangeDate.cast(TimestampType())) \
.withColumn('Count', df_diario.Count.cast(LongType())) \
.withColumn('CommentCount', df_diario.CommentCount.cast(IntegerType())) \
.withColumn('ABIProprietaryMethodology_IssueCreatedDate', df_diario.ABIProprietaryMethodology_IssueCreatedDate.cast(TimestampType())) \
.withColumn('ABIProprietaryMethodology_IssueOwnerSK', df_diario.ABIProprietaryMethodology_IssueOwnerSK.cast(StringType())) \
.withColumn('Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866', df_diario.Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866.cast(StringType())) \
.withColumn('Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17', df_diario.Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17.cast(StringType())) \
.withColumn('Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422', df_diario.Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422.cast(StringType())) \
.withColumn('Custom_AccordingwithSOXcontrols', df_diario.Custom_AccordingwithSOXcontrols.cast(StringType())) \
.withColumn('Custom_ActualWork', df_diario.Custom_ActualWork.cast(FloatType())) \
.withColumn('Custom_Add_Remove_Fields', df_diario.Custom_Add_Remove_Fields.cast(StringType())) \
.withColumn('Custom_Affected_APPs_Transactions', df_diario.Custom_Affected_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_Affected_Workstream', df_diario.Custom_Affected_Workstream.cast(StringType())) \
.withColumn('Custom_Analytics_Relevant', df_diario.Custom_Analytics_Relevant.cast(StringType())) \
.withColumn('Custom_Analytics_Transport_Request', df_diario.Custom_Analytics_Transport_Request.cast(StringType())) \
.withColumn('Custom_APIs_Description', df_diario.Custom_APIs_Description.cast(StringType())) \
.withColumn('Custom_Applicable_Contingencies_Workarounds', df_diario.Custom_Applicable_Contingencies_Workarounds.cast(StringType())) \
.withColumn('Custom_APPs_Transactions', df_diario.Custom_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_AssociatedProject', df_diario.Custom_AssociatedProject.cast(StringType())) \
.withColumn('Custom_Aurora_Category', df_diario.Custom_Aurora_Category.cast(StringType())) \
.withColumn('Custom_Automation', df_diario.Custom_Automation.cast(StringType())) \
.withColumn('Custom_AutomationBlankField', df_diario.Custom_AutomationBlankField.cast(StringType())) \
.withColumn('Custom_AutomationNeeded', df_diario.Custom_AutomationNeeded.cast(StringType())) \
.withColumn('Custom_AutomationNeeded2', df_diario.Custom_AutomationNeeded2.cast(StringType())) \
.withColumn('Custom_Block_Workstream_Requester', df_diario.Custom_Block_Workstream_Requester.cast(StringType())) \
.withColumn('Custom_Blocked_TestCase', df_diario.Custom_Blocked_TestCase.cast(StringType())) \
.withColumn('Custom_BlockedJustification', df_diario.Custom_BlockedJustification.cast(StringType())) \
.withColumn('Custom_BuC_Approved_Test_Result', df_diario.Custom_BuC_Approved_Test_Result.cast(StringType())) \
.withColumn('Custom_BuC_Approved_Test_Script', df_diario.Custom_BuC_Approved_Test_Script.cast(StringType())) \
.withColumn('Custom_BuC_Impacted', df_diario.Custom_BuC_Impacted.cast(StringType())) \
.withColumn('Custom_Bug_Phase', df_diario.Custom_Bug_Phase.cast(StringType())) \
.withColumn('Custom_Bug_Root_Cause', df_diario.Custom_Bug_Root_Cause.cast(StringType())) \
.withColumn('Custom_Bug_Workstream_Requester', df_diario.Custom_Bug_Workstream_Requester.cast(StringType())) \
.withColumn('Custom_BuildType', df_diario.Custom_BuildType.cast(StringType())) \
.withColumn('Custom_BusinessProcessImpact', df_diario.Custom_BusinessProcessImpact.cast(StringType())) \
.withColumn('Custom_BusinessRolesPersona', df_diario.Custom_BusinessRolesPersona.cast(StringType())) \
.withColumn('Custom_Centralization', df_diario.Custom_Centralization.cast(StringType())) \
.withColumn('Custom_CID', df_diario.Custom_CID.cast(StringType())) \
.withColumn('Custom_Classification', df_diario.Custom_Classification.cast(StringType())) \
.withColumn('Custom_Closed_Date_Transport_Request', df_diario.Custom_Closed_Date_Transport_Request.cast(TimestampType())) \
.withColumn('Custom_Complexity', df_diario.Custom_Complexity.cast(StringType())) \
.withColumn('Custom_ConditionName', df_diario.Custom_ConditionName.cast(StringType())) \
.withColumn('Custom_Contingency', df_diario.Custom_Contingency.cast(StringType())) \
.withColumn('Custom_Control_Name', df_diario.Custom_Control_Name.cast(StringType())) \
.withColumn('Custom_Criticality', df_diario.Custom_Criticality.cast(StringType())) \
.withColumn('Custom_Custom2', df_diario.Custom_Custom2.cast(StringType())) \
.withColumn('Custom_DARelated', df_diario.Custom_DARelated.cast(StringType())) \
.withColumn('Custom_DateIdentified', df_diario.Custom_DateIdentified.cast(TimestampType())) \
.withColumn('Custom_Deadline', df_diario.Custom_Deadline.cast(TimestampType())) \
.withColumn('Custom_Dependency', df_diario.Custom_Dependency.cast(StringType())) \
.withColumn('Custom_DependencyConsumer', df_diario.Custom_DependencyConsumer.cast(StringType())) \
.withColumn('Custom_DependencyProvider', df_diario.Custom_DependencyProvider.cast(StringType())) \
.withColumn('Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8', df_diario.Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8.cast(StringType())) \
.withColumn('Custom_E2E_Impacted', df_diario.Custom_E2E_Impacted.cast(StringType())) \
.withColumn('Custom_E2E_Name', df_diario.Custom_E2E_Name.cast(StringType())) \
.withColumn('Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e', df_diario.Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e.cast(StringType())) \
.withColumn('Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e', df_diario.Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e.cast(TimestampType())) \
.withColumn('Custom_Effort_Aurora', df_diario.Custom_Effort_Aurora.cast(LongType())) \
.withColumn('Custom_email', df_diario.Custom_email.cast(StringType())) \
.withColumn('Custom_EnhancementGAPID', df_diario.Custom_EnhancementGAPID.cast(StringType())) \
.withColumn('Custom_Environment_Field_TEC', df_diario.Custom_Environment_Field_TEC.cast(StringType())) \
.withColumn('Custom_ER_Affected_APPs_Transactions', df_diario.Custom_ER_Affected_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_ER_Affected_Workstream', df_diario.Custom_ER_Affected_Workstream.cast(StringType())) \
.withColumn('Custom_ER_Analytics_Transport_Request', df_diario.Custom_ER_Analytics_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_Applicable_Contingencies_Workarounds', df_diario.Custom_ER_Applicable_Contingencies_Workarounds.cast(StringType())) \
.withColumn('Custom_ER_APPs_Transactions', df_diario.Custom_ER_APPs_Transactions.cast(StringType())) \
.withColumn('Custom_ER_Blocked_TestCase', df_diario.Custom_ER_Blocked_TestCase.cast(StringType())) \
.withColumn('Custom_ER_E2E_Impacted', df_diario.Custom_ER_E2E_Impacted.cast(StringType())) \
.withColumn('Custom_ER_GAP_APP_ID', df_diario.Custom_ER_GAP_APP_ID.cast(StringType())) \
.withColumn('Custom_ER_Impact_Information', df_diario.Custom_ER_Impact_Information.cast(StringType())) \
.withColumn('Custom_ER_Logistics_Transport_Request', df_diario.Custom_ER_Logistics_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_MDG_Transport_Request', df_diario.Custom_ER_MDG_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_OTC_Transport_Request', df_diario.Custom_ER_OTC_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_P2P_Transport_Request', df_diario.Custom_ER_P2P_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_RTR_Transport_Request', df_diario.Custom_ER_RTR_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_SAP_Change', df_diario.Custom_ER_SAP_Change.cast(StringType())) \
.withColumn('Custom_ER_Security_Authorizations_Impacts', df_diario.Custom_ER_Security_Authorizations_Impacts.cast(StringType())) \
.withColumn('Custom_ER_SIT_Committee_Approval', df_diario.Custom_ER_SIT_Committee_Approval.cast(StringType())) \
.withColumn('Custom_ER_SIT_Committee_Comments', df_diario.Custom_ER_SIT_Committee_Comments.cast(StringType())) \
.withColumn('Custom_ER_Supply_Transport_Request', df_diario.Custom_ER_Supply_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_TEC_Transport_Request', df_diario.Custom_ER_TEC_Transport_Request.cast(StringType())) \
.withColumn('Custom_ER_Transport_List', df_diario.Custom_ER_Transport_List.cast(StringType())) \
.withColumn('Custom_ER_Transport_Status', df_diario.Custom_ER_Transport_Status.cast(StringType())) \
.withColumn('Custom_ER_WorkItem_Charm_Number', df_diario.Custom_ER_WorkItem_Charm_Number.cast(StringType())) \
.withColumn('Custom_EscalationDate', df_diario.Custom_EscalationDate.cast(TimestampType())) \
.withColumn('Custom_EscalationNeeded', df_diario.Custom_EscalationNeeded.cast(StringType())) \
.withColumn('Custom_Executed_Today', df_diario.Custom_Executed_Today.cast(FloatType())) \
.withColumn('Custom_FeatureCategory', df_diario.Custom_FeatureCategory.cast(StringType())) \
.withColumn('Custom_FeatureType', df_diario.Custom_FeatureType.cast(StringType())) \
.withColumn('Custom_FioriApp_System', df_diario.Custom_FioriApp_System.cast(StringType())) \
.withColumn('Custom_FirstDueDate', df_diario.Custom_FirstDueDate.cast(TimestampType())) \
.withColumn('Custom_FIT_GAP', df_diario.Custom_FIT_GAP.cast(StringType())) \
.withColumn('Custom_FIT_ID', df_diario.Custom_FIT_ID.cast(StringType())) \
.withColumn('Custom_Flow_FSEC_1', df_diario.Custom_Flow_FSEC_1.cast(StringType())) \
.withColumn('Custom_Flow_FSEC_2', df_diario.Custom_Flow_FSEC_2.cast(StringType())) \
.withColumn('Custom_Flow_FSEC_3', df_diario.Custom_Flow_FSEC_3.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_CPO_Approval', df_diario.Custom_FS_Submitted_for_CPO_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_GRC_M_Approval', df_diario.Custom_FS_Submitted_for_GRC_M_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_GRC_SOX_Approval', df_diario.Custom_FS_Submitted_for_GRC_SOX_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_GRC_Testing_Approval', df_diario.Custom_FS_Submitted_for_GRC_Testing_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_PO_Approval', df_diario.Custom_FS_Submitted_for_PO_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_QA_Approval', df_diario.Custom_FS_Submitted_for_QA_Approval.cast(StringType())) \
.withColumn('Custom_FS_Submitted_for_TI_Approval', df_diario.Custom_FS_Submitted_for_TI_Approval.cast(StringType())) \
.withColumn('Custom_FSID', df_diario.Custom_FSID.cast(StringType())) \
.withColumn('Custom_FutureRelease', df_diario.Custom_FutureRelease.cast(StringType())) \
.withColumn('Custom_GAP_APP_ID_Related', df_diario.Custom_GAP_APP_ID_Related.cast(StringType())) \
.withColumn('Custom_GAP_Reprioritization', df_diario.Custom_GAP_Reprioritization.cast(StringType())) \
.withColumn('Custom_GAPAPPID', df_diario.Custom_GAPAPPID.cast(StringType())) \
.withColumn('Custom_GAPCategory', df_diario.Custom_GAPCategory.cast(StringType())) \
.withColumn('Custom_GAPEnhancement', df_diario.Custom_GAPEnhancement.cast(StringType())) \
.withColumn('Custom_GAPEstimation', df_diario.Custom_GAPEstimation.cast(StringType())) \
.withColumn('Custom_GAPNumber', df_diario.Custom_GAPNumber.cast(StringType())) \
.withColumn('Custom_Glassif', df_diario.Custom_Glassif.cast(StringType())) \
.withColumn('Custom_Global_Local_Escope', df_diario.Custom_Global_Local_Escope.cast(StringType())) \
.withColumn('Custom_GRC_BUC_APPROVED', df_diario.Custom_GRC_BUC_APPROVED.cast(StringType())) \
.withColumn('Custom_GRC_TESTING_APPROVED', df_diario.Custom_GRC_TESTING_APPROVED.cast(StringType())) \
.withColumn('Custom_GRC_UAM_APPROVED', df_diario.Custom_GRC_UAM_APPROVED.cast(StringType())) \
.withColumn('Custom_GUID', df_diario.Custom_GUID.cast(StringType())) \
.withColumn('Custom_Impact', df_diario.Custom_Impact.cast(StringType())) \
.withColumn('Custom_Impact_Information', df_diario.Custom_Impact_Information.cast(StringType())) \
.withColumn('Custom_IndexE2E', df_diario.Custom_IndexE2E.cast(StringType())) \
.withColumn('Custom_IndexProcess', df_diario.Custom_IndexProcess.cast(StringType())) \
.withColumn('Custom_Interim_Solution', df_diario.Custom_Interim_Solution.cast(StringType())) \
.withColumn('Custom_IsGlobalSteerCoRequired', df_diario.Custom_IsGlobalSteerCoRequired.cast(StringType())) \
.withColumn('Custom_IsReportRequired', df_diario.Custom_IsReportRequired.cast(StringType())) \
.withColumn('Custom_IssuePriority', df_diario.Custom_IssuePriority.cast(StringType())) \
.withColumn('Custom_L2', df_diario.Custom_L2.cast(StringType())) \
.withColumn('Custom_L2Product', df_diario.Custom_L2Product.cast(StringType())) \
.withColumn('Custom_L3', df_diario.Custom_L3.cast(StringType())) \
.withColumn('Custom_L3Product', df_diario.Custom_L3Product.cast(StringType())) \
.withColumn('Custom_L4Product', df_diario.Custom_L4Product.cast(StringType())) \
.withColumn('Custom_L4WorkShopDescription', df_diario.Custom_L4WorkShopDescription.cast(StringType())) \
.withColumn('Custom_LE_December', df_diario.Custom_LE_December.cast(StringType())) \
.withColumn('Custom_LE_January', df_diario.Custom_LE_January.cast(StringType())) \
.withColumn('Custom_LE_November', df_diario.Custom_LE_November.cast(StringType())) \
.withColumn('Custom_LE_October', df_diario.Custom_LE_October.cast(StringType())) \
.withColumn('Custom_LeadershipApproval', df_diario.Custom_LeadershipApproval.cast(StringType())) \
.withColumn('Custom_Legacies_Integration', df_diario.Custom_Legacies_Integration.cast(StringType())) \
.withColumn('Custom_LegaciesDescription', df_diario.Custom_LegaciesDescription.cast(StringType())) \
.withColumn('Custom_Logistics_Transport_Request', df_diario.Custom_Logistics_Transport_Request.cast(StringType())) \
.withColumn('Custom_Main_Blocker', df_diario.Custom_Main_Blocker.cast(StringType())) \
.withColumn('Custom_MDG_Relevant', df_diario.Custom_MDG_Relevant.cast(StringType())) \
.withColumn('Custom_MDG_Transport_Request', df_diario.Custom_MDG_Transport_Request.cast(StringType())) \
.withColumn('Custom_Microservices_Change', df_diario.Custom_Microservices_Change.cast(StringType())) \
.withColumn('Custom_Microservices_Description', df_diario.Custom_Microservices_Description.cast(StringType())) \
.withColumn('Custom_MoscowPriority', df_diario.Custom_MoscowPriority.cast(StringType())) \
.withColumn('Custom_Name_Of_Environment', df_diario.Custom_Name_Of_Environment.cast(StringType())) \
.withColumn('Custom_NonSAP', df_diario.Custom_NonSAP.cast(StringType())) \
.withColumn('Custom_NoSAPApprovalJustification', df_diario.Custom_NoSAPApprovalJustification.cast(StringType())) \
.withColumn('Custom_NotAllowedForLocalTeam', df_diario.Custom_NotAllowedForLocalTeam.cast(StringType())) \
.withColumn('Custom_NotAllowedtothisState', df_diario.Custom_NotAllowedtothisState.cast(StringType())) \
.withColumn('Custom_OD_LE_Dec', df_diario.Custom_OD_LE_Dec.cast(StringType())) \
.withColumn('Custom_OD_LE_Nov', df_diario.Custom_OD_LE_Nov.cast(StringType())) \
.withColumn('Custom_OD_LE_Oct', df_diario.Custom_OD_LE_Oct.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_Consultant_Approval', df_diario.Custom_OD_Submitted_for_Consultant_Approval.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_CPO_Approval', df_diario.Custom_OD_Submitted_for_CPO_Approval.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_DA_Approval_CCPO', df_diario.Custom_OD_Submitted_for_DA_Approval_CCPO.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_PO_Approval', df_diario.Custom_OD_Submitted_for_PO_Approval.cast(StringType())) \
.withColumn('Custom_OD_Submitted_for_TI_Approval', df_diario.Custom_OD_Submitted_for_TI_Approval.cast(StringType())) \
.withColumn('Custom_OD_Target_Dec', df_diario.Custom_OD_Target_Dec.cast(StringType())) \
.withColumn('Custom_OD_Target_Nov', df_diario.Custom_OD_Target_Nov.cast(StringType())) \
.withColumn('Custom_OD_Target_Oct', df_diario.Custom_OD_Target_Oct.cast(StringType())) \
.withColumn('Custom_OD_YTD_Dec', df_diario.Custom_OD_YTD_Dec.cast(StringType())) \
.withColumn('Custom_OD_YTD_Nov', df_diario.Custom_OD_YTD_Nov.cast(StringType())) \
.withColumn('Custom_OD_YTD_Oct', df_diario.Custom_OD_YTD_Oct.cast(StringType())) \
.withColumn('Custom_OpenDesignID', df_diario.Custom_OpenDesignID.cast(StringType())) \
.withColumn('Custom_OpenDesignType', df_diario.Custom_OpenDesignType.cast(StringType())) \
.withColumn('Custom_OTC_Transport_Request', df_diario.Custom_OTC_Transport_Request.cast(StringType())) \
.withColumn('Custom_Out_Scope', df_diario.Custom_Out_Scope.cast(StringType())) \
.withColumn('Custom_OutsideAuroraRecoveryDate', df_diario.Custom_OutsideAuroraRecoveryDate.cast(TimestampType())) \
.withColumn('Custom_OutsideAuroraStartDate', df_diario.Custom_OutsideAuroraStartDate.cast(TimestampType())) \
.withColumn('Custom_OverviewNonSAP', df_diario.Custom_OverviewNonSAP.cast(StringType())) \
.withColumn('Custom_P2P_Transport_Request', df_diario.Custom_P2P_Transport_Request.cast(StringType())) \
.withColumn('Custom_PartnerAnalysisTargetDate', df_diario.Custom_PartnerAnalysisTargetDate.cast(TimestampType())) \
.withColumn('Custom_PartnerReleaseTargetDate', df_diario.Custom_PartnerReleaseTargetDate.cast(TimestampType())) \
.withColumn('Custom_PartnerResponsible', df_diario.Custom_PartnerResponsible.cast(StringType())) \
.withColumn('Custom_PartnerStatus', df_diario.Custom_PartnerStatus.cast(StringType())) \
.withColumn('Custom_PlannedDate', df_diario.Custom_PlannedDate.cast(TimestampType())) \
.withColumn('Custom_PlannedStartDatetoBuild', df_diario.Custom_PlannedStartDatetoBuild.cast(TimestampType())) \
.withColumn('Custom_PlannedWeeklySprint', df_diario.Custom_PlannedWeeklySprint.cast(TimestampType())) \
.withColumn('Custom_PotentialImpact', df_diario.Custom_PotentialImpact.cast(StringType())) \
.withColumn('Custom_PriorityforRelease', df_diario.Custom_PriorityforRelease.cast(LongType())) \
.withColumn('Custom_PriorityRank', df_diario.Custom_PriorityRank.cast(LongType())) \
.withColumn('Custom_ProbabilityofOccurrence', df_diario.Custom_ProbabilityofOccurrence.cast(StringType())) \
.withColumn('Custom_Product_Field_TEC', df_diario.Custom_Product_Field_TEC.cast(StringType())) \
.withColumn('Custom_ProfileAccessInformation', df_diario.Custom_ProfileAccessInformation.cast(StringType())) \
.withColumn('Custom_RAIDPriority', df_diario.Custom_RAIDPriority.cast(StringType())) \
.withColumn('Custom_ReasonforFutureRelease', df_diario.Custom_ReasonforFutureRelease.cast(StringType())) \
.withColumn('Custom_Related_Interface', df_diario.Custom_Related_Interface.cast(StringType())) \
.withColumn('Custom_Related_Interface_Description', df_diario.Custom_Related_Interface_Description.cast(StringType())) \
.withColumn('Custom_Related_Interface_Systems', df_diario.Custom_Related_Interface_Systems.cast(FloatType())) \
.withColumn('Custom_Related_LIB', df_diario.Custom_Related_LIB.cast(StringType())) \
.withColumn('Custom_RelatedWorkStream', df_diario.Custom_RelatedWorkStream.cast(StringType())) \
.withColumn('Custom_RelativeMassValuation', df_diario.Custom_RelativeMassValuation.cast(LongType())) \
.withColumn('Custom_ResolvedTargetDate', df_diario.Custom_ResolvedTargetDate.cast(TimestampType())) \
.withColumn('Custom_RiskLevel', df_diario.Custom_RiskLevel.cast(StringType())) \
.withColumn('Custom_RiskorIssuesCategory', df_diario.Custom_RiskorIssuesCategory.cast(StringType())) \
.withColumn('Custom_RiskorIssuesEscalationLevel', df_diario.Custom_RiskorIssuesEscalationLevel.cast(StringType())) \
.withColumn('Custom_RTR_Transport_Request', df_diario.Custom_RTR_Transport_Request.cast(StringType())) \
.withColumn('Custom_SAP_Change', df_diario.Custom_SAP_Change.cast(StringType())) \
.withColumn('Custom_SAPAdherence', df_diario.Custom_SAPAdherence.cast(StringType())) \
.withColumn('Custom_SAPRole', df_diario.Custom_SAPRole.cast(StringType())) \
.withColumn('Custom_Scenario', df_diario.Custom_Scenario.cast(StringType())) \
.withColumn('Custom_Scenario_Name', df_diario.Custom_Scenario_Name.cast(StringType())) \
.withColumn('Custom_Scenario_Order', df_diario.Custom_Scenario_Order.cast(FloatType())) \
.withColumn('Custom_ScopeItem', df_diario.Custom_ScopeItem.cast(StringType())) \
.withColumn('Custom_Security_Authorizations_Impacts', df_diario.Custom_Security_Authorizations_Impacts.cast(StringType())) \
.withColumn('Custom_Servicenow_Close_Notes', df_diario.Custom_Servicenow_Close_Notes.cast(StringType())) \
.withColumn('Custom_Servicenow_ticket_ID', df_diario.Custom_Servicenow_ticket_ID.cast(StringType())) \
.withColumn('Custom_SIT_Committee_Approval', df_diario.Custom_SIT_Committee_Approval.cast(StringType())) \
.withColumn('Custom_SIT_Committee_Comments', df_diario.Custom_SIT_Committee_Comments.cast(StringType())) \
.withColumn('Custom_SolutionName', df_diario.Custom_SolutionName.cast(StringType())) \
.withColumn('Custom_SolutionType', df_diario.Custom_SolutionType.cast(StringType())) \
.withColumn('Custom_Source', df_diario.Custom_Source.cast(StringType())) \
.withColumn('Custom_StoryType', df_diario.Custom_StoryType.cast(StringType())) \
.withColumn('Custom_SubmittedforIntegrationTeam', df_diario.Custom_SubmittedforIntegrationTeam.cast(StringType())) \
.withColumn('Custom_SuccessfullyTestedinQAEnvironment', df_diario.Custom_SuccessfullyTestedinQAEnvironment.cast(StringType())) \
.withColumn('Custom_Supply_Transport_Request', df_diario.Custom_Supply_Transport_Request.cast(StringType())) \
.withColumn('Custom_System', df_diario.Custom_System.cast(StringType())) \
.withColumn('Custom_SystemName', df_diario.Custom_SystemName.cast(StringType())) \
.withColumn('Custom_Target_Dec', df_diario.Custom_Target_Dec.cast(StringType())) \
.withColumn('Custom_Target_Jan', df_diario.Custom_Target_Jan.cast(StringType())) \
.withColumn('Custom_Target_Nov', df_diario.Custom_Target_Nov.cast(StringType())) \
.withColumn('Custom_Target_Oct', df_diario.Custom_Target_Oct.cast(StringType())) \
.withColumn('Custom_TargetDateFuncSpec', df_diario.Custom_TargetDateFuncSpec.cast(TimestampType())) \
.withColumn('Custom_TargetDateGAP', df_diario.Custom_TargetDateGAP.cast(TimestampType())) \
.withColumn('Custom_TC_Automation_status', df_diario.Custom_TC_Automation_status.cast(StringType())) \
.withColumn('Custom_TEC_Transport_Request', df_diario.Custom_TEC_Transport_Request.cast(StringType())) \
.withColumn('Custom_Test_Case_ExecutionValidation', df_diario.Custom_Test_Case_ExecutionValidation.cast(StringType())) \
.withColumn('Custom_Test_Case_Phase', df_diario.Custom_Test_Case_Phase.cast(StringType())) \
.withColumn('Custom_Test_FSEC_1', df_diario.Custom_Test_FSEC_1.cast(StringType())) \
.withColumn('Custom_Test_FSEC_2', df_diario.Custom_Test_FSEC_2.cast(StringType())) \
.withColumn('Custom_Test_FSEC_3', df_diario.Custom_Test_FSEC_3.cast(StringType())) \
.withColumn('Custom_Test_FSEC_4', df_diario.Custom_Test_FSEC_4.cast(StringType())) \
.withColumn('Custom_Test_FSEC_6', df_diario.Custom_Test_FSEC_6.cast(LongType())) \
.withColumn('Custom_Test_Scenario_Description', df_diario.Custom_Test_Scenario_Description.cast(StringType())) \
.withColumn('Custom_Test_Suite_Phase', df_diario.Custom_Test_Suite_Phase.cast(StringType())) \
.withColumn('Custom_TestScenario', df_diario.Custom_TestScenario.cast(StringType())) \
.withColumn('Custom_Transport_List', df_diario.Custom_Transport_List.cast(StringType())) \
.withColumn('Custom_Transport_Request_Type', df_diario.Custom_Transport_Request_Type.cast(StringType())) \
.withColumn('Custom_Transport_Status', df_diario.Custom_Transport_Status.cast(StringType())) \
.withColumn('Custom_TransportNeeded', df_diario.Custom_TransportNeeded.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_CPO_Approval', df_diario.Custom_UC_Submitted_for_CPO_Approval.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_DA_Approval_CCPO', df_diario.Custom_UC_Submitted_for_DA_Approval_CCPO.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_GRC_Validation_PO', df_diario.Custom_UC_Submitted_for_GRC_Validation_PO.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_PO_approval', df_diario.Custom_UC_Submitted_for_PO_approval.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_SAP_Approval', df_diario.Custom_UC_Submitted_for_SAP_Approval.cast(StringType())) \
.withColumn('Custom_UC_Submitted_for_TI_Approval', df_diario.Custom_UC_Submitted_for_TI_Approval.cast(StringType())) \
.withColumn('Custom_US_Priority', df_diario.Custom_US_Priority.cast(StringType())) \
.withColumn('Custom_User_Exist_BADIs', df_diario.Custom_User_Exist_BADIs.cast(StringType())) \
.withColumn('Custom_User_Exist_BADIs_Description', df_diario.Custom_User_Exist_BADIs_Description.cast(StringType())) \
.withColumn('Custom_UserStoryAPPID', df_diario.Custom_UserStoryAPPID.cast(StringType())) \
.withColumn('Custom_UserStoryType', df_diario.Custom_UserStoryType.cast(StringType())) \
.withColumn('Custom_WaitingBuCApproval', df_diario.Custom_WaitingBuCApproval.cast(StringType())) \
.withColumn('Custom_Work_Team', df_diario.Custom_Work_Team.cast(StringType())) \
.withColumn('Custom_Workaround', df_diario.Custom_Workaround.cast(StringType())) \
.withColumn('Custom_WorkItem_Charm_Number', df_diario.Custom_WorkItem_Charm_Number.cast(StringType())) \
.withColumn('Custom_workitem_ID', df_diario.Custom_workitem_ID.cast(StringType())) \
.withColumn('Custom_workpackage_ID', df_diario.Custom_workpackage_ID.cast(StringType())) \
.withColumn('Custom_Workshop_ID', df_diario.Custom_Workshop_ID.cast(StringType())) \
.withColumn('Custom_Workshoptobepresented', df_diario.Custom_Workshoptobepresented.cast(StringType())) \
.withColumn('Custom_WRICEF', df_diario.Custom_WRICEF.cast(StringType())) \
.withColumn('Custom_YTD_December', df_diario.Custom_YTD_December.cast(StringType())) \
.withColumn('Custom_YTD_Jan', df_diario.Custom_YTD_Jan.cast(StringType())) \
.withColumn('Custom_YTD_November', df_diario.Custom_YTD_November.cast(StringType())) \
.withColumn('Custom_YTD_Oct', df_diario.Custom_YTD_Oct.cast(StringType())) \
.withColumn('Custom_Zone_Field_TEC', df_diario.Custom_Zone_Field_TEC.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_AcceptedBySK', df_diario.Microsoft_VSTS_CodeReview_AcceptedBySK.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_AcceptedDate', df_diario.Microsoft_VSTS_CodeReview_AcceptedDate.cast(TimestampType())) \
.withColumn('Microsoft_VSTS_CodeReview_ClosedStatus', df_diario.Microsoft_VSTS_CodeReview_ClosedStatus.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_ClosedStatusCode', df_diario.Microsoft_VSTS_CodeReview_ClosedStatusCode.cast(LongType())) \
.withColumn('Microsoft_VSTS_CodeReview_ClosingComment', df_diario.Microsoft_VSTS_CodeReview_ClosingComment.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_Context', df_diario.Microsoft_VSTS_CodeReview_Context.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_ContextCode', df_diario.Microsoft_VSTS_CodeReview_ContextCode.cast(LongType())) \
.withColumn('Microsoft_VSTS_CodeReview_ContextOwner', df_diario.Microsoft_VSTS_CodeReview_ContextOwner.cast(StringType())) \
.withColumn('Microsoft_VSTS_CodeReview_ContextType', df_diario.Microsoft_VSTS_CodeReview_ContextType.cast(StringType())) \
.withColumn('Microsoft_VSTS_Common_ReviewedBySK', df_diario.Microsoft_VSTS_Common_ReviewedBySK.cast(StringType())) \
.withColumn('Microsoft_VSTS_Common_StateCode', df_diario.Microsoft_VSTS_Common_StateCode.cast(LongType())) \
.withColumn('Microsoft_VSTS_Feedback_ApplicationType', df_diario.Microsoft_VSTS_Feedback_ApplicationType.cast(StringType())) \
.withColumn('Microsoft_VSTS_TCM_TestSuiteType', df_diario.Microsoft_VSTS_TCM_TestSuiteType.cast(StringType())) \
.withColumn('Microsoft_VSTS_TCM_TestSuiteTypeId', df_diario.Microsoft_VSTS_TCM_TestSuiteTypeId.cast(LongType()))

# COMMAND ----------

# Retirando linhas duplicadas

df_diario = df_diario.distinct()

# COMMAND ----------

# Salvando tabela diário como stage no banco para ser utilizanda como upsert na camada consume

df_diario.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", url)\
    .option("dbtable", "dbo.stgLOGWorkItemAurora")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

# Merge dos dois dataframes

df_merge = df_full.union(df_diario)

# COMMAND ----------

# Criando uma coluna de nome rank, ranqueando os WorkItemId pela maior data em ChangedDateSK, ou seja, o mesmo WorkItemId terá o rank maior para aquele com a data de atualização mais recente

df_merge = df_merge.withColumn(
  'rank', dense_rank().over(Window.partitionBy('WorkItemRevisionSK').orderBy(desc('ChangedDate'), desc('DataCarregamento')))
)

# COMMAND ----------

print(f'{(df_merge.filter(df_merge.rank == 2)).count()} linhas serão excluidas, pois os seus WorkItemRevisionSK correspondentes possuem atualizações')

# COMMAND ----------

# Criando o Dataframe final, filtrando apenas as linhas que resultaram em rank = 1 e retirando as colunas de controle

df = df_merge.filter(df_merge.rank == 1).drop('rank')
print(f'Qtd de colunas final: {len(df.columns)}')

num_linhas_final = df.count()

print(f'Qtd de linhas final: {num_linhas_final}')

print(f'Originalmente havia {num_linhas_full} linhas na tabela full e {num_linhas_diario} linhas na carga diaria. {num_linhas_final - num_linhas_full} linhas foram adicionadas.')

# COMMAND ----------

print('QTD LINHAS ANTES DO DISTINCT: ', df.count())
df = df.distinct()
print('QTD LINHAS DEPOIS DO DISTINCT: ', df.count())

# COMMAND ----------

# Salva a tabela de volta em modo parquet no caminho especificado

sinkPath = aurora_standardized_folder + sourceFile + '/' + max_data
print(sinkPath)

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

update_log(sourceFile, 'RAW', 'STANDARDIZED', duracao_notebook, df.count(), 2)

# COMMAND ----------

# Fim carga Stand WorkItemRevisions Diario
