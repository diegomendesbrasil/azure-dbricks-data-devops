# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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

sourceFile = 'WorkItems'
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

# Lê o arquivo parquet do path (carga full) e salva em um spark dataframe

df_diario = spark.read.format('avro').load(sourcePath_diario)

# COMMAND ----------

# A carga diária pega todas as colunas, portanto, este comando seleciona no df_diario apenas as colunas realmente utilizadas

df_diario = df_diario.select(
'ABIProprietaryMethodology_IssueCreatedDate','ABIProprietaryMethodology_IssueOwnerSK','ActivatedByUserSK','ActivatedDateSK','Activity','AreaSK','AssignedToUserSK','AutomatedTestId','AutomatedTestName',
'AutomatedTestStorage','AutomatedTestType','AutomationStatus','BacklogPriority','Blocked','BusinessValue','ChangedByUserSK','ClosedByUserSK','ClosedDateSK','CommentCount','CreatedByUserSK','CreatedDateSK','Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866','Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422','Custom_AccordingwithSOXcontrols','Custom_AssociatedProject','Custom_Aurora_Category','Custom_Automation','Custom_BlockedJustification','Custom_BuC_Approved_Test_Result','Custom_BuC_Approved_Test_Script','Custom_BuC_Impacted','Custom_BuildType','Custom_BusinessProcessImpact','Custom_BusinessRolesPersona','Custom_Centralization','Custom_Complexity','Custom_Contingency','Custom_Control_Name','Custom_Criticality','Custom_DateIdentified','Custom_Deadline','Custom_Dependency','Custom_DependencyConsumer','Custom_DependencyProvider','Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8','Custom_E2E_Name','Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e','Custom_Environment_Field_TEC','Custom_EscalationNeeded','Custom_Executed_Today','Custom_FeatureCategory','Custom_FeatureType','Custom_FioriApp_System','Custom_FIT_GAP','Custom_FS_Submitted_for_CPO_Approval','Custom_FS_Submitted_for_GRC_M_Approval','Custom_FS_Submitted_for_GRC_SOX_Approval','Custom_FS_Submitted_for_GRC_Testing_Approval','Custom_FS_Submitted_for_PO_Approval','Custom_FS_Submitted_for_QA_Approval','Custom_FS_Submitted_for_TI_Approval','Custom_FSID','Custom_FutureRelease','Custom_GAP_Reprioritization','Custom_GAPAPPID','Custom_GAPCategory','Custom_GAPEstimation','Custom_GAPNumber','Custom_Glassif','Custom_Global_Local_Escope','Custom_GRC_BUC_APPROVED','Custom_GRC_TESTING_APPROVED','Custom_GRC_UAM_APPROVED','Custom_GUID','Custom_IndexProcess','Custom_IsReportRequired','Custom_IssuePriority','Custom_L2','Custom_L2Product','Custom_L3','Custom_L3Product','Custom_L4Product','Custom_L4WorkShopDescription','Custom_LE_December','Custom_LE_January','Custom_LE_November','Custom_LE_October','Custom_LeadershipApproval','Custom_Legacies_Integration','Custom_LegaciesDescription','Custom_Main_Blocker','Custom_MDG_Relevant','Custom_MoscowPriority','Custom_Name_Of_Environment','Custom_NonSAP','Custom_NoSAPApprovalJustification','Custom_NotAllowedtothisState','Custom_OD_LE_Dec','Custom_OD_LE_Nov','Custom_OD_LE_Oct','Custom_OD_Submitted_for_Consultant_Approval','Custom_OD_Submitted_for_CPO_Approval','Custom_OD_Submitted_for_DA_Approval_CCPO','WorkItemId','ChangedDateSK','Custom_OD_Submitted_for_PO_Approval','Custom_OD_Submitted_for_TI_Approval','Custom_OD_Target_Dec','Custom_OD_Target_Nov','Custom_OD_Target_Oct','Custom_OD_YTD_Dec','Custom_OD_YTD_Nov','Custom_OD_YTD_Oct','Custom_OpenDesignID','Custom_OpenDesignType','Custom_Out_Scope','Custom_OutsideAuroraStartDate','Custom_OverviewNonSAP','Custom_PartnerResponsible','Custom_PartnerStatus','Custom_PlannedDate','Custom_PotentialImpact','Custom_PriorityforRelease','Custom_PriorityRank','Custom_ProbabilityofOccurrence','Custom_Product_Field_TEC','Custom_ReasonforFutureRelease','Custom_ResolvedTargetDate','Custom_RiskLevel','Custom_RiskorIssuesCategory','Custom_RiskorIssuesEscalationLevel','Custom_SAPAdherence','Custom_SAPRole','Custom_Scenario','Custom_Scenario_Name','Custom_Scenario_Order','Custom_ScopeItem','Custom_SolutionType','Custom_Source','Custom_StoryType','Custom_SystemName','Custom_Target_Dec','Custom_Target_Jan','Custom_Target_Nov','Custom_Target_Oct','Custom_TargetDateFuncSpec','Custom_TC_Automation_status','Custom_Test_Case_ExecutionValidation','Custom_Test_Case_Phase','Custom_Test_Suite_Phase','Custom_TestScenario','Custom_UC_Submitted_for_CPO_Approval','Custom_UC_Submitted_for_DA_Approval_CCPO','Custom_UC_Submitted_for_GRC_Validation_PO','Custom_UC_Submitted_for_PO_approval','Custom_UC_Submitted_for_SAP_Approval','Custom_UC_Submitted_for_TI_Approval','Custom_US_Priority','Custom_UserStoryAPPID','Custom_UserStoryType','Custom_Work_Team','Custom_Workshop_ID','Custom_Workshoptobepresented','Custom_WRICEF','Custom_YTD_December','Custom_YTD_Jan','Custom_YTD_November','Custom_YTD_Oct','Custom_Zone_Field_TEC','DueDate','Effort','FoundIn','InProgressDateSK','IntegrationBuild','IterationSK','Microsoft_VSTS_TCM_TestSuiteType','Microsoft_VSTS_TCM_TestSuiteTypeId','ParentWorkItemId','Priority','Reason','RemainingWork','ResolvedByUserSK','ResolvedDateSK','Revision','Severity','StartDate','State','StateCategory','StateChangeDateSK','TagNames','TargetDate','TimeCriticality','Title','ValueArea','Watermark','WorkItemRevisionSK','WorkItemType','Custom_IndexE2E','Custom_TargetDateGAP','Custom_Classification', 'DataCarregamento')

# COMMAND ----------

# Substitui campos None para None do Python

df_diario = df_diario.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

# Tipagem das colunas do Dataframe (Schema)

df_diario = df_diario\
  .withColumn('ABIProprietaryMethodology_IssueCreatedDate', df_diario.ABIProprietaryMethodology_IssueCreatedDate.cast(TimestampType()))\
  .withColumn('ABIProprietaryMethodology_IssueOwnerSK', df_diario.ABIProprietaryMethodology_IssueOwnerSK.cast(StringType()))\
  .withColumn('ActivatedByUserSK', df_diario.ActivatedByUserSK.cast(StringType()))\
  .withColumn('ActivatedDateSK', df_diario.ActivatedDateSK.cast(IntegerType()))\
  .withColumn('Activity', df_diario.Activity.cast(StringType()))\
  .withColumn('AreaSK', df_diario.AreaSK.cast(StringType()))\
  .withColumn('AssignedToUserSK', df_diario.AssignedToUserSK.cast(StringType()))\
  .withColumn('AutomatedTestId', df_diario.AutomatedTestId.cast(StringType()))\
  .withColumn('AutomatedTestName', df_diario.AutomatedTestName.cast(StringType()))\
  .withColumn('AutomatedTestStorage', df_diario.AutomatedTestStorage.cast(StringType()))\
  .withColumn('AutomatedTestType', df_diario.AutomatedTestType.cast(StringType()))\
  .withColumn('AutomationStatus', df_diario.AutomationStatus.cast(StringType()))\
  .withColumn('BacklogPriority', df_diario.BacklogPriority.cast(IntegerType()))\
  .withColumn('Blocked', df_diario.Blocked.cast(StringType()))\
  .withColumn('BusinessValue', df_diario.BusinessValue.cast(IntegerType()))\
  .withColumn('ChangedByUserSK', df_diario.ChangedByUserSK.cast(StringType()))\
  .withColumn('ChangedDateSK', df_diario.ChangedDateSK.cast(IntegerType()))\
  .withColumn('ClosedByUserSK', df_diario.ClosedByUserSK.cast(StringType()))\
  .withColumn('ClosedDateSK', df_diario.ClosedDateSK.cast(IntegerType()))\
  .withColumn('CommentCount', df_diario.CommentCount.cast(IntegerType()))\
  .withColumn('CreatedByUserSK', df_diario.CreatedByUserSK.cast(StringType()))\
  .withColumn('CreatedDateSK', df_diario.CreatedDateSK.cast(IntegerType()))\
  .withColumn('Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866', df_diario.Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866.cast(StringType()))\
  .withColumn('Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422', df_diario.Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422.cast(StringType()))\
  .withColumn('Custom_AccordingwithSOXcontrols', df_diario.Custom_AccordingwithSOXcontrols.cast(StringType()))\
  .withColumn('Custom_AssociatedProject', df_diario.Custom_AssociatedProject.cast(StringType()))\
  .withColumn('Custom_Aurora_Category', df_diario.Custom_Aurora_Category.cast(StringType()))\
  .withColumn('Custom_Automation', df_diario.Custom_Automation.cast(StringType()))\
  .withColumn('Custom_BlockedJustification', df_diario.Custom_BlockedJustification.cast(StringType()))\
  .withColumn('Custom_BuC_Approved_Test_Result', df_diario.Custom_BuC_Approved_Test_Result.cast(StringType()))\
  .withColumn('Custom_BuC_Approved_Test_Script', df_diario.Custom_BuC_Approved_Test_Script.cast(StringType()))\
  .withColumn('Custom_BuC_Impacted', df_diario.Custom_BuC_Impacted.cast(StringType()))\
  .withColumn('Custom_BuildType', df_diario.Custom_BuildType.cast(StringType()))\
  .withColumn('Custom_BusinessProcessImpact', df_diario.Custom_BusinessProcessImpact.cast(StringType()))\
  .withColumn('Custom_BusinessRolesPersona', df_diario.Custom_BusinessRolesPersona.cast(StringType()))\
  .withColumn('Custom_Centralization', df_diario.Custom_Centralization.cast(StringType()))\
  .withColumn('Custom_Complexity', df_diario.Custom_Complexity.cast(StringType()))\
  .withColumn('Custom_Contingency', df_diario.Custom_Contingency.cast(StringType()))\
  .withColumn('Custom_Control_Name', df_diario.Custom_Control_Name.cast(StringType()))\
  .withColumn('Custom_Criticality', df_diario.Custom_Criticality.cast(StringType()))\
  .withColumn('Custom_DateIdentified', df_diario.Custom_DateIdentified.cast(TimestampType()))\
  .withColumn('Custom_Deadline', df_diario.Custom_Deadline.cast(TimestampType()))\
  .withColumn('Custom_Dependency', df_diario.Custom_Dependency.cast(StringType()))\
  .withColumn('Custom_DependencyConsumer', df_diario.Custom_DependencyConsumer.cast(StringType()))\
  .withColumn('Custom_DependencyProvider', df_diario.Custom_DependencyProvider.cast(StringType()))\
  .withColumn('Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8', df_diario.Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8.cast(StringType()))\
  .withColumn('Custom_E2E_Name', df_diario.Custom_E2E_Name.cast(StringType()))\
  .withColumn('Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e', df_diario.Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e.cast(StringType()))\
  .withColumn('Custom_IndexE2E', df_diario.Custom_IndexE2E.cast(StringType()))\
  .withColumn('Custom_TargetDateGAP', df_diario.Custom_TargetDateGAP.cast(IntegerType()))\
  .withColumn('Custom_Classification', df_diario.Custom_Classification.cast(StringType()))\
  .withColumn('Custom_Environment_Field_TEC', df_diario.Custom_Environment_Field_TEC.cast(StringType()))\
  .withColumn('Custom_EscalationNeeded', df_diario.Custom_EscalationNeeded.cast(StringType()))\
  .withColumn('Custom_Executed_Today', df_diario.Custom_Executed_Today.cast(DecimalType()))\
  .withColumn('Custom_FeatureCategory', df_diario.Custom_FeatureCategory.cast(StringType()))\
  .withColumn('Custom_FeatureType', df_diario.Custom_FeatureType.cast(StringType()))\
  .withColumn('Custom_FioriApp_System', df_diario.Custom_FioriApp_System.cast(StringType()))\
  .withColumn('Custom_FIT_GAP', df_diario.Custom_FIT_GAP.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_CPO_Approval', df_diario.Custom_FS_Submitted_for_CPO_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_GRC_M_Approval', df_diario.Custom_FS_Submitted_for_GRC_M_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_GRC_SOX_Approval', df_diario.Custom_FS_Submitted_for_GRC_SOX_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_GRC_Testing_Approval', df_diario.Custom_FS_Submitted_for_GRC_Testing_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_PO_Approval', df_diario.Custom_FS_Submitted_for_PO_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_QA_Approval', df_diario.Custom_FS_Submitted_for_QA_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_TI_Approval', df_diario.Custom_FS_Submitted_for_TI_Approval.cast(StringType()))\
  .withColumn('Custom_FSID', df_diario.Custom_FSID.cast(StringType()))\
  .withColumn('Custom_FutureRelease', df_diario.Custom_FutureRelease.cast(StringType()))\
  .withColumn('Custom_GAP_Reprioritization', df_diario.Custom_GAP_Reprioritization.cast(StringType()))\
  .withColumn('Custom_GAPAPPID', df_diario.Custom_GAPAPPID.cast(StringType()))\
  .withColumn('Custom_GAPCategory', df_diario.Custom_GAPCategory.cast(StringType()))\
  .withColumn('Custom_GAPEstimation', df_diario.Custom_GAPEstimation.cast(StringType()))\
  .withColumn('Custom_GAPNumber', df_diario.Custom_GAPNumber.cast(StringType()))\
  .withColumn('Custom_Glassif', df_diario.Custom_Glassif.cast(StringType()))\
  .withColumn('Custom_Global_Local_Escope', df_diario.Custom_Global_Local_Escope.cast(StringType()))\
  .withColumn('Custom_GRC_BUC_APPROVED', df_diario.Custom_GRC_BUC_APPROVED.cast(StringType()))\
  .withColumn('Custom_GRC_TESTING_APPROVED', df_diario.Custom_GRC_TESTING_APPROVED.cast(StringType()))\
  .withColumn('Custom_GRC_UAM_APPROVED', df_diario.Custom_GRC_UAM_APPROVED.cast(StringType()))\
  .withColumn('Custom_GUID', df_diario.Custom_GUID.cast(StringType()))\
  .withColumn('Custom_IndexProcess', df_diario.Custom_IndexProcess.cast(StringType()))\
  .withColumn('Custom_IsReportRequired', df_diario.Custom_IsReportRequired.cast(StringType()))\
  .withColumn('Custom_IssuePriority', df_diario.Custom_IssuePriority.cast(StringType()))\
  .withColumn('Custom_L2', df_diario.Custom_L2.cast(StringType()))\
  .withColumn('Custom_L2Product', df_diario.Custom_L2Product.cast(StringType()))\
  .withColumn('Custom_L3', df_diario.Custom_L3.cast(StringType()))\
  .withColumn('Custom_L3Product', df_diario.Custom_L3Product.cast(StringType()))\
  .withColumn('Custom_L4Product', df_diario.Custom_L4Product.cast(StringType()))\
  .withColumn('Custom_L4WorkShopDescription', df_diario.Custom_L4WorkShopDescription.cast(StringType()))\
  .withColumn('Custom_LE_December', df_diario.Custom_LE_December.cast(StringType()))\
  .withColumn('Custom_LE_January', df_diario.Custom_LE_January.cast(StringType()))\
  .withColumn('Custom_LE_November', df_diario.Custom_LE_November.cast(StringType()))\
  .withColumn('Custom_LE_October', df_diario.Custom_LE_October.cast(StringType()))\
  .withColumn('Custom_LeadershipApproval', df_diario.Custom_LeadershipApproval.cast(StringType()))\
  .withColumn('Custom_Legacies_Integration', df_diario.Custom_Legacies_Integration.cast(StringType()))\
  .withColumn('Custom_LegaciesDescription', df_diario.Custom_LegaciesDescription.cast(StringType()))\
  .withColumn('Custom_Main_Blocker', df_diario.Custom_Main_Blocker.cast(StringType()))\
  .withColumn('Custom_MDG_Relevant', df_diario.Custom_MDG_Relevant.cast(StringType()))\
  .withColumn('Custom_MoscowPriority', df_diario.Custom_MoscowPriority.cast(StringType()))\
  .withColumn('Custom_Name_Of_Environment', df_diario.Custom_Name_Of_Environment.cast(StringType()))\
  .withColumn('Custom_NonSAP', df_diario.Custom_NonSAP.cast(StringType()))\
  .withColumn('Custom_NoSAPApprovalJustification', df_diario.Custom_NoSAPApprovalJustification.cast(StringType()))\
  .withColumn('Custom_NotAllowedtothisState', df_diario.Custom_NotAllowedtothisState.cast(StringType()))\
  .withColumn('Custom_OD_LE_Dec', df_diario.Custom_OD_LE_Dec.cast(StringType()))\
  .withColumn('Custom_OD_LE_Nov', df_diario.Custom_OD_LE_Nov.cast(StringType()))\
  .withColumn('Custom_OD_LE_Oct', df_diario.Custom_OD_LE_Oct.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_Consultant_Approval', df_diario.Custom_OD_Submitted_for_Consultant_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_CPO_Approval', df_diario.Custom_OD_Submitted_for_CPO_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_DA_Approval_CCPO', df_diario.Custom_OD_Submitted_for_DA_Approval_CCPO.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_PO_Approval', df_diario.Custom_OD_Submitted_for_PO_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_TI_Approval', df_diario.Custom_OD_Submitted_for_TI_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Target_Dec', df_diario.Custom_OD_Target_Dec.cast(StringType()))\
  .withColumn('Custom_OD_Target_Nov', df_diario.Custom_OD_Target_Nov.cast(StringType()))\
  .withColumn('Custom_OD_Target_Oct', df_diario.Custom_OD_Target_Oct.cast(StringType()))\
  .withColumn('Custom_OD_YTD_Dec', df_diario.Custom_OD_YTD_Dec.cast(StringType()))\
  .withColumn('Custom_OD_YTD_Nov', df_diario.Custom_OD_YTD_Nov.cast(StringType()))\
  .withColumn('Custom_OD_YTD_Oct', df_diario.Custom_OD_YTD_Oct.cast(StringType()))\
  .withColumn('Custom_OpenDesignID', df_diario.Custom_OpenDesignID.cast(StringType()))\
  .withColumn('Custom_OpenDesignType', df_diario.Custom_OpenDesignType.cast(StringType()))\
  .withColumn('Custom_Out_Scope', df_diario.Custom_Out_Scope.cast(StringType()))\
  .withColumn('Custom_OutsideAuroraStartDate', df_diario.Custom_OutsideAuroraStartDate.cast(TimestampType()))\
  .withColumn('Custom_OverviewNonSAP', df_diario.Custom_OverviewNonSAP.cast(StringType()))\
  .withColumn('Custom_PartnerResponsible', df_diario.Custom_PartnerResponsible.cast(StringType()))\
  .withColumn('Custom_PartnerStatus', df_diario.Custom_PartnerStatus.cast(StringType()))\
  .withColumn('Custom_PlannedDate', df_diario.Custom_PlannedDate.cast(TimestampType()))\
  .withColumn('Custom_PotentialImpact', df_diario.Custom_PotentialImpact.cast(StringType()))\
  .withColumn('Custom_PriorityforRelease', df_diario.Custom_PriorityforRelease.cast(IntegerType()))\
  .withColumn('Custom_PriorityRank', df_diario.Custom_PriorityRank.cast(IntegerType()))\
  .withColumn('Custom_ProbabilityofOccurrence', df_diario.Custom_ProbabilityofOccurrence.cast(StringType()))\
  .withColumn('Custom_Product_Field_TEC', df_diario.Custom_Product_Field_TEC.cast(StringType()))\
  .withColumn('Custom_ReasonforFutureRelease', df_diario.Custom_ReasonforFutureRelease.cast(StringType()))\
  .withColumn('Custom_ResolvedTargetDate', df_diario.Custom_ResolvedTargetDate.cast(TimestampType()))\
  .withColumn('Custom_RiskLevel', df_diario.Custom_RiskLevel.cast(StringType()))\
  .withColumn('Custom_RiskorIssuesCategory', df_diario.Custom_RiskorIssuesCategory.cast(StringType()))\
  .withColumn('Custom_RiskorIssuesEscalationLevel', df_diario.Custom_RiskorIssuesEscalationLevel.cast(StringType()))\
  .withColumn('Custom_SAPAdherence', df_diario.Custom_SAPAdherence.cast(StringType()))\
  .withColumn('Custom_SAPRole', df_diario.Custom_SAPRole.cast(StringType()))\
  .withColumn('Custom_Scenario', df_diario.Custom_Scenario.cast(StringType()))\
  .withColumn('Custom_Scenario_Name', df_diario.Custom_Scenario_Name.cast(StringType()))\
  .withColumn('Custom_Scenario_Order', df_diario.Custom_Scenario_Order.cast(DecimalType()))\
  .withColumn('Custom_ScopeItem', df_diario.Custom_ScopeItem.cast(StringType()))\
  .withColumn('Custom_SolutionType', df_diario.Custom_SolutionType.cast(StringType()))\
  .withColumn('Custom_Source', df_diario.Custom_Source.cast(StringType()))\
  .withColumn('Custom_StoryType', df_diario.Custom_StoryType.cast(StringType()))\
  .withColumn('Custom_SystemName', df_diario.Custom_SystemName.cast(StringType()))\
  .withColumn('Custom_Target_Dec', df_diario.Custom_Target_Dec.cast(StringType()))\
  .withColumn('Custom_Target_Jan', df_diario.Custom_Target_Jan.cast(StringType()))\
  .withColumn('Custom_Target_Nov', df_diario.Custom_Target_Nov.cast(StringType()))\
  .withColumn('Custom_Target_Oct', df_diario.Custom_Target_Oct.cast(StringType()))\
  .withColumn('Custom_TargetDateFuncSpec', df_diario.Custom_TargetDateFuncSpec.cast(TimestampType()))\
  .withColumn('Custom_TC_Automation_status', df_diario.Custom_TC_Automation_status.cast(StringType()))\
  .withColumn('Custom_Test_Case_ExecutionValidation', df_diario.Custom_Test_Case_ExecutionValidation.cast(StringType()))\
  .withColumn('Custom_Test_Case_Phase', df_diario.Custom_Test_Case_Phase.cast(StringType()))\
  .withColumn('Custom_Test_Suite_Phase', df_diario.Custom_Test_Suite_Phase.cast(StringType()))\
  .withColumn('Custom_TestScenario', df_diario.Custom_TestScenario.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_CPO_Approval', df_diario.Custom_UC_Submitted_for_CPO_Approval.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_DA_Approval_CCPO', df_diario.Custom_UC_Submitted_for_DA_Approval_CCPO.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_GRC_Validation_PO', df_diario.Custom_UC_Submitted_for_GRC_Validation_PO.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_PO_approval', df_diario.Custom_UC_Submitted_for_PO_approval.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_SAP_Approval', df_diario.Custom_UC_Submitted_for_SAP_Approval.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_TI_Approval', df_diario.Custom_UC_Submitted_for_TI_Approval.cast(StringType()))\
  .withColumn('Custom_US_Priority', df_diario.Custom_US_Priority.cast(StringType()))\
  .withColumn('Custom_UserStoryAPPID', df_diario.Custom_UserStoryAPPID.cast(StringType()))\
  .withColumn('Custom_UserStoryType', df_diario.Custom_UserStoryType.cast(StringType()))\
  .withColumn('Custom_Work_Team', df_diario.Custom_Work_Team.cast(StringType()))\
  .withColumn('Custom_Workshop_ID', df_diario.Custom_Workshop_ID.cast(StringType()))\
  .withColumn('Custom_Workshoptobepresented', df_diario.Custom_Workshoptobepresented.cast(StringType()))\
  .withColumn('Custom_WRICEF', df_diario.Custom_WRICEF.cast(StringType()))\
  .withColumn('Custom_YTD_December', df_diario.Custom_YTD_December.cast(StringType()))\
  .withColumn('Custom_YTD_Jan', df_diario.Custom_YTD_Jan.cast(StringType()))\
  .withColumn('Custom_YTD_November', df_diario.Custom_YTD_November.cast(StringType()))\
  .withColumn('Custom_YTD_Oct', df_diario.Custom_YTD_Oct.cast(StringType()))\
  .withColumn('Custom_Zone_Field_TEC', df_diario.Custom_Zone_Field_TEC.cast(StringType()))\
  .withColumn('DueDate', df_diario.DueDate.cast(TimestampType()))\
  .withColumn('Effort', df_diario.Effort.cast(DecimalType()))\
  .withColumn('FoundIn', df_diario.FoundIn.cast(StringType()))\
  .withColumn('InProgressDateSK', df_diario.InProgressDateSK.cast(IntegerType()))\
  .withColumn('IntegrationBuild', df_diario.IntegrationBuild.cast(StringType()))\
  .withColumn('IterationSK', df_diario.IterationSK.cast(StringType()))\
  .withColumn('Microsoft_VSTS_TCM_TestSuiteType', df_diario.Microsoft_VSTS_TCM_TestSuiteType.cast(StringType()))\
  .withColumn('Microsoft_VSTS_TCM_TestSuiteTypeId', df_diario.Microsoft_VSTS_TCM_TestSuiteTypeId.cast(IntegerType()))\
  .withColumn('ParentWorkItemId', df_diario.ParentWorkItemId.cast(IntegerType()))\
  .withColumn('Priority', df_diario.Priority.cast(IntegerType()))\
  .withColumn('Reason', df_diario.Reason.cast(StringType()))\
  .withColumn('RemainingWork', df_diario.RemainingWork.cast(IntegerType()))\
  .withColumn('ResolvedByUserSK', df_diario.ResolvedByUserSK.cast(StringType()))\
  .withColumn('ResolvedDateSK', df_diario.ResolvedDateSK.cast(IntegerType()))\
  .withColumn('Revision', df_diario.Revision.cast(IntegerType()))\
  .withColumn('Severity', df_diario.Severity.cast(StringType()))\
  .withColumn('StartDate', df_diario.StartDate.cast(TimestampType()))\
  .withColumn('State', df_diario.State.cast(StringType()))\
  .withColumn('StateCategory', df_diario.StateCategory.cast(StringType()))\
  .withColumn('StateChangeDateSK', df_diario.StateChangeDateSK.cast(IntegerType()))\
  .withColumn('TagNames', df_diario.TagNames.cast(StringType()))\
  .withColumn('TargetDate', df_diario.TargetDate.cast(TimestampType()))\
  .withColumn('TimeCriticality', df_diario.TimeCriticality.cast(IntegerType()))\
  .withColumn('Title', df_diario.Title.cast(StringType()))\
  .withColumn('ValueArea', df_diario.ValueArea.cast(StringType()))\
  .withColumn('Watermark', df_diario.Watermark.cast(IntegerType()))\
  .withColumn('WorkItemId', df_diario.WorkItemId.cast(LongType()))\
  .withColumn('WorkItemRevisionSK', df_diario.WorkItemRevisionSK.cast(LongType()))\
  .withColumn('WorkItemType', df_diario.WorkItemType.cast(StringType())
)

# COMMAND ----------

# Número de linhas e de colunas do dataframe com a carga diária

num_linhas_diario = df_diario.count()

print(f'Número de colunas no df diario: {len(df_diario.columns)}')
print(f'Número de linhas no df diario: {num_linhas_diario}')

# COMMAND ----------

# Colocando as colunas em ordem alfabética

df_full = df_full.select(
col('ABIProprietaryMethodology_IssueCreatedDate'),
col('ABIProprietaryMethodology_IssueOwnerSK'),
col('ActivatedByUserSK'),
col('ActivatedDateSK'),
col('Activity'),
col('AreaSK'),
col('AssignedToUserSK'),
col('AutomatedTestId'),
col('AutomatedTestName'),
col('AutomatedTestStorage'),
col('AutomatedTestType'),
col('AutomationStatus'),
col('BacklogPriority'),
col('Blocked'),
col('BusinessValue'),
col('ChangedByUserSK'),
col('ChangedDateSK'),
col('ClosedByUserSK'),
col('ClosedDateSK'),
col('CommentCount'),
col('CreatedByUserSK'),
col('CreatedDateSK'),
col('Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866'),
col('Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422'),
col('Custom_AccordingwithSOXcontrols'),
col('Custom_AssociatedProject'),
col('Custom_Aurora_Category'),
col('Custom_Automation'),
col('Custom_BlockedJustification'),
col('Custom_BuC_Approved_Test_Result'),
col('Custom_BuC_Approved_Test_Script'),
col('Custom_BuC_Impacted'),
col('Custom_BuildType'),
col('Custom_BusinessProcessImpact'),
col('Custom_BusinessRolesPersona'),
col('Custom_Centralization'),
col('Custom_Classification'),
col('Custom_Complexity'),
col('Custom_Contingency'),
col('Custom_Control_Name'),
col('Custom_Criticality'),
col('Custom_DateIdentified'),
col('Custom_Deadline'),
col('Custom_Dependency'),
col('Custom_DependencyConsumer'),
col('Custom_DependencyProvider'),
col('Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8'),
col('Custom_E2E_Name'),
col('Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e'),
col('Custom_Environment_Field_TEC'),
col('Custom_EscalationNeeded'),
col('Custom_Executed_Today'),
col('Custom_FeatureCategory'),
col('Custom_FeatureType'),
col('Custom_FioriApp_System'),
col('Custom_FIT_GAP'),
col('Custom_FS_Submitted_for_CPO_Approval'),
col('Custom_FS_Submitted_for_GRC_M_Approval'),
col('Custom_FS_Submitted_for_GRC_SOX_Approval'),
col('Custom_FS_Submitted_for_GRC_Testing_Approval'),
col('Custom_FS_Submitted_for_PO_Approval'),
col('Custom_FS_Submitted_for_QA_Approval'),
col('Custom_FS_Submitted_for_TI_Approval'),
col('Custom_FSID'),
col('Custom_FutureRelease'),
col('Custom_GAP_Reprioritization'),
col('Custom_GAPAPPID'),
col('Custom_GAPCategory'),
col('Custom_GAPEstimation'),
col('Custom_GAPNumber'),
col('Custom_Glassif'),
col('Custom_Global_Local_Escope'),
col('Custom_GRC_BUC_APPROVED'),
col('Custom_GRC_TESTING_APPROVED'),
col('Custom_GRC_UAM_APPROVED'),
col('Custom_GUID'),
col('Custom_IndexE2E'),
col('Custom_IndexProcess'),
col('Custom_IsReportRequired'),
col('Custom_IssuePriority'),
col('Custom_L2'),
col('Custom_L2Product'),
col('Custom_L3'),
col('Custom_L3Product'),
col('Custom_L4Product'),
col('Custom_L4WorkShopDescription'),
col('Custom_LE_December'),
col('Custom_LE_January'),
col('Custom_LE_November'),
col('Custom_LE_October'),
col('Custom_LeadershipApproval'),
col('Custom_Legacies_Integration'),
col('Custom_LegaciesDescription'),
col('Custom_Main_Blocker'),
col('Custom_MDG_Relevant'),
col('Custom_MoscowPriority'),
col('Custom_Name_Of_Environment'),
col('Custom_NonSAP'),
col('Custom_NoSAPApprovalJustification'),
col('Custom_NotAllowedtothisState'),
col('Custom_OD_LE_Dec'),
col('Custom_OD_LE_Nov'),
col('Custom_OD_LE_Oct'),
col('Custom_OD_Submitted_for_Consultant_Approval'),
col('Custom_OD_Submitted_for_CPO_Approval'),
col('Custom_OD_Submitted_for_DA_Approval_CCPO'),
col('Custom_OD_Submitted_for_PO_Approval'),
col('Custom_OD_Submitted_for_TI_Approval'),
col('Custom_OD_Target_Dec'),
col('Custom_OD_Target_Nov'),
col('Custom_OD_Target_Oct'),
col('Custom_OD_YTD_Dec'),
col('Custom_OD_YTD_Nov'),
col('Custom_OD_YTD_Oct'),
col('Custom_OpenDesignID'),
col('Custom_OpenDesignType'),
col('Custom_Out_Scope'),
col('Custom_OutsideAuroraStartDate'),
col('Custom_OverviewNonSAP'),
col('Custom_PartnerResponsible'),
col('Custom_PartnerStatus'),
col('Custom_PlannedDate'),
col('Custom_PotentialImpact'),
col('Custom_PriorityforRelease'),
col('Custom_PriorityRank'),
col('Custom_ProbabilityofOccurrence'),
col('Custom_Product_Field_TEC'),
col('Custom_ReasonforFutureRelease'),
col('Custom_ResolvedTargetDate'),
col('Custom_RiskLevel'),
col('Custom_RiskorIssuesCategory'),
col('Custom_RiskorIssuesEscalationLevel'),
col('Custom_SAPAdherence'),
col('Custom_SAPRole'),
col('Custom_Scenario'),
col('Custom_Scenario_Name'),
col('Custom_Scenario_Order'),
col('Custom_ScopeItem'),
col('Custom_SolutionType'),
col('Custom_Source'),
col('Custom_StoryType'),
col('Custom_SystemName'),
col('Custom_Target_Dec'),
col('Custom_Target_Jan'),
col('Custom_Target_Nov'),
col('Custom_Target_Oct'),
col('Custom_TargetDateFuncSpec'),
col('Custom_TargetDateGAP'),
col('Custom_TC_Automation_status'),
col('Custom_Test_Case_ExecutionValidation'),
col('Custom_Test_Case_Phase'),
col('Custom_Test_Suite_Phase'),
col('Custom_TestScenario'),
col('Custom_UC_Submitted_for_CPO_Approval'),
col('Custom_UC_Submitted_for_DA_Approval_CCPO'),
col('Custom_UC_Submitted_for_GRC_Validation_PO'),
col('Custom_UC_Submitted_for_PO_approval'),
col('Custom_UC_Submitted_for_SAP_Approval'),
col('Custom_UC_Submitted_for_TI_Approval'),
col('Custom_US_Priority'),
col('Custom_UserStoryAPPID'),
col('Custom_UserStoryType'),
col('Custom_Work_Team'),
col('Custom_Workshop_ID'),
col('Custom_Workshoptobepresented'),
col('Custom_WRICEF'),
col('Custom_YTD_December'),
col('Custom_YTD_Jan'),
col('Custom_YTD_November'),
col('Custom_YTD_Oct'),
col('Custom_Zone_Field_TEC'),
col('DataCarregamento'),
col('DueDate'),
col('Effort'),
col('FoundIn'),
col('InProgressDateSK'),
col('IntegrationBuild'),
col('IterationSK'),
col('Microsoft_VSTS_TCM_TestSuiteType'),
col('Microsoft_VSTS_TCM_TestSuiteTypeId'),
col('ParentWorkItemId'),
col('Priority'),
col('Reason'),
col('RemainingWork'),
col('ResolvedByUserSK'),
col('ResolvedDateSK'),
col('Revision'),
col('Severity'),
col('StartDate'),
col('State'),
col('StateCategory'),
col('StateChangeDateSK'),
col('TagNames'),
col('TargetDate'),
col('TimeCriticality'),
col('Title'),
col('ValueArea'),
col('Watermark'),
col('WorkItemId'),
col('WorkItemRevisionSK'),
col('WorkItemType')
)

# COMMAND ----------

# Criando uma coluna de nome origem contento a string carga_full

df_full = df_full.withColumn('origem', lit('carga_full'))

# COMMAND ----------

# Colocando as colunas em ordem alfabética

df_diario = df_diario.select(
col('ABIProprietaryMethodology_IssueCreatedDate'),
col('ABIProprietaryMethodology_IssueOwnerSK'),
col('ActivatedByUserSK'),
col('ActivatedDateSK'),
col('Activity'),
col('AreaSK'),
col('AssignedToUserSK'),
col('AutomatedTestId'),
col('AutomatedTestName'),
col('AutomatedTestStorage'),
col('AutomatedTestType'),
col('AutomationStatus'),
col('BacklogPriority'),
col('Blocked'),
col('BusinessValue'),
col('ChangedByUserSK'),
col('ChangedDateSK'),
col('ClosedByUserSK'),
col('ClosedDateSK'),
col('CommentCount'),
col('CreatedByUserSK'),
col('CreatedDateSK'),
col('Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866'),
col('Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422'),
col('Custom_AccordingwithSOXcontrols'),
col('Custom_AssociatedProject'),
col('Custom_Aurora_Category'),
col('Custom_Automation'),
col('Custom_BlockedJustification'),
col('Custom_BuC_Approved_Test_Result'),
col('Custom_BuC_Approved_Test_Script'),
col('Custom_BuC_Impacted'),
col('Custom_BuildType'),
col('Custom_BusinessProcessImpact'),
col('Custom_BusinessRolesPersona'),
col('Custom_Centralization'),
col('Custom_Classification'),
col('Custom_Complexity'),
col('Custom_Contingency'),
col('Custom_Control_Name'),
col('Custom_Criticality'),
col('Custom_DateIdentified'),
col('Custom_Deadline'),
col('Custom_Dependency'),
col('Custom_DependencyConsumer'),
col('Custom_DependencyProvider'),
col('Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8'),
col('Custom_E2E_Name'),
col('Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e'),
col('Custom_Environment_Field_TEC'),
col('Custom_EscalationNeeded'),
col('Custom_Executed_Today'),
col('Custom_FeatureCategory'),
col('Custom_FeatureType'),
col('Custom_FioriApp_System'),
col('Custom_FIT_GAP'),
col('Custom_FS_Submitted_for_CPO_Approval'),
col('Custom_FS_Submitted_for_GRC_M_Approval'),
col('Custom_FS_Submitted_for_GRC_SOX_Approval'),
col('Custom_FS_Submitted_for_GRC_Testing_Approval'),
col('Custom_FS_Submitted_for_PO_Approval'),
col('Custom_FS_Submitted_for_QA_Approval'),
col('Custom_FS_Submitted_for_TI_Approval'),
col('Custom_FSID'),
col('Custom_FutureRelease'),
col('Custom_GAP_Reprioritization'),
col('Custom_GAPAPPID'),
col('Custom_GAPCategory'),
col('Custom_GAPEstimation'),
col('Custom_GAPNumber'),
col('Custom_Glassif'),
col('Custom_Global_Local_Escope'),
col('Custom_GRC_BUC_APPROVED'),
col('Custom_GRC_TESTING_APPROVED'),
col('Custom_GRC_UAM_APPROVED'),
col('Custom_GUID'),
col('Custom_IndexE2E'),
col('Custom_IndexProcess'),
col('Custom_IsReportRequired'),
col('Custom_IssuePriority'),
col('Custom_L2'),
col('Custom_L2Product'),
col('Custom_L3'),
col('Custom_L3Product'),
col('Custom_L4Product'),
col('Custom_L4WorkShopDescription'),
col('Custom_LE_December'),
col('Custom_LE_January'),
col('Custom_LE_November'),
col('Custom_LE_October'),
col('Custom_LeadershipApproval'),
col('Custom_Legacies_Integration'),
col('Custom_LegaciesDescription'),
col('Custom_Main_Blocker'),
col('Custom_MDG_Relevant'),
col('Custom_MoscowPriority'),
col('Custom_Name_Of_Environment'),
col('Custom_NonSAP'),
col('Custom_NoSAPApprovalJustification'),
col('Custom_NotAllowedtothisState'),
col('Custom_OD_LE_Dec'),
col('Custom_OD_LE_Nov'),
col('Custom_OD_LE_Oct'),
col('Custom_OD_Submitted_for_Consultant_Approval'),
col('Custom_OD_Submitted_for_CPO_Approval'),
col('Custom_OD_Submitted_for_DA_Approval_CCPO'),
col('Custom_OD_Submitted_for_PO_Approval'),
col('Custom_OD_Submitted_for_TI_Approval'),
col('Custom_OD_Target_Dec'),
col('Custom_OD_Target_Nov'),
col('Custom_OD_Target_Oct'),
col('Custom_OD_YTD_Dec'),
col('Custom_OD_YTD_Nov'),
col('Custom_OD_YTD_Oct'),
col('Custom_OpenDesignID'),
col('Custom_OpenDesignType'),
col('Custom_Out_Scope'),
col('Custom_OutsideAuroraStartDate'),
col('Custom_OverviewNonSAP'),
col('Custom_PartnerResponsible'),
col('Custom_PartnerStatus'),
col('Custom_PlannedDate'),
col('Custom_PotentialImpact'),
col('Custom_PriorityforRelease'),
col('Custom_PriorityRank'),
col('Custom_ProbabilityofOccurrence'),
col('Custom_Product_Field_TEC'),
col('Custom_ReasonforFutureRelease'),
col('Custom_ResolvedTargetDate'),
col('Custom_RiskLevel'),
col('Custom_RiskorIssuesCategory'),
col('Custom_RiskorIssuesEscalationLevel'),
col('Custom_SAPAdherence'),
col('Custom_SAPRole'),
col('Custom_Scenario'),
col('Custom_Scenario_Name'),
col('Custom_Scenario_Order'),
col('Custom_ScopeItem'),
col('Custom_SolutionType'),
col('Custom_Source'),
col('Custom_StoryType'),
col('Custom_SystemName'),
col('Custom_Target_Dec'),
col('Custom_Target_Jan'),
col('Custom_Target_Nov'),
col('Custom_Target_Oct'),
col('Custom_TargetDateFuncSpec'),
col('Custom_TargetDateGAP'),
col('Custom_TC_Automation_status'),
col('Custom_Test_Case_ExecutionValidation'),
col('Custom_Test_Case_Phase'),
col('Custom_Test_Suite_Phase'),
col('Custom_TestScenario'),
col('Custom_UC_Submitted_for_CPO_Approval'),
col('Custom_UC_Submitted_for_DA_Approval_CCPO'),
col('Custom_UC_Submitted_for_GRC_Validation_PO'),
col('Custom_UC_Submitted_for_PO_approval'),
col('Custom_UC_Submitted_for_SAP_Approval'),
col('Custom_UC_Submitted_for_TI_Approval'),
col('Custom_US_Priority'),
col('Custom_UserStoryAPPID'),
col('Custom_UserStoryType'),
col('Custom_Work_Team'),
col('Custom_Workshop_ID'),
col('Custom_Workshoptobepresented'),
col('Custom_WRICEF'),
col('Custom_YTD_December'),
col('Custom_YTD_Jan'),
col('Custom_YTD_November'),
col('Custom_YTD_Oct'),
col('Custom_Zone_Field_TEC'),
col('DataCarregamento'),
col('DueDate'),
col('Effort'),
col('FoundIn'),
col('InProgressDateSK'),
col('IntegrationBuild'),
col('IterationSK'),
col('Microsoft_VSTS_TCM_TestSuiteType'),
col('Microsoft_VSTS_TCM_TestSuiteTypeId'),
col('ParentWorkItemId'),
col('Priority'),
col('Reason'),
col('RemainingWork'),
col('ResolvedByUserSK'),
col('ResolvedDateSK'),
col('Revision'),
col('Severity'),
col('StartDate'),
col('State'),
col('StateCategory'),
col('StateChangeDateSK'),
col('TagNames'),
col('TargetDate'),
col('TimeCriticality'),
col('Title'),
col('ValueArea'),
col('Watermark'),
col('WorkItemId'),
col('WorkItemRevisionSK'),
col('WorkItemType')
)

# COMMAND ----------

# Criando uma coluna de nome origem contento a string carga_diario

df_diario = df_diario.withColumn('origem', lit('carga_diario'))

# COMMAND ----------

# Merge dos dois dataframes

df_merge = df_full.union(df_diario)

# COMMAND ----------

# Criando uma coluna de nome rank, ranqueando os WorkItemId pela maior data em ChangedDateSK, ou seja, o mesmo WorkItemId terá o rank maior para aquele com a data de atualização mais recente

df_merge = df_merge.withColumn(
  'rank', dense_rank().over(Window.partitionBy('WorkItemId').orderBy(desc('ChangedDateSK'), desc('DataCarregamento')))
)

# COMMAND ----------

print(f'{(df_merge.filter(df_merge.rank == 2)).count()} linhas serão excluidas, pois os seus WorkItemId correspondentes possuem atualizações')

# COMMAND ----------

# Criando o Dataframe final, filtrando apenas as linhas que resultaram em rank = 1 e retirando as colunas de controle

df = df_merge.filter(df_merge.rank == 1).drop('rank').drop('origem')
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

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

# Fim carga Stand WorkItems Diario
