# Databricks notebook source
# Importanto bibliotecas

from pyspark.sql.functions import *
from pyspark.sql.types import *
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

# Assunto e path a ser buscado no Datalake

sourceFile = 'WorkItems_P1'
sourcePath = aurora_raw_folder + sourceFile + '/'

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data and '.parquet' not in i.name:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Lê o arquivo avro do path e salva em um spark dataframe

df_P1 = spark.read.format('avro').load(sourcePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Chamada da segunda pasta

# COMMAND ----------

# Assunto e path a ser buscado no Datalake

sourceFile = 'WorkItems_P2'
sourcePath = aurora_raw_folder + sourceFile + '/'

# COMMAND ----------

# Loop em todas as pastas do assunto no Datalake para identificar aquela que contem os registros mais recentes

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Lê o arquivo avro do path e salva em um spark dataframe

df_P2 = spark.read.format('avro').load(sourcePath)

# COMMAND ----------

# Renomeando colunas a serem usadas em join e criando as tabelas temporários dos df's

df_P2 = df_P2.withColumnRenamed("WorkItemId", "xWorkItemId")
df_P2 = df_P2.withColumnRenamed("ChangedDateSK", "xChangedDateSK")
df_P2 = df_P2.withColumnRenamed("DataCarregamento", "xDataCarregamento")
df_P1.createOrReplaceTempView("table_p1")
df_P2.createOrReplaceTempView("table_p2")

# COMMAND ----------

# Merge das duas tabelas utilizando WorkItemId e ChangedDate

df=spark.sql("""
SELECT 
  *
FROM
  table_p1 a
    JOIN table_p2 b
    ON a.WorkItemId = b.xWorkItemId and a.ChangedDateSK == b.xChangedDateSK
""")


# COMMAND ----------

# Substitui campos None para None do Python

df = df.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

# Tipagem das colunas do Dataframe (Schema)

DfFinal = df\
  .withColumn('ABIProprietaryMethodology_IssueCreatedDate', df.ABIProprietaryMethodology_IssueCreatedDate.cast(TimestampType()))\
  .withColumn('ABIProprietaryMethodology_IssueOwnerSK', df.ABIProprietaryMethodology_IssueOwnerSK.cast(StringType()))\
  .withColumn('ActivatedByUserSK', df.ActivatedByUserSK.cast(StringType()))\
  .withColumn('ActivatedDateSK', df.ActivatedDateSK.cast(IntegerType()))\
  .withColumn('Activity', df.Activity.cast(StringType()))\
  .withColumn('AreaSK', df.AreaSK.cast(StringType()))\
  .withColumn('AssignedToUserSK', df.AssignedToUserSK.cast(StringType()))\
  .withColumn('AutomatedTestId', df.AutomatedTestId.cast(StringType()))\
  .withColumn('AutomatedTestName', df.AutomatedTestName.cast(StringType()))\
  .withColumn('AutomatedTestStorage', df.AutomatedTestStorage.cast(StringType()))\
  .withColumn('AutomatedTestType', df.AutomatedTestType.cast(StringType()))\
  .withColumn('AutomationStatus', df.AutomationStatus.cast(StringType()))\
  .withColumn('BacklogPriority', df.BacklogPriority.cast(IntegerType()))\
  .withColumn('Blocked', df.Blocked.cast(StringType()))\
  .withColumn('BusinessValue', df.BusinessValue.cast(IntegerType()))\
  .withColumn('ChangedByUserSK', df.ChangedByUserSK.cast(StringType()))\
  .withColumn('ChangedDateSK', df.ChangedDateSK.cast(IntegerType()))\
  .withColumn('ClosedByUserSK', df.ClosedByUserSK.cast(StringType()))\
  .withColumn('ClosedDateSK', df.ClosedDateSK.cast(IntegerType()))\
  .withColumn('CommentCount', df.CommentCount.cast(IntegerType()))\
  .withColumn('CreatedByUserSK', df.CreatedByUserSK.cast(StringType()))\
  .withColumn('CreatedDateSK', df.CreatedDateSK.cast(IntegerType()))\
  .withColumn('Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866', df.Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866.cast(StringType()))\
  .withColumn('Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422', df.Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422.cast(StringType()))\
  .withColumn('Custom_AccordingwithSOXcontrols', df.Custom_AccordingwithSOXcontrols.cast(StringType()))\
  .withColumn('Custom_AssociatedProject', df.Custom_AssociatedProject.cast(StringType()))\
  .withColumn('Custom_Aurora_Category', df.Custom_Aurora_Category.cast(StringType()))\
  .withColumn('Custom_Automation', df.Custom_Automation.cast(StringType()))\
  .withColumn('Custom_BlockedJustification', df.Custom_BlockedJustification.cast(StringType()))\
  .withColumn('Custom_BuC_Approved_Test_Result', df.Custom_BuC_Approved_Test_Result.cast(StringType()))\
  .withColumn('Custom_BuC_Approved_Test_Script', df.Custom_BuC_Approved_Test_Script.cast(StringType()))\
  .withColumn('Custom_BuC_Impacted', df.Custom_BuC_Impacted.cast(StringType()))\
  .withColumn('Custom_BuildType', df.Custom_BuildType.cast(StringType()))\
  .withColumn('Custom_BusinessProcessImpact', df.Custom_BusinessProcessImpact.cast(StringType()))\
  .withColumn('Custom_BusinessRolesPersona', df.Custom_BusinessRolesPersona.cast(StringType()))\
  .withColumn('Custom_Centralization', df.Custom_Centralization.cast(StringType()))\
  .withColumn('Custom_Complexity', df.Custom_Complexity.cast(StringType()))\
  .withColumn('Custom_Contingency', df.Custom_Contingency.cast(StringType()))\
  .withColumn('Custom_Control_Name', df.Custom_Control_Name.cast(StringType()))\
  .withColumn('Custom_Criticality', df.Custom_Criticality.cast(StringType()))\
  .withColumn('Custom_DateIdentified', df.Custom_DateIdentified.cast(TimestampType()))\
  .withColumn('Custom_Deadline', df.Custom_Deadline.cast(TimestampType()))\
  .withColumn('Custom_Dependency', df.Custom_Dependency.cast(StringType()))\
  .withColumn('Custom_DependencyConsumer', df.Custom_DependencyConsumer.cast(StringType()))\
  .withColumn('Custom_DependencyProvider', df.Custom_DependencyProvider.cast(StringType()))\
  .withColumn('Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8', df.Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8.cast(StringType()))\
  .withColumn('Custom_E2E_Name', df.Custom_E2E_Name.cast(StringType()))\
  .withColumn('Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e', df.Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e.cast(StringType()))\
  .withColumn('Custom_IndexE2E', df.Custom_IndexE2E.cast(StringType()))\
  .withColumn('Custom_TargetDateGAP', df.Custom_TargetDateGAP.cast(IntegerType()))\
  .withColumn('Custom_Classification', df.Custom_Classification.cast(StringType()))\
  .withColumn('Custom_Environment_Field_TEC', df.Custom_Environment_Field_TEC.cast(StringType()))\
  .withColumn('Custom_EscalationNeeded', df.Custom_EscalationNeeded.cast(StringType()))\
  .withColumn('Custom_Executed_Today', df.Custom_Executed_Today.cast(DecimalType()))\
  .withColumn('Custom_FeatureCategory', df.Custom_FeatureCategory.cast(StringType()))\
  .withColumn('Custom_FeatureType', df.Custom_FeatureType.cast(StringType()))\
  .withColumn('Custom_FioriApp_System', df.Custom_FioriApp_System.cast(StringType()))\
  .withColumn('Custom_FIT_GAP', df.Custom_FIT_GAP.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_CPO_Approval', df.Custom_FS_Submitted_for_CPO_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_GRC_M_Approval', df.Custom_FS_Submitted_for_GRC_M_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_GRC_SOX_Approval', df.Custom_FS_Submitted_for_GRC_SOX_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_GRC_Testing_Approval', df.Custom_FS_Submitted_for_GRC_Testing_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_PO_Approval', df.Custom_FS_Submitted_for_PO_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_QA_Approval', df.Custom_FS_Submitted_for_QA_Approval.cast(StringType()))\
  .withColumn('Custom_FS_Submitted_for_TI_Approval', df.Custom_FS_Submitted_for_TI_Approval.cast(StringType()))\
  .withColumn('Custom_FSID', df.Custom_FSID.cast(StringType()))\
  .withColumn('Custom_FutureRelease', df.Custom_FutureRelease.cast(StringType()))\
  .withColumn('Custom_GAP_Reprioritization', df.Custom_GAP_Reprioritization.cast(StringType()))\
  .withColumn('Custom_GAPAPPID', df.Custom_GAPAPPID.cast(StringType()))\
  .withColumn('Custom_GAPCategory', df.Custom_GAPCategory.cast(StringType()))\
  .withColumn('Custom_GAPEstimation', df.Custom_GAPEstimation.cast(StringType()))\
  .withColumn('Custom_GAPNumber', df.Custom_GAPNumber.cast(StringType()))\
  .withColumn('Custom_Glassif', df.Custom_Glassif.cast(StringType()))\
  .withColumn('Custom_Global_Local_Escope', df.Custom_Global_Local_Escope.cast(StringType()))\
  .withColumn('Custom_GRC_BUC_APPROVED', df.Custom_GRC_BUC_APPROVED.cast(StringType()))\
  .withColumn('Custom_GRC_TESTING_APPROVED', df.Custom_GRC_TESTING_APPROVED.cast(StringType()))\
  .withColumn('Custom_GRC_UAM_APPROVED', df.Custom_GRC_UAM_APPROVED.cast(StringType()))\
  .withColumn('Custom_GUID', df.Custom_GUID.cast(StringType()))\
  .withColumn('Custom_IndexProcess', df.Custom_IndexProcess.cast(StringType()))\
  .withColumn('Custom_IsReportRequired', df.Custom_IsReportRequired.cast(StringType()))\
  .withColumn('Custom_IssuePriority', df.Custom_IssuePriority.cast(StringType()))\
  .withColumn('Custom_L2', df.Custom_L2.cast(StringType()))\
  .withColumn('Custom_L2Product', df.Custom_L2Product.cast(StringType()))\
  .withColumn('Custom_L3', df.Custom_L3.cast(StringType()))\
  .withColumn('Custom_L3Product', df.Custom_L3Product.cast(StringType()))\
  .withColumn('Custom_L4Product', df.Custom_L4Product.cast(StringType()))\
  .withColumn('Custom_L4WorkShopDescription', df.Custom_L4WorkShopDescription.cast(StringType()))\
  .withColumn('Custom_LE_December', df.Custom_LE_December.cast(StringType()))\
  .withColumn('Custom_LE_January', df.Custom_LE_January.cast(StringType()))\
  .withColumn('Custom_LE_November', df.Custom_LE_November.cast(StringType()))\
  .withColumn('Custom_LE_October', df.Custom_LE_October.cast(StringType()))\
  .withColumn('Custom_LeadershipApproval', df.Custom_LeadershipApproval.cast(StringType()))\
  .withColumn('Custom_Legacies_Integration', df.Custom_Legacies_Integration.cast(StringType()))\
  .withColumn('Custom_LegaciesDescription', df.Custom_LegaciesDescription.cast(StringType()))\
  .withColumn('Custom_Main_Blocker', df.Custom_Main_Blocker.cast(StringType()))\
  .withColumn('Custom_MDG_Relevant', df.Custom_MDG_Relevant.cast(StringType()))\
  .withColumn('Custom_MoscowPriority', df.Custom_MoscowPriority.cast(StringType()))\
  .withColumn('Custom_Name_Of_Environment', df.Custom_Name_Of_Environment.cast(StringType()))\
  .withColumn('Custom_NonSAP', df.Custom_NonSAP.cast(StringType()))\
  .withColumn('Custom_NoSAPApprovalJustification', df.Custom_NoSAPApprovalJustification.cast(StringType()))\
  .withColumn('Custom_NotAllowedtothisState', df.Custom_NotAllowedtothisState.cast(StringType()))\
  .withColumn('Custom_OD_LE_Dec', df.Custom_OD_LE_Dec.cast(StringType()))\
  .withColumn('Custom_OD_LE_Nov', df.Custom_OD_LE_Nov.cast(StringType()))\
  .withColumn('Custom_OD_LE_Oct', df.Custom_OD_LE_Oct.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_Consultant_Approval', df.Custom_OD_Submitted_for_Consultant_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_CPO_Approval', df.Custom_OD_Submitted_for_CPO_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_DA_Approval_CCPO', df.Custom_OD_Submitted_for_DA_Approval_CCPO.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_PO_Approval', df.Custom_OD_Submitted_for_PO_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Submitted_for_TI_Approval', df.Custom_OD_Submitted_for_TI_Approval.cast(StringType()))\
  .withColumn('Custom_OD_Target_Dec', df.Custom_OD_Target_Dec.cast(StringType()))\
  .withColumn('Custom_OD_Target_Nov', df.Custom_OD_Target_Nov.cast(StringType()))\
  .withColumn('Custom_OD_Target_Oct', df.Custom_OD_Target_Oct.cast(StringType()))\
  .withColumn('Custom_OD_YTD_Dec', df.Custom_OD_YTD_Dec.cast(StringType()))\
  .withColumn('Custom_OD_YTD_Nov', df.Custom_OD_YTD_Nov.cast(StringType()))\
  .withColumn('Custom_OD_YTD_Oct', df.Custom_OD_YTD_Oct.cast(StringType()))\
  .withColumn('Custom_OpenDesignID', df.Custom_OpenDesignID.cast(StringType()))\
  .withColumn('Custom_OpenDesignType', df.Custom_OpenDesignType.cast(StringType()))\
  .withColumn('Custom_Out_Scope', df.Custom_Out_Scope.cast(StringType()))\
  .withColumn('Custom_OutsideAuroraStartDate', df.Custom_OutsideAuroraStartDate.cast(TimestampType()))\
  .withColumn('Custom_OverviewNonSAP', df.Custom_OverviewNonSAP.cast(StringType()))\
  .withColumn('Custom_PartnerResponsible', df.Custom_PartnerResponsible.cast(StringType()))\
  .withColumn('Custom_PartnerStatus', df.Custom_PartnerStatus.cast(StringType()))\
  .withColumn('Custom_PlannedDate', df.Custom_PlannedDate.cast(TimestampType()))\
  .withColumn('Custom_PotentialImpact', df.Custom_PotentialImpact.cast(StringType()))\
  .withColumn('Custom_PriorityforRelease', df.Custom_PriorityforRelease.cast(IntegerType()))\
  .withColumn('Custom_PriorityRank', df.Custom_PriorityRank.cast(IntegerType()))\
  .withColumn('Custom_ProbabilityofOccurrence', df.Custom_ProbabilityofOccurrence.cast(StringType()))\
  .withColumn('Custom_Product_Field_TEC', df.Custom_Product_Field_TEC.cast(StringType()))\
  .withColumn('Custom_ReasonforFutureRelease', df.Custom_ReasonforFutureRelease.cast(StringType()))\
  .withColumn('Custom_ResolvedTargetDate', df.Custom_ResolvedTargetDate.cast(TimestampType()))\
  .withColumn('Custom_RiskLevel', df.Custom_RiskLevel.cast(StringType()))\
  .withColumn('Custom_RiskorIssuesCategory', df.Custom_RiskorIssuesCategory.cast(StringType()))\
  .withColumn('Custom_RiskorIssuesEscalationLevel', df.Custom_RiskorIssuesEscalationLevel.cast(StringType()))\
  .withColumn('Custom_SAPAdherence', df.Custom_SAPAdherence.cast(StringType()))\
  .withColumn('Custom_SAPRole', df.Custom_SAPRole.cast(StringType()))\
  .withColumn('Custom_Scenario', df.Custom_Scenario.cast(StringType()))\
  .withColumn('Custom_Scenario_Name', df.Custom_Scenario_Name.cast(StringType()))\
  .withColumn('Custom_Scenario_Order', df.Custom_Scenario_Order.cast(DecimalType()))\
  .withColumn('Custom_ScopeItem', df.Custom_ScopeItem.cast(StringType()))\
  .withColumn('Custom_SolutionType', df.Custom_SolutionType.cast(StringType()))\
  .withColumn('Custom_Source', df.Custom_Source.cast(StringType()))\
  .withColumn('Custom_StoryType', df.Custom_StoryType.cast(StringType()))\
  .withColumn('Custom_SystemName', df.Custom_SystemName.cast(StringType()))\
  .withColumn('Custom_Target_Dec', df.Custom_Target_Dec.cast(StringType()))\
  .withColumn('Custom_Target_Jan', df.Custom_Target_Jan.cast(StringType()))\
  .withColumn('Custom_Target_Nov', df.Custom_Target_Nov.cast(StringType()))\
  .withColumn('Custom_Target_Oct', df.Custom_Target_Oct.cast(StringType()))\
  .withColumn('Custom_TargetDateFuncSpec', df.Custom_TargetDateFuncSpec.cast(TimestampType()))\
  .withColumn('Custom_TC_Automation_status', df.Custom_TC_Automation_status.cast(StringType()))\
  .withColumn('Custom_Test_Case_ExecutionValidation', df.Custom_Test_Case_ExecutionValidation.cast(StringType()))\
  .withColumn('Custom_Test_Case_Phase', df.Custom_Test_Case_Phase.cast(StringType()))\
  .withColumn('Custom_Test_Suite_Phase', df.Custom_Test_Suite_Phase.cast(StringType()))\
  .withColumn('Custom_TestScenario', df.Custom_TestScenario.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_CPO_Approval', df.Custom_UC_Submitted_for_CPO_Approval.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_DA_Approval_CCPO', df.Custom_UC_Submitted_for_DA_Approval_CCPO.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_GRC_Validation_PO', df.Custom_UC_Submitted_for_GRC_Validation_PO.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_PO_approval', df.Custom_UC_Submitted_for_PO_approval.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_SAP_Approval', df.Custom_UC_Submitted_for_SAP_Approval.cast(StringType()))\
  .withColumn('Custom_UC_Submitted_for_TI_Approval', df.Custom_UC_Submitted_for_TI_Approval.cast(StringType()))\
  .withColumn('Custom_US_Priority', df.Custom_US_Priority.cast(StringType()))\
  .withColumn('Custom_UserStoryAPPID', df.Custom_UserStoryAPPID.cast(StringType()))\
  .withColumn('Custom_UserStoryType', df.Custom_UserStoryType.cast(StringType()))\
  .withColumn('Custom_Work_Team', df.Custom_Work_Team.cast(StringType()))\
  .withColumn('Custom_Workshop_ID', df.Custom_Workshop_ID.cast(StringType()))\
  .withColumn('Custom_Workshoptobepresented', df.Custom_Workshoptobepresented.cast(StringType()))\
  .withColumn('Custom_WRICEF', df.Custom_WRICEF.cast(StringType()))\
  .withColumn('Custom_YTD_December', df.Custom_YTD_December.cast(StringType()))\
  .withColumn('Custom_YTD_Jan', df.Custom_YTD_Jan.cast(StringType()))\
  .withColumn('Custom_YTD_November', df.Custom_YTD_November.cast(StringType()))\
  .withColumn('Custom_YTD_Oct', df.Custom_YTD_Oct.cast(StringType()))\
  .withColumn('Custom_Zone_Field_TEC', df.Custom_Zone_Field_TEC.cast(StringType()))\
  .withColumn('DueDate', df.DueDate.cast(TimestampType()))\
  .withColumn('Effort', df.Effort.cast(DecimalType()))\
  .withColumn('FoundIn', df.FoundIn.cast(StringType()))\
  .withColumn('InProgressDateSK', df.InProgressDateSK.cast(IntegerType()))\
  .withColumn('IntegrationBuild', df.IntegrationBuild.cast(StringType()))\
  .withColumn('IterationSK', df.IterationSK.cast(StringType()))\
  .withColumn('Microsoft_VSTS_TCM_TestSuiteType', df.Microsoft_VSTS_TCM_TestSuiteType.cast(StringType()))\
  .withColumn('Microsoft_VSTS_TCM_TestSuiteTypeId', df.Microsoft_VSTS_TCM_TestSuiteTypeId.cast(IntegerType()))\
  .withColumn('ParentWorkItemId', df.ParentWorkItemId.cast(IntegerType()))\
  .withColumn('Priority', df.Priority.cast(IntegerType()))\
  .withColumn('Reason', df.Reason.cast(StringType()))\
  .withColumn('RemainingWork', df.RemainingWork.cast(IntegerType()))\
  .withColumn('ResolvedByUserSK', df.ResolvedByUserSK.cast(StringType()))\
  .withColumn('ResolvedDateSK', df.ResolvedDateSK.cast(IntegerType()))\
  .withColumn('Revision', df.Revision.cast(IntegerType()))\
  .withColumn('Severity', df.Severity.cast(StringType()))\
  .withColumn('StartDate', df.StartDate.cast(TimestampType()))\
  .withColumn('State', df.State.cast(StringType()))\
  .withColumn('StateCategory', df.StateCategory.cast(StringType()))\
  .withColumn('StateChangeDateSK', df.StateChangeDateSK.cast(IntegerType()))\
  .withColumn('TagNames', df.TagNames.cast(StringType()))\
  .withColumn('TargetDate', df.TargetDate.cast(TimestampType()))\
  .withColumn('TimeCriticality', df.TimeCriticality.cast(IntegerType()))\
  .withColumn('Title', df.Title.cast(StringType()))\
  .withColumn('ValueArea', df.ValueArea.cast(StringType()))\
  .withColumn('Watermark', df.Watermark.cast(IntegerType()))\
  .withColumn('WorkItemId', df.WorkItemId.cast(LongType()))\
  .withColumn('WorkItemRevisionSK', df.WorkItemRevisionSK.cast(LongType()))\
  .withColumn('WorkItemType', df.WorkItemType.cast(StringType()))


# COMMAND ----------

# Dropando colunas a mais

DfFinal = DfFinal.drop('xWorkItemId').drop('xChangedDateSK').drop('xDataCarregamento')

# COMMAND ----------

# Criando path para salvar os dados

nameFile = 'WorkItems'
sinkPath = aurora_standardized_folder + nameFile + '/' + max_data
print(sinkPath)

# COMMAND ----------

print("Número de linhas atual: ", DfFinal.count())

DfFinal = DfFinal.distinct()

print("Número de linhas após distinct: ", DfFinal.count())

# COMMAND ----------

# Salva a tabela de volta em modo parquet no caminho especificado

DfFinal.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
duracao_notebook = str((end_time - start_time)).split('.')[0]
print(f'Tempo de execução do notebook: {duracao_notebook}')

# COMMAND ----------

# Fim carga Stand WorkItems
