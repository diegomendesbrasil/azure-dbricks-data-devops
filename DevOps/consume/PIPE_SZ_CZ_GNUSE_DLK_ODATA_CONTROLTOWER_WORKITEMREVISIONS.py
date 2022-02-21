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

sourceFile = 'WorkItemRevisions'
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

df.count()

# COMMAND ----------

# Salva a tabela em modo parquet no caminho especificado

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

script = """
MERGE
  [dbo].[LOGWorkItemAurora] AS DIM  
USING
  [dbo].[stgLOGWorkItemAurora] AS SOURCE ON (SOURCE.WorkItemRevisionSK = DIM.WorkItemRevisionSK)
WHEN MATCHED THEN
  UPDATE SET
DIM.[WorkItemId] =  SOURCE.[WorkItemId],
DIM.[Revision] =  SOURCE.[Revision],
DIM.[RevisedDate] =  SOURCE.[RevisedDate],
DIM.[RevisedDateSK] =  SOURCE.[RevisedDateSK],
DIM.[DateSK] =  SOURCE.[DateSK],
DIM.[IsCurrent] =  SOURCE.[IsCurrent],
DIM.[IsLastRevisionOfDay] =  SOURCE.[IsLastRevisionOfDay],
DIM.[AnalyticsUpdatedDate] =  SOURCE.[AnalyticsUpdatedDate],
DIM.[ProjectSK] =  SOURCE.[ProjectSK],
DIM.[AreaSK] =  SOURCE.[AreaSK],
DIM.[IterationSK] =  SOURCE.[IterationSK],
DIM.[AssignedToUserSK] =  SOURCE.[AssignedToUserSK],
DIM.[ChangedByUserSK] =  SOURCE.[ChangedByUserSK],
DIM.[CreatedByUserSK] =  SOURCE.[CreatedByUserSK],
DIM.[ActivatedByUserSK] =  SOURCE.[ActivatedByUserSK],
DIM.[ClosedByUserSK] =  SOURCE.[ClosedByUserSK],
DIM.[ResolvedByUserSK] =  SOURCE.[ResolvedByUserSK],
DIM.[ActivatedDateSK] =  SOURCE.[ActivatedDateSK],
DIM.[ChangedDateSK] =  SOURCE.[ChangedDateSK],
DIM.[ClosedDateSK] =  SOURCE.[ClosedDateSK],
DIM.[CreatedDateSK] =  SOURCE.[CreatedDateSK],
DIM.[ResolvedDateSK] =  SOURCE.[ResolvedDateSK],
DIM.[StateChangeDateSK] =  SOURCE.[StateChangeDateSK],
DIM.[InProgressDateSK] =  SOURCE.[InProgressDateSK],
DIM.[CompletedDateSK] =  SOURCE.[CompletedDateSK],
DIM.[Watermark] =  SOURCE.[Watermark],
DIM.[Title] =  SOURCE.[Title],
DIM.[WorkItemType] =  SOURCE.[WorkItemType],
DIM.[ChangedDate] =  SOURCE.[ChangedDate],
DIM.[CreatedDate] =  SOURCE.[CreatedDate],
DIM.[State] =  SOURCE.[State],
DIM.[Reason] =  SOURCE.[Reason],
DIM.[FoundIn] =  SOURCE.[FoundIn],
DIM.[IntegrationBuild] =  SOURCE.[IntegrationBuild],
DIM.[ActivatedDate] =  SOURCE.[ActivatedDate],
DIM.[Activity] =  SOURCE.[Activity],
DIM.[BacklogPriority] =  SOURCE.[BacklogPriority],
DIM.[BusinessValue] =  SOURCE.[BusinessValue],
DIM.[ClosedDate] =  SOURCE.[ClosedDate],
DIM.[Issue] =  SOURCE.[Issue],
DIM.[Priority] =  SOURCE.[Priority],
DIM.[Rating] =  SOURCE.[Rating],
DIM.[ResolvedDate] =  SOURCE.[ResolvedDate],
DIM.[Severity] =  SOURCE.[Severity],
DIM.[TimeCriticality] =  SOURCE.[TimeCriticality],
DIM.[ValueArea] =  SOURCE.[ValueArea],
DIM.[DueDate] =  SOURCE.[DueDate],
DIM.[Effort] =  SOURCE.[Effort],
DIM.[FinishDate] =  SOURCE.[FinishDate],
DIM.[RemainingWork] =  SOURCE.[RemainingWork],
DIM.[StartDate] =  SOURCE.[StartDate],
DIM.[TargetDate] =  SOURCE.[TargetDate],
DIM.[Blocked] =  SOURCE.[Blocked],
DIM.[ParentWorkItemId] =  SOURCE.[ParentWorkItemId],
DIM.[TagNames] =  SOURCE.[TagNames],
DIM.[StateCategory] =  SOURCE.[StateCategory],
DIM.[InProgressDate] =  SOURCE.[InProgressDate],
DIM.[CompletedDate] =  SOURCE.[CompletedDate],
DIM.[LeadTimeDays] =  SOURCE.[LeadTimeDays],
DIM.[CycleTimeDays] =  SOURCE.[CycleTimeDays],
DIM.[AutomatedTestId] =  SOURCE.[AutomatedTestId],
DIM.[AutomatedTestName] =  SOURCE.[AutomatedTestName],
DIM.[AutomatedTestStorage] =  SOURCE.[AutomatedTestStorage],
DIM.[AutomatedTestType] =  SOURCE.[AutomatedTestType],
DIM.[AutomationStatus] =  SOURCE.[AutomationStatus],
DIM.[StateChangeDate] =  SOURCE.[StateChangeDate],
DIM.[Count] =  SOURCE.[Count],
DIM.[CommentCount] =  SOURCE.[CommentCount],
DIM.[ABIProprietaryMethodology_IssueCreatedDate] =  SOURCE.[ABIProprietaryMethodology_IssueCreatedDate],
DIM.[ABIProprietaryMethodology_IssueOwnerSK] =  SOURCE.[ABIProprietaryMethodology_IssueOwnerSK],
DIM.[Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866] =  SOURCE.[Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866],
DIM.[Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17] =  SOURCE.[Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17],
DIM.[Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422] =  SOURCE.[Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422],
DIM.[Custom_AccordingwithSOXcontrols] =  SOURCE.[Custom_AccordingwithSOXcontrols],
DIM.[Custom_ActualWork] =  SOURCE.[Custom_ActualWork],
DIM.[Custom_Add_Remove_Fields] =  SOURCE.[Custom_Add_Remove_Fields],
DIM.[Custom_Affected_APPs_Transactions] =  SOURCE.[Custom_Affected_APPs_Transactions],
DIM.[Custom_Affected_Workstream] =  SOURCE.[Custom_Affected_Workstream],
DIM.[Custom_Analytics_Relevant] =  SOURCE.[Custom_Analytics_Relevant],
DIM.[Custom_Analytics_Transport_Request] =  SOURCE.[Custom_Analytics_Transport_Request],
DIM.[Custom_APIs_Description] =  SOURCE.[Custom_APIs_Description],
DIM.[Custom_Applicable_Contingencies_Workarounds] =  SOURCE.[Custom_Applicable_Contingencies_Workarounds],
DIM.[Custom_APPs_Transactions] =  SOURCE.[Custom_APPs_Transactions],
DIM.[Custom_AssociatedProject] =  SOURCE.[Custom_AssociatedProject],
DIM.[Custom_Aurora_Category] =  SOURCE.[Custom_Aurora_Category],
DIM.[Custom_Automation] =  SOURCE.[Custom_Automation],
DIM.[Custom_AutomationBlankField] =  SOURCE.[Custom_AutomationBlankField],
DIM.[Custom_AutomationNeeded] =  SOURCE.[Custom_AutomationNeeded],
DIM.[Custom_AutomationNeeded2] =  SOURCE.[Custom_AutomationNeeded2],
DIM.[Custom_Block_Workstream_Requester] =  SOURCE.[Custom_Block_Workstream_Requester],
DIM.[Custom_Blocked_TestCase] =  SOURCE.[Custom_Blocked_TestCase],
DIM.[Custom_BlockedJustification] =  SOURCE.[Custom_BlockedJustification],
DIM.[Custom_BuC_Approved_Test_Result] =  SOURCE.[Custom_BuC_Approved_Test_Result],
DIM.[Custom_BuC_Approved_Test_Script] =  SOURCE.[Custom_BuC_Approved_Test_Script],
DIM.[Custom_BuC_Impacted] =  SOURCE.[Custom_BuC_Impacted],
DIM.[Custom_Bug_Phase] =  SOURCE.[Custom_Bug_Phase],
DIM.[Custom_Bug_Root_Cause] =  SOURCE.[Custom_Bug_Root_Cause],
DIM.[Custom_Bug_Workstream_Requester] =  SOURCE.[Custom_Bug_Workstream_Requester],
DIM.[Custom_BuildType] =  SOURCE.[Custom_BuildType],
DIM.[Custom_BusinessProcessImpact] =  SOURCE.[Custom_BusinessProcessImpact],
DIM.[Custom_BusinessRolesPersona] =  SOURCE.[Custom_BusinessRolesPersona],
DIM.[Custom_Centralization] =  SOURCE.[Custom_Centralization],
DIM.[Custom_CID] =  SOURCE.[Custom_CID],
DIM.[Custom_Classification] =  SOURCE.[Custom_Classification],
DIM.[Custom_Closed_Date_Transport_Request] =  SOURCE.[Custom_Closed_Date_Transport_Request],
DIM.[Custom_Complexity] =  SOURCE.[Custom_Complexity],
DIM.[Custom_ConditionName] =  SOURCE.[Custom_ConditionName],
DIM.[Custom_Contingency] =  SOURCE.[Custom_Contingency],
DIM.[Custom_Control_Name] =  SOURCE.[Custom_Control_Name],
DIM.[Custom_Criticality] =  SOURCE.[Custom_Criticality],
DIM.[Custom_Custom2] =  SOURCE.[Custom_Custom2],
DIM.[Custom_DARelated] =  SOURCE.[Custom_DARelated],
DIM.[Custom_DateIdentified] =  SOURCE.[Custom_DateIdentified],
DIM.[Custom_Deadline] =  SOURCE.[Custom_Deadline],
DIM.[Custom_Dependency] =  SOURCE.[Custom_Dependency],
DIM.[Custom_DependencyConsumer] =  SOURCE.[Custom_DependencyConsumer],
DIM.[Custom_DependencyProvider] =  SOURCE.[Custom_DependencyProvider],
DIM.[Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8] =  SOURCE.[Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8],
DIM.[Custom_E2E_Impacted] =  SOURCE.[Custom_E2E_Impacted],
DIM.[Custom_E2E_Name] =  SOURCE.[Custom_E2E_Name],
DIM.[Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e] =  SOURCE.[Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e],
DIM.[Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e] =  SOURCE.[Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e],
DIM.[Custom_Effort_Aurora] =  SOURCE.[Custom_Effort_Aurora],
DIM.[Custom_email] =  SOURCE.[Custom_email],
DIM.[Custom_EnhancementGAPID] =  SOURCE.[Custom_EnhancementGAPID],
DIM.[Custom_Environment_Field_TEC] =  SOURCE.[Custom_Environment_Field_TEC],
DIM.[Custom_ER_Affected_APPs_Transactions] =  SOURCE.[Custom_ER_Affected_APPs_Transactions],
DIM.[Custom_ER_Affected_Workstream] =  SOURCE.[Custom_ER_Affected_Workstream],
DIM.[Custom_ER_Analytics_Transport_Request] =  SOURCE.[Custom_ER_Analytics_Transport_Request],
DIM.[Custom_ER_Applicable_Contingencies_Workarounds] =  SOURCE.[Custom_ER_Applicable_Contingencies_Workarounds],
DIM.[Custom_ER_APPs_Transactions] =  SOURCE.[Custom_ER_APPs_Transactions],
DIM.[Custom_ER_Blocked_TestCase] =  SOURCE.[Custom_ER_Blocked_TestCase],
DIM.[Custom_ER_E2E_Impacted] =  SOURCE.[Custom_ER_E2E_Impacted],
DIM.[Custom_ER_GAP_APP_ID] =  SOURCE.[Custom_ER_GAP_APP_ID],
DIM.[Custom_ER_Impact_Information] =  SOURCE.[Custom_ER_Impact_Information],
DIM.[Custom_ER_Logistics_Transport_Request] =  SOURCE.[Custom_ER_Logistics_Transport_Request],
DIM.[Custom_ER_MDG_Transport_Request] =  SOURCE.[Custom_ER_MDG_Transport_Request],
DIM.[Custom_ER_OTC_Transport_Request] =  SOURCE.[Custom_ER_OTC_Transport_Request],
DIM.[Custom_ER_P2P_Transport_Request] =  SOURCE.[Custom_ER_P2P_Transport_Request],
DIM.[Custom_ER_RTR_Transport_Request] =  SOURCE.[Custom_ER_RTR_Transport_Request],
DIM.[Custom_ER_SAP_Change] =  SOURCE.[Custom_ER_SAP_Change],
DIM.[Custom_ER_Security_Authorizations_Impacts] =  SOURCE.[Custom_ER_Security_Authorizations_Impacts],
DIM.[Custom_ER_SIT_Committee_Approval] =  SOURCE.[Custom_ER_SIT_Committee_Approval],
DIM.[Custom_ER_SIT_Committee_Comments] =  SOURCE.[Custom_ER_SIT_Committee_Comments],
DIM.[Custom_ER_Supply_Transport_Request] =  SOURCE.[Custom_ER_Supply_Transport_Request],
DIM.[Custom_ER_TEC_Transport_Request] =  SOURCE.[Custom_ER_TEC_Transport_Request],
DIM.[Custom_ER_Transport_List] =  SOURCE.[Custom_ER_Transport_List],
DIM.[Custom_ER_Transport_Status] =  SOURCE.[Custom_ER_Transport_Status],
DIM.[Custom_ER_WorkItem_Charm_Number] =  SOURCE.[Custom_ER_WorkItem_Charm_Number],
DIM.[Custom_EscalationDate] =  SOURCE.[Custom_EscalationDate],
DIM.[Custom_EscalationNeeded] =  SOURCE.[Custom_EscalationNeeded],
DIM.[Custom_Executed_Today] =  SOURCE.[Custom_Executed_Today],
DIM.[Custom_FeatureCategory] =  SOURCE.[Custom_FeatureCategory],
DIM.[Custom_FeatureType] =  SOURCE.[Custom_FeatureType],
DIM.[Custom_FioriApp_System] =  SOURCE.[Custom_FioriApp_System],
DIM.[Custom_FirstDueDate] =  SOURCE.[Custom_FirstDueDate],
DIM.[Custom_FIT_GAP] =  SOURCE.[Custom_FIT_GAP],
DIM.[Custom_FIT_ID] =  SOURCE.[Custom_FIT_ID],
DIM.[Custom_Flow_FSEC_1] =  SOURCE.[Custom_Flow_FSEC_1],
DIM.[Custom_Flow_FSEC_2] =  SOURCE.[Custom_Flow_FSEC_2],
DIM.[Custom_Flow_FSEC_3] =  SOURCE.[Custom_Flow_FSEC_3],
DIM.[Custom_FS_Submitted_for_CPO_Approval] =  SOURCE.[Custom_FS_Submitted_for_CPO_Approval],
DIM.[Custom_FS_Submitted_for_GRC_M_Approval] =  SOURCE.[Custom_FS_Submitted_for_GRC_M_Approval],
DIM.[Custom_FS_Submitted_for_GRC_SOX_Approval] =  SOURCE.[Custom_FS_Submitted_for_GRC_SOX_Approval],
DIM.[Custom_FS_Submitted_for_GRC_Testing_Approval] =  SOURCE.[Custom_FS_Submitted_for_GRC_Testing_Approval],
DIM.[Custom_FS_Submitted_for_PO_Approval] =  SOURCE.[Custom_FS_Submitted_for_PO_Approval],
DIM.[Custom_FS_Submitted_for_QA_Approval] =  SOURCE.[Custom_FS_Submitted_for_QA_Approval],
DIM.[Custom_FS_Submitted_for_TI_Approval] =  SOURCE.[Custom_FS_Submitted_for_TI_Approval],
DIM.[Custom_FSID] =  SOURCE.[Custom_FSID],
DIM.[Custom_FutureRelease] =  SOURCE.[Custom_FutureRelease],
DIM.[Custom_GAP_APP_ID_Related] =  SOURCE.[Custom_GAP_APP_ID_Related],
DIM.[Custom_GAP_Reprioritization] =  SOURCE.[Custom_GAP_Reprioritization],
DIM.[Custom_GAPAPPID] =  SOURCE.[Custom_GAPAPPID],
DIM.[Custom_GAPCategory] =  SOURCE.[Custom_GAPCategory],
DIM.[Custom_GAPEnhancement] =  SOURCE.[Custom_GAPEnhancement],
DIM.[Custom_GAPEstimation] =  SOURCE.[Custom_GAPEstimation],
DIM.[Custom_GAPNumber] =  SOURCE.[Custom_GAPNumber],
DIM.[Custom_Glassif] =  SOURCE.[Custom_Glassif],
DIM.[Custom_Global_Local_Escope] =  SOURCE.[Custom_Global_Local_Escope],
DIM.[Custom_GRC_BUC_APPROVED] =  SOURCE.[Custom_GRC_BUC_APPROVED],
DIM.[Custom_GRC_TESTING_APPROVED] =  SOURCE.[Custom_GRC_TESTING_APPROVED],
DIM.[Custom_GRC_UAM_APPROVED] =  SOURCE.[Custom_GRC_UAM_APPROVED],
DIM.[Custom_GUID] =  SOURCE.[Custom_GUID],
DIM.[Custom_Impact] =  SOURCE.[Custom_Impact],
DIM.[Custom_Impact_Information] =  SOURCE.[Custom_Impact_Information],
DIM.[Custom_IndexE2E] =  SOURCE.[Custom_IndexE2E],
DIM.[Custom_IndexProcess] =  SOURCE.[Custom_IndexProcess],
DIM.[Custom_Interim_Solution] =  SOURCE.[Custom_Interim_Solution],
DIM.[Custom_IsGlobalSteerCoRequired] =  SOURCE.[Custom_IsGlobalSteerCoRequired],
DIM.[Custom_IsReportRequired] =  SOURCE.[Custom_IsReportRequired],
DIM.[Custom_IssuePriority] =  SOURCE.[Custom_IssuePriority],
DIM.[Custom_L2] =  SOURCE.[Custom_L2],
DIM.[Custom_L2Product] =  SOURCE.[Custom_L2Product],
DIM.[Custom_L3] =  SOURCE.[Custom_L3],
DIM.[Custom_L3Product] =  SOURCE.[Custom_L3Product],
DIM.[Custom_L4Product] =  SOURCE.[Custom_L4Product],
DIM.[Custom_L4WorkShopDescription] =  SOURCE.[Custom_L4WorkShopDescription],
DIM.[Custom_LE_December] =  SOURCE.[Custom_LE_December],
DIM.[Custom_LE_January] =  SOURCE.[Custom_LE_January],
DIM.[Custom_LE_November] =  SOURCE.[Custom_LE_November],
DIM.[Custom_LE_October] =  SOURCE.[Custom_LE_October],
DIM.[Custom_LeadershipApproval] =  SOURCE.[Custom_LeadershipApproval],
DIM.[Custom_Legacies_Integration] =  SOURCE.[Custom_Legacies_Integration],
DIM.[Custom_LegaciesDescription] =  SOURCE.[Custom_LegaciesDescription],
DIM.[Custom_Logistics_Transport_Request] =  SOURCE.[Custom_Logistics_Transport_Request],
DIM.[Custom_Main_Blocker] =  SOURCE.[Custom_Main_Blocker],
DIM.[Custom_MDG_Relevant] =  SOURCE.[Custom_MDG_Relevant],
DIM.[Custom_MDG_Transport_Request] =  SOURCE.[Custom_MDG_Transport_Request],
DIM.[Custom_Microservices_Change] =  SOURCE.[Custom_Microservices_Change],
DIM.[Custom_Microservices_Description] =  SOURCE.[Custom_Microservices_Description],
DIM.[Custom_MoscowPriority] =  SOURCE.[Custom_MoscowPriority],
DIM.[Custom_Name_Of_Environment] =  SOURCE.[Custom_Name_Of_Environment],
DIM.[Custom_NonSAP] =  SOURCE.[Custom_NonSAP],
DIM.[Custom_NoSAPApprovalJustification] =  SOURCE.[Custom_NoSAPApprovalJustification],
DIM.[Custom_NotAllowedForLocalTeam] =  SOURCE.[Custom_NotAllowedForLocalTeam],
DIM.[Custom_NotAllowedtothisState] =  SOURCE.[Custom_NotAllowedtothisState],
DIM.[Custom_OD_LE_Dec] =  SOURCE.[Custom_OD_LE_Dec],
DIM.[Custom_OD_LE_Nov] =  SOURCE.[Custom_OD_LE_Nov],
DIM.[Custom_OD_LE_Oct] =  SOURCE.[Custom_OD_LE_Oct],
DIM.[Custom_OD_Submitted_for_Consultant_Approval] =  SOURCE.[Custom_OD_Submitted_for_Consultant_Approval],
DIM.[Custom_OD_Submitted_for_CPO_Approval] =  SOURCE.[Custom_OD_Submitted_for_CPO_Approval],
DIM.[Custom_OD_Submitted_for_DA_Approval_CCPO] =  SOURCE.[Custom_OD_Submitted_for_DA_Approval_CCPO],
DIM.[Custom_OD_Submitted_for_PO_Approval] =  SOURCE.[Custom_OD_Submitted_for_PO_Approval],
DIM.[Custom_OD_Submitted_for_TI_Approval] =  SOURCE.[Custom_OD_Submitted_for_TI_Approval],
DIM.[Custom_OD_Target_Dec] =  SOURCE.[Custom_OD_Target_Dec],
DIM.[Custom_OD_Target_Nov] =  SOURCE.[Custom_OD_Target_Nov],
DIM.[Custom_OD_Target_Oct] =  SOURCE.[Custom_OD_Target_Oct],
DIM.[Custom_OD_YTD_Dec] =  SOURCE.[Custom_OD_YTD_Dec],
DIM.[Custom_OD_YTD_Nov] =  SOURCE.[Custom_OD_YTD_Nov],
DIM.[Custom_OD_YTD_Oct] =  SOURCE.[Custom_OD_YTD_Oct],
DIM.[Custom_OpenDesignID] =  SOURCE.[Custom_OpenDesignID],
DIM.[Custom_OpenDesignType] =  SOURCE.[Custom_OpenDesignType],
DIM.[Custom_OTC_Transport_Request] =  SOURCE.[Custom_OTC_Transport_Request],
DIM.[Custom_Out_Scope] =  SOURCE.[Custom_Out_Scope],
DIM.[Custom_OutsideAuroraRecoveryDate] =  SOURCE.[Custom_OutsideAuroraRecoveryDate],
DIM.[Custom_OutsideAuroraStartDate] =  SOURCE.[Custom_OutsideAuroraStartDate],
DIM.[Custom_OverviewNonSAP] =  SOURCE.[Custom_OverviewNonSAP],
DIM.[Custom_P2P_Transport_Request] =  SOURCE.[Custom_P2P_Transport_Request],
DIM.[Custom_PartnerAnalysisTargetDate] =  SOURCE.[Custom_PartnerAnalysisTargetDate],
DIM.[Custom_PartnerReleaseTargetDate] =  SOURCE.[Custom_PartnerReleaseTargetDate],
DIM.[Custom_PartnerResponsible] =  SOURCE.[Custom_PartnerResponsible],
DIM.[Custom_PartnerStatus] =  SOURCE.[Custom_PartnerStatus],
DIM.[Custom_PlannedDate] =  SOURCE.[Custom_PlannedDate],
DIM.[Custom_PlannedStartDatetoBuild] =  SOURCE.[Custom_PlannedStartDatetoBuild],
DIM.[Custom_PlannedWeeklySprint] =  SOURCE.[Custom_PlannedWeeklySprint],
DIM.[Custom_PotentialImpact] =  SOURCE.[Custom_PotentialImpact],
DIM.[Custom_PriorityforRelease] =  SOURCE.[Custom_PriorityforRelease],
DIM.[Custom_PriorityRank] =  SOURCE.[Custom_PriorityRank],
DIM.[Custom_ProbabilityofOccurrence] =  SOURCE.[Custom_ProbabilityofOccurrence],
DIM.[Custom_Product_Field_TEC] =  SOURCE.[Custom_Product_Field_TEC],
DIM.[Custom_ProfileAccessInformation] =  SOURCE.[Custom_ProfileAccessInformation],
DIM.[Custom_RAIDPriority] =  SOURCE.[Custom_RAIDPriority],
DIM.[Custom_ReasonforFutureRelease] =  SOURCE.[Custom_ReasonforFutureRelease],
DIM.[Custom_Related_Interface] =  SOURCE.[Custom_Related_Interface],
DIM.[Custom_Related_Interface_Description] =  SOURCE.[Custom_Related_Interface_Description],
DIM.[Custom_Related_Interface_Systems] =  SOURCE.[Custom_Related_Interface_Systems],
DIM.[Custom_Related_LIB] =  SOURCE.[Custom_Related_LIB],
DIM.[Custom_RelatedWorkStream] =  SOURCE.[Custom_RelatedWorkStream],
DIM.[Custom_RelativeMassValuation] =  SOURCE.[Custom_RelativeMassValuation],
DIM.[Custom_ResolvedTargetDate] =  SOURCE.[Custom_ResolvedTargetDate],
DIM.[Custom_RiskLevel] =  SOURCE.[Custom_RiskLevel],
DIM.[Custom_RiskorIssuesCategory] =  SOURCE.[Custom_RiskorIssuesCategory],
DIM.[Custom_RiskorIssuesEscalationLevel] =  SOURCE.[Custom_RiskorIssuesEscalationLevel],
DIM.[Custom_RTR_Transport_Request] =  SOURCE.[Custom_RTR_Transport_Request],
DIM.[Custom_SAP_Change] =  SOURCE.[Custom_SAP_Change],
DIM.[Custom_SAPAdherence] =  SOURCE.[Custom_SAPAdherence],
DIM.[Custom_SAPRole] =  SOURCE.[Custom_SAPRole],
DIM.[Custom_Scenario] =  SOURCE.[Custom_Scenario],
DIM.[Custom_Scenario_Name] =  SOURCE.[Custom_Scenario_Name],
DIM.[Custom_Scenario_Order] =  SOURCE.[Custom_Scenario_Order],
DIM.[Custom_ScopeItem] =  SOURCE.[Custom_ScopeItem],
DIM.[Custom_Security_Authorizations_Impacts] =  SOURCE.[Custom_Security_Authorizations_Impacts],
DIM.[Custom_Servicenow_Close_Notes] =  SOURCE.[Custom_Servicenow_Close_Notes],
DIM.[Custom_Servicenow_ticket_ID] =  SOURCE.[Custom_Servicenow_ticket_ID],
DIM.[Custom_SIT_Committee_Approval] =  SOURCE.[Custom_SIT_Committee_Approval],
DIM.[Custom_SIT_Committee_Comments] =  SOURCE.[Custom_SIT_Committee_Comments],
DIM.[Custom_SolutionName] =  SOURCE.[Custom_SolutionName],
DIM.[Custom_SolutionType] =  SOURCE.[Custom_SolutionType],
DIM.[Custom_Source] =  SOURCE.[Custom_Source],
DIM.[Custom_StoryType] =  SOURCE.[Custom_StoryType],
DIM.[Custom_SubmittedforIntegrationTeam] =  SOURCE.[Custom_SubmittedforIntegrationTeam],
DIM.[Custom_SuccessfullyTestedinQAEnvironment] =  SOURCE.[Custom_SuccessfullyTestedinQAEnvironment],
DIM.[Custom_Supply_Transport_Request] =  SOURCE.[Custom_Supply_Transport_Request],
DIM.[Custom_System] =  SOURCE.[Custom_System],
DIM.[Custom_SystemName] =  SOURCE.[Custom_SystemName],
DIM.[Custom_Target_Dec] =  SOURCE.[Custom_Target_Dec],
DIM.[Custom_Target_Jan] =  SOURCE.[Custom_Target_Jan],
DIM.[Custom_Target_Nov] =  SOURCE.[Custom_Target_Nov],
DIM.[Custom_Target_Oct] =  SOURCE.[Custom_Target_Oct],
DIM.[Custom_TargetDateFuncSpec] =  SOURCE.[Custom_TargetDateFuncSpec],
DIM.[Custom_TargetDateGAP] =  SOURCE.[Custom_TargetDateGAP],
DIM.[Custom_TC_Automation_status] =  SOURCE.[Custom_TC_Automation_status],
DIM.[Custom_TEC_Transport_Request] =  SOURCE.[Custom_TEC_Transport_Request],
DIM.[Custom_Test_Case_ExecutionValidation] =  SOURCE.[Custom_Test_Case_ExecutionValidation],
DIM.[Custom_Test_Case_Phase] =  SOURCE.[Custom_Test_Case_Phase],
DIM.[Custom_Test_FSEC_1] =  SOURCE.[Custom_Test_FSEC_1],
DIM.[Custom_Test_FSEC_2] =  SOURCE.[Custom_Test_FSEC_2],
DIM.[Custom_Test_FSEC_3] =  SOURCE.[Custom_Test_FSEC_3],
DIM.[Custom_Test_FSEC_4] =  SOURCE.[Custom_Test_FSEC_4],
DIM.[Custom_Test_FSEC_6] =  SOURCE.[Custom_Test_FSEC_6],
DIM.[Custom_Test_Scenario_Description] =  SOURCE.[Custom_Test_Scenario_Description],
DIM.[Custom_Test_Suite_Phase] =  SOURCE.[Custom_Test_Suite_Phase],
DIM.[Custom_TestScenario] =  SOURCE.[Custom_TestScenario],
DIM.[Custom_Transport_List] =  SOURCE.[Custom_Transport_List],
DIM.[Custom_Transport_Request_Type] =  SOURCE.[Custom_Transport_Request_Type],
DIM.[Custom_Transport_Status] =  SOURCE.[Custom_Transport_Status],
DIM.[Custom_TransportNeeded] =  SOURCE.[Custom_TransportNeeded],
DIM.[Custom_UC_Submitted_for_CPO_Approval] =  SOURCE.[Custom_UC_Submitted_for_CPO_Approval],
DIM.[Custom_UC_Submitted_for_DA_Approval_CCPO] =  SOURCE.[Custom_UC_Submitted_for_DA_Approval_CCPO],
DIM.[Custom_UC_Submitted_for_GRC_Validation_PO] =  SOURCE.[Custom_UC_Submitted_for_GRC_Validation_PO],
DIM.[Custom_UC_Submitted_for_PO_approval] =  SOURCE.[Custom_UC_Submitted_for_PO_approval],
DIM.[Custom_UC_Submitted_for_SAP_Approval] =  SOURCE.[Custom_UC_Submitted_for_SAP_Approval],
DIM.[Custom_UC_Submitted_for_TI_Approval] =  SOURCE.[Custom_UC_Submitted_for_TI_Approval],
DIM.[Custom_US_Priority] =  SOURCE.[Custom_US_Priority],
DIM.[Custom_User_Exist_BADIs] =  SOURCE.[Custom_User_Exist_BADIs],
DIM.[Custom_User_Exist_BADIs_Description] =  SOURCE.[Custom_User_Exist_BADIs_Description],
DIM.[Custom_UserStoryAPPID] =  SOURCE.[Custom_UserStoryAPPID],
DIM.[Custom_UserStoryType] =  SOURCE.[Custom_UserStoryType],
DIM.[Custom_WaitingBuCApproval] =  SOURCE.[Custom_WaitingBuCApproval],
DIM.[Custom_Work_Team] =  SOURCE.[Custom_Work_Team],
DIM.[Custom_Workaround] =  SOURCE.[Custom_Workaround],
DIM.[Custom_WorkItem_Charm_Number] =  SOURCE.[Custom_WorkItem_Charm_Number],
DIM.[Custom_workitem_ID] =  SOURCE.[Custom_workitem_ID],
DIM.[Custom_workpackage_ID] =  SOURCE.[Custom_workpackage_ID],
DIM.[Custom_Workshop_ID] =  SOURCE.[Custom_Workshop_ID],
DIM.[Custom_Workshoptobepresented] =  SOURCE.[Custom_Workshoptobepresented],
DIM.[Custom_WRICEF] =  SOURCE.[Custom_WRICEF],
DIM.[Custom_YTD_December] =  SOURCE.[Custom_YTD_December],
DIM.[Custom_YTD_Jan] =  SOURCE.[Custom_YTD_Jan],
DIM.[Custom_YTD_November] =  SOURCE.[Custom_YTD_November],
DIM.[Custom_YTD_Oct] =  SOURCE.[Custom_YTD_Oct],
DIM.[Custom_Zone_Field_TEC] =  SOURCE.[Custom_Zone_Field_TEC],
DIM.[Microsoft_VSTS_CodeReview_AcceptedBySK] =  SOURCE.[Microsoft_VSTS_CodeReview_AcceptedBySK],
DIM.[Microsoft_VSTS_CodeReview_AcceptedDate] =  SOURCE.[Microsoft_VSTS_CodeReview_AcceptedDate],
DIM.[Microsoft_VSTS_CodeReview_ClosedStatus] =  SOURCE.[Microsoft_VSTS_CodeReview_ClosedStatus],
DIM.[Microsoft_VSTS_CodeReview_ClosedStatusCode] =  SOURCE.[Microsoft_VSTS_CodeReview_ClosedStatusCode],
DIM.[Microsoft_VSTS_CodeReview_ClosingComment] =  SOURCE.[Microsoft_VSTS_CodeReview_ClosingComment],
DIM.[Microsoft_VSTS_CodeReview_Context] =  SOURCE.[Microsoft_VSTS_CodeReview_Context],
DIM.[Microsoft_VSTS_CodeReview_ContextCode] =  SOURCE.[Microsoft_VSTS_CodeReview_ContextCode],
DIM.[Microsoft_VSTS_CodeReview_ContextOwner] =  SOURCE.[Microsoft_VSTS_CodeReview_ContextOwner],
DIM.[Microsoft_VSTS_CodeReview_ContextType] =  SOURCE.[Microsoft_VSTS_CodeReview_ContextType],
DIM.[Microsoft_VSTS_Common_ReviewedBySK] =  SOURCE.[Microsoft_VSTS_Common_ReviewedBySK],
DIM.[Microsoft_VSTS_Common_StateCode] =  SOURCE.[Microsoft_VSTS_Common_StateCode],
DIM.[Microsoft_VSTS_Feedback_ApplicationType] =  SOURCE.[Microsoft_VSTS_Feedback_ApplicationType],
DIM.[Microsoft_VSTS_TCM_TestSuiteType] =  SOURCE.[Microsoft_VSTS_TCM_TestSuiteType],
DIM.[Microsoft_VSTS_TCM_TestSuiteTypeId] =  SOURCE.[Microsoft_VSTS_TCM_TestSuiteTypeId],
DIM.[DataCarregamento] =  SOURCE.[DataCarregamento]
WHEN NOT MATCHED THEN
INSERT (WorkItemRevisionSK, WorkItemId,Revision,RevisedDate,RevisedDateSK,DateSK,IsCurrent,IsLastRevisionOfDay,AnalyticsUpdatedDate,ProjectSK,AreaSK,IterationSK,AssignedToUserSK,ChangedByUserSK,CreatedByUserSK,ActivatedByUserSK,ClosedByUserSK,ResolvedByUserSK,ActivatedDateSK,ChangedDateSK,ClosedDateSK,CreatedDateSK,ResolvedDateSK,StateChangeDateSK,InProgressDateSK,CompletedDateSK,Watermark,Title,WorkItemType,ChangedDate,CreatedDate,State,Reason,FoundIn,IntegrationBuild,ActivatedDate,Activity,BacklogPriority,BusinessValue,ClosedDate,Issue,Priority,Rating,ResolvedDate,Severity,TimeCriticality,ValueArea,DueDate,Effort,FinishDate,RemainingWork,StartDate,TargetDate,Blocked,ParentWorkItemId,TagNames,StateCategory,InProgressDate,CompletedDate,LeadTimeDays,CycleTimeDays,AutomatedTestId,AutomatedTestName,AutomatedTestStorage,AutomatedTestType,AutomationStatus,StateChangeDate,Count,CommentCount,ABIProprietaryMethodology_IssueCreatedDate,ABIProprietaryMethodology_IssueOwnerSK,Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866,Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17,Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422,Custom_AccordingwithSOXcontrols,Custom_ActualWork,Custom_Add_Remove_Fields,Custom_Affected_APPs_Transactions,Custom_Affected_Workstream,Custom_Analytics_Relevant,Custom_Analytics_Transport_Request,Custom_APIs_Description,Custom_Applicable_Contingencies_Workarounds,Custom_APPs_Transactions,Custom_AssociatedProject,Custom_Aurora_Category,Custom_Automation,Custom_AutomationBlankField,Custom_AutomationNeeded,Custom_AutomationNeeded2,Custom_Block_Workstream_Requester,Custom_Blocked_TestCase,Custom_BlockedJustification,Custom_BuC_Approved_Test_Result,Custom_BuC_Approved_Test_Script,Custom_BuC_Impacted,Custom_Bug_Phase,Custom_Bug_Root_Cause,Custom_Bug_Workstream_Requester,Custom_BuildType,Custom_BusinessProcessImpact,Custom_BusinessRolesPersona,Custom_Centralization,Custom_CID,Custom_Classification,Custom_Closed_Date_Transport_Request,Custom_Complexity,Custom_ConditionName,Custom_Contingency,Custom_Control_Name,Custom_Criticality,Custom_Custom2,Custom_DARelated,Custom_DateIdentified,Custom_Deadline,Custom_Dependency,Custom_DependencyConsumer,Custom_DependencyProvider,Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8,Custom_E2E_Impacted,Custom_E2E_Name,Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e,Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e,Custom_Effort_Aurora,Custom_email,Custom_EnhancementGAPID,Custom_Environment_Field_TEC,Custom_ER_Affected_APPs_Transactions,Custom_ER_Affected_Workstream,Custom_ER_Analytics_Transport_Request,Custom_ER_Applicable_Contingencies_Workarounds,Custom_ER_APPs_Transactions,Custom_ER_Blocked_TestCase,Custom_ER_E2E_Impacted,Custom_ER_GAP_APP_ID,Custom_ER_Impact_Information,Custom_ER_Logistics_Transport_Request,Custom_ER_MDG_Transport_Request,Custom_ER_OTC_Transport_Request,Custom_ER_P2P_Transport_Request,Custom_ER_RTR_Transport_Request,Custom_ER_SAP_Change,Custom_ER_Security_Authorizations_Impacts,Custom_ER_SIT_Committee_Approval,Custom_ER_SIT_Committee_Comments,Custom_ER_Supply_Transport_Request,Custom_ER_TEC_Transport_Request,Custom_ER_Transport_List,Custom_ER_Transport_Status,Custom_ER_WorkItem_Charm_Number,Custom_EscalationDate,Custom_EscalationNeeded,Custom_Executed_Today,Custom_FeatureCategory,Custom_FeatureType,Custom_FioriApp_System,Custom_FirstDueDate,Custom_FIT_GAP,Custom_FIT_ID,Custom_Flow_FSEC_1,Custom_Flow_FSEC_2,Custom_Flow_FSEC_3,Custom_FS_Submitted_for_CPO_Approval,Custom_FS_Submitted_for_GRC_M_Approval,Custom_FS_Submitted_for_GRC_SOX_Approval,Custom_FS_Submitted_for_GRC_Testing_Approval,Custom_FS_Submitted_for_PO_Approval,Custom_FS_Submitted_for_QA_Approval,Custom_FS_Submitted_for_TI_Approval,Custom_FSID,Custom_FutureRelease,Custom_GAP_APP_ID_Related,Custom_GAP_Reprioritization,Custom_GAPAPPID,Custom_GAPCategory,Custom_GAPEnhancement,Custom_GAPEstimation,Custom_GAPNumber,Custom_Glassif,Custom_Global_Local_Escope,Custom_GRC_BUC_APPROVED,Custom_GRC_TESTING_APPROVED,Custom_GRC_UAM_APPROVED,Custom_GUID,Custom_Impact,Custom_Impact_Information,Custom_IndexE2E,Custom_IndexProcess,Custom_Interim_Solution,Custom_IsGlobalSteerCoRequired,Custom_IsReportRequired,Custom_IssuePriority,Custom_L2,Custom_L2Product,Custom_L3,Custom_L3Product,Custom_L4Product,Custom_L4WorkShopDescription,Custom_LE_December,Custom_LE_January,Custom_LE_November,Custom_LE_October,Custom_LeadershipApproval,Custom_Legacies_Integration,Custom_LegaciesDescription,Custom_Logistics_Transport_Request,Custom_Main_Blocker,Custom_MDG_Relevant,Custom_MDG_Transport_Request,Custom_Microservices_Change,Custom_Microservices_Description,Custom_MoscowPriority,Custom_Name_Of_Environment,Custom_NonSAP,Custom_NoSAPApprovalJustification,Custom_NotAllowedForLocalTeam,Custom_NotAllowedtothisState,Custom_OD_LE_Dec,Custom_OD_LE_Nov,Custom_OD_LE_Oct,Custom_OD_Submitted_for_Consultant_Approval,Custom_OD_Submitted_for_CPO_Approval,Custom_OD_Submitted_for_DA_Approval_CCPO,Custom_OD_Submitted_for_PO_Approval,Custom_OD_Submitted_for_TI_Approval,Custom_OD_Target_Dec,Custom_OD_Target_Nov,Custom_OD_Target_Oct,Custom_OD_YTD_Dec,Custom_OD_YTD_Nov,Custom_OD_YTD_Oct,Custom_OpenDesignID,Custom_OpenDesignType,Custom_OTC_Transport_Request,Custom_Out_Scope,Custom_OutsideAuroraRecoveryDate,Custom_OutsideAuroraStartDate,Custom_OverviewNonSAP,Custom_P2P_Transport_Request,Custom_PartnerAnalysisTargetDate,Custom_PartnerReleaseTargetDate,Custom_PartnerResponsible,Custom_PartnerStatus,Custom_PlannedDate,Custom_PlannedStartDatetoBuild,Custom_PlannedWeeklySprint,Custom_PotentialImpact,Custom_PriorityforRelease,Custom_PriorityRank,Custom_ProbabilityofOccurrence,Custom_Product_Field_TEC,Custom_ProfileAccessInformation,Custom_RAIDPriority,Custom_ReasonforFutureRelease,Custom_Related_Interface,Custom_Related_Interface_Description,Custom_Related_Interface_Systems,Custom_Related_LIB,Custom_RelatedWorkStream,Custom_RelativeMassValuation,Custom_ResolvedTargetDate,Custom_RiskLevel,Custom_RiskorIssuesCategory,Custom_RiskorIssuesEscalationLevel,Custom_RTR_Transport_Request,Custom_SAP_Change,Custom_SAPAdherence,Custom_SAPRole,Custom_Scenario,Custom_Scenario_Name,Custom_Scenario_Order,Custom_ScopeItem,Custom_Security_Authorizations_Impacts,Custom_Servicenow_Close_Notes,Custom_Servicenow_ticket_ID,Custom_SIT_Committee_Approval,Custom_SIT_Committee_Comments,Custom_SolutionName,Custom_SolutionType,Custom_Source,Custom_StoryType,Custom_SubmittedforIntegrationTeam,Custom_SuccessfullyTestedinQAEnvironment,Custom_Supply_Transport_Request,Custom_System,Custom_SystemName,Custom_Target_Dec,Custom_Target_Jan,Custom_Target_Nov,Custom_Target_Oct,Custom_TargetDateFuncSpec,Custom_TargetDateGAP,Custom_TC_Automation_status,Custom_TEC_Transport_Request,Custom_Test_Case_ExecutionValidation,Custom_Test_Case_Phase,Custom_Test_FSEC_1,Custom_Test_FSEC_2,Custom_Test_FSEC_3,Custom_Test_FSEC_4,Custom_Test_FSEC_6,Custom_Test_Scenario_Description,Custom_Test_Suite_Phase,Custom_TestScenario,Custom_Transport_List,Custom_Transport_Request_Type,Custom_Transport_Status,Custom_TransportNeeded,Custom_UC_Submitted_for_CPO_Approval,Custom_UC_Submitted_for_DA_Approval_CCPO,Custom_UC_Submitted_for_GRC_Validation_PO,Custom_UC_Submitted_for_PO_approval,Custom_UC_Submitted_for_SAP_Approval,Custom_UC_Submitted_for_TI_Approval,Custom_US_Priority,Custom_User_Exist_BADIs,Custom_User_Exist_BADIs_Description,Custom_UserStoryAPPID,Custom_UserStoryType,Custom_WaitingBuCApproval,Custom_Work_Team,Custom_Workaround,Custom_WorkItem_Charm_Number,Custom_workitem_ID,Custom_workpackage_ID,Custom_Workshop_ID,Custom_Workshoptobepresented,Custom_WRICEF,Custom_YTD_December,Custom_YTD_Jan,Custom_YTD_November,Custom_YTD_Oct,Custom_Zone_Field_TEC,Microsoft_VSTS_CodeReview_AcceptedBySK,Microsoft_VSTS_CodeReview_AcceptedDate,Microsoft_VSTS_CodeReview_ClosedStatus,Microsoft_VSTS_CodeReview_ClosedStatusCode,Microsoft_VSTS_CodeReview_ClosingComment,Microsoft_VSTS_CodeReview_Context,Microsoft_VSTS_CodeReview_ContextCode,Microsoft_VSTS_CodeReview_ContextOwner,Microsoft_VSTS_CodeReview_ContextType,Microsoft_VSTS_Common_ReviewedBySK,Microsoft_VSTS_Common_StateCode,Microsoft_VSTS_Feedback_ApplicationType,Microsoft_VSTS_TCM_TestSuiteType,Microsoft_VSTS_TCM_TestSuiteTypeId, DataCarregamento) 
VALUES (SOURCE.[WorkItemRevisionSK], SOURCE.[WorkItemId],SOURCE.[Revision],SOURCE.[RevisedDate],SOURCE.[RevisedDateSK],SOURCE.[DateSK],SOURCE.[IsCurrent],SOURCE.[IsLastRevisionOfDay],SOURCE.[AnalyticsUpdatedDate],SOURCE.[ProjectSK],SOURCE.[AreaSK],SOURCE.[IterationSK],SOURCE.[AssignedToUserSK],SOURCE.[ChangedByUserSK],SOURCE.[CreatedByUserSK],SOURCE.[ActivatedByUserSK],SOURCE.[ClosedByUserSK],SOURCE.[ResolvedByUserSK],SOURCE.[ActivatedDateSK],SOURCE.[ChangedDateSK],SOURCE.[ClosedDateSK],SOURCE.[CreatedDateSK],SOURCE.[ResolvedDateSK],SOURCE.[StateChangeDateSK],SOURCE.[InProgressDateSK],SOURCE.[CompletedDateSK],SOURCE.[Watermark],SOURCE.[Title],SOURCE.[WorkItemType],SOURCE.[ChangedDate],SOURCE.[CreatedDate],SOURCE.[State],SOURCE.[Reason],SOURCE.[FoundIn],SOURCE.[IntegrationBuild],SOURCE.[ActivatedDate],SOURCE.[Activity],SOURCE.[BacklogPriority],SOURCE.[BusinessValue],SOURCE.[ClosedDate],SOURCE.[Issue],SOURCE.[Priority],SOURCE.[Rating],SOURCE.[ResolvedDate],SOURCE.[Severity],SOURCE.[TimeCriticality],SOURCE.[ValueArea],SOURCE.[DueDate],SOURCE.[Effort],SOURCE.[FinishDate],SOURCE.[RemainingWork],SOURCE.[StartDate],SOURCE.[TargetDate],SOURCE.[Blocked],SOURCE.[ParentWorkItemId],SOURCE.[TagNames],SOURCE.[StateCategory],SOURCE.[InProgressDate],SOURCE.[CompletedDate],SOURCE.[LeadTimeDays],SOURCE.[CycleTimeDays],SOURCE.[AutomatedTestId],SOURCE.[AutomatedTestName],SOURCE.[AutomatedTestStorage],SOURCE.[AutomatedTestType],SOURCE.[AutomationStatus],SOURCE.[StateChangeDate],SOURCE.[Count],SOURCE.[CommentCount],SOURCE.[ABIProprietaryMethodology_IssueCreatedDate],SOURCE.[ABIProprietaryMethodology_IssueOwnerSK],SOURCE.[Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866],SOURCE.[Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17],SOURCE.[Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422],SOURCE.[Custom_AccordingwithSOXcontrols],SOURCE.[Custom_ActualWork],SOURCE.[Custom_Add_Remove_Fields],SOURCE.[Custom_Affected_APPs_Transactions],SOURCE.[Custom_Affected_Workstream],SOURCE.[Custom_Analytics_Relevant],SOURCE.[Custom_Analytics_Transport_Request],SOURCE.[Custom_APIs_Description],SOURCE.[Custom_Applicable_Contingencies_Workarounds],SOURCE.[Custom_APPs_Transactions],SOURCE.[Custom_AssociatedProject],SOURCE.[Custom_Aurora_Category],SOURCE.[Custom_Automation],SOURCE.[Custom_AutomationBlankField],SOURCE.[Custom_AutomationNeeded],SOURCE.[Custom_AutomationNeeded2],SOURCE.[Custom_Block_Workstream_Requester],SOURCE.[Custom_Blocked_TestCase],SOURCE.[Custom_BlockedJustification],SOURCE.[Custom_BuC_Approved_Test_Result],SOURCE.[Custom_BuC_Approved_Test_Script],SOURCE.[Custom_BuC_Impacted],SOURCE.[Custom_Bug_Phase],SOURCE.[Custom_Bug_Root_Cause],SOURCE.[Custom_Bug_Workstream_Requester],SOURCE.[Custom_BuildType],SOURCE.[Custom_BusinessProcessImpact],SOURCE.[Custom_BusinessRolesPersona],SOURCE.[Custom_Centralization],SOURCE.[Custom_CID],SOURCE.[Custom_Classification],SOURCE.[Custom_Closed_Date_Transport_Request],SOURCE.[Custom_Complexity],SOURCE.[Custom_ConditionName],SOURCE.[Custom_Contingency],SOURCE.[Custom_Control_Name],SOURCE.[Custom_Criticality],SOURCE.[Custom_Custom2],SOURCE.[Custom_DARelated],SOURCE.[Custom_DateIdentified],SOURCE.[Custom_Deadline],SOURCE.[Custom_Dependency],SOURCE.[Custom_DependencyConsumer],SOURCE.[Custom_DependencyProvider],SOURCE.[Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8],SOURCE.[Custom_E2E_Impacted],SOURCE.[Custom_E2E_Name],SOURCE.[Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e],SOURCE.[Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e],SOURCE.[Custom_Effort_Aurora],SOURCE.[Custom_email],SOURCE.[Custom_EnhancementGAPID],SOURCE.[Custom_Environment_Field_TEC],SOURCE.[Custom_ER_Affected_APPs_Transactions],SOURCE.[Custom_ER_Affected_Workstream],SOURCE.[Custom_ER_Analytics_Transport_Request],SOURCE.[Custom_ER_Applicable_Contingencies_Workarounds],SOURCE.[Custom_ER_APPs_Transactions],SOURCE.[Custom_ER_Blocked_TestCase],SOURCE.[Custom_ER_E2E_Impacted],SOURCE.[Custom_ER_GAP_APP_ID],SOURCE.[Custom_ER_Impact_Information],SOURCE.[Custom_ER_Logistics_Transport_Request],SOURCE.[Custom_ER_MDG_Transport_Request],SOURCE.[Custom_ER_OTC_Transport_Request],SOURCE.[Custom_ER_P2P_Transport_Request],SOURCE.[Custom_ER_RTR_Transport_Request],SOURCE.[Custom_ER_SAP_Change],SOURCE.[Custom_ER_Security_Authorizations_Impacts],SOURCE.[Custom_ER_SIT_Committee_Approval],SOURCE.[Custom_ER_SIT_Committee_Comments],SOURCE.[Custom_ER_Supply_Transport_Request],SOURCE.[Custom_ER_TEC_Transport_Request],SOURCE.[Custom_ER_Transport_List],SOURCE.[Custom_ER_Transport_Status],SOURCE.[Custom_ER_WorkItem_Charm_Number],SOURCE.[Custom_EscalationDate],SOURCE.[Custom_EscalationNeeded],SOURCE.[Custom_Executed_Today],SOURCE.[Custom_FeatureCategory],SOURCE.[Custom_FeatureType],SOURCE.[Custom_FioriApp_System],SOURCE.[Custom_FirstDueDate],SOURCE.[Custom_FIT_GAP],SOURCE.[Custom_FIT_ID],SOURCE.[Custom_Flow_FSEC_1],SOURCE.[Custom_Flow_FSEC_2],SOURCE.[Custom_Flow_FSEC_3],SOURCE.[Custom_FS_Submitted_for_CPO_Approval],SOURCE.[Custom_FS_Submitted_for_GRC_M_Approval],SOURCE.[Custom_FS_Submitted_for_GRC_SOX_Approval],SOURCE.[Custom_FS_Submitted_for_GRC_Testing_Approval],SOURCE.[Custom_FS_Submitted_for_PO_Approval],SOURCE.[Custom_FS_Submitted_for_QA_Approval],SOURCE.[Custom_FS_Submitted_for_TI_Approval],SOURCE.[Custom_FSID],SOURCE.[Custom_FutureRelease],SOURCE.[Custom_GAP_APP_ID_Related],SOURCE.[Custom_GAP_Reprioritization],SOURCE.[Custom_GAPAPPID],SOURCE.[Custom_GAPCategory],SOURCE.[Custom_GAPEnhancement],SOURCE.[Custom_GAPEstimation],SOURCE.[Custom_GAPNumber],SOURCE.[Custom_Glassif],SOURCE.[Custom_Global_Local_Escope],SOURCE.[Custom_GRC_BUC_APPROVED],SOURCE.[Custom_GRC_TESTING_APPROVED],SOURCE.[Custom_GRC_UAM_APPROVED],SOURCE.[Custom_GUID],SOURCE.[Custom_Impact],SOURCE.[Custom_Impact_Information],SOURCE.[Custom_IndexE2E],SOURCE.[Custom_IndexProcess],SOURCE.[Custom_Interim_Solution],SOURCE.[Custom_IsGlobalSteerCoRequired],SOURCE.[Custom_IsReportRequired],SOURCE.[Custom_IssuePriority],SOURCE.[Custom_L2],SOURCE.[Custom_L2Product],SOURCE.[Custom_L3],SOURCE.[Custom_L3Product],SOURCE.[Custom_L4Product],SOURCE.[Custom_L4WorkShopDescription],SOURCE.[Custom_LE_December],SOURCE.[Custom_LE_January],SOURCE.[Custom_LE_November],SOURCE.[Custom_LE_October],SOURCE.[Custom_LeadershipApproval],SOURCE.[Custom_Legacies_Integration],SOURCE.[Custom_LegaciesDescription],SOURCE.[Custom_Logistics_Transport_Request],SOURCE.[Custom_Main_Blocker],SOURCE.[Custom_MDG_Relevant],SOURCE.[Custom_MDG_Transport_Request],SOURCE.[Custom_Microservices_Change],SOURCE.[Custom_Microservices_Description],SOURCE.[Custom_MoscowPriority],SOURCE.[Custom_Name_Of_Environment],SOURCE.[Custom_NonSAP],SOURCE.[Custom_NoSAPApprovalJustification],SOURCE.[Custom_NotAllowedForLocalTeam],SOURCE.[Custom_NotAllowedtothisState],SOURCE.[Custom_OD_LE_Dec],SOURCE.[Custom_OD_LE_Nov],SOURCE.[Custom_OD_LE_Oct],SOURCE.[Custom_OD_Submitted_for_Consultant_Approval],SOURCE.[Custom_OD_Submitted_for_CPO_Approval],SOURCE.[Custom_OD_Submitted_for_DA_Approval_CCPO],SOURCE.[Custom_OD_Submitted_for_PO_Approval],SOURCE.[Custom_OD_Submitted_for_TI_Approval],SOURCE.[Custom_OD_Target_Dec],SOURCE.[Custom_OD_Target_Nov],SOURCE.[Custom_OD_Target_Oct],SOURCE.[Custom_OD_YTD_Dec],SOURCE.[Custom_OD_YTD_Nov],SOURCE.[Custom_OD_YTD_Oct],SOURCE.[Custom_OpenDesignID],SOURCE.[Custom_OpenDesignType],SOURCE.[Custom_OTC_Transport_Request],SOURCE.[Custom_Out_Scope],SOURCE.[Custom_OutsideAuroraRecoveryDate],SOURCE.[Custom_OutsideAuroraStartDate],SOURCE.[Custom_OverviewNonSAP],SOURCE.[Custom_P2P_Transport_Request],SOURCE.[Custom_PartnerAnalysisTargetDate],SOURCE.[Custom_PartnerReleaseTargetDate],SOURCE.[Custom_PartnerResponsible],SOURCE.[Custom_PartnerStatus],SOURCE.[Custom_PlannedDate],SOURCE.[Custom_PlannedStartDatetoBuild],SOURCE.[Custom_PlannedWeeklySprint],SOURCE.[Custom_PotentialImpact],SOURCE.[Custom_PriorityforRelease],SOURCE.[Custom_PriorityRank],SOURCE.[Custom_ProbabilityofOccurrence],SOURCE.[Custom_Product_Field_TEC],SOURCE.[Custom_ProfileAccessInformation],SOURCE.[Custom_RAIDPriority],SOURCE.[Custom_ReasonforFutureRelease],SOURCE.[Custom_Related_Interface],SOURCE.[Custom_Related_Interface_Description],SOURCE.[Custom_Related_Interface_Systems],SOURCE.[Custom_Related_LIB],SOURCE.[Custom_RelatedWorkStream],SOURCE.[Custom_RelativeMassValuation],SOURCE.[Custom_ResolvedTargetDate],SOURCE.[Custom_RiskLevel],SOURCE.[Custom_RiskorIssuesCategory],SOURCE.[Custom_RiskorIssuesEscalationLevel],SOURCE.[Custom_RTR_Transport_Request],SOURCE.[Custom_SAP_Change],SOURCE.[Custom_SAPAdherence],SOURCE.[Custom_SAPRole],SOURCE.[Custom_Scenario],SOURCE.[Custom_Scenario_Name],SOURCE.[Custom_Scenario_Order],SOURCE.[Custom_ScopeItem],SOURCE.[Custom_Security_Authorizations_Impacts],SOURCE.[Custom_Servicenow_Close_Notes],SOURCE.[Custom_Servicenow_ticket_ID],SOURCE.[Custom_SIT_Committee_Approval],SOURCE.[Custom_SIT_Committee_Comments],SOURCE.[Custom_SolutionName],SOURCE.[Custom_SolutionType],SOURCE.[Custom_Source],SOURCE.[Custom_StoryType],SOURCE.[Custom_SubmittedforIntegrationTeam],SOURCE.[Custom_SuccessfullyTestedinQAEnvironment],SOURCE.[Custom_Supply_Transport_Request],SOURCE.[Custom_System],SOURCE.[Custom_SystemName],SOURCE.[Custom_Target_Dec],SOURCE.[Custom_Target_Jan],SOURCE.[Custom_Target_Nov],SOURCE.[Custom_Target_Oct],SOURCE.[Custom_TargetDateFuncSpec],SOURCE.[Custom_TargetDateGAP],SOURCE.[Custom_TC_Automation_status],SOURCE.[Custom_TEC_Transport_Request],SOURCE.[Custom_Test_Case_ExecutionValidation],SOURCE.[Custom_Test_Case_Phase],SOURCE.[Custom_Test_FSEC_1],SOURCE.[Custom_Test_FSEC_2],SOURCE.[Custom_Test_FSEC_3],SOURCE.[Custom_Test_FSEC_4],SOURCE.[Custom_Test_FSEC_6],SOURCE.[Custom_Test_Scenario_Description],SOURCE.[Custom_Test_Suite_Phase],SOURCE.[Custom_TestScenario],SOURCE.[Custom_Transport_List],SOURCE.[Custom_Transport_Request_Type],SOURCE.[Custom_Transport_Status],SOURCE.[Custom_TransportNeeded],SOURCE.[Custom_UC_Submitted_for_CPO_Approval],SOURCE.[Custom_UC_Submitted_for_DA_Approval_CCPO],SOURCE.[Custom_UC_Submitted_for_GRC_Validation_PO],SOURCE.[Custom_UC_Submitted_for_PO_approval],SOURCE.[Custom_UC_Submitted_for_SAP_Approval],SOURCE.[Custom_UC_Submitted_for_TI_Approval],SOURCE.[Custom_US_Priority],SOURCE.[Custom_User_Exist_BADIs],SOURCE.[Custom_User_Exist_BADIs_Description],SOURCE.[Custom_UserStoryAPPID],SOURCE.[Custom_UserStoryType],SOURCE.[Custom_WaitingBuCApproval],SOURCE.[Custom_Work_Team],SOURCE.[Custom_Workaround],SOURCE.[Custom_WorkItem_Charm_Number],SOURCE.[Custom_workitem_ID],SOURCE.[Custom_workpackage_ID],SOURCE.[Custom_Workshop_ID],SOURCE.[Custom_Workshoptobepresented],SOURCE.[Custom_WRICEF],SOURCE.[Custom_YTD_December],SOURCE.[Custom_YTD_Jan],SOURCE.[Custom_YTD_November],SOURCE.[Custom_YTD_Oct],SOURCE.[Custom_Zone_Field_TEC],SOURCE.[Microsoft_VSTS_CodeReview_AcceptedBySK],SOURCE.[Microsoft_VSTS_CodeReview_AcceptedDate],SOURCE.[Microsoft_VSTS_CodeReview_ClosedStatus],SOURCE.[Microsoft_VSTS_CodeReview_ClosedStatusCode],SOURCE.[Microsoft_VSTS_CodeReview_ClosingComment],SOURCE.[Microsoft_VSTS_CodeReview_Context],SOURCE.[Microsoft_VSTS_CodeReview_ContextCode],SOURCE.[Microsoft_VSTS_CodeReview_ContextOwner],SOURCE.[Microsoft_VSTS_CodeReview_ContextType],SOURCE.[Microsoft_VSTS_Common_ReviewedBySK],SOURCE.[Microsoft_VSTS_Common_StateCode],SOURCE.[Microsoft_VSTS_Feedback_ApplicationType],SOURCE.[Microsoft_VSTS_TCM_TestSuiteType],SOURCE.[Microsoft_VSTS_TCM_TestSuiteTypeId],SOURCE.[DataCarregamento]);
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

# Fim carga Consume WorkItemRevisions
