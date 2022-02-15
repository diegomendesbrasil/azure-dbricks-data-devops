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

# Lê o arquivo parquet do path (carga full) e salva em um spark dataframe

df_diario = spark.read.format('avro').load(sourcePath_diario)

# COMMAND ----------

df_diario = df_diario.select('WorkItemId','Revision','RevisedDate','RevisedDateSK','DateSK','IsCurrent','IsLastRevisionOfDay','AnalyticsUpdatedDate','ProjectSK','WorkItemRevisionSK','AreaSK','IterationSK','AssignedToUserSK','ChangedByUserSK','CreatedByUserSK','ActivatedByUserSK','ClosedByUserSK','ResolvedByUserSK','ActivatedDateSK','ChangedDateSK','ClosedDateSK','CreatedDateSK','ResolvedDateSK','StateChangeDateSK','InProgressDateSK','CompletedDateSK','Watermark','Title','WorkItemType','ChangedDate','CreatedDate','State','Reason','FoundIn','IntegrationBuild','ActivatedDate','Activity','BacklogPriority','BusinessValue','ClosedDate','Issue','Priority','Rating','ResolvedDate','Severity','TimeCriticality','ValueArea','DueDate','Effort','FinishDate','RemainingWork','StartDate','TargetDate','Blocked','ParentWorkItemId','TagNames','StateCategory','InProgressDate','CompletedDate','LeadTimeDays','CycleTimeDays','AutomatedTestId','AutomatedTestName','AutomatedTestStorage','AutomatedTestType','AutomationStatus','StateChangeDate','Count','CommentCount','ABIProprietaryMethodology_IssueCreatedDate','ABIProprietaryMethodology_IssueOwnerSK','Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866','Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17','Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422','Custom_AccordingwithSOXcontrols','Custom_Analytics_Relevant','Custom_AssociatedProject','Custom_Aurora_Category','Custom_Automation','Custom_AutomationBlankField','Custom_AutomationNeeded','Custom_AutomationNeeded2','Custom_Block_Workstream_Requester','Custom_BlockedJustification','Custom_Bug_Phase','Custom_Bug_Root_Cause','Custom_Bug_Workstream_Requester','Custom_BuildType','Custom_BusinessProcessImpact','Custom_Centralization','Custom_Classification','Custom_Complexity','Custom_Criticality','Custom_Custom2','Custom_DARelated','Custom_DateIdentified','Custom_Deadline','Custom_DependencyConsumer','Custom_DependencyProvider','Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8','Custom_E2E_Name','Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e','Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e','Custom_Effort_Aurora','Custom_EscalationDate','Custom_EscalationNeeded','Custom_FeatureCategory','Custom_FeatureType','Custom_FirstDueDate','Custom_FIT_GAP','Custom_Flow_FSEC_1','Custom_Flow_FSEC_2','Custom_Flow_FSEC_3','Custom_FS_Submitted_for_CPO_Approval','Custom_FS_Submitted_for_GRC_M_Approval','Custom_FS_Submitted_for_GRC_SOX_Approval','Custom_FS_Submitted_for_GRC_Testing_Approval','Custom_FS_Submitted_for_PO_Approval','Custom_FS_Submitted_for_QA_Approval','Custom_FS_Submitted_for_TI_Approval','Custom_FSID','Custom_FutureRelease','Custom_GAP_Reprioritization','Custom_GAPAPPID','Custom_GAPCategory','Custom_GAPEstimation','Custom_GAPNumber','Custom_Glassif','Custom_Global_Local_Escope','Custom_GRC_BUC_APPROVED','Custom_GRC_TESTING_APPROVED','Custom_GRC_UAM_APPROVED','Custom_GUID','Custom_Impact','Custom_IndexE2E','Custom_IndexProcess','Custom_Interim_Solution','Custom_IsGlobalSteerCoRequired','Custom_IsReportRequired','Custom_IssuePriority','Custom_L2Product','Custom_L3Product','Custom_L4Product','Custom_L4WorkShopDescription','Custom_LE_December','Custom_LE_January','Custom_LE_November','Custom_LE_October','Custom_LeadershipApproval','Custom_Legacies_Integration','Custom_LegaciesDescription','Custom_MDG_Relevant','Custom_MoscowPriority','Custom_NonSAP','Custom_NoSAPApprovalJustification','Custom_NotAllowedtothisState','Custom_OD_LE_Dec','Custom_OD_LE_Nov','Custom_OD_LE_Oct','Custom_OD_Submitted_for_Consultant_Approval','Custom_OD_Submitted_for_CPO_Approval','Custom_OD_Submitted_for_DA_Approval_CCPO','Custom_OD_Submitted_for_PO_Approval','Custom_OD_Submitted_for_TI_Approval','Custom_OD_Target_Dec','Custom_OD_Target_Nov','Custom_OD_Target_Oct','Custom_OD_YTD_Dec','Custom_OD_YTD_Nov','Custom_OD_YTD_Oct','Custom_OpenDesignID','Custom_OpenDesignType','Custom_Out_Scope','Custom_OutsideAuroraRecoveryDate','Custom_OutsideAuroraStartDate','Custom_OverviewNonSAP','Custom_PartnerAnalysisTargetDate','Custom_PartnerReleaseTargetDate','Custom_PartnerResponsible','Custom_PartnerStatus','Custom_PlannedStartDatetoBuild','Custom_PotentialImpact','Custom_PriorityforRelease','Custom_PriorityRank','Custom_ProbabilityofOccurrence','Custom_ProfileAccessInformation','Custom_RAIDPriority','Custom_ReasonforFutureRelease','Custom_RelatedWorkStream','Custom_RelativeMassValuation','Custom_ResolvedTargetDate','Custom_RiskLevel','Custom_RiskorIssuesCategory','Custom_RiskorIssuesEscalationLevel','Custom_SAPAdherence','Custom_SAPRole','Custom_Scenario','Custom_Scenario_Name','Custom_Scenario_Order','Custom_ScopeItem','Custom_SolutionName','Custom_SolutionType','Custom_Source','Custom_StoryType','Custom_SystemName','Custom_Target_Dec','Custom_Target_Jan','Custom_Target_Nov','Custom_Target_Oct','Custom_TargetDateFuncSpec','Custom_TargetDateGAP','Custom_TC_Automation_status','Custom_Test_FSEC_1','Custom_Test_FSEC_2','Custom_Test_FSEC_3','Custom_Test_FSEC_4','Custom_Test_FSEC_6','Custom_Test_Suite_Phase','Custom_UC_Submitted_for_CPO_Approval','Custom_UC_Submitted_for_DA_Approval_CCPO','Custom_UC_Submitted_for_GRC_Validation_PO','Custom_UC_Submitted_for_PO_approval','Custom_UC_Submitted_for_SAP_Approval','Custom_UC_Submitted_for_TI_Approval','Custom_US_Priority','Custom_UserStoryAPPID','Custom_UserStoryType','Custom_Workshop_ID','Custom_Workshoptobepresented','Custom_WRICEF','Custom_YTD_December','Custom_YTD_Jan','Custom_YTD_November','Custom_YTD_Oct','Microsoft_VSTS_CodeReview_AcceptedBySK','Microsoft_VSTS_CodeReview_AcceptedDate','Microsoft_VSTS_CodeReview_ClosedStatus','Microsoft_VSTS_CodeReview_ClosedStatusCode','Microsoft_VSTS_CodeReview_ClosingComment','Microsoft_VSTS_CodeReview_Context','Microsoft_VSTS_CodeReview_ContextCode','Microsoft_VSTS_CodeReview_ContextOwner','Microsoft_VSTS_CodeReview_ContextType','Microsoft_VSTS_Common_ReviewedBySK','Microsoft_VSTS_Common_StateCode','Microsoft_VSTS_Feedback_ApplicationType','Microsoft_VSTS_TCM_TestSuiteType','Microsoft_VSTS_TCM_TestSuiteTypeId','DataCarregamento')

# COMMAND ----------

# Número de linhas e de colunas do dataframe com a carga diária

num_linhas_diario = df_diario.count()

print(f'Número de colunas no df diario: {len(df_diario.columns)}')
print(f'Número de linhas no df diario: {num_linhas_diario}')

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

# Substitui campos None para None do Python

df = df.replace(['NaN', 'nan', 'Nan', 'NAN', 'null', 'Null', 'NULL', 'none', 'None', 'NONE', ''], None).replace(float('nan'), None)

# COMMAND ----------

# Salva a tabela de volta em modo parquet no caminho especificado

sinkPath = aurora_standardized_folder + sourceFile + '/' + max_data
print(sinkPath)

# COMMAND ----------

df = df \
.withColumn('WorkItemId', df.WorkItemId.cast(IntegerType()))\
.withColumn('Revision', df.Revision.cast(IntegerType()))\
.withColumn('RevisedDate', df.RevisedDate.cast(TimestampType()))\
.withColumn('RevisedDateSK', df.RevisedDateSK.cast(IntegerType()))\
.withColumn('DateSK', df.DateSK.cast(IntegerType()))\
.withColumn('IsCurrent', df.IsCurrent.cast(IntegerType()))\
.withColumn('IsLastRevisionOfDay', df.IsLastRevisionOfDay.cast(IntegerType()))\
.withColumn('AnalyticsUpdatedDate', df.AnalyticsUpdatedDate.cast(TimestampType()))\
.withColumn('ProjectSK', df.ProjectSK.cast(StringType()))\
.withColumn('WorkItemRevisionSK', df.WorkItemRevisionSK.cast(IntegerType()))\
.withColumn('AreaSK', df.AreaSK.cast(StringType()))\
.withColumn('IterationSK', df.IterationSK.cast(StringType()))\
.withColumn('AssignedToUserSK', df.AssignedToUserSK.cast(StringType()))\
.withColumn('ChangedByUserSK', df.ChangedByUserSK.cast(StringType()))\
.withColumn('CreatedByUserSK', df.CreatedByUserSK.cast(StringType()))\
.withColumn('ActivatedByUserSK', df.ActivatedByUserSK.cast(StringType()))\
.withColumn('ClosedByUserSK', df.ClosedByUserSK.cast(StringType()))\
.withColumn('ResolvedByUserSK', df.ResolvedByUserSK.cast(StringType()))\
.withColumn('ActivatedDateSK', df.ActivatedDateSK.cast(IntegerType()))\
.withColumn('ChangedDateSK', df.ChangedDateSK.cast(IntegerType()))\
.withColumn('ClosedDateSK', df.ClosedDateSK.cast(IntegerType()))\
.withColumn('CreatedDateSK', df.CreatedDateSK.cast(IntegerType()))\
.withColumn('ResolvedDateSK', df.ResolvedDateSK.cast(IntegerType()))\
.withColumn('StateChangeDateSK', df.StateChangeDateSK.cast(IntegerType()))\
.withColumn('InProgressDateSK', df.InProgressDateSK.cast(IntegerType()))\
.withColumn('CompletedDateSK', df.CompletedDateSK.cast(IntegerType()))\
.withColumn('Watermark', df.Watermark.cast(IntegerType()))\
.withColumn('Title', df.Title.cast(StringType()))\
.withColumn('WorkItemType', df.WorkItemType.cast(StringType()))\
.withColumn('ChangedDate', df.ChangedDate.cast(TimestampType()))\
.withColumn('CreatedDate', df.CreatedDate.cast(TimestampType()))\
.withColumn('State', df.State.cast(StringType()))\
.withColumn('Reason', df.Reason.cast(StringType()))\
.withColumn('FoundIn', df.FoundIn.cast(StringType()))\
.withColumn('IntegrationBuild', df.IntegrationBuild.cast(StringType()))\
.withColumn('ActivatedDate', df.ActivatedDate.cast(TimestampType()))\
.withColumn('Activity', df.Activity.cast(StringType()))\
.withColumn('BacklogPriority', df.BacklogPriority.cast(FloatType()))\
.withColumn('BusinessValue', df.BusinessValue.cast(IntegerType()))\
.withColumn('ClosedDate', df.ClosedDate.cast(TimestampType()))\
.withColumn('Issue', df.Issue.cast(StringType()))\
.withColumn('Priority', df.Priority.cast(IntegerType()))\
.withColumn('Rating', df.Rating.cast(StringType()))\
.withColumn('ResolvedDate', df.ResolvedDate.cast(TimestampType()))\
.withColumn('Severity', df.Severity.cast(StringType()))\
.withColumn('TimeCriticality', df.TimeCriticality.cast(FloatType()))\
.withColumn('ValueArea', df.ValueArea.cast(StringType()))\
.withColumn('DueDate', df.DueDate.cast(TimestampType()))\
.withColumn('Effort', df.Effort.cast(FloatType()))\
.withColumn('FinishDate', df.FinishDate.cast(TimestampType()))\
.withColumn('RemainingWork', df.RemainingWork.cast(FloatType()))\
.withColumn('StartDate', df.StartDate.cast(TimestampType()))\
.withColumn('TargetDate', df.TargetDate.cast(TimestampType()))\
.withColumn('Blocked', df.Blocked.cast(StringType()))\
.withColumn('ParentWorkItemId', df.ParentWorkItemId.cast(IntegerType()))\
.withColumn('TagNames', df.TagNames.cast(StringType()))\
.withColumn('StateCategory', df.StateCategory.cast(StringType()))\
.withColumn('InProgressDate', df.InProgressDate.cast(TimestampType()))\
.withColumn('CompletedDate', df.CompletedDate.cast(TimestampType()))\
.withColumn('LeadTimeDays', df.LeadTimeDays.cast(FloatType()))\
.withColumn('CycleTimeDays', df.CycleTimeDays.cast(FloatType()))\
.withColumn('AutomatedTestId', df.AutomatedTestId.cast(StringType()))\
.withColumn('AutomatedTestName', df.AutomatedTestName.cast(StringType()))\
.withColumn('AutomatedTestStorage', df.AutomatedTestStorage.cast(StringType()))\
.withColumn('AutomatedTestType', df.AutomatedTestType.cast(StringType()))\
.withColumn('AutomationStatus', df.AutomationStatus.cast(StringType()))\
.withColumn('StateChangeDate', df.StateChangeDate.cast(TimestampType()))\
.withColumn('Count', df.Count.cast(LongType()))\
.withColumn('CommentCount', df.CommentCount.cast(IntegerType()))\
.withColumn('ABIProprietaryMethodology_IssueCreatedDate', df.ABIProprietaryMethodology_IssueCreatedDate.cast(TimestampType()))\
.withColumn('ABIProprietaryMethodology_IssueOwnerSK', df.ABIProprietaryMethodology_IssueOwnerSK.cast(StringType()))\
.withColumn('Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866', df.Custom_1528ce41__002De687__002D400a__002Db193__002Dbebf89991866.cast(StringType()))\
.withColumn('Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17', df.Custom_4fd5ee76__002D2ecd__002D4fd8__002Da654__002D9eaa2b393b17.cast(StringType()))\
.withColumn('Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422', df.Custom_65c83dbc__002D102f__002D4de5__002D8f48__002D638af7217422.cast(StringType()))\
.withColumn('Custom_AccordingwithSOXcontrols', df.Custom_AccordingwithSOXcontrols.cast(StringType()))\
.withColumn('Custom_Analytics_Relevant', df.Custom_Analytics_Relevant.cast(StringType()))\
.withColumn('Custom_AssociatedProject', df.Custom_AssociatedProject.cast(StringType()))\
.withColumn('Custom_Aurora_Category', df.Custom_Aurora_Category.cast(StringType()))\
.withColumn('Custom_Automation', df.Custom_Automation.cast(StringType()))\
.withColumn('Custom_AutomationBlankField', df.Custom_AutomationBlankField.cast(StringType()))\
.withColumn('Custom_AutomationNeeded', df.Custom_AutomationNeeded.cast(StringType()))\
.withColumn('Custom_AutomationNeeded2', df.Custom_AutomationNeeded2.cast(StringType()))\
.withColumn('Custom_Block_Workstream_Requester', df.Custom_Block_Workstream_Requester.cast(StringType()))\
.withColumn('Custom_BlockedJustification', df.Custom_BlockedJustification.cast(StringType()))\
.withColumn('Custom_Bug_Phase', df.Custom_Bug_Phase.cast(StringType()))\
.withColumn('Custom_Bug_Root_Cause', df.Custom_Bug_Root_Cause.cast(StringType()))\
.withColumn('Custom_Bug_Workstream_Requester', df.Custom_Bug_Workstream_Requester.cast(StringType()))\
.withColumn('Custom_BuildType', df.Custom_BuildType.cast(StringType()))\
.withColumn('Custom_BusinessProcessImpact', df.Custom_BusinessProcessImpact.cast(StringType()))\
.withColumn('Custom_Centralization', df.Custom_Centralization.cast(StringType()))\
.withColumn('Custom_Classification', df.Custom_Classification.cast(StringType()))\
.withColumn('Custom_Complexity', df.Custom_Complexity.cast(StringType()))\
.withColumn('Custom_Criticality', df.Custom_Criticality.cast(StringType()))\
.withColumn('Custom_Custom2', df.Custom_Custom2.cast(StringType()))\
.withColumn('Custom_DARelated', df.Custom_DARelated.cast(StringType()))\
.withColumn('Custom_DateIdentified', df.Custom_DateIdentified.cast(TimestampType()))\
.withColumn('Custom_Deadline', df.Custom_Deadline.cast(TimestampType()))\
.withColumn('Custom_DependencyConsumer', df.Custom_DependencyConsumer.cast(StringType()))\
.withColumn('Custom_DependencyProvider', df.Custom_DependencyProvider.cast(StringType()))\
.withColumn('Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8', df.Custom_e07bcef1__002Dc728__002D449b__002D8f8e__002D5d19c38b66f8.cast(StringType()))\
.withColumn('Custom_E2E_Name', df.Custom_E2E_Name.cast(StringType()))\
.withColumn('Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e', df.Custom_e5efe877__002D84d3__002D4019__002Da026__002D89ec57481e5e.cast(StringType()))\
.withColumn('Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e', df.Custom_edbf29fb__002D79cd__002D4e2f__002Db5a0__002Db0040c1f613e.cast(TimestampType()))\
.withColumn('Custom_Effort_Aurora', df.Custom_Effort_Aurora.cast(LongType()))\
.withColumn('Custom_EscalationDate', df.Custom_EscalationDate.cast(TimestampType()))\
.withColumn('Custom_EscalationNeeded', df.Custom_EscalationNeeded.cast(StringType()))\
.withColumn('Custom_FeatureCategory', df.Custom_FeatureCategory.cast(StringType()))\
.withColumn('Custom_FeatureType', df.Custom_FeatureType.cast(StringType()))\
.withColumn('Custom_FirstDueDate', df.Custom_FirstDueDate.cast(TimestampType()))\
.withColumn('Custom_FIT_GAP', df.Custom_FIT_GAP.cast(StringType()))\
.withColumn('Custom_Flow_FSEC_1', df.Custom_Flow_FSEC_1.cast(StringType()))\
.withColumn('Custom_Flow_FSEC_2', df.Custom_Flow_FSEC_2.cast(StringType()))\
.withColumn('Custom_Flow_FSEC_3', df.Custom_Flow_FSEC_3.cast(StringType()))\
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
.withColumn('Custom_Impact', df.Custom_Impact.cast(StringType()))\
.withColumn('Custom_IndexE2E', df.Custom_IndexE2E.cast(StringType()))\
.withColumn('Custom_IndexProcess', df.Custom_IndexProcess.cast(StringType()))\
.withColumn('Custom_Interim_Solution', df.Custom_Interim_Solution.cast(StringType()))\
.withColumn('Custom_IsGlobalSteerCoRequired', df.Custom_IsGlobalSteerCoRequired.cast(StringType()))\
.withColumn('Custom_IsReportRequired', df.Custom_IsReportRequired.cast(StringType()))\
.withColumn('Custom_IssuePriority', df.Custom_IssuePriority.cast(StringType()))\
.withColumn('Custom_L2Product', df.Custom_L2Product.cast(StringType()))\
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
.withColumn('Custom_MDG_Relevant', df.Custom_MDG_Relevant.cast(StringType()))\
.withColumn('Custom_MoscowPriority', df.Custom_MoscowPriority.cast(StringType()))\
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
.withColumn('Custom_OutsideAuroraRecoveryDate', df.Custom_OutsideAuroraRecoveryDate.cast(TimestampType()))\
.withColumn('Custom_OutsideAuroraStartDate', df.Custom_OutsideAuroraStartDate.cast(TimestampType()))\
.withColumn('Custom_OverviewNonSAP', df.Custom_OverviewNonSAP.cast(StringType()))\
.withColumn('Custom_PartnerAnalysisTargetDate', df.Custom_PartnerAnalysisTargetDate.cast(TimestampType()))\
.withColumn('Custom_PartnerReleaseTargetDate', df.Custom_PartnerReleaseTargetDate.cast(TimestampType()))\
.withColumn('Custom_PartnerResponsible', df.Custom_PartnerResponsible.cast(StringType()))\
.withColumn('Custom_PartnerStatus', df.Custom_PartnerStatus.cast(StringType()))\
.withColumn('Custom_PlannedStartDatetoBuild', df.Custom_PlannedStartDatetoBuild.cast(TimestampType()))\
.withColumn('Custom_PotentialImpact', df.Custom_PotentialImpact.cast(StringType()))\
.withColumn('Custom_PriorityforRelease', df.Custom_PriorityforRelease.cast(LongType()))\
.withColumn('Custom_PriorityRank', df.Custom_PriorityRank.cast(LongType()))\
.withColumn('Custom_ProbabilityofOccurrence', df.Custom_ProbabilityofOccurrence.cast(StringType()))\
.withColumn('Custom_ProfileAccessInformation', df.Custom_ProfileAccessInformation.cast(StringType()))\
.withColumn('Custom_RAIDPriority', df.Custom_RAIDPriority.cast(StringType()))\
.withColumn('Custom_ReasonforFutureRelease', df.Custom_ReasonforFutureRelease.cast(StringType()))\
.withColumn('Custom_RelatedWorkStream', df.Custom_RelatedWorkStream.cast(StringType()))\
.withColumn('Custom_RelativeMassValuation', df.Custom_RelativeMassValuation.cast(LongType()))\
.withColumn('Custom_ResolvedTargetDate', df.Custom_ResolvedTargetDate.cast(TimestampType()))\
.withColumn('Custom_RiskLevel', df.Custom_RiskLevel.cast(StringType()))\
.withColumn('Custom_RiskorIssuesCategory', df.Custom_RiskorIssuesCategory.cast(StringType()))\
.withColumn('Custom_RiskorIssuesEscalationLevel', df.Custom_RiskorIssuesEscalationLevel.cast(StringType()))\
.withColumn('Custom_SAPAdherence', df.Custom_SAPAdherence.cast(StringType()))\
.withColumn('Custom_SAPRole', df.Custom_SAPRole.cast(StringType()))\
.withColumn('Custom_Scenario', df.Custom_Scenario.cast(StringType()))\
.withColumn('Custom_Scenario_Name', df.Custom_Scenario_Name.cast(StringType()))\
.withColumn('Custom_Scenario_Order', df.Custom_Scenario_Order.cast(FloatType()))\
.withColumn('Custom_ScopeItem', df.Custom_ScopeItem.cast(StringType()))\
.withColumn('Custom_SolutionName', df.Custom_SolutionName.cast(StringType()))\
.withColumn('Custom_SolutionType', df.Custom_SolutionType.cast(StringType()))\
.withColumn('Custom_Source', df.Custom_Source.cast(StringType()))\
.withColumn('Custom_StoryType', df.Custom_StoryType.cast(StringType()))\
.withColumn('Custom_SystemName', df.Custom_SystemName.cast(StringType()))\
.withColumn('Custom_Target_Dec', df.Custom_Target_Dec.cast(StringType()))\
.withColumn('Custom_Target_Jan', df.Custom_Target_Jan.cast(StringType()))\
.withColumn('Custom_Target_Nov', df.Custom_Target_Nov.cast(StringType()))\
.withColumn('Custom_Target_Oct', df.Custom_Target_Oct.cast(StringType()))\
.withColumn('Custom_TargetDateFuncSpec', df.Custom_TargetDateFuncSpec.cast(TimestampType()))\
.withColumn('Custom_TargetDateGAP', df.Custom_TargetDateGAP.cast(TimestampType()))\
.withColumn('Custom_TC_Automation_status', df.Custom_TC_Automation_status.cast(StringType()))\
.withColumn('Custom_Test_FSEC_1', df.Custom_Test_FSEC_1.cast(StringType()))\
.withColumn('Custom_Test_FSEC_2', df.Custom_Test_FSEC_2.cast(StringType()))\
.withColumn('Custom_Test_FSEC_3', df.Custom_Test_FSEC_3.cast(StringType()))\
.withColumn('Custom_Test_FSEC_4', df.Custom_Test_FSEC_4.cast(StringType()))\
.withColumn('Custom_Test_FSEC_6', df.Custom_Test_FSEC_6.cast(LongType()))\
.withColumn('Custom_Test_Suite_Phase', df.Custom_Test_Suite_Phase.cast(StringType()))\
.withColumn('Custom_UC_Submitted_for_CPO_Approval', df.Custom_UC_Submitted_for_CPO_Approval.cast(StringType()))\
.withColumn('Custom_UC_Submitted_for_DA_Approval_CCPO', df.Custom_UC_Submitted_for_DA_Approval_CCPO.cast(StringType()))\
.withColumn('Custom_UC_Submitted_for_GRC_Validation_PO', df.Custom_UC_Submitted_for_GRC_Validation_PO.cast(StringType()))\
.withColumn('Custom_UC_Submitted_for_PO_approval', df.Custom_UC_Submitted_for_PO_approval.cast(StringType()))\
.withColumn('Custom_UC_Submitted_for_SAP_Approval', df.Custom_UC_Submitted_for_SAP_Approval.cast(StringType()))\
.withColumn('Custom_UC_Submitted_for_TI_Approval', df.Custom_UC_Submitted_for_TI_Approval.cast(StringType()))\
.withColumn('Custom_US_Priority', df.Custom_US_Priority.cast(StringType()))\
.withColumn('Custom_UserStoryAPPID', df.Custom_UserStoryAPPID.cast(StringType()))\
.withColumn('Custom_UserStoryType', df.Custom_UserStoryType.cast(StringType()))\
.withColumn('Custom_Workshop_ID', df.Custom_Workshop_ID.cast(StringType()))\
.withColumn('Custom_Workshoptobepresented', df.Custom_Workshoptobepresented.cast(StringType()))\
.withColumn('Custom_WRICEF', df.Custom_WRICEF.cast(StringType()))\
.withColumn('Custom_YTD_December', df.Custom_YTD_December.cast(StringType()))\
.withColumn('Custom_YTD_Jan', df.Custom_YTD_Jan.cast(StringType()))\
.withColumn('Custom_YTD_November', df.Custom_YTD_November.cast(StringType()))\
.withColumn('Custom_YTD_Oct', df.Custom_YTD_Oct.cast(StringType()))\
.withColumn('Microsoft_VSTS_CodeReview_AcceptedBySK', df.Microsoft_VSTS_CodeReview_AcceptedBySK.cast(StringType()))\
.withColumn('Microsoft_VSTS_CodeReview_AcceptedDate', df.Microsoft_VSTS_CodeReview_AcceptedDate.cast(TimestampType()))\
.withColumn('Microsoft_VSTS_CodeReview_ClosedStatus', df.Microsoft_VSTS_CodeReview_ClosedStatus.cast(StringType()))\
.withColumn('Microsoft_VSTS_CodeReview_ClosedStatusCode', df.Microsoft_VSTS_CodeReview_ClosedStatusCode.cast(LongType()))\
.withColumn('Microsoft_VSTS_CodeReview_ClosingComment', df.Microsoft_VSTS_CodeReview_ClosingComment.cast(StringType()))\
.withColumn('Microsoft_VSTS_CodeReview_Context', df.Microsoft_VSTS_CodeReview_Context.cast(StringType()))\
.withColumn('Microsoft_VSTS_CodeReview_ContextCode', df.Microsoft_VSTS_CodeReview_ContextCode.cast(LongType()))\
.withColumn('Microsoft_VSTS_CodeReview_ContextOwner', df.Microsoft_VSTS_CodeReview_ContextOwner.cast(StringType()))\
.withColumn('Microsoft_VSTS_CodeReview_ContextType', df.Microsoft_VSTS_CodeReview_ContextType.cast(StringType()))\
.withColumn('Microsoft_VSTS_Common_ReviewedBySK', df.Microsoft_VSTS_Common_ReviewedBySK.cast(StringType()))\
.withColumn('Microsoft_VSTS_Common_StateCode', df.Microsoft_VSTS_Common_StateCode.cast(LongType()))\
.withColumn('Microsoft_VSTS_Feedback_ApplicationType', df.Microsoft_VSTS_Feedback_ApplicationType.cast(StringType()))\
.withColumn('Microsoft_VSTS_TCM_TestSuiteType', df.Microsoft_VSTS_TCM_TestSuiteType.cast(StringType()))\
.withColumn('Microsoft_VSTS_TCM_TestSuiteTypeId', df.Microsoft_VSTS_TCM_TestSuiteTypeId.cast(LongType()))

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
