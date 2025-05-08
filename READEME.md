项目结构分析与总结
1. 核心应用 (medical_data_extractor.py):
框架: 使用 PySide6 构建图形用户界面 (GUI)。
主窗口: MedicalDataExtractor (QMainWindow) 包含一个 QTabWidget。
模块化: 应用被划分为多个逻辑标签页，每个标签页代表数据提取流程中的一个步骤或功能：
ConnectionTab: 处理数据库连接参数和建立连接。
StructureTab: 可视化数据库的模式 (schema) 和表结构。
QueryDiseaseTab: 允许用户使用 ConditionGroupWidget 定义疾病队列，并创建初始的队列数据表 (例如 first_DISEASE_admissions)。
BaseInfoDataExtractionTab: 向队列数据表中添加预定义的“基础”信息集（人口统计学、生命体征、实验室检查、合并症等）。它使用 base_info_sql.py 中的 SQL 逻辑，并通过 SQLWorker 在后台执行。
SpecialInfoDataExtractionTab: 这是一个更灵活的标签页，用于向队列数据表中添加特定的数据点（实验室检查、药物、操作/手术、诊断）。它使用 ConditionGroupWidget 进行项目筛选，并生成复杂的 SQL 来合并数据。
DataExportTab: 允许用户选择数据库中的表（可来自不同 schema），并将其导出为 CSV、Parquet 或 Excel 文件。
集中的数据库参数: 其他标签页通过 get_db_params_func (从 ConnectionTab 传递) 获取数据库连接详情。
标签页间通信: 通过信号 (Signal) 实现基本的协调，例如 ConnectionTab 的 connected_signal 和 SpecialInfoDataExtractionTab 发送给 DataExportTab 的 request_preview_signal。
2. 核心 GUI 组件 (conditiongroup.py):
ConditionGroupWidget: 一个设计精良、支持递归的自定义控件。
允许用户构建复杂的布尔 (AND/OR) 查询条件。
支持添加单个关键词（带有“包含/排除”逻辑）和嵌套的条件组。
根据用户输入动态生成 SQL WHERE 子句片段。
当条件改变时，会发出 condition_changed 信号，以便 UI 的其他部分可以做出响应。
search_field 参数使其能够适应不同的查询目标（例如，d_icd_diagnoses 表中的 long_title，或 d_labitems 表中的 label）。
3. SQL 逻辑 (base_info_sql.py):
包含一系列 Python 函数，每个函数负责生成大段的 SQL 语句。
这些 SQL 块主要执行以下操作：
ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...: 向目标表添加新列。
UPDATE ... SET ... FROM ... WHERE ...: 通过与 MIMIC-IV 的衍生视图 (derived views) 或基础表连接来填充这些新列。
部分函数会创建并删除临时表 (例如 mimiciv_data.heart_rate_arv)。
add_past_diagnostic 函数比较特殊，它接收一个数据库连接对象 (conn)，以便在 SQL 生成阶段动态查询与关键词匹配的 ICD 代码。
4. 关键数据流与操作:
连接 (ConnectionTab): 用户提供数据库凭据。
(可选) 查看结构 (StructureTab): 用户检查数据库结构。
创建队列 (QueryDiseaseTab):
用户使用 ConditionGroupWidget 定义疾病标准。
应用程序查询 mimiciv_hosp.d_icd_diagnoses 表。
创建一个新表 (例如 mimiciv_data.first_sepsis_admissions)，包含符合条件的患者的 subject_id、hadm_id、stay_id 以及首次 ICU 入住的详细信息。
添加基础信息 (BaseInfoDataExtractionTab):
用户选择一个队列数据表（在上一步创建）。
用户勾选所需的数据类别（人口统计学、生命体征等）。
应用程序生成并执行一系列 ALTER TABLE 和 UPDATE 语句（来自 base_info_sql.py），向选定的队列数据表添加许多列。
添加专项信息 (SpecialInfoDataExtractionTab):
用户选择一个队列数据表。
用户选择数据来源（化验、用药、操作、诊断）。
用户使用 ConditionGroupWidget 从数据源中筛选项目（例如，特定的化验项目）。
用户定义聚合逻辑（首次、末次、最小值、最大值、平均值、计数、是否存在）和时间窗口。
应用程序生成复杂的 SQL（使用 CTE 进行筛选，然后进行聚合/窗口函数操作，最后是 ALTER TABLE 和 UPDATE），向队列数据表添加新的自定义列。
导出 (DataExportTab):
用户选择数据库中的任何表。
用户选择导出格式和路径。
数据被导出。
5. 总体印象:这对于使用 MIMIC-IV 的研究人员来说是一个非常强大的工具。基于标签页的工作流程直观易懂。ConditionGroupWidget 是一个用于复杂查询的可重用 UI 组件，非常出色。关注点分离（UI、SQL 生成、执行）总体上做得很好。
