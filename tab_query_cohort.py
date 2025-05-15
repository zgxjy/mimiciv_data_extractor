# --- START OF FILE tab_query_cohort.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QDialog, QLineEdit, QFormLayout,
                          QApplication, QProgressBar, QGroupBox, QComboBox,
                          QRadioButton, QButtonGroup, QScrollArea,QGroupBox) # 增加了 QScrollArea
from PySide6.QtCore import Qt, Signal, QObject, QThread, Slot
import psycopg2
from psycopg2 import sql as psql
import re
import time
import traceback
from conditiongroup import ConditionGroupWidget # ConditionGroupWidget 现在没有 search_field 初始化参数

# --- Constants for Cohort Types (Admission criteria) ---
COHORT_TYPE_FIRST_EVENT_STR = "首次事件入院 (基于该事件的患者首次入院)"
COHORT_TYPE_ALL_EVENTS_STR = "所有事件入院 (所有包含该事件的入院)"
COHORT_TYPE_FIRST_EVENT_KEY = "first_event_admission"
COHORT_TYPE_ALL_EVENTS_KEY = "all_event_admissions"

# --- Constants for Source Mode (Disease or Procedure) ---
MODE_DISEASE_KEY = "disease"
MODE_PROCEDURE_KEY = "procedure"

class CohortCreationWorker(QObject):
    finished = Signal(str, int) # table_name, count
    error = Signal(str)
    progress = Signal(int, int) # current_step, total_steps
    log = Signal(str)

    def __init__(self, db_params, target_table_name_str,
                 condition_sql_template, condition_params,
                 admission_cohort_type, source_mode_details):
        super().__init__()
        self.db_params = db_params
        self.target_table_name_str = target_table_name_str
        self.condition_sql_template = condition_sql_template
        self.condition_params = condition_params
        self.admission_cohort_type = admission_cohort_type
        self.source_mode_details = source_mode_details
        self.is_cancelled = False

    def cancel(self):
        self.log.emit("队列创建操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn = None
        total_steps = 6 # 1.Schema, 2.EventTemp, 3.ICUTemp, 4.TargetTable, 5.Indexes, 6.Commit/Count
        current_step = 0

        event_table_full_name = self.source_mode_details["event_table"]
        dictionary_table_full_name = self.source_mode_details["dictionary_table"]
        event_code_col_name = self.source_mode_details["event_icd_col"] # Name of the code column in event_table
        dict_code_col_name = self.source_mode_details["dict_icd_col"]   # Name of the code column in dictionary_table
        event_seq_num_col_name = self.source_mode_details["event_seq_num_col"]
        event_time_col_name = self.source_mode_details.get("event_time_col")
        event_source_type_str = self.source_mode_details["source_type"]

        event_table_schema, event_table_name_only = event_table_full_name.split('.')
        event_table_ident = psql.Identifier(event_table_schema, event_table_name_only)
        dict_table_schema, dict_table_name_only = dictionary_table_full_name.split('.')
        dict_table_ident = psql.Identifier(dict_table_schema, dict_table_name_only)

        event_code_col_ident = psql.Identifier(event_code_col_name)
        dict_code_col_ident = psql.Identifier(dict_code_col_name)
        event_seq_num_col_ident = psql.Identifier(event_seq_num_col_name)
        event_time_col_ident = psql.Identifier(event_time_col_name) if event_time_col_name else None

        try:
            self.log.emit(f"开始创建队列数据表: {self.target_table_name_str} (类型: {self.admission_cohort_type}, 来源: {event_source_type_str})...")
            self.progress.emit(current_step, total_steps)
            self.log.emit("连接数据库...")
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            conn.autocommit = False
            self.log.emit("数据库已连接。")

            current_step += 1 # Step 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 确保 'mimiciv_data' schema 存在...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS mimiciv_data;")
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            target_table_ident = psql.Identifier('mimiciv_data', self.target_table_name_str)
            selected_event_ad_temp_ident = psql.Identifier('selected_event_ad_temp_cohort_q')
            first_icu_stays_temp_ident = psql.Identifier('first_icu_stays_temp_cohort_q')
            
            current_step += 1 # Step 2
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建临时表 (符合事件条件的入院记录)...")

            base_event_select_list = [
                psql.SQL("e.subject_id"),
                psql.SQL("e.hadm_id"),
                psql.SQL("adm.admittime"),
                psql.SQL("e.{event_code_col} AS qualifying_event_code").format(event_code_col=event_code_col_ident),
                psql.SQL("e.icd_version AS qualifying_event_icd_version"),
                psql.SQL("dd.long_title AS qualifying_event_title"),
                psql.SQL("e.{event_seq_num_col} AS qualifying_event_seq_num").format(event_seq_num_col=event_seq_num_col_ident)
            ]
            if event_time_col_ident:
                base_event_select_list.append(psql.SQL("e.{} AS qualifying_event_time").format(event_time_col_ident))

            base_event_select_sql = psql.SQL("""
                SELECT {select_cols}
                FROM {event_table} e
                JOIN {dict_table} dd ON e.{event_code_col} = dd.{dict_code_col} AND e.icd_version = dd.icd_version
                JOIN mimiciv_hosp.admissions adm ON e.hadm_id = adm.hadm_id
                WHERE ({condition_template_placeholder})
            """).format(
                select_cols=psql.SQL(', ').join(base_event_select_list),
                event_table=event_table_ident,
                dict_table=dict_table_ident,
                event_code_col=event_code_col_ident,
                dict_code_col=dict_code_col_ident,
                condition_template_placeholder=psql.SQL(self.condition_sql_template)
            )

            if self.admission_cohort_type == COHORT_TYPE_FIRST_EVENT_KEY:
                order_by_parts_for_rank = [
                    psql.SQL("base.admittime ASC"),
                    psql.SQL("base.hadm_id ASC")
                ]
                if event_time_col_ident and event_source_type_str == MODE_PROCEDURE_KEY:
                    order_by_parts_for_rank.append(psql.SQL("base.qualifying_event_time ASC NULLS LAST"))
                order_by_parts_for_rank.append(psql.SQL("base.qualifying_event_seq_num ASC"))

                create_event_admission_sql = psql.SQL("""
                    DROP TABLE IF EXISTS {temp_table_ident};
                    CREATE TEMPORARY TABLE {temp_table_ident} AS
                    SELECT * FROM (
                        SELECT base.*,
                               ROW_NUMBER() OVER(PARTITION BY base.subject_id ORDER BY {order_by_clause}) AS admission_rank_for_event
                        FROM ({base_select}) AS base
                    ) ranked_base
                    WHERE ranked_base.admission_rank_for_event = 1;
                """).format(
                    temp_table_ident=selected_event_ad_temp_ident,
                    base_select=base_event_select_sql,
                    order_by_clause=psql.SQL(', ').join(order_by_parts_for_rank)
                )
            elif self.admission_cohort_type == COHORT_TYPE_ALL_EVENTS_KEY:
                create_event_admission_sql = psql.SQL("""
                    DROP TABLE IF EXISTS {temp_table_ident};
                    CREATE TEMPORARY TABLE {temp_table_ident} AS ({base_select});
                """).format(
                    temp_table_ident=selected_event_ad_temp_ident,
                    base_select=base_event_select_sql
                )
            else:
                raise ValueError(f"未知的队列类型: {self.admission_cohort_type}")

            cur.execute(create_event_admission_sql, self.condition_params)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            current_step += 1 # Step 3
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建临时表 (首次ICU入住)...")
            create_first_icu_stay_for_admission_sql = psql.SQL("""
                DROP TABLE IF EXISTS {temp_table_ident};
                CREATE TEMPORARY TABLE {temp_table_ident} AS (
                  SELECT * FROM (
                    SELECT icu.subject_id, icu.hadm_id, icu.stay_id, icu.intime AS icu_intime, icu.outtime AS icu_outtime,
                           EXTRACT(EPOCH FROM (icu.outtime - icu.intime)) / 3600.0 AS los_icu_hours,
                           ROW_NUMBER() OVER (PARTITION BY icu.hadm_id ORDER BY icu.intime ASC, icu.stay_id ASC) AS icu_stay_rank_in_admission
                    FROM mimiciv_icu.icustays icu
                    WHERE EXISTS (SELECT 1 FROM {selected_event_temp_table} seat WHERE seat.hadm_id = icu.hadm_id)
                  ) sub WHERE icu_stay_rank_in_admission = 1);
            """).format(temp_table_ident=first_icu_stays_temp_ident, selected_event_temp_table=selected_event_ad_temp_ident)
            cur.execute(create_first_icu_stay_for_admission_sql)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            current_step += 1 # Step 4
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建目标队列数据表 {self.target_table_name_str}...")
            
            target_select_list = [
                psql.SQL("evt_ad.subject_id"), psql.SQL("evt_ad.hadm_id"),
                psql.SQL("evt_ad.admittime"),
                psql.SQL("adm.dischtime"),
                psql.SQL("icu.stay_id"), psql.SQL("icu.icu_intime"), psql.SQL("icu.icu_outtime"), psql.SQL("icu.los_icu_hours"),
                psql.SQL("evt_ad.qualifying_event_code"),
                psql.SQL("evt_ad.qualifying_event_icd_version"),
                psql.SQL("CAST({source_literal} AS VARCHAR(20)) AS {col_alias}").format(
                    source_literal=psql.Literal(event_source_type_str),
                    col_alias=psql.Identifier("qualifying_event_source")
                ),
                psql.SQL("evt_ad.qualifying_event_title"),
                psql.SQL("evt_ad.qualifying_event_seq_num")
            ]
            if event_time_col_ident:
                 target_select_list.append(psql.SQL("evt_ad.qualifying_event_time"))

            main_diag_join_sql = psql.SQL("")
            if event_source_type_str == MODE_PROCEDURE_KEY:
                target_select_list.extend([
                    psql.SQL("primary_dx.icd_code AS primary_diag_code"),
                    psql.SQL("primary_dx.icd_version AS primary_diag_icd_version"),
                    psql.SQL("primary_d_dx.long_title AS primary_diag_title")
                ])
                main_diag_join_sql = psql.SQL("""
                LEFT JOIN (
                    SELECT dx.hadm_id, dx.icd_code, dx.icd_version, dx.seq_num
                    FROM mimiciv_hosp.diagnoses_icd dx
                    WHERE dx.seq_num = 1
                ) primary_dx ON evt_ad.hadm_id = primary_dx.hadm_id
                LEFT JOIN mimiciv_hosp.d_icd_diagnoses primary_d_dx
                    ON primary_dx.icd_code = primary_d_dx.icd_code
                    AND primary_dx.icd_version = primary_d_dx.icd_version
                """)

            create_target_table_sql = psql.SQL("""
                DROP TABLE IF EXISTS {target_ident};
                CREATE TABLE {target_ident} AS (
                 SELECT {select_cols}
                 FROM {event_ad_temp_table} evt_ad
                 JOIN mimiciv_hosp.admissions adm ON evt_ad.hadm_id = adm.hadm_id
                 LEFT JOIN {icu_temp_table} icu ON evt_ad.hadm_id = icu.hadm_id
                 {main_diag_join} 
                );
            """).format(
                target_ident=target_table_ident,
                select_cols=psql.SQL(', ').join(target_select_list),
                event_ad_temp_table=selected_event_ad_temp_ident,
                icu_temp_table=first_icu_stays_temp_ident,
                main_diag_join=main_diag_join_sql
            )
            cur.execute(create_target_table_sql)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            current_step += 1 # Step 5
            self.log.emit(f"步骤 {current_step}/{total_steps}: 为表 {self.target_table_name_str} 创建索引...")
            idx_prefix_base = f"{event_source_type_str}_{self.target_table_name_str.replace('first_', '').replace('all_', '').replace('_admissions', '')}"[:15]
            indexes_to_create = [
                (f"idx_{idx_prefix_base}_sub", "subject_id"), (f"idx_{idx_prefix_base}_hadm", "hadm_id"),
                (f"idx_{idx_prefix_base}_stay", "stay_id"), (f"idx_{idx_prefix_base}_admt", "admittime"),
                (f"idx_{idx_prefix_base}_icuin", "icu_intime"), (f"idx_{idx_prefix_base}_evcode", "qualifying_event_code"),
                (f"idx_{idx_prefix_base}_evvers", "qualifying_event_icd_version")
            ]
            if event_source_type_str == MODE_PROCEDURE_KEY:
                indexes_to_create.append((f"idx_{idx_prefix_base}_pdxcode", "primary_diag_code"))

            for index_name_str, column_name_str in indexes_to_create:
                if len(index_name_str) > 63: index_name_str = index_name_str[:63]
                index_ident = psql.Identifier(index_name_str)
                column_ident = psql.Identifier(column_name_str)
                cur.execute(psql.SQL("SELECT 1 FROM information_schema.columns WHERE table_schema = 'mimiciv_data' AND table_name = %s AND column_name = %s"),
                            (self.target_table_name_str, column_name_str))
                if cur.fetchone():
                    create_index_sql = psql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({});").format(index_ident, target_table_ident, column_ident)
                    self.log.emit(f"    创建索引 {index_name_str} on {column_name_str}...")
                    cur.execute(create_index_sql)
                else:
                    self.log.emit(f"    跳过索引 {index_name_str}，列 {column_name_str} 在表 {self.target_table_name_str} 中不存在。")
                if self.is_cancelled: raise InterruptedError("操作已取消")
            self.log.emit("所有索引创建完毕。")
            self.progress.emit(current_step, total_steps)

            current_step += 1 # Step 6
            self.log.emit(f"步骤 {current_step}/{total_steps}: 正在提交更改并获取行数...")
            cur.execute(psql.SQL("DROP TABLE IF EXISTS {temp_table_ident};").format(temp_table_ident=first_icu_stays_temp_ident))
            cur.execute(psql.SQL("DROP TABLE IF EXISTS {temp_table_ident};").format(temp_table_ident=selected_event_ad_temp_ident))
            conn.commit()
            self.log.emit("更改已成功提交。")

            cur.execute(psql.SQL("SELECT COUNT(*) FROM {}").format(target_table_ident))
            count = cur.fetchone()[0]
            self.progress.emit(current_step, total_steps)
            self.finished.emit(self.target_table_name_str, count)

        except InterruptedError: 
            if conn: conn.rollback()
            self.log.emit("队列创建操作被用户取消。")
            self.error.emit("操作已取消")
        except (Exception, psycopg2.Error) as error:
            if conn: conn.rollback()
            err_msg = f"创建队列数据表时出错: {error}\n{traceback.format_exc()}"
            self.log.emit(err_msg)
            self.error.emit(err_msg)
        finally:
            if conn:
                self.log.emit("关闭数据库连接。")
                conn.close()


class QueryCohortTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.last_query_condition_template = None
        self.last_query_params = None
        self.cohort_worker_thread = None
        self.cohort_worker = None
        self.current_mode_key = MODE_DISEASE_KEY # Default
        self.init_ui()
        # self.on_mode_changed() # 会在 init_ui 中通过 setChecked 触发，或在最后显式调用

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        controls_and_preview_widget = QWidget()
        controls_and_preview_layout = QVBoxLayout(controls_and_preview_widget)
        splitter.addWidget(controls_and_preview_widget)

        mode_selection_groupbox = QGroupBox("队列筛选模式")
        mode_layout = QHBoxLayout()
        self.mode_selection_group = QButtonGroup(self)
        self.rb_mode_disease = QRadioButton("按疾病ICD筛选")
        self.rb_mode_procedure = QRadioButton("按手术/操作ICD筛选")
        self.mode_selection_group.addButton(self.rb_mode_disease, 1)
        self.mode_selection_group.addButton(self.rb_mode_procedure, 2)
        # self.rb_mode_disease.setChecked(True) # 移到 init_ui 末尾或通过 on_mode_changed 初始调用
        mode_layout.addWidget(self.rb_mode_disease); mode_layout.addWidget(self.rb_mode_procedure)
        mode_selection_groupbox.setLayout(mode_layout)
        controls_and_preview_layout.addWidget(mode_selection_groupbox)

        self.rb_mode_disease.toggled.connect(self._on_mode_radio_toggled) # 连接新的槽函数

        instruction_label = QLabel("使用下方条件组构建筛选条件 (主要针对\"标题/描述\"列):")
        controls_and_preview_layout.addWidget(instruction_label)
        
        self.condition_group = ConditionGroupWidget(is_root=True) 
        self.condition_group.condition_changed.connect(self.update_button_states)
        
        cg_scroll_area = QScrollArea()
        cg_scroll_area.setWidgetResizable(True)
        cg_scroll_area.setWidget(self.condition_group)
        cg_scroll_area.setMinimumHeight(200) # 至少保证能看到几行
        controls_and_preview_layout.addWidget(cg_scroll_area)

        cohort_type_layout = QHBoxLayout()
        self.admission_type_label = QLabel("选择入院类型:")
        cohort_type_layout.addWidget(self.admission_type_label)
        self.admission_type_combo = QComboBox()
        cohort_type_layout.addWidget(self.admission_type_combo); cohort_type_layout.addStretch()
        controls_and_preview_layout.addLayout(cohort_type_layout)

        btn_layout = QHBoxLayout()
        self.query_btn = QPushButton("查询ICD")
        self.query_btn.clicked.connect(self.execute_query); self.query_btn.setEnabled(False)
        btn_layout.addWidget(self.query_btn)
        self.preview_btn = QPushButton("预览查询SQL")
        self.preview_btn.clicked.connect(self.preview_sql_action); self.preview_btn.setEnabled(False)
        btn_layout.addWidget(self.preview_btn)
        self.create_table_btn = QPushButton("创建目标队列数据表")
        self.create_table_btn.clicked.connect(self.create_cohort_table_with_preview); self.create_table_btn.setEnabled(False)
        btn_layout.addWidget(self.create_table_btn)
        controls_and_preview_layout.addLayout(btn_layout)

        self.cohort_creation_status_group = QGroupBox("队列创建状态")
        cohort_status_layout = QVBoxLayout(self.cohort_creation_status_group)
        self.cohort_creation_progress = QProgressBar(); self.cohort_creation_progress.setRange(0, 6); self.cohort_creation_progress.setValue(0)
        cohort_status_layout.addWidget(self.cohort_creation_progress)
        self.cohort_creation_log = QTextEdit(); self.cohort_creation_log.setReadOnly(True); self.cohort_creation_log.setMaximumHeight(100)
        cohort_status_layout.addWidget(self.cohort_creation_log)
        self.cohort_creation_status_group.setVisible(False)
        controls_and_preview_layout.addWidget(self.cohort_creation_status_group)

        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(150)
        controls_and_preview_layout.addWidget(self.sql_preview)

        result_display_widget = QWidget()
        result_display_layout = QVBoxLayout(result_display_widget)
        self.table_content_label = QLabel("当前表格内容: ICD/Procedure代码查询结果")
        result_display_layout.addWidget(self.table_content_label)
        self.result_table = QTableWidget(); self.result_table.setAlternatingRowColors(True)
        result_display_layout.addWidget(self.result_table)
        splitter.addWidget(result_display_widget)
        
        splitter.setSizes([controls_and_preview_widget.sizeHint().height() + 50, 250]) # 尝试动态调整
        
        self.rb_mode_disease.setChecked(True) # 在UI元素都创建完毕后设置，确保 on_mode_changed 能正确工作
        # self.update_button_states() # on_mode_changed会调用它

    def on_db_connected(self):
        self.update_button_states()

    @Slot(bool)
    def _on_mode_radio_toggled(self, checked):
        if checked: # 只有当按钮被选中时才执行
            self.on_mode_changed()

    def on_mode_changed(self):
        self.result_table.setRowCount(0)
        self.sql_preview.clear()
        self.last_query_condition_template = None
        self.last_query_params = None
        self.table_content_label.setText("当前表格内容: ICD/Procedure代码查询结果")

        current_admission_type_key = None
        if self.admission_type_combo.count() > 0:
            current_admission_type_key = self.admission_type_combo.currentData()
        self.admission_type_combo.clear()

        available_fields_for_cg = [] # (db_col_name, display_name)

        if self.rb_mode_disease.isChecked():
            self.current_mode_key = MODE_DISEASE_KEY
            # if hasattr(self, 'condition_group'): self.condition_group.set_search_field("long_title") # 旧逻辑
            self.query_btn.setText("查询疾病ICD")
            self.preview_btn.setText("预览疾病查询SQL")
            self.admission_type_label.setText("选择疾病入院类型:")
            self.admission_type_combo.addItem(COHORT_TYPE_FIRST_EVENT_STR.replace("事件", "诊断"), COHORT_TYPE_FIRST_EVENT_KEY)
            self.admission_type_combo.addItem(COHORT_TYPE_ALL_EVENTS_STR.replace("事件", "诊断"), COHORT_TYPE_ALL_EVENTS_KEY)
            self.dict_table_for_query = "mimiciv_hosp.d_icd_diagnoses"
            self.dict_code_col_for_query = "icd_code"
            self.dict_title_col_for_query = "long_title"
            available_fields_for_cg = [("long_title", "标题/描述")] # 为ConditionGroupWidget设置可用字段
        
        elif self.rb_mode_procedure.isChecked():
            self.current_mode_key = MODE_PROCEDURE_KEY
            # if hasattr(self, 'condition_group'): self.condition_group.set_search_field("long_title") # 旧逻辑
            self.query_btn.setText("查询手术/操作ICD")
            self.preview_btn.setText("预览手术查询SQL")
            self.admission_type_label.setText("选择手术/操作入院类型:")
            self.admission_type_combo.addItem(COHORT_TYPE_FIRST_EVENT_STR.replace("事件", "操作"), COHORT_TYPE_FIRST_EVENT_KEY)
            self.admission_type_combo.addItem(COHORT_TYPE_ALL_EVENTS_STR.replace("事件", "操作"), COHORT_TYPE_ALL_EVENTS_KEY)
            self.dict_table_for_query = "mimiciv_hosp.d_icd_procedures"
            self.dict_code_col_for_query = "icd_code"
            self.dict_title_col_for_query = "long_title"
            available_fields_for_cg = [("long_title", "标题/描述")] # 为ConditionGroupWidget设置可用字段

        # 更新 ConditionGroupWidget 的可用字段并清空
        if hasattr(self, 'condition_group'):
            self.condition_group.set_available_search_fields(available_fields_for_cg)
            self.condition_group.clear_all() # 清空条件，因为可用字段列表或其上下文已改变

        if current_admission_type_key:
            idx = self.admission_type_combo.findData(current_admission_type_key)
            self.admission_type_combo.setCurrentIndex(idx if idx != -1 else 0)
        elif self.admission_type_combo.count() > 0:
            self.admission_type_combo.setCurrentIndex(0)
        
        self.update_button_states() # 更新按钮状态

    def prepare_for_cohort_creation(self, starting=True):
        self.cohort_creation_status_group.setVisible(starting)
        if starting:
            self.cohort_creation_progress.setValue(0)
            self.cohort_creation_log.clear()
            self.update_cohort_creation_log("开始创建队列...")
        
        is_enabled = not starting
        # 更新按钮的启用状态，依赖于 update_button_states 的逻辑
        self.condition_group.setEnabled(is_enabled)
        self.admission_type_combo.setEnabled(is_enabled)
        self.rb_mode_disease.setEnabled(is_enabled)
        self.rb_mode_procedure.setEnabled(is_enabled)
        
        if not starting: # 操作结束
            self.cohort_worker = None
            self.cohort_worker_thread = None
        
        # 总是调用 update_button_states 来决定按钮的最终状态
        self.update_button_states()


    def update_cohort_creation_progress(self, value, max_value):
        if self.cohort_creation_progress.maximum() != max_value: self.cohort_creation_progress.setMaximum(max_value)
        self.cohort_creation_progress.setValue(value)

    def update_cohort_creation_log(self, message):
        self.cohort_creation_log.append(message); QApplication.processEvents()

    def _can_create_table_check(self):
        # 创建表的条件：有条件输入，并且上次查询的模板是有效的（非空，非"1=1"）
        # 注意：self.last_query_condition_template 在 execute_query 中被设置
        return (self.condition_group.has_valid_input() and
                self.last_query_condition_template and 
                self.last_query_condition_template.strip() and 
                self.last_query_condition_template.lower() != "true" and # "TRUE"也可能是无意义的
                self.last_query_condition_template != "1=1")


    def update_button_states(self):
        db_connected = bool(self.get_db_params())
        has_valid_conditions = self.condition_group.has_valid_input()
        is_worker_running = self.cohort_worker_thread is not None and self.cohort_worker_thread.isRunning()

        self.query_btn.setEnabled(db_connected and has_valid_conditions and not is_worker_running)
        self.preview_btn.setEnabled(db_connected and has_valid_conditions and not is_worker_running)
        
        # 创建表的按钮依赖于 _can_create_table_check 的结果
        can_create = db_connected and self._can_create_table_check() and not is_worker_running
        self.create_table_btn.setEnabled(can_create)

        if not has_valid_conditions and not is_worker_running:
            self.sql_preview.setPlaceholderText("在此处将显示生成的SQL语句预览...")
            # 如果没有有效条件，确保 last_query_condition_template 不会残留旧值导致创建按钮误判
            # 但也不应该在这里随意清空它，它只应在 execute_query 时更新
            if not has_valid_conditions:
                 self.create_table_btn.setEnabled(False) # 再次确保


    def _build_query_parts(self):
        condition_template, params = self.condition_group.get_condition()
        # 注意：这里的 dict_code_col_for_query 和 dict_title_col_for_query 是固定的
        # 因为这个查询总是针对字典表（d_icd_diagnoses 或 d_icd_procedures）的特定列
        base_query = psql.SQL("SELECT {code_col}, {title_col} FROM {dict_table}").format(
            code_col=psql.Identifier(self.dict_code_col_for_query),
            title_col=psql.Identifier(self.dict_title_col_for_query),
            dict_table=psql.SQL(self.dict_table_for_query) 
        )
        if condition_template:
            full_query = psql.SQL("{base} WHERE {condition}").format(
                base=base_query, 
                condition=psql.SQL(condition_template) # ConditionGroupWidget 返回的已经是 WHERE 之后的部分
            )
        else:
            full_query = base_query # 如果没有条件，查询整个表 (通常应有LIMIT)
            params = []
        return full_query, params

    def preview_sql_action(self):
        query_obj, params = self._build_query_parts()
        preview_sql_filled = "" 
        query_type_str = "疾病ICD" if self.current_mode_key == MODE_DISEASE_KEY else "手术/操作ICD"
        query_template_for_mogrify = ""
        db_params = self.get_db_params()

        if not db_params:
            try: # 尝试获取模板字符串，即使没有真实连接
                dummy_conn = psycopg2.connect("dbname=dummy user=dummy") # 更完整的虚拟连接字符串
                query_template_for_mogrify = query_obj.as_string(dummy_conn) 
                dummy_conn.close()
            except: query_template_for_mogrify = str(query_obj) 
            self.sql_preview.setText(f"SQL Template ({query_type_str} 查询):\n{query_template_for_mogrify}\n\nParameters:\n{params}\n\n(无法连接数据库以生成完整预览)")
            return

        temp_conn_for_preview = None
        try:
            temp_conn_for_preview = psycopg2.connect(**db_params)
            query_template_for_mogrify = query_obj.as_string(temp_conn_for_preview) 
            if params:
                preview_sql_filled = temp_conn_for_preview.cursor().mogrify(query_template_for_mogrify, params).decode(temp_conn_for_preview.encoding or 'utf-8')
                self.sql_preview.setText(f"-- SQL Preview ({query_type_str} 查询, 带参数预览):\n{preview_sql_filled}\n\n-- Note: Actual execution uses server-side parameter binding.")
            else:
                self.sql_preview.setText(f"-- SQL Preview ({query_type_str} 查询, 无参数):\n{query_template_for_mogrify}")
        except (Exception, psycopg2.Error) as e:
            print(f"Error creating preview SQL: {e}")
            try:
                dummy_conn = psycopg2.connect("dbname=dummy user=dummy")
                fallback_template = query_obj.as_string(dummy_conn)
                dummy_conn.close()
            except: fallback_template = str(query_obj)
            self.sql_preview.setText(f"SQL Template ({query_type_str} 查询):\n{fallback_template}\n\nParameters:\n{params}\n\n(生成预览时出错: {e})")
        finally:
            if temp_conn_for_preview:
                temp_conn_for_preview.close()

    def execute_query(self):
        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库"); return
        
        query_obj, params = self._build_query_parts()
        
        # 关键：在实际执行查询前，从 condition_group 获取最新的条件模板和参数
        # 并将它们存储起来，供创建队列时使用。
        self.last_query_condition_template, self.last_query_params = self.condition_group.get_condition()
        if not self.last_query_condition_template and self.condition_group.has_valid_input():
             # 如果有输入但 get_condition() 返回空（可能是一个空的根组但有子组未输入），
             # 可以设为一个无害的条件，或者依赖按钮状态来阻止。
             # 这里假设如果 has_valid_input 为 true，get_condition 会返回一些东西。
             # 如果 get_condition 返回空字符串，表示没有有效的条件可以形成。
             # self.last_query_condition_template = "TRUE" # 或者 "1=1"
             pass # 依赖 update_button_states 中 _can_create_table_check 的判断

        query_type_str = "疾病ICD" if self.current_mode_key == MODE_DISEASE_KEY else "手术/操作ICD"
        self.table_content_label.setText(f"当前表格内容: {query_type_str} 代码查询结果")
        self.preview_sql_action() # 更新SQL预览区以反映将要执行的查询
        
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            # 使用从 _build_query_parts 获取的 query_obj 和 params 执行
            cur.execute(query_obj, params)
            columns = [desc[0] for desc in cur.description]; rows = cur.fetchall()
            
            self.result_table.setRowCount(0)
            self.result_table.setColumnCount(len(columns))
            self.result_table.setHorizontalHeaderLabels(columns)
            for i, row_data in enumerate(rows):
                self.result_table.insertRow(i)
                for j, value in enumerate(row_data):
                    self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
            self.result_table.resizeColumnsToContents()
            QMessageBox.information(self, "查询完成", f"共找到 {len(rows)} 条 {query_type_str} 记录")
        except (Exception, psycopg2.Error) as error:
            QMessageBox.critical(self, "查询失败", f"无法执行 {query_type_str} 查询: {error}\n{traceback.format_exc()}")
            self.last_query_condition_template = None # 查询失败，清除上次条件
            self.last_query_params = None
        finally:
            if conn: conn.close()
            self.update_button_states() # 查询后更新按钮状态（特别是创建表按钮）

    def _get_source_mode_details(self):
        if self.current_mode_key == MODE_DISEASE_KEY:
            return {
                "source_type": MODE_DISEASE_KEY, "event_table": "mimiciv_hosp.diagnoses_icd",
                "dictionary_table": "mimiciv_hosp.d_icd_diagnoses", "event_icd_col": "icd_code",
                "dict_icd_col": "icd_code", "dict_title_col": "long_title",
                "event_seq_num_col": "seq_num", "event_time_col": None
            }
        elif self.current_mode_key == MODE_PROCEDURE_KEY:
            return {
                "source_type": MODE_PROCEDURE_KEY, "event_table": "mimiciv_hosp.procedures_icd",
                "dictionary_table": "mimiciv_hosp.d_icd_procedures", "event_icd_col": "icd_code",
                "dict_icd_col": "icd_code", "dict_title_col": "long_title",
                "event_seq_num_col": "seq_num", "event_time_col": "chartdate"
            }
        return None

    def _generate_cohort_creation_sql_preview(self, target_table_name_str,
                                             condition_sql_template_str, condition_params_list,
                                             admission_cohort_type, source_mode_details):
        # (内容不变)
        db_params = self.get_db_params()
        if not db_params: return False, "-- 数据库未连接，无法生成队列创建SQL预览。--"
        event_table_full = source_mode_details["event_table"]; dict_table_full = source_mode_details["dictionary_table"]
        event_code_col = source_mode_details["event_icd_col"]; dict_code_col = source_mode_details["dict_icd_col"]
        event_seq_num_col = source_mode_details["event_seq_num_col"]; event_time_col = source_mode_details.get("event_time_col")
        event_source_type_str = source_mode_details["source_type"]
        event_table_ident = psql.Identifier(*event_table_full.split('.')); dict_table_ident = psql.Identifier(*dict_table_full.split('.'))
        event_code_col_ident = psql.Identifier(event_code_col); dict_code_col_ident = psql.Identifier(dict_code_col)
        event_seq_num_col_ident = psql.Identifier(event_seq_num_col); event_time_col_ident = psql.Identifier(event_time_col) if event_time_col else None
        conn = None
        try:
            conn = psycopg2.connect(**db_params); cur = conn.cursor()
            target_ident = psql.Identifier('mimiciv_data', target_table_name_str)
            selected_event_ad_temp_ident = psql.Identifier('selected_event_ad_temp_cohort_q')
            first_icu_stays_temp_ident = psql.Identifier('first_icu_stays_temp_cohort_q')
            base_event_select_preview_list = [ psql.SQL("e.subject_id"), psql.SQL("e.hadm_id"), psql.SQL("adm.admittime"), psql.SQL("e.{event_code_col} AS qualifying_event_code").format(event_code_col=event_code_col_ident), psql.SQL("e.icd_version AS qualifying_event_icd_version"), psql.SQL("dd.long_title AS qualifying_event_title"), psql.SQL("e.{event_seq_num_col} AS qualifying_event_seq_num").format(event_seq_num_col=event_seq_num_col_ident) ]
            if event_time_col_ident: base_event_select_preview_list.append(psql.SQL("e.{} AS qualifying_event_time").format(event_time_col_ident))
            base_event_select_sql_preview = psql.SQL("SELECT {select_cols} FROM {event_table} e JOIN {dict_table} dd ON e.{event_code_col} = dd.{dict_code_col} AND e.icd_version = dd.icd_version JOIN mimiciv_hosp.admissions adm ON e.hadm_id = adm.hadm_id WHERE ({condition_template_placeholder})").format(select_cols=psql.SQL(", ").join(base_event_select_preview_list), event_table=event_table_ident, dict_table=dict_table_ident, event_code_col=event_code_col_ident, dict_code_col=dict_code_col_ident, condition_template_placeholder=psql.SQL(condition_sql_template_str))
            if admission_cohort_type == COHORT_TYPE_FIRST_EVENT_KEY:
                order_by_list_preview = [psql.SQL("base.admittime ASC"), psql.SQL("base.hadm_id ASC")]
                if event_time_col_ident and source_mode_details['source_type'] == MODE_PROCEDURE_KEY: order_by_list_preview.append(psql.SQL("base.qualifying_event_time ASC NULLS LAST"))
                order_by_list_preview.append(psql.SQL("base.qualifying_event_seq_num ASC"))
                create_event_temp_sql_obj = psql.SQL("DROP TABLE IF EXISTS {temp_table_ident}; CREATE TEMPORARY TABLE {temp_table_ident} AS SELECT * FROM (SELECT base.*, ROW_NUMBER() OVER(PARTITION BY base.subject_id ORDER BY {order_by_clause}) AS admission_rank_for_event FROM ({base_select_preview_inner}) AS base) ranked_base WHERE ranked_base.admission_rank_for_event = 1;").format(temp_table_ident=selected_event_ad_temp_ident, base_select_preview_inner=base_event_select_sql_preview, order_by_clause=psql.SQL(', ').join(order_by_list_preview))
            elif admission_cohort_type == COHORT_TYPE_ALL_EVENTS_KEY:
                create_event_temp_sql_obj = psql.SQL("DROP TABLE IF EXISTS {temp_table_ident}; CREATE TEMPORARY TABLE {temp_table_ident} AS ({base_select_preview});").format(temp_table_ident=selected_event_ad_temp_ident, base_select_preview=base_event_select_sql_preview)
            else: return False, f"-- 未知的入院类型: {admission_cohort_type} --"
            readable_sql1 = cur.mogrify(create_event_temp_sql_obj.as_string(conn), condition_params_list).decode(conn.encoding or 'utf-8')
            create_first_icu_sql_obj = psql.SQL("DROP TABLE IF EXISTS {temp_table_ident}; CREATE TEMPORARY TABLE {temp_table_ident} AS (SELECT * FROM (SELECT icu.subject_id, icu.hadm_id, icu.stay_id, icu.intime AS icu_intime, icu.outtime AS icu_outtime, EXTRACT(EPOCH FROM (icu.outtime - icu.intime)) / 3600.0 AS los_icu_hours, ROW_NUMBER() OVER (PARTITION BY icu.hadm_id ORDER BY icu.intime ASC, icu.stay_id ASC) AS icu_stay_rank_in_admission FROM mimiciv_icu.icustays icu WHERE EXISTS (SELECT 1 FROM {selected_event_temp_table} seat WHERE seat.hadm_id = icu.hadm_id)) sub WHERE icu_stay_rank_in_admission = 1);").format(temp_table_ident=first_icu_stays_temp_ident, selected_event_temp_table=selected_event_ad_temp_ident)
            readable_sql2 = create_first_icu_sql_obj.as_string(conn)
            target_select_list_preview = [ psql.SQL("evt_ad.subject_id"), psql.SQL("evt_ad.hadm_id"), psql.SQL("evt_ad.admittime"), psql.SQL("adm.dischtime"), psql.SQL("icu.stay_id"), psql.SQL("icu.icu_intime"), psql.SQL("icu.icu_outtime"), psql.SQL("icu.los_icu_hours"), psql.SQL("evt_ad.qualifying_event_code"), psql.SQL("evt_ad.qualifying_event_icd_version"), psql.SQL("CAST({source_literal} AS VARCHAR(20)) AS {col_alias}").format(source_literal=psql.Literal(event_source_type_str), col_alias=psql.Identifier("qualifying_event_source")), psql.SQL("evt_ad.qualifying_event_title"), psql.SQL("evt_ad.qualifying_event_seq_num")]
            if event_time_col_ident: target_select_list_preview.append(psql.SQL("evt_ad.qualifying_event_time"))
            main_diag_join_sql_preview = psql.SQL("")
            if event_source_type_str == MODE_PROCEDURE_KEY:
                target_select_list_preview.extend([ psql.SQL("primary_dx.icd_code AS primary_diag_code"), psql.SQL("primary_dx.icd_version AS primary_diag_icd_version"), psql.SQL("primary_d_dx.long_title AS primary_diag_title")])
                main_diag_join_sql_preview = psql.SQL(" LEFT JOIN ( SELECT dx.hadm_id, dx.icd_code, dx.icd_version, dx.seq_num FROM mimiciv_hosp.diagnoses_icd dx WHERE dx.seq_num = 1 ) primary_dx ON evt_ad.hadm_id = primary_dx.hadm_id LEFT JOIN mimiciv_hosp.d_icd_diagnoses primary_d_dx ON primary_dx.icd_code = primary_d_dx.icd_code AND primary_dx.icd_version = primary_d_dx.icd_version ")
            create_target_sql_obj = psql.SQL("DROP TABLE IF EXISTS {target_ident}; CREATE TABLE {target_ident} AS (SELECT {select_cols} FROM {event_ad_temp_table} evt_ad JOIN mimiciv_hosp.admissions adm ON evt_ad.hadm_id = adm.hadm_id LEFT JOIN {icu_temp_table} icu ON evt_ad.hadm_id = icu.hadm_id {main_diag_join});").format(target_ident=target_ident, select_cols=psql.SQL(', ').join(target_select_list_preview), event_ad_temp_table=selected_event_ad_temp_ident, icu_temp_table=first_icu_stays_temp_ident, main_diag_join=main_diag_join_sql_preview)
            readable_sql3 = create_target_sql_obj.as_string(conn)
            index_preview_str = f"-- Followed by CREATE INDEX statements on {target_table_name_str}...\n"; schema_sql_str = "CREATE SCHEMA IF NOT EXISTS mimiciv_data;\n"
            full_preview = (f"-- ===== Cohort Creation SQL Preview =====\n\n-- Cohort Source: {source_mode_details['source_type']}\n-- Admission Type: {self.admission_type_combo.currentText()}\n-- Target Table: {target_table_name_str}\n\n-- Step 0: Ensure schema exists --\n{schema_sql_str}\n-- Step 1: Create temporary table for selected event admissions --\n{readable_sql1}\n\n-- Step 2: Create temporary table for first ICU stays --\n{readable_sql2}\n\n-- Step 3: Create final cohort table '{target_table_name_str}' --\n{readable_sql3}\n\n-- Step 4: Create indexes --\n{index_preview_str}\n-- Step 5: Clean up --\n-- ===================================== --")
            return True, full_preview
        except (Exception, psycopg2.Error) as e: return False, f"-- Error generating preview: {e}\n{traceback.format_exc()} --"
        finally:
            if conn: conn.close()


    def create_cohort_table_with_preview(self): # (逻辑基本不变)
        if not self.last_query_condition_template or not self.condition_group.has_valid_input() or self.last_query_condition_template.strip() == "" or self.last_query_condition_template.lower() == "true" or self.last_query_condition_template == "1=1":
            QMessageBox.warning(self, "缺少有效查询条件", "请先通过“查询ICD”或“预览查询SQL”生成一个有效的、非空的查询条件。")
            self.sql_preview.setText("-- 无法创建队列：缺少有效的、具体的查询条件。")
            return
        
        raw_cohort_identifier, ok = self.get_cohort_identifier_name()
        if not ok or not raw_cohort_identifier:
            self.sql_preview.setText("-- 队列创建已取消：未提供队列标识符。"); return
        
        cleaned_identifier = re.sub(r'[^a-z0-9_]+', '_', raw_cohort_identifier.lower()).strip('_')
        if not cleaned_identifier or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', cleaned_identifier):
            QMessageBox.warning(self, "名称格式错误", f"队列标识符 '{raw_cohort_identifier}' -> '{cleaned_identifier}' 不符合规则。"); self.sql_preview.setText(f"-- 队列创建失败：标识符 '{cleaned_identifier}' 格式错误。"); return
        
        selected_admission_type_key = self.admission_type_combo.currentData()
        table_prefix = "first_" if selected_admission_type_key == COHORT_TYPE_FIRST_EVENT_KEY else "all_"
        source_prefix = "dis_" if self.current_mode_key == MODE_DISEASE_KEY else "proc_"
        target_table_name_str = f"{table_prefix}{source_prefix}{cleaned_identifier}_admissions"
        if len(target_table_name_str) > 63:
             QMessageBox.warning(self, "名称过长", f"生成的表名 '{target_table_name_str}' 过长。请缩短队列标识符。"); self.sql_preview.setText(f"-- 队列创建失败：表名 '{target_table_name_str}' 过长。"); return
        
        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库"); self.sql_preview.setText("-- 队列创建失败：数据库未连接。"); return
        
        current_source_mode_details = self._get_source_mode_details()
        if not current_source_mode_details: QMessageBox.critical(self, "内部错误", "无法确定当前的筛选模式详情。"); return
        
        # 使用 self.last_query_condition_template 和 self.last_query_params
        success, preview_sql_str = self._generate_cohort_creation_sql_preview(
            target_table_name_str, self.last_query_condition_template, self.last_query_params,
            selected_admission_type_key, current_source_mode_details)
        
        self.sql_preview.setText(preview_sql_str); QApplication.processEvents()
        if not success: 
            QMessageBox.critical(self, "预览失败", "无法生成队列创建SQL的预览。\n请查看SQL预览区域的错误信息。"); return
        
        mode_text = "疾病ICD" if self.current_mode_key == MODE_DISEASE_KEY else "手术/操作ICD"
        reply = QMessageBox.question(self, '确认创建队列', 
                                     f"将要创建基于 '{mode_text}' 的队列数据表 '{target_table_name_str}'.\n"
                                     f"入院类型: {self.admission_type_combo.currentText()}.\n"
                                     "请检查SQL预览区域显示的语句。\n\n确定要继续吗?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, 
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No: 
            self.sql_preview.append("\n\n-- 用户取消了队列创建操作。 --")
            self.update_button_states()
            return
        
        self.prepare_for_cohort_creation(True)
        self.cohort_worker = CohortCreationWorker(db_params, target_table_name_str, 
                                                self.last_query_condition_template, self.last_query_params, 
                                                selected_admission_type_key, current_source_mode_details)
        self.cohort_worker_thread = QThread()
        self.cohort_worker.moveToThread(self.cohort_worker_thread)
        self.cohort_worker_thread.started.connect(self.cohort_worker.run)
        self.cohort_worker.finished.connect(self.on_cohort_creation_finished)
        self.cohort_worker.error.connect(self.on_cohort_creation_error)
        self.cohort_worker.progress.connect(self.update_cohort_creation_progress)
        self.cohort_worker.log.connect(self.update_cohort_creation_log)
        
        self.cohort_worker.finished.connect(self.cohort_worker_thread.quit)
        self.cohort_worker.error.connect(self.cohort_worker_thread.quit)
        self.cohort_worker_thread.finished.connect(self.cohort_worker_thread.deleteLater)
        self.cohort_worker.finished.connect(lambda: setattr(self, 'cohort_worker', None)) # Clean up worker
        self.cohort_worker.error.connect(lambda: setattr(self, 'cohort_worker', None)) # Clean up worker
        self.cohort_worker_thread.start()

    @Slot(str, int)
    def on_cohort_creation_finished(self, table_name, count):
        self.update_cohort_creation_log(f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        QMessageBox.information(self, "创建成功", f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        self.prepare_for_cohort_creation(False)
        self.preview_created_cohort_table('mimiciv_data', table_name)

    @Slot(str)
    def on_cohort_creation_error(self, error_message):
        self.update_cohort_creation_log(f"队列创建失败: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "创建失败", f"无法创建队列数据表: {error_message}")
        else: 
            QMessageBox.information(self, "操作取消", "队列创建操作已取消。")
        self.prepare_for_cohort_creation(False)

    def preview_created_cohort_table(self, schema_name, table_name): # (内容不变)
        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "预览失败", "数据库未连接。"); return
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            table_identifier = psql.Identifier(schema_name, table_name)
            preview_query = psql.SQL("SELECT * FROM {} ORDER BY subject_id, hadm_id LIMIT 100;").format(table_identifier)
            self.sql_preview.append(f"\n-- 队列表预览SQL:\n{preview_query.as_string(conn)}")
            cur.execute(preview_query)
            columns = [desc[0] for desc in cur.description]; rows = cur.fetchall()
            self.result_table.setRowCount(0); self.result_table.setColumnCount(len(columns))
            self.result_table.setHorizontalHeaderLabels(columns)
            for i, row_data in enumerate(rows):
                self.result_table.insertRow(i)
                for j, value in enumerate(row_data):
                    self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
            self.result_table.resizeColumnsToContents()
            self.table_content_label.setText(f"当前表格内容: 队列表 '{schema_name}.{table_name}' 预览 (前100行)")
            QMessageBox.information(self, "队列表预览", f"已加载队列表 '{table_name}' 的预览数据。")
        except (Exception, psycopg2.Error) as error:
            QMessageBox.critical(self, "队列表预览失败", f"无法预览队列表 '{table_name}': {error}")
            self.table_content_label.setText(f"当前表格内容: 无法加载队列表 '{table_name}' 预览")
        finally:
            if conn: conn.close()


    def get_cohort_identifier_name(self): # (内容不变)
        dialog = QDialog(self); dialog.setWindowTitle("输入队列基础标识符")
        layout = QVBoxLayout(dialog); form_layout = QFormLayout(); name_input = QLineEdit()
        form_layout.addRow("队列基础标识符 (英文,数字,下划线):", name_input); layout.addLayout(form_layout)
        info_label = QLabel("注意: 此标识符将用于构成数据库表名...\n只能包含英文字母、数字和下划线，且必须以字母或下划线开头。")
        info_label.setWordWrap(True); layout.addWidget(info_label)
        btn_layout = QHBoxLayout(); ok_btn = QPushButton("确定"); cancel_btn = QPushButton("取消")
        btn_layout.addWidget(ok_btn); btn_layout.addWidget(cancel_btn); layout.addLayout(btn_layout)
        ok_btn.clicked.connect(dialog.accept); cancel_btn.clicked.connect(dialog.reject)
        result = dialog.exec_(); return name_input.text().strip(), result == QDialog.Accepted

    def closeEvent(self, event): # (内容不变)
        if self.cohort_worker_thread and self.cohort_worker_thread.isRunning():
            if self.cohort_worker: self.cohort_worker.cancel()
            self.cohort_worker_thread.quit()
            if not self.cohort_worker_thread.wait(1000): print("Warning: Cohort creation thread did not quit in time.")
        super().closeEvent(event)

# --- END OF FILE tab_query_cohort.py ---