# --- START OF FILE tab_query_cohort.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QDialog, QLineEdit, QFormLayout,
                          QApplication, QProgressBar, QGroupBox, QComboBox,
                          QRadioButton, QButtonGroup) # Added QRadioButton, QButtonGroup
from PySide6.QtCore import Qt, Signal, QObject, QThread, Slot
import psycopg2
from psycopg2 import sql as psql
import re
import time
import traceback
from conditiongroup import ConditionGroupWidget # Assuming ConditionGroupWidget has set_search_field method

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
        self.admission_cohort_type = admission_cohort_type # e.g., first_event_admission
        self.source_mode_details = source_mode_details # Dict with event_table, dict_table, etc.
        self.is_cancelled = False

    def cancel(self):
        self.log.emit("队列创建操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn = None
        total_steps = 6
        current_step = 0

        # Unpack source_mode_details for easier access
        event_table_full_name = self.source_mode_details["event_table"] # e.g., "mimiciv_hosp.diagnoses_icd"
        dictionary_table_full_name = self.source_mode_details["dictionary_table"]
        event_icd_col = self.source_mode_details["event_icd_col"]
        dict_icd_col = self.source_mode_details["dict_icd_col"]
        # dict_title_col = self.source_mode_details["dict_title_col"] # For display in target table
        event_seq_num_col = self.source_mode_details["event_seq_num_col"]
        event_time_col = self.source_mode_details.get("event_time_col") # Optional, e.g., chartdate for procedures

        event_table_schema, event_table_name_only = event_table_full_name.split('.')
        event_table_ident = psql.Identifier(event_table_schema, event_table_name_only)

        dict_table_schema, dict_table_name_only = dictionary_table_full_name.split('.')
        dict_table_ident = psql.Identifier(dict_table_schema, dict_table_name_only)

        event_icd_col_ident = psql.Identifier(event_icd_col)
        dict_icd_col_ident = psql.Identifier(dict_icd_col)
        # dict_title_col_ident = psql.Identifier(dict_title_col)
        event_seq_num_col_ident = psql.Identifier(event_seq_num_col)
        event_time_col_ident = psql.Identifier(event_time_col) if event_time_col else None


        try:
            self.log.emit(f"开始创建队列数据表: {self.target_table_name_str} (类型: {self.admission_cohort_type}, 来源: {self.source_mode_details['source_type']})...")
            self.progress.emit(current_step, total_steps)

            self.log.emit("连接数据库...")
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            conn.autocommit = False
            self.log.emit("数据库已连接。")

            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 确保 'mimiciv_data' schema 存在...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS mimiciv_data;")
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            target_table_ident = psql.Identifier('mimiciv_data', self.target_table_name_str)
            # This temp table holds admissions that meet the event criteria
            selected_event_ad_temp_ident = psql.Identifier('selected_event_ad_temp_cohort_q')
            first_icu_stays_temp_ident = psql.Identifier('first_icu_stays_temp_cohort_q')

            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建临时表 (符合事件条件的入院记录)...")

            # Base SELECT for the event temp table (without ROW_NUMBER)
            base_event_select_sql = psql.SQL("""
                SELECT e.subject_id, e.hadm_id, e.{event_icd_col} AS qualifying_event_code,
                       dd.long_title AS qualifying_event_title, e.{event_seq_num_col} AS qualifying_event_seq_num
                       {event_time_col_select}
                FROM {event_table} e
                JOIN {dict_table} dd ON e.{event_icd_col} = dd.{dict_icd_col}
                JOIN mimiciv_hosp.admissions adm ON e.hadm_id = adm.hadm_id
                WHERE ({condition_template_placeholder})
            """).format(
                event_icd_col=event_icd_col_ident,
                event_seq_num_col=event_seq_num_col_ident,
                event_time_col_select=psql.SQL(", e.{} AS qualifying_event_time").format(event_time_col_ident) if event_time_col_ident else psql.SQL(""),
                event_table=event_table_ident,
                dict_table=dict_table_ident,
                dict_icd_col=dict_icd_col_ident,
                condition_template_placeholder=psql.SQL(self.condition_sql_template)
            )

            if self.admission_cohort_type == COHORT_TYPE_FIRST_EVENT_KEY:
                order_by_list_for_rank = [
                    psql.SQL("adm.admittime ASC"),
                    psql.SQL("e.hadm_id ASC") # Tie-breaker for same admittime (rare but possible)
                ]
                if event_time_col_ident and self.source_mode_details['source_type'] == MODE_PROCEDURE_KEY: # For procedures, consider specific event time
                    order_by_list_for_rank.append(psql.SQL("e.{} ASC").format(event_time_col_ident))
                order_by_list_for_rank.append(psql.SQL("e.{} ASC").format(event_seq_num_col_ident)) # Final tie-breaker

                create_event_admission_sql = psql.SQL("""
                    DROP TABLE IF EXISTS {temp_table_ident};
                    CREATE TEMPORARY TABLE {temp_table_ident} AS (
                      SELECT * FROM (
                        SELECT base.*,
                               ROW_NUMBER() OVER(PARTITION BY base.subject_id ORDER BY {order_by_clause}) AS admission_rank_for_event
                        FROM ({base_select}) AS base
                        JOIN mimiciv_hosp.admissions adm ON base.hadm_id = adm.hadm_id -- Join admissions here for admittime
                        {join_event_table_for_ordering} -- Conditional join for event_time_col if not in base
                      ) sub WHERE admission_rank_for_event = 1);
                """).format(
                    temp_table_ident=selected_event_ad_temp_ident,
                    base_select=base_event_select_sql, # The FROM clause for base_select already joins admissions
                    order_by_clause=psql.SQL(', ').join(order_by_list_for_rank),
                    # We need to ensure 'e' alias is available if event_time_col is used in order_by.
                    # This part is tricky as base_select might not have 'e' alias if we restructure too much.
                    # Let's adjust base_select to include adm.admittime and e.chartdate for ranking
                    # And ensure the base_event_select_sql always uses 'e' for the event_table
                    join_event_table_for_ordering=psql.SQL("JOIN {event_table} e ON base.hadm_id = e.hadm_id AND base.qualifying_event_code = e.{event_icd_col} AND base.qualifying_event_seq_num = e.{event_seq_num_col}")
                                                    .format(event_table=event_table_ident, event_icd_col=event_icd_col_ident, event_seq_num_col=event_seq_num_col_ident)
                                                    if event_time_col_ident and self.source_mode_details['source_type'] == MODE_PROCEDURE_KEY else psql.SQL("")
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

            current_step += 1
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

            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建目标队列数据表 {self.target_table_name_str}...")
            target_select_list = [
                psql.SQL("evt_ad.subject_id"),
                psql.SQL("evt_ad.hadm_id"),
                psql.SQL("adm.admittime"),
                psql.SQL("adm.dischtime"),
                psql.SQL("icu.stay_id"),
                psql.SQL("icu.icu_intime"),
                psql.SQL("icu.icu_outtime"),
                psql.SQL("icu.los_icu_hours"),
                psql.SQL("evt_ad.qualifying_event_code"),
                psql.SQL("evt_ad.qualifying_event_title"),
                psql.SQL("evt_ad.qualifying_event_seq_num")
            ]
            if event_time_col_ident: # If event_time was selected into the temp table
                 target_select_list.append(psql.SQL("evt_ad.qualifying_event_time"))


            create_target_table_sql = psql.SQL("""
                DROP TABLE IF EXISTS {target_ident};
                CREATE TABLE {target_ident} AS (
                 SELECT {select_cols}
                 FROM {event_ad_temp_table} evt_ad
                 JOIN mimiciv_hosp.admissions adm ON evt_ad.hadm_id = adm.hadm_id
                 LEFT JOIN {icu_temp_table} icu ON evt_ad.hadm_id = icu.hadm_id);
            """).format(
                target_ident=target_table_ident,
                select_cols=psql.SQL(', ').join(target_select_list),
                event_ad_temp_table=selected_event_ad_temp_ident,
                icu_temp_table=first_icu_stays_temp_ident
            )
            cur.execute(create_target_table_sql)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 为表 {self.target_table_name_str} 创建索引...")
            # Index names might get long with prefix + source_type + identifier
            idx_prefix_base = f"{self.source_mode_details['source_type']}_{self.target_table_name_str.replace('first_', '').replace('all_', '').replace('_admissions', '')}"[:15]

            indexes_to_create = [
                (f"idx_{idx_prefix_base}_sub", "subject_id"),
                (f"idx_{idx_prefix_base}_hadm", "hadm_id"),
                (f"idx_{idx_prefix_base}_stay", "stay_id"),
                (f"idx_{idx_prefix_base}_admt", "admittime"),
                (f"idx_{idx_prefix_base}_icuin", "icu_intime"),
                (f"idx_{idx_prefix_base}_evcode", "qualifying_event_code")
            ]
            for index_name_str, column_name_str in indexes_to_create:
                if len(index_name_str) > 63: index_name_str = index_name_str[:63] # Ensure not too long
                index_ident = psql.Identifier(index_name_str)
                column_ident = psql.Identifier(column_name_str)
                # Check if column exists before creating index (important if qualifying_event_time is optional)
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

            current_step += 1
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


class QueryCohortTab(QWidget): # Renamed from QueryDiseaseTab
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()
        self.last_query_condition_template = None
        self.last_query_params = None
        self.cohort_worker_thread = None
        self.cohort_worker = None
        # Default to disease mode
        self.current_mode_key = MODE_DISEASE_KEY
        self.on_mode_changed() # Initialize mode-specific settings


    def init_ui(self):
        main_layout = QVBoxLayout(self)
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)

        # --- Mode Selection (Disease vs Procedure) ---
        mode_selection_groupbox = QGroupBox("队列筛选模式")
        mode_layout = QHBoxLayout()
        self.mode_selection_group = QButtonGroup(self)
        self.rb_mode_disease = QRadioButton("按疾病ICD筛选")
        self.rb_mode_procedure = QRadioButton("按手术/操作ICD筛选")
        self.mode_selection_group.addButton(self.rb_mode_disease, 1)
        self.mode_selection_group.addButton(self.rb_mode_procedure, 2)
        self.rb_mode_disease.setChecked(True)
        mode_layout.addWidget(self.rb_mode_disease)
        mode_layout.addWidget(self.rb_mode_procedure)
        mode_selection_groupbox.setLayout(mode_layout)
        top_layout.addWidget(mode_selection_groupbox)

        self.rb_mode_disease.toggled.connect(self.on_mode_changed)
        # procedure toggle also calls on_mode_changed if disease is unchecked

        instruction_label = QLabel("使用下方条件组构建筛选条件，可以添加多个关键词（可选择包含或排除）和嵌套条件组")
        top_layout.addWidget(instruction_label)
        self.condition_group = ConditionGroupWidget(is_root=True, search_field="long_title") # search_field updated in on_mode_changed
        self.condition_group.condition_changed.connect(self.update_button_states)
        top_layout.addWidget(self.condition_group)

        # --- Cohort Admission Type Selection ---
        cohort_type_layout = QHBoxLayout()
        self.admission_type_label = QLabel("选择入院类型:") # Text will be updated
        cohort_type_layout.addWidget(self.admission_type_label)
        self.admission_type_combo = QComboBox()
        # Items added in on_mode_changed
        cohort_type_layout.addWidget(self.admission_type_combo)
        cohort_type_layout.addStretch()
        top_layout.addLayout(cohort_type_layout)

        btn_layout = QHBoxLayout()
        self.query_btn = QPushButton("查询ICD"); # Text updated in on_mode_changed
        self.query_btn.clicked.connect(self.execute_query); self.query_btn.setEnabled(False)
        btn_layout.addWidget(self.query_btn)
        self.preview_btn = QPushButton("预览查询SQL"); # Text updated
        self.preview_btn.clicked.connect(self.preview_sql_action); self.preview_btn.setEnabled(False)
        btn_layout.addWidget(self.preview_btn)
        self.create_table_btn = QPushButton("创建目标队列数据表");
        self.create_table_btn.clicked.connect(self.create_cohort_table_with_preview); self.create_table_btn.setEnabled(False)
        btn_layout.addWidget(self.create_table_btn)
        top_layout.addLayout(btn_layout)

        self.cohort_creation_status_group = QGroupBox("队列创建状态")
        cohort_status_layout = QVBoxLayout(self.cohort_creation_status_group)
        self.cohort_creation_progress = QProgressBar(); self.cohort_creation_progress.setRange(0, 6); self.cohort_creation_progress.setValue(0)
        cohort_status_layout.addWidget(self.cohort_creation_progress)
        self.cohort_creation_log = QTextEdit(); self.cohort_creation_log.setReadOnly(True); self.cohort_creation_log.setMaximumHeight(100)
        cohort_status_layout.addWidget(self.cohort_creation_log)
        self.cohort_creation_status_group.setVisible(False)
        top_layout.addWidget(self.cohort_creation_status_group)

        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(150)
        top_layout.addWidget(self.sql_preview)

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(top_widget)
        self.result_table = QTableWidget(); self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)
        splitter.setSizes([700, 250]) # Adjusted for new radio buttons
        main_layout.addWidget(splitter)
        self.update_button_states()

    def on_mode_changed(self):
        self.result_table.setRowCount(0) # Clear previous results
        self.sql_preview.clear()
        self.last_query_condition_template = None
        self.last_query_params = None

        current_admission_type_key = None
        if self.admission_type_combo.count() > 0:
            current_admission_type_key = self.admission_type_combo.currentData()

        self.admission_type_combo.clear()

        if self.rb_mode_disease.isChecked():
            self.current_mode_key = MODE_DISEASE_KEY
            self.condition_group.set_search_field("long_title") # Assuming d_icd_diagnoses.long_title
            self.query_btn.setText("查询疾病ICD")
            self.preview_btn.setText("预览疾病查询SQL")
            self.admission_type_label.setText("选择疾病入院类型:")
            self.admission_type_combo.addItem(COHORT_TYPE_FIRST_EVENT_STR.replace("事件", "诊断"), COHORT_TYPE_FIRST_EVENT_KEY)
            self.admission_type_combo.addItem(COHORT_TYPE_ALL_EVENTS_STR.replace("事件", "诊断"), COHORT_TYPE_ALL_EVENTS_KEY)
            self.dict_table_for_query = "mimiciv_hosp.d_icd_diagnoses"
            self.dict_code_col_for_query = "icd_code"
            self.dict_title_col_for_query = "long_title"
        elif self.rb_mode_procedure.isChecked():
            self.current_mode_key = MODE_PROCEDURE_KEY
            self.condition_group.set_search_field("long_title") # Assuming d_icd_procedures.long_title
            self.query_btn.setText("查询手术/操作ICD")
            self.preview_btn.setText("预览手术查询SQL")
            self.admission_type_label.setText("选择手术/操作入院类型:")
            self.admission_type_combo.addItem(COHORT_TYPE_FIRST_EVENT_STR.replace("事件", "操作"), COHORT_TYPE_FIRST_EVENT_KEY)
            self.admission_type_combo.addItem(COHORT_TYPE_ALL_EVENTS_STR.replace("事件", "操作"), COHORT_TYPE_ALL_EVENTS_KEY)
            self.dict_table_for_query = "mimiciv_hosp.d_icd_procedures"
            self.dict_code_col_for_query = "icd_code" # Or specific version if needed
            self.dict_title_col_for_query = "long_title"
        else: # Should not happen with radio buttons
            self.current_mode_key = None

        if current_admission_type_key:
            idx = self.admission_type_combo.findData(current_admission_type_key)
            if idx != -1:
                self.admission_type_combo.setCurrentIndex(idx)
            elif self.admission_type_combo.count() > 0:
                 self.admission_type_combo.setCurrentIndex(0)
        elif self.admission_type_combo.count() > 0:
             self.admission_type_combo.setCurrentIndex(0)


        self.update_button_states()


    def prepare_for_cohort_creation(self, starting=True):
        if starting:
            self.cohort_creation_status_group.setVisible(True)
            self.cohort_creation_progress.setValue(0)
            self.cohort_creation_log.clear()
            self.update_cohort_creation_log("开始创建队列...")
            self.create_table_btn.setEnabled(False)
            self.query_btn.setEnabled(False)
            self.preview_btn.setEnabled(False)
            self.condition_group.setEnabled(False)
            self.admission_type_combo.setEnabled(False)
            self.rb_mode_disease.setEnabled(False)
            self.rb_mode_procedure.setEnabled(False)
        else:
            self.update_button_states() # This will re-evaluate create_table_btn
            self.query_btn.setEnabled(self.condition_group.has_valid_input())
            self.preview_btn.setEnabled(self.condition_group.has_valid_input())
            self.condition_group.setEnabled(True)
            self.admission_type_combo.setEnabled(True)
            self.rb_mode_disease.setEnabled(True)
            self.rb_mode_procedure.setEnabled(True)
            self.cohort_worker = None
            self.cohort_worker_thread = None


    def update_cohort_creation_progress(self, value, max_value):
        if self.cohort_creation_progress.maximum() != max_value:
            self.cohort_creation_progress.setMaximum(max_value)
        self.cohort_creation_progress.setValue(value)

    def update_cohort_creation_log(self, message):
        self.cohort_creation_log.append(message)
        QApplication.processEvents()

    def update_button_states(self):
        has_input = self.condition_group.has_valid_input()
        self.query_btn.setEnabled(has_input)
        self.preview_btn.setEnabled(has_input)
        if has_input and self.result_table.rowCount() > 0 :
             if self.last_query_condition_template and self.last_query_condition_template != "1=1":
                 self.create_table_btn.setEnabled(True)
             else:
                 self.create_table_btn.setEnabled(False)
        else:
            self.create_table_btn.setEnabled(False)

        if not has_input:
            self.last_query_condition_template = None
            self.last_query_params = None
            self.sql_preview.setPlaceholderText("在此处将显示生成的SQL语句预览...")

    def _build_query_parts(self): # For querying ICD codes or Procedure codes for the list
        condition_template, params = self.condition_group.get_condition()
        # self.dict_table_for_query etc. are set in on_mode_changed
        base_query_template = f"SELECT {self.dict_code_col_for_query}, {self.dict_title_col_for_query} FROM {self.dict_table_for_query}"
        if condition_template:
            full_query_template = f"{base_query_template} WHERE {condition_template}"
        else: # Should not happen if buttons are disabled without input
            full_query_template = base_query_template # Potentially list all - user should be warned
            params = []
        return full_query_template, params

    def preview_sql_action(self): # For the ICD/Procedure code listing query
        query_template, params = self._build_query_parts()
        preview_sql_filled = query_template
        query_type_str = "疾病ICD" if self.current_mode_key == MODE_DISEASE_KEY else "手术/操作ICD"

        if params:
            try:
                db_params = self.get_db_params()
                if db_params:
                    with psycopg2.connect(**db_params) as temp_conn:
                        with temp_conn.cursor() as temp_cur:
                            preview_sql_filled = temp_cur.mogrify(query_template, params).decode(temp_conn.encoding or 'utf-8')
                    self.sql_preview.setText(f"-- SQL Preview ({query_type_str} 查询, 带参数预览):\n{preview_sql_filled}\n\n-- Note: Actual execution uses server-side parameter binding.")
                else:
                    self.sql_preview.setText(f"SQL Template ({query_type_str} 查询):\n{query_template}\n\nParameters:\n{params}\n\n(无法连接数据库以生成完整预览)")
            except Exception as e:
                print(f"Error creating preview SQL with mogrify: {e}")
                self.sql_preview.setText(f"SQL Template ({query_type_str} 查询):\n{query_template}\n\nParameters:\n{params}\n\n(生成带参数预览时出错)")
        else:
            self.sql_preview.setText(f"-- SQL Preview ({query_type_str} 查询, 无参数):\n{query_template}")

    def execute_query(self): # For the ICD/Procedure code listing
        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库"); return

        query_template, params = self._build_query_parts()
        has_meaningful_condition = self.condition_group.has_valid_input()
        query_type_str = "疾病ICD" if self.current_mode_key == MODE_DISEASE_KEY else "手术/操作ICD"


        if not has_meaningful_condition:
             # print(f"Warning: Executing {query_type_str} query without specific conditions.")
             self.create_table_btn.setEnabled(False)

        self.last_query_condition_template, self.last_query_params = self.condition_group.get_condition()
        if not self.last_query_condition_template and has_meaningful_condition:
            self.last_query_condition_template = "1=1" # Should be actual condition
            self.last_query_params = []


        self.preview_sql_action()
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute(query_template, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            self.result_table.setRowCount(0)
            self.result_table.setColumnCount(len(columns))
            self.result_table.setHorizontalHeaderLabels(columns)
            for i, row_data in enumerate(rows):
                self.result_table.insertRow(i)
                for j, value in enumerate(row_data):
                    self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
            self.result_table.resizeColumnsToContents()
            QMessageBox.information(self, "查询完成", f"共找到 {len(rows)} 条 {query_type_str} 记录")

            if rows and has_meaningful_condition and self.last_query_condition_template and self.last_query_condition_template != "1=1":
                self.create_table_btn.setEnabled(True)
            else:
                self.create_table_btn.setEnabled(False)
        except (Exception, psycopg2.Error) as error:
            QMessageBox.critical(self, "查询失败", f"无法执行 {query_type_str} 查询: {error}\n{traceback.format_exc()}")
            self.create_table_btn.setEnabled(False)
        finally:
            if conn: conn.close()

    def _get_source_mode_details(self):
        if self.current_mode_key == MODE_DISEASE_KEY:
            return {
                "source_type": MODE_DISEASE_KEY,
                "event_table": "mimiciv_hosp.diagnoses_icd",
                "dictionary_table": "mimiciv_hosp.d_icd_diagnoses",
                "event_icd_col": "icd_code",
                "dict_icd_col": "icd_code",
                "dict_title_col": "long_title", # Used by worker now
                "event_seq_num_col": "seq_num",
                "event_time_col": None # admittime from admissions table is primary
            }
        elif self.current_mode_key == MODE_PROCEDURE_KEY:
            return {
                "source_type": MODE_PROCEDURE_KEY,
                "event_table": "mimiciv_hosp.procedures_icd",
                "dictionary_table": "mimiciv_hosp.d_icd_procedures",
                "event_icd_col": "icd_code",
                "dict_icd_col": "icd_code",
                "dict_title_col": "long_title", # Used by worker now
                "event_seq_num_col": "seq_num",
                "event_time_col": "chartdate" # Can be used for finer sorting
            }
        return None


    def _generate_cohort_creation_sql_preview(self, target_table_name_str,
                                             condition_sql_template_str, condition_params_list,
                                             admission_cohort_type, source_mode_details):
        db_params = self.get_db_params()
        if not db_params:
            return False, "-- 数据库未连接，无法生成队列创建SQL预览。--"

        # Unpack source_mode_details
        event_table_full = source_mode_details["event_table"]
        dict_table_full = source_mode_details["dictionary_table"]
        event_icd_col = source_mode_details["event_icd_col"]
        dict_icd_col = source_mode_details["dict_icd_col"]
        event_seq_num_col = source_mode_details["event_seq_num_col"]
        event_time_col = source_mode_details.get("event_time_col")

        event_table_ident = psql.Identifier(*event_table_full.split('.'))
        dict_table_ident = psql.Identifier(*dict_table_full.split('.'))
        event_icd_col_ident = psql.Identifier(event_icd_col)
        dict_icd_col_ident = psql.Identifier(dict_icd_col)
        event_seq_num_col_ident = psql.Identifier(event_seq_num_col)
        event_time_col_ident = psql.Identifier(event_time_col) if event_time_col else None

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()

            target_ident = psql.Identifier('mimiciv_data', target_table_name_str)
            selected_event_ad_temp_ident = psql.Identifier('selected_event_ad_temp_cohort_q')
            first_icu_stays_temp_ident = psql.Identifier('first_icu_stays_temp_cohort_q')


            base_event_select_sql_preview = psql.SQL("""
                SELECT e.subject_id, e.hadm_id, e.{event_icd_col} AS qualifying_event_code,
                       dd.long_title AS qualifying_event_title, e.{event_seq_num_col} AS qualifying_event_seq_num
                       {event_time_col_select}
                FROM {event_table} e
                JOIN {dict_table} dd ON e.{event_icd_col} = dd.{dict_icd_col}
                JOIN mimiciv_hosp.admissions adm ON e.hadm_id = adm.hadm_id -- Ensure adm alias for WHERE and ORDER BY
                WHERE ({condition_template_placeholder})
            """).format(
                event_icd_col=event_icd_col_ident,
                event_seq_num_col=event_seq_num_col_ident,
                event_time_col_select=psql.SQL(", e.{} AS qualifying_event_time").format(event_time_col_ident) if event_time_col_ident else psql.SQL(""),
                event_table=event_table_ident,
                dict_table=dict_table_ident,
                dict_icd_col=dict_icd_col_ident,
                condition_template_placeholder=psql.SQL(condition_sql_template_str)
            )

            if admission_cohort_type == COHORT_TYPE_FIRST_EVENT_KEY:
                order_by_list_preview = [psql.SQL("adm.admittime ASC"), psql.SQL("e.hadm_id ASC")]
                if event_time_col_ident and source_mode_details['source_type'] == MODE_PROCEDURE_KEY:
                    order_by_list_preview.append(psql.SQL("e.{} ASC").format(event_time_col_ident))
                order_by_list_preview.append(psql.SQL("e.{} ASC").format(event_seq_num_col_ident))

                create_event_temp_sql_obj = psql.SQL("""
                    DROP TABLE IF EXISTS {temp_table_ident};
                    CREATE TEMPORARY TABLE {temp_table_ident} AS (
                      SELECT * FROM (
                        SELECT base.*,
                               ROW_NUMBER() OVER(PARTITION BY base.subject_id ORDER BY {order_by_clause}) AS admission_rank_for_event
                        FROM ({base_select_preview}) AS base
                        JOIN mimiciv_hosp.admissions adm ON base.hadm_id = adm.hadm_id -- Re-join for clarity or ensure adm columns are in base
                        {join_event_table_for_ordering}
                      ) sub WHERE admission_rank_for_event = 1);
                """).format(
                    temp_table_ident=selected_event_ad_temp_ident,
                    base_select_preview=base_event_select_sql_preview, # This now contains the join to admissions
                    order_by_clause=psql.SQL(', ').join(order_by_list_preview),
                    join_event_table_for_ordering=psql.SQL("JOIN {event_table} e ON base.hadm_id = e.hadm_id AND base.qualifying_event_code = e.{event_icd_col} AND base.qualifying_event_seq_num = e.{event_seq_num_col}")
                                                    .format(event_table=event_table_ident, event_icd_col=event_icd_col_ident, event_seq_num_col=event_seq_num_col_ident)
                                                    if event_time_col_ident and source_mode_details['source_type'] == MODE_PROCEDURE_KEY else psql.SQL("")
                )
            elif admission_cohort_type == COHORT_TYPE_ALL_EVENTS_KEY:
                create_event_temp_sql_obj = psql.SQL("""
                    DROP TABLE IF EXISTS {temp_table_ident};
                    CREATE TEMPORARY TABLE {temp_table_ident} AS ({base_select_preview});
                """).format(
                    temp_table_ident=selected_event_ad_temp_ident,
                    base_select_preview=base_event_select_sql_preview
                )
            else: return False, f"-- 未知的入院类型: {admission_cohort_type} --"

            readable_sql1 = cur.mogrify(create_event_temp_sql_obj.as_string(conn), condition_params_list).decode(conn.encoding or 'utf-8')


            create_first_icu_sql_obj = psql.SQL("""
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
            readable_sql2 = create_first_icu_sql_obj.as_string(conn)


            target_select_list_preview = [
                psql.SQL("evt_ad.subject_id"), psql.SQL("evt_ad.hadm_id"), psql.SQL("adm.admittime"), psql.SQL("adm.dischtime"),
                psql.SQL("icu.stay_id"), psql.SQL("icu.icu_intime"), psql.SQL("icu.icu_outtime"), psql.SQL("icu.los_icu_hours"),
                psql.SQL("evt_ad.qualifying_event_code"), psql.SQL("evt_ad.qualifying_event_title"), psql.SQL("evt_ad.qualifying_event_seq_num")
            ]
            if event_time_col_ident:
                 target_select_list_preview.append(psql.SQL("evt_ad.qualifying_event_time"))

            create_target_sql_obj = psql.SQL("""
                DROP TABLE IF EXISTS {target_ident};
                CREATE TABLE {target_ident} AS (
                 SELECT {select_cols}
                 FROM {event_ad_temp_table} evt_ad
                 JOIN mimiciv_hosp.admissions adm ON evt_ad.hadm_id = adm.hadm_id
                 LEFT JOIN {icu_temp_table} icu ON evt_ad.hadm_id = icu.hadm_id);
            """).format(
                target_ident=target_ident,
                select_cols=psql.SQL(', ').join(target_select_list_preview),
                event_ad_temp_table=selected_event_ad_temp_ident,
                icu_temp_table=first_icu_stays_temp_ident
            )
            readable_sql3 = create_target_sql_obj.as_string(conn)

            index_preview_str = f"-- Followed by CREATE INDEX statements on {target_table_name_str} for subject_id, hadm_id, stay_id, qualifying_event_code etc.\n"
            schema_sql_str = "CREATE SCHEMA IF NOT EXISTS mimiciv_data;\n"

            full_preview = (
                "-- ===== Cohort Creation SQL Preview =====\n\n"
                f"-- Cohort Source: {source_mode_details['source_type']}\n"
                f"-- Admission Type: {self.admission_type_combo.currentText()}\n"
                f"-- Target Table: {target_table_name_str}\n\n"
                f"-- Step 0: Ensure schema exists --\n{schema_sql_str}\n"
                f"-- Step 1: Create temporary table for selected event admissions --\n{readable_sql1}\n\n"
                f"-- Step 2: Create temporary table for first ICU stays for those admissions --\n{readable_sql2}\n\n"
                f"-- Step 3: Create the final cohort table '{target_table_name_str}' --\n{readable_sql3}\n\n"
                f"-- Step 4: Create indexes --\n{index_preview_str}\n"
                f"-- Step 5: Clean up temporary tables (implicit in TEMPORARY) --\n"
                "-- ===================================== --"
            )
            return True, full_preview

        except (Exception, psycopg2.Error) as e:
            return False, f"-- Error generating cohort creation SQL preview: {e}\n{traceback.format_exc()} --"
        finally:
            if conn: conn.close()


    def create_cohort_table_with_preview(self): # Renamed from create_disease_table_with_preview
        if not self.last_query_condition_template or not self.condition_group.has_valid_input() or self.last_query_condition_template == "1=1":
            QMessageBox.warning(self, "缺少有效查询条件", "请先执行一次有效的筛选条件（指定了筛选条件并有结果）。")
            self.sql_preview.setText("-- 无法创建队列：缺少有效的查询条件。")
            return

        raw_cohort_identifier, ok = self.get_cohort_identifier_name() # Generic name
        if not ok or not raw_cohort_identifier:
            self.sql_preview.setText("-- 队列创建已取消：未提供队列标识符。")
            return

        cleaned_identifier = re.sub(r'[^a-z0-9_]+', '_', raw_cohort_identifier.lower()).strip('_')
        if not cleaned_identifier or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', cleaned_identifier):
            QMessageBox.warning(self, "名称格式错误", f"队列标识符 '{raw_cohort_identifier}' -> '{cleaned_identifier}' 不符合规则。")
            self.sql_preview.setText(f"-- 队列创建失败：标识符 '{cleaned_identifier}' 格式错误。")
            return

        selected_admission_type_key = self.admission_type_combo.currentData()
        table_prefix = "first_" if selected_admission_type_key == COHORT_TYPE_FIRST_EVENT_KEY else "all_"
        source_prefix = "dis_" if self.current_mode_key == MODE_DISEASE_KEY else "proc_"

        target_table_name_str = f"{table_prefix}{source_prefix}{cleaned_identifier}_admissions"

        if len(target_table_name_str) > 63:
             QMessageBox.warning(self, "名称过长", f"生成的表名 '{target_table_name_str}' 过长 (最长63字符)。请缩短队列标识符。")
             self.sql_preview.setText(f"-- 队列创建失败：表名 '{target_table_name_str}' 过长。")
             return

        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            self.sql_preview.setText("-- 队列创建失败：数据库未连接。")
            return

        current_source_mode_details = self._get_source_mode_details()
        if not current_source_mode_details:
             QMessageBox.critical(self, "内部错误", "无法确定当前的筛选模式详情。")
             return

        success, preview_sql_str = self._generate_cohort_creation_sql_preview(
            target_table_name_str,
            self.last_query_condition_template,
            self.last_query_params,
            selected_admission_type_key,
            current_source_mode_details
        )
        self.sql_preview.setText(preview_sql_str)
        QApplication.processEvents()

        if not success:
            QMessageBox.critical(self, "预览失败", "无法生成队列创建SQL的预览。\n请查看SQL预览区域的错误信息。")
            return

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

        self.cohort_worker = CohortCreationWorker(
            db_params,
            target_table_name_str,
            self.last_query_condition_template,
            self.last_query_params,
            selected_admission_type_key,
            current_source_mode_details
        )
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
        self.cohort_worker.finished.connect(lambda: setattr(self, 'cohort_worker', None))
        self.cohort_worker.error.connect(lambda: setattr(self, 'cohort_worker', None))

        self.cohort_worker_thread.start()

    @Slot(str, int)
    def on_cohort_creation_finished(self, table_name, count):
        self.update_cohort_creation_log(f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        QMessageBox.information(self, "创建成功", f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        self.prepare_for_cohort_creation(False)
        self.update_button_states()

    @Slot(str)
    def on_cohort_creation_error(self, error_message):
        self.update_cohort_creation_log(f"队列创建失败: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "创建失败", f"无法创建队列数据表: {error_message}")
        else:
            QMessageBox.information(self, "操作取消", "队列创建操作已取消。")
        self.prepare_for_cohort_creation(False)
        self.update_button_states()

    def get_cohort_identifier_name(self): # Renamed from get_disease_name
        dialog = QDialog(self); dialog.setWindowTitle("输入队列基础标识符")
        layout = QVBoxLayout(dialog); form_layout = QFormLayout(); name_input = QLineEdit()
        form_layout.addRow("队列基础标识符 (英文,数字,下划线):", name_input); layout.addLayout(form_layout)
        info_label = QLabel("注意: 此标识符将用于构成数据库表名，例如:\n"
                            "'first_dis_标识符_admissions' 或 'all_proc_标识符_admissions')。\n"
                            "只能包含英文字母、数字和下划线，且必须以字母或下划线开头。")
        info_label.setWordWrap(True); layout.addWidget(info_label)
        btn_layout = QHBoxLayout(); ok_btn = QPushButton("确定"); cancel_btn = QPushButton("取消")
        btn_layout.addWidget(ok_btn); btn_layout.addWidget(cancel_btn); layout.addLayout(btn_layout)
        ok_btn.clicked.connect(dialog.accept); cancel_btn.clicked.connect(dialog.reject)
        result = dialog.exec_(); return name_input.text().strip(), result == QDialog.Accepted

    def closeEvent(self, event):
        if self.cohort_worker_thread and self.cohort_worker_thread.isRunning():
            if self.cohort_worker: self.cohort_worker.cancel()
            self.cohort_worker_thread.quit()
            if not self.cohort_worker_thread.wait(1000):
                 print("Warning: Cohort creation thread did not quit in time.")
        super().closeEvent(event)

# --- END OF FILE tab_query_cohort.py ---