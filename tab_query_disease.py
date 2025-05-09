# --- START OF FILE tab_query_disease.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QDialog, QLineEdit, QFormLayout,
                          QApplication, QProgressBar, QGroupBox) # Added QApplication, QProgressBar, QGroupBox
from PySide6.QtCore import Qt, Signal, QObject, QThread, Slot # Added Signal, QObject, QThread
import psycopg2
from psycopg2 import sql as psql
import re
import time # For potential delays or detailed timing if needed
import traceback
from conditiongroup import ConditionGroupWidget

# --- Worker Class for Cohort Creation ---
class CohortCreationWorker(QObject):
    finished = Signal(str, int) # table_name, count
    error = Signal(str)
    progress = Signal(int, int) # current_step, total_steps
    log = Signal(str)

    def __init__(self, db_params, target_table_name_str, condition_sql_template, condition_params):
        super().__init__()
        self.db_params = db_params
        self.target_table_name_str = target_table_name_str
        self.condition_sql_template = condition_sql_template
        self.condition_params = condition_params
        self.is_cancelled = False # Placeholder for future cancellation logic if needed

    def cancel(self): # Basic cancellation flag, actual interrupt depends on DB call
        self.log.emit("队列创建操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn = None
        # Define steps for progress bar
        # 1. Create schema if not exists
        # 2. Create first_diag_ad_temp_cohort
        # 3. Create first_icu_stays_temp_cohort
        # 4. Create target cohort table
        # 5. Create indexes (can be multiple sub-steps or one conceptual step)
        # 6. Count and cleanup
        total_steps = 6
        current_step = 0

        try:
            self.log.emit(f"开始创建队列数据表: {self.target_table_name_str}...")
            self.progress.emit(current_step, total_steps)

            self.log.emit("连接数据库...")
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            conn.autocommit = False # Use transaction
            self.log.emit("数据库已连接。")

            # Step 1: Create schema
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 确保 'mimiciv_data' schema 存在...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS mimiciv_data;")
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            target_table_ident = psql.Identifier('mimiciv_data', self.target_table_name_str)
            first_diag_ad_temp_ident = psql.Identifier('first_diag_ad_temp_cohort_q') # Unique temp name
            first_icu_stays_temp_ident = psql.Identifier('first_icu_stays_temp_cohort_q') # Unique temp name

            # Step 2: Create first_diag_ad_temp_cohort
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建临时表 (首次诊断)...")
            create_first_diag_admission_sql = psql.SQL("""
                DROP TABLE IF EXISTS {temp_table_ident};
                CREATE TEMPORARY TABLE {temp_table_ident} AS (
                  SELECT * FROM (
                    SELECT d.subject_id, d.hadm_id, d.icd_code, d.seq_num, dd.long_title,
                           ROW_NUMBER() OVER(PARTITION BY d.subject_id ORDER BY adm.admittime ASC, d.hadm_id ASC, d.seq_num ASC) AS admission_rank_for_disease
                    FROM mimiciv_hosp.diagnoses_icd d
                    JOIN mimiciv_hosp.d_icd_diagnoses dd ON d.icd_code = dd.icd_code
                    JOIN mimiciv_hosp.admissions adm ON d.hadm_id = adm.hadm_id
                    WHERE ({condition_template_placeholder})
                  ) sub WHERE admission_rank_for_disease = 1);
            """).format(temp_table_ident=first_diag_ad_temp_ident, condition_template_placeholder=psql.SQL(self.condition_sql_template))
            cur.execute(create_first_diag_admission_sql, self.condition_params)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            # Step 3: Create first_icu_stays_temp_cohort
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
                    WHERE EXISTS (SELECT 1 FROM {first_diag_temp_table} fdat WHERE fdat.hadm_id = icu.hadm_id)
                  ) sub WHERE icu_stay_rank_in_admission = 1);
            """).format(temp_table_ident=first_icu_stays_temp_ident, first_diag_temp_table=first_diag_ad_temp_ident)
            cur.execute(create_first_icu_stay_for_admission_sql)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            # Step 4: Create target cohort table
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 创建目标队列数据表 {self.target_table_name_str}...")
            create_target_table_sql = psql.SQL("""
                DROP TABLE IF EXISTS {target_ident};
                CREATE TABLE {target_ident} AS (
                 SELECT diag.subject_id, diag.hadm_id, adm.admittime, adm.dischtime,
                        icu.stay_id, icu.icu_intime, icu.icu_outtime, icu.los_icu_hours,
                        diag.icd_code AS first_target_diag_icd,
                        diag.long_title AS first_target_diag_title,
                        diag.seq_num AS first_target_diag_seq_num
                 FROM {diag_temp_table} diag
                 JOIN mimiciv_hosp.admissions adm ON diag.hadm_id = adm.hadm_id
                 LEFT JOIN {icu_temp_table} icu ON diag.hadm_id = icu.hadm_id);
            """).format(target_ident=target_table_ident, diag_temp_table=first_diag_ad_temp_ident, icu_temp_table=first_icu_stays_temp_ident)
            cur.execute(create_target_table_sql)
            self.progress.emit(current_step, total_steps)
            if self.is_cancelled: raise InterruptedError("操作已取消")

            # Step 5: Create Indexes
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 为表 {self.target_table_name_str} 创建索引...")
            indexes_to_create = [
                ("idx_{tbl_name}_subject_id".format(tbl_name=self.target_table_name_str[:25]), "subject_id"),
                ("idx_{tbl_name}_hadm_id".format(tbl_name=self.target_table_name_str[:25]), "hadm_id"),
                ("idx_{tbl_name}_stay_id".format(tbl_name=self.target_table_name_str[:25]), "stay_id"),
                ("idx_{tbl_name}_admittime".format(tbl_name=self.target_table_name_str[:25]), "admittime"),
                ("idx_{tbl_name}_icu_intime".format(tbl_name=self.target_table_name_str[:25]), "icu_intime"),
            ]
            for index_name_str, column_name_str in indexes_to_create:
                if len(index_name_str) > 63: index_name_str = index_name_str[:63]
                index_ident = psql.Identifier(index_name_str)
                column_ident = psql.Identifier(column_name_str)
                create_index_sql = psql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({});").format(index_ident, target_table_ident, column_ident)
                self.log.emit(f"    创建索引 {index_name_str}...")
                cur.execute(create_index_sql)
                if self.is_cancelled: raise InterruptedError("操作已取消")
            self.log.emit("所有索引创建完毕。")
            self.progress.emit(current_step, total_steps)


            # Step 6: Count and Cleanup (Commit is crucial)
            current_step += 1
            self.log.emit(f"步骤 {current_step}/{total_steps}: 正在提交更改并获取行数...")
            cur.execute(psql.SQL("DROP TABLE IF EXISTS {temp_table_ident};").format(temp_table_ident=first_icu_stays_temp_ident)) # Drop temp
            cur.execute(psql.SQL("DROP TABLE IF EXISTS {temp_table_ident};").format(temp_table_ident=first_diag_ad_temp_ident))  # Drop temp
            conn.commit()
            self.log.emit("更改已成功提交。")

            cur.execute(psql.SQL("SELECT COUNT(*) FROM {}").format(target_table_ident))
            count = cur.fetchone()[0]
            self.progress.emit(current_step, total_steps)
            self.finished.emit(self.target_table_name_str, count)

        except InterruptedError: # Handle our custom cancellation
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
# --- End Worker Class ---


class QueryDiseaseTab(QWidget): # Main class continues
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()
        self.last_query_condition_template = None
        self.last_query_params = None
        self.cohort_worker_thread = None # Thread for cohort creation worker
        self.cohort_worker = None      # Worker object instance

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        instruction_label = QLabel("使用下方条件组构建疾病查询条件，可以添加多个关键词（可选择包含或排除）和嵌套条件组")
        top_layout.addWidget(instruction_label)
        self.condition_group = ConditionGroupWidget(is_root=True, search_field="long_title")
        self.condition_group.condition_changed.connect(self.update_button_states)
        top_layout.addWidget(self.condition_group)
        btn_layout = QHBoxLayout()
        self.query_btn = QPushButton("查询"); self.query_btn.clicked.connect(self.execute_query); self.query_btn.setEnabled(False)
        btn_layout.addWidget(self.query_btn)
        self.preview_btn = QPushButton("预览SQL"); self.preview_btn.clicked.connect(self.preview_sql_action); self.preview_btn.setEnabled(False)
        btn_layout.addWidget(self.preview_btn)
        self.create_table_btn = QPushButton("创建首次诊断目标病种表"); self.create_table_btn.clicked.connect(self.create_disease_table); self.create_table_btn.setEnabled(False)
        btn_layout.addWidget(self.create_table_btn)
        top_layout.addLayout(btn_layout)

        # --- Add Execution Status Group for Cohort Creation ---
        self.cohort_creation_status_group = QGroupBox("队列创建状态")
        cohort_status_layout = QVBoxLayout(self.cohort_creation_status_group)
        self.cohort_creation_progress = QProgressBar()
        self.cohort_creation_progress.setRange(0, 6) # Max steps defined in worker
        self.cohort_creation_progress.setValue(0)
        cohort_status_layout.addWidget(self.cohort_creation_progress)
        self.cohort_creation_log = QTextEdit()
        self.cohort_creation_log.setReadOnly(True); self.cohort_creation_log.setMaximumHeight(100)
        cohort_status_layout.addWidget(self.cohort_creation_log)
        self.cohort_creation_status_group.setVisible(False) # Initially hidden
        top_layout.addWidget(self.cohort_creation_status_group)
        # --- End Execution Status Group ---

        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(100) # Reduced height for this one
        top_layout.addWidget(self.sql_preview)

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(top_widget)
        self.result_table = QTableWidget(); self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)
        splitter.setSizes([550, 300]) # Adjusted sizes
        main_layout.addWidget(splitter)
        self.update_button_states()


    # --- Methods for Cohort Creation Status UI ---
    def prepare_for_cohort_creation(self, starting=True):
        if starting:
            self.cohort_creation_status_group.setVisible(True)
            self.cohort_creation_progress.setValue(0)
            self.cohort_creation_log.clear()
            self.update_cohort_creation_log("开始创建队列...")
            self.create_table_btn.setEnabled(False) # Disable during creation
            self.query_btn.setEnabled(False)
            self.preview_btn.setEnabled(False)
            self.condition_group.setEnabled(False)
            # Add cancel button if implemented in worker
            # self.cancel_cohort_btn.setEnabled(True)
        else:
            # Re-enable based on state after operation
            self.update_button_states() # This will re-evaluate create_table_btn
            self.query_btn.setEnabled(self.condition_group.has_valid_input())
            self.preview_btn.setEnabled(self.condition_group.has_valid_input())
            self.condition_group.setEnabled(True)
            # self.cancel_cohort_btn.setEnabled(False)
            self.cohort_worker = None # Clear worker
            self.cohort_worker_thread = None # Clear thread


    def update_cohort_creation_progress(self, value, max_value):
        if self.cohort_creation_progress.maximum() != max_value:
            self.cohort_creation_progress.setMaximum(max_value)
        self.cohort_creation_progress.setValue(value)

    def update_cohort_creation_log(self, message):
        self.cohort_creation_log.append(message)
        QApplication.processEvents()
    # --- End Cohort Status UI Methods ---


    # ... (update_button_states, _build_query_parts, preview_sql_action, execute_query - keep as is, ensure execute_query enables create_table_btn correctly) ...
    def update_button_states(self):
        has_input = self.condition_group.has_valid_input()
        self.query_btn.setEnabled(has_input)
        self.preview_btn.setEnabled(has_input)
        # create_table_btn enabled only if results and meaningful condition
        if has_input and self.result_table.rowCount() > 0 :
             # Further ensure last_query_condition was set from a meaningful query
             if self.last_query_condition_template and self.last_query_condition_template != "1=1":
                 self.create_table_btn.setEnabled(True)
             else:
                 self.create_table_btn.setEnabled(False)
        else:
            self.create_table_btn.setEnabled(False)

        if not has_input:
            self.last_query_condition_template = None
            self.last_query_params = None

    def _build_query_parts(self):
        condition_template, params = self.condition_group.get_condition()
        base_query_template = "SELECT icd_code, long_title FROM mimiciv_hosp.d_icd_diagnoses"
        if condition_template:
            full_query_template = f"{base_query_template} WHERE {condition_template}"
        else:
            full_query_template = base_query_template
            params = []
        return full_query_template, params

    def preview_sql_action(self):
        query_template, params = self._build_query_parts()
        preview_sql_filled = query_template
        if params:
            try:
                db_params = self.get_db_params()
                if db_params:
                    with psycopg2.connect(**db_params) as temp_conn:
                        with temp_conn.cursor() as temp_cur:
                            preview_sql_filled = temp_cur.mogrify(query_template, params).decode(temp_conn.encoding or 'utf-8')
                    self.sql_preview.setText(f"-- SQL Preview (with parameters applied for display):\n{preview_sql_filled}\n\n-- Note: Actual execution uses server-side parameter binding.")
                else:
                    self.sql_preview.setText(f"SQL Template:\n{query_template}\n\nParameters:\n{params}\n\n(无法连接数据库以生成完整预览)")
            except Exception as e:
                print(f"Error creating preview SQL with mogrify: {e}")
                self.sql_preview.setText(f"SQL Template:\n{query_template}\n\nParameters:\n{params}\n\n(生成带参数预览时出错)")
        else:
            self.sql_preview.setText(f"-- SQL Preview (no parameters):\n{query_template}")

    def execute_query(self):
        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库"); return

        query_template, params = self._build_query_parts()
        has_meaningful_condition = self.condition_group.has_valid_input()

        if not has_meaningful_condition:
             print("Warning: Executing query without specific conditions (will fetch all ICD codes).")
             self.create_table_btn.setEnabled(False)

        self.last_query_condition_template, self.last_query_params = self.condition_group.get_condition()
        if not self.last_query_condition_template and has_meaningful_condition: # Should not happen
            self.last_query_condition_template = "1=1" # Should be actual condition if has_meaningful_condition is true
            self.last_query_params = []

        self.sql_preview.setText(f"Executing SQL Template:\n{query_template}\n\nWith Parameters:\n{params}")
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
            QMessageBox.information(self, "查询完成", f"共找到 {len(rows)} 条记录")

            if rows and has_meaningful_condition and self.last_query_condition_template and self.last_query_condition_template != "1=1":
                self.create_table_btn.setEnabled(True)
            else:
                self.create_table_btn.setEnabled(False)
        except (Exception, psycopg2.Error) as error:
            QMessageBox.critical(self, "查询失败", f"无法执行查询: {error}\n{traceback.format_exc()}")
            self.create_table_btn.setEnabled(False)
        finally:
            if conn: conn.close()


    def create_disease_table(self):
        if not self.last_query_condition_template or not self.condition_group.has_valid_input() or self.last_query_condition_template == "1=1":
            QMessageBox.warning(self, "缺少有效查询条件", "请先执行一次有效的疾病查询（指定了筛选条件并有结果）。")
            return

        raw_disease_name, ok = self.get_disease_name()
        if not ok or not raw_disease_name: return
        disease_name_cleaned = re.sub(r'[^a-z0-9_]+', '_', raw_disease_name.lower()).strip('_')
        if not disease_name_cleaned or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', disease_name_cleaned):
            QMessageBox.warning(self, "名称格式错误", f"队列标识符 '{raw_disease_name}' -> '{disease_name_cleaned}' 不符合规则。"); return
        target_table_name_str = f"first_{disease_name_cleaned}_admissions"
        if len(target_table_name_str) > 63:
             QMessageBox.warning(self, "名称过长", f"生成的表名过长 (最长63字符)。请缩短标识符。"); return

        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库"); return

        self.prepare_for_cohort_creation(True)

        self.cohort_worker = CohortCreationWorker(
            db_params,
            target_table_name_str,
            self.last_query_condition_template,
            self.last_query_params
        )
        self.cohort_worker_thread = QThread()
        self.cohort_worker.moveToThread(self.cohort_worker_thread)

        # Connect signals
        self.cohort_worker_thread.started.connect(self.cohort_worker.run)
        self.cohort_worker.finished.connect(self.on_cohort_creation_finished)
        self.cohort_worker.error.connect(self.on_cohort_creation_error)
        self.cohort_worker.progress.connect(self.update_cohort_creation_progress)
        self.cohort_worker.log.connect(self.update_cohort_creation_log)

        # Cleanup
        self.cohort_worker.finished.connect(self.cohort_worker_thread.quit)
        self.cohort_worker.error.connect(self.cohort_worker_thread.quit)
        self.cohort_worker_thread.finished.connect(self.cohort_worker_thread.deleteLater)
        self.cohort_worker.finished.connect(self.cohort_worker.deleteLater)
        self.cohort_worker.error.connect(self.cohort_worker.deleteLater) # Cleanup on error too

        self.cohort_worker_thread.start()

    # --- Slots for Cohort Creation Worker ---
    @Slot(str, int)
    def on_cohort_creation_finished(self, table_name, count):
        self.update_cohort_creation_log(f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        QMessageBox.information(self, "创建成功", f"队列数据表 {table_name} 创建成功，包含 {count} 条记录。")
        self.prepare_for_cohort_creation(False)
        self.create_table_btn.setEnabled(False) # Disable after successful creation

    @Slot(str)
    def on_cohort_creation_error(self, error_message):
        self.update_cohort_creation_log(f"队列创建失败: {error_message}")
        if "操作已取消" not in error_message: # Check if it's a user cancellation
            QMessageBox.critical(self, "创建失败", f"无法创建队列数据表: {error_message}")
        else:
            QMessageBox.information(self, "操作取消", "队列创建操作已取消。")
        self.prepare_for_cohort_creation(False)
        # Keep create_table_btn enabled if error was not due to invalid input, so user can retry
        self.update_button_states() # Re-evaluate button states
    # --- End Worker Slots ---


    def get_disease_name(self): # Keep as is
        dialog = QDialog(self); dialog.setWindowTitle("输入病种队列标识符")
        layout = QVBoxLayout(dialog); form_layout = QFormLayout(); name_input = QLineEdit()
        form_layout.addRow("队列标识符 (英文,数字,下划线):", name_input); layout.addLayout(form_layout)
        info_label = QLabel("注意: 此标识符将用于创建数据库表名 (例如: first_标识符_admissions)。\n"
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
            self.cohort_worker_thread.wait(1000)
        super().closeEvent(event)

# --- END OF FILE tab_query_disease.py ---