# --- START OF FILE tab_combine_special_info.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QFormLayout, QRadioButton, QButtonGroup,
                          QLineEdit, QSpinBox, QListWidget, QListWidgetItem, QAbstractItemView,
                          QApplication, QProgressBar) # Added QProgressBar
from PySide6.QtCore import Qt, Signal, Slot, QObject, QThread # Added QObject, QThread
import psycopg2
import psycopg2.sql as pgsql
import re
import pandas as pd
import time # Import time for worker delays simulation if needed
import traceback # For detailed error printing
from conditiongroup import ConditionGroupWidget

# --- Worker Class for Asynchronous Merging ---
class MergeSQLWorker(QObject):
    finished = Signal() # Signal on successful completion
    error = Signal(str) # Signal on error
    progress = Signal(int, int) # Signal for progress (step, total_steps)
    log = Signal(str) # Signal for logging messages

    def __init__(self, db_params, execution_sql_obj, params_list, target_table_name, new_col_name_str):
        super().__init__()
        self.db_params = db_params
        self.execution_sql_obj = execution_sql_obj # This is the combined ALTER+UPDATE SQL object
        self.params_list = params_list
        self.target_table_name = target_table_name # For logging/messages
        self.new_col_name_str = new_col_name_str # For logging/messages
        self.is_cancelled = False

    def cancel(self):
        self.log.emit("合并操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn_merge = None
        total_steps = 2 # 1: ALTER, 2: UPDATE+COMMIT
        try:
            self.log.emit(f"准备为表 '{self.target_table_name}' 添加/更新列 '{self.new_col_name_str}'...")
            self.progress.emit(0, total_steps)

            self.log.emit("连接数据库...")
            conn_merge = psycopg2.connect(**self.db_params)
            conn_merge.autocommit = False # Use transactions
            cur = conn_merge.cursor()
            self.log.emit("数据库已连接。")

            # Although execution_sql_obj contains both, we might want to execute them
            # separately if cancellation between steps is desired, or log progress better.
            # For simplicity now, execute as one block, but emit progress conceptually.
            # If precise separation needed, _build_merge_query should return separate ALTER/UPDATE.

            sql_string_for_exec = self.execution_sql_obj.as_string(conn_merge) # Get string for logging/error
            params_for_exec = self.params_list if self.params_list else None

            # --- Conceptual Step 1: ALTER TABLE ---
            self.log.emit(f"执行 ALTER/UPDATE 语句...")
            # (Simulating progress before the potentially long combined execution)
            # If split: cur.execute(alter_sql_part); self.progress.emit(1, total_steps)

            if self.is_cancelled:
                self.log.emit("操作在执行前被取消。")
                self.error.emit("操作已取消")
                if conn_merge: conn_merge.rollback()
                return

            start_time = time.time()
            cur.execute(self.execution_sql_obj, params_for_exec)
            end_time = time.time()
            self.log.emit(f"ALTER/UPDATE 语句执行成功 (耗时: {end_time - start_time:.2f} 秒)。")
            self.progress.emit(1, total_steps) # Progress after execution

            # --- Conceptual Step 2: Commit ---
            if self.is_cancelled:
                self.log.emit("操作在提交前被取消，正在回滚...")
                if conn_merge: conn_merge.rollback()
                self.error.emit("操作已取消")
                return

            self.log.emit("正在提交事务...")
            conn_merge.commit()
            self.log.emit("事务提交成功。")
            self.progress.emit(2, total_steps) # Final progress

            self.finished.emit() # Signal success

        except psycopg2.Error as db_err:
            self.log.emit(f"数据库合并操作出错: {db_err}")
            self.log.emit(f"执行的SQL (部分): {sql_string_for_exec[:500]}...") # Log part of the failing SQL
            self.log.emit(f"参数: {self.params_list}")
            if conn_merge: conn_merge.rollback()
            self.log.emit("事务已回滚。")
            self.error.emit(f"数据库错误: {db_err}")
        except Exception as e:
            self.log.emit(f"合并过程中发生意外错误: {str(e)}")
            self.log.emit(f"Traceback: {traceback.format_exc()}")
            if conn_merge: conn_merge.rollback()
            self.log.emit("事务已回滚。")
            self.error.emit(f"意外错误: {str(e)}")
        finally:
            if conn_merge:
                self.log.emit("关闭数据库连接。")
                conn_merge.close()
# --- End Worker Class ---


class SpecialInfoDataExtractionTab(QWidget):
    request_preview_signal = Signal(str, str)

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_cohort_table = None
        self.db_conn = None
        self.db_cursor = None
        self.worker_thread = None # Thread for merge worker
        self.merge_worker = None # Worker object instance
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        # --- Top Configuration Panel ---
        config_widget = QWidget()
        config_layout = QVBoxLayout(config_widget)
        config_layout.setContentsMargins(10, 10, 10, 10); config_layout.setSpacing(10)
        splitter.addWidget(config_widget)

        # Groups 1, 2, 3... (Cohort, Source, Logic - same as before)
        cohort_group = QGroupBox("1. 选择目标队列数据表")
        cohort_layout = QHBoxLayout(cohort_group)
        cohort_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox(); self.table_combo.setMinimumWidth(250)
        self.table_combo.currentIndexChanged.connect(self.on_cohort_table_selected)
        cohort_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新列表"); self.refresh_btn.clicked.connect(self.refresh_cohort_tables); self.refresh_btn.setEnabled(False)
        cohort_layout.addWidget(self.refresh_btn); cohort_layout.addStretch()
        config_layout.addWidget(cohort_group)

        source_group = QGroupBox("2. 选择数据来源和筛选项目")
        source_main_layout = QVBoxLayout(source_group)
        source_select_layout = QHBoxLayout()
        source_select_layout.addWidget(QLabel("数据来源:"))
        self.direction_group = QButtonGroup(self)
        self.rb_lab = QRadioButton("化验 (labevents)"); self.rb_med = QRadioButton("用药 (prescriptions)")
        self.rb_proc = QRadioButton("操作/手术 (procedures_icd)"); self.rb_diag = QRadioButton("诊断 (diagnoses_icd)")
        self.rb_lab.setChecked(True)
        for idx, rb in enumerate([self.rb_lab, self.rb_med, self.rb_proc, self.rb_diag]):
            self.direction_group.addButton(rb, idx + 1); source_select_layout.addWidget(rb)
        source_select_layout.addStretch()
        source_main_layout.addLayout(source_select_layout)
        condition_filter_layout = QHBoxLayout()
        self.condition_widget = ConditionGroupWidget(is_root=True, search_field="label")
        condition_filter_layout.addWidget(self.condition_widget)
        self.filter_items_btn = QPushButton("筛选项目"); self.filter_items_btn.clicked.connect(self.filter_source_items)
        condition_filter_layout.addWidget(self.filter_items_btn, 0, Qt.AlignmentFlag.AlignTop)
        source_main_layout.addLayout(condition_filter_layout)
        item_list_layout = QVBoxLayout()
        item_list_layout.addWidget(QLabel("筛选出的项目 (多选合并时，列名基于首选项):"))
        self.item_list = QListWidget(); self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.item_list.itemSelectionChanged.connect(self.update_selected_items_count)
        self.item_list.itemSelectionChanged.connect(self.update_default_col_name)
        item_list_layout.addWidget(self.item_list)
        self.selected_items_label = QLabel("已选项目: 0")
        item_list_layout.addWidget(self.selected_items_label)
        source_main_layout.addLayout(item_list_layout)
        config_layout.addWidget(source_group)

        self.rb_lab.toggled.connect(lambda checked: self.update_source_config(checked, "labevents", "label", "itemid"))
        self.rb_med.toggled.connect(lambda checked: self.update_source_config(checked, "prescriptions", "drug", "drug"))
        self.rb_proc.toggled.connect(lambda checked: self.update_source_config(checked, "procedures_icd", "long_title", "icd_code"))
        self.rb_diag.toggled.connect(lambda checked: self.update_source_config(checked, "diagnoses_icd", "long_title", "icd_code"))
        self.direction_group.buttonToggled.connect(self.update_default_col_name)

        logic_group = QGroupBox("3. 定义提取逻辑和列名")
        logic_layout = QGridLayout(logic_group)
        logic_layout.addWidget(QLabel("新列名 (自动生成, 可修改):"), 0, 0)
        self.new_column_name_input = QLineEdit(); self.new_column_name_input.setPlaceholderText("自动生成或手动输入列名...")
        self.new_column_name_input.textChanged.connect(self.update_action_buttons_state)
        logic_layout.addWidget(self.new_column_name_input, 0, 1, 1, 3)
        self.lab_options_widget = QWidget()
        lab_logic_layout = QFormLayout(self.lab_options_widget); lab_logic_layout.setContentsMargins(0,0,0,0)
        self.lab_agg_combo = QComboBox(); self.lab_agg_combo.addItems(["首次 (First)", "末次 (Last)", "最小值 (Min)", "最大值 (Max)", "平均值 (Mean)", "计数 (Count)"])
        lab_logic_layout.addRow("提取方式:", self.lab_agg_combo)
        self.lab_time_window_combo = QComboBox(); self.lab_time_window_combo.addItems(["ICU入住后24小时", "ICU入住后48小时", "整个ICU期间", "首次住院期间"])
        lab_logic_layout.addRow("时间窗口:", self.lab_time_window_combo)
        logic_layout.addWidget(self.lab_options_widget, 1, 0, 1, 4)
        self.lab_agg_combo.currentTextChanged.connect(self.update_default_col_name)
        self.lab_time_window_combo.currentTextChanged.connect(self.update_default_col_name)
        self.event_options_widget = QWidget()
        event_logic_layout = QFormLayout(self.event_options_widget); event_logic_layout.setContentsMargins(0,0,0,0)
        self.event_output_combo = QComboBox(); self.event_output_combo.addItems(["是否存在 (Boolean)", "发生次数 (Count)"])
        event_logic_layout.addRow("输出类型:", self.event_output_combo)
        self.event_time_window_combo = QComboBox(); self.event_time_window_combo.addItems(["首次住院期间", "整个ICU期间"])
        event_logic_layout.addRow("时间窗口:", self.event_time_window_combo)
        logic_layout.addWidget(self.event_options_widget, 2, 0, 1, 4)
        self.event_options_widget.setVisible(False)
        self.event_output_combo.currentTextChanged.connect(self.update_default_col_name)
        self.event_time_window_combo.currentTextChanged.connect(self.update_default_col_name)
        config_layout.addWidget(logic_group)

        # --- Add Execution Status Group ---
        self.execution_status_group = QGroupBox("合并执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,2); self.execution_progress.setValue(0) # 2 steps
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit(); self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(100)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False) # Initially hidden
        config_layout.addWidget(self.execution_status_group)
        # --- End Execution Status Group ---

        action_layout = QHBoxLayout()
        self.preview_merge_btn = QPushButton("预览待合并数据"); self.preview_merge_btn.clicked.connect(self.preview_merge_data); self.preview_merge_btn.setEnabled(False)
        action_layout.addWidget(self.preview_merge_btn)
        self.execute_merge_btn = QPushButton("执行合并到表"); self.execute_merge_btn.clicked.connect(self.execute_merge); self.execute_merge_btn.setEnabled(False)
        action_layout.addWidget(self.execute_merge_btn)
        self.cancel_merge_btn = QPushButton("取消合并"); self.cancel_merge_btn.clicked.connect(self.cancel_merge); self.cancel_merge_btn.setEnabled(False)
        action_layout.addWidget(self.cancel_merge_btn) # Add cancel button
        config_layout.addLayout(action_layout)

        # --- Bottom Results Panel (Remains the same) ---
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(10, 10, 10, 10); result_layout.setSpacing(10)
        splitter.addWidget(result_widget)
        result_layout.addWidget(QLabel("SQL预览 (仅供参考):"))
        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(120)
        result_layout.addWidget(self.sql_preview)
        result_layout.addWidget(QLabel("数据预览 (最多100条):"))
        self.preview_table = QTableWidget(); self.preview_table.setAlternatingRowColors(True); self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        result_layout.addWidget(self.preview_table)

        splitter.setSizes([750, 250]) # Adjust splitter
        self._update_logic_options_visibility()
        self.update_default_col_name()

    # --- Helper Methods ---
    def _connect_db(self): # Keep this helper as is
        if self.db_conn and self.db_conn.closed == 0:
            try:
                if not self.db_cursor or self.db_cursor.closed: self.db_cursor = self.db_conn.cursor()
            except psycopg2.InterfaceError: self.db_cursor = self.db_conn.cursor()
            return True
        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库"); return False
        try:
            self.db_conn = psycopg2.connect(**db_params); self.db_cursor = self.db_conn.cursor()
            return True
        except Exception as e:
            QMessageBox.critical(self, "数据库连接失败", f"无法连接: {str(e)}"); self.db_conn = None; self.db_cursor = None; return False

    def _close_db(self): # Keep this helper as is
        if self.db_cursor: self.db_cursor.close(); self.db_cursor = None
        if self.db_conn: self.db_conn.close(); self.db_conn = None

    def _get_current_source_config(self): # Keep as is
        checked_id = self.direction_group.checkedId()
        if checked_id == 1: return "mimiciv_hosp.labevents", "mimiciv_hosp.d_labitems", "label", "itemid"
        elif checked_id == 2: return "mimiciv_hosp.prescriptions", None, "drug", "drug"
        elif checked_id == 3: return "mimiciv_hosp.procedures_icd", "mimiciv_hosp.d_icd_procedures", "long_title", "icd_code"
        elif checked_id == 4: return "mimiciv_hosp.diagnoses_icd", "mimiciv_hosp.d_icd_diagnoses", "long_title", "icd_code"
        return None, None, None, None

    def _update_logic_options_visibility(self): # Keep as is
        is_lab = self.rb_lab.isChecked()
        self.lab_options_widget.setVisible(is_lab)
        self.event_options_widget.setVisible(not is_lab)

    def _validate_column_name(self, name): # Keep as is (or use the more detailed one from previous response)
        if not name: return False, "列名不能为空。"
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
             if re.match(r'^_[a-zA-Z0-9_]+$', name): pass
             else: return False, "列名只能包含字母、数字和下划线，且以字母或下划线开头。"
        keywords = {"SELECT", "TABLE", "UPDATE", "COLUMN", "ADD", "ALTER", "WHERE", "FROM"} # Simplified list
        if name.upper() in keywords: return False, f"列名 '{name}' 是 SQL 关键字。"
        if len(name) > 63: return False, f"列名 '{name}' 过长 (最多63个字符)。"
        return True, ""

    def _sanitize_name(self, name): # Keep as is (or the improved one)
        if not name: return ""
        name = re.sub(r'[ /\\:,;()\[\]{}"\'.\-+*?^$|<>%]+', '_', name)
        name = re.sub(r'_+', '_', name)
        name = re.sub(r'[^\w]+', '', name)
        name = name.lower().strip('_')
        if name and name[0].isdigit(): name = '_' + name
        return name[:60] if name else "item"

    def _generate_default_column_name(self): # Keep as is
        parts = []
        logic_code = ""
        if self.rb_lab.isChecked():
            logic_map = {"首次 (First)": "first", "末次 (Last)": "last", "最小值 (Min)": "min", "最大值 (Max)": "max", "平均值 (Mean)": "mean", "计数 (Count)": "count"}
            logic_code = logic_map.get(self.lab_agg_combo.currentText(), "val")
        else:
            logic_map = {"是否存在 (Boolean)": "has", "发生次数 (Count)": "count"}
            logic_code = logic_map.get(self.event_output_combo.currentText(), "evt")
        parts.append(logic_code)

        selected_list_items = self.item_list.selectedItems()
        item_name_part = "item"
        if selected_list_items:
            try:
                data = selected_list_items[0].data(Qt.ItemDataRole.UserRole)
                if data and len(data) > 1 and data[1]: item_name_part = self._sanitize_name(data[1])
                else: item_name_part = self._sanitize_name(selected_list_items[0].text().split('(')[0].strip())
            except Exception: item_name_part = self._sanitize_name(selected_list_items[0].text().split('(')[0].strip())
        parts.append(item_name_part)

        time_code = ""
        if self.rb_lab.isChecked():
            time_map = {"ICU入住后24小时": "24h", "ICU入住后48小时": "48h", "整个ICU期间": "icu", "首次住院期间": "hosp"}
            time_code = time_map.get(self.lab_time_window_combo.currentText(), "")
        else:
            time_map = {"首次住院期间": "hosp", "整个ICU期间": "icu"}
            time_code = time_map.get(self.event_time_window_combo.currentText(), "")
        if time_code: parts.append(time_code)
        default_name = "_".join(filter(None, parts))
        if not default_name: default_name = "new_column"
        if default_name[0].isdigit(): default_name = "_" + default_name
        return default_name[:60]


    # --- New Helper Methods for Status ---
    def prepare_for_long_operation(self, starting=True):
        """Enable/disable UI elements during long merge operation."""
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行合并操作...")
            # Disable buttons
            self.preview_merge_btn.setEnabled(False)
            self.execute_merge_btn.setEnabled(False)
            self.refresh_btn.setEnabled(False)
            self.table_combo.setEnabled(False)
            self.filter_items_btn.setEnabled(False)
            self.item_list.setEnabled(False)
            self.condition_widget.setEnabled(False)
            self.new_column_name_input.setEnabled(False)
            # Disable logic options
            self.lab_options_widget.setEnabled(False)
            self.event_options_widget.setEnabled(False)
            # Enable cancel button
            self.cancel_merge_btn.setEnabled(True)
        else: # Operation finished or cancelled
            # Re-enable based on current state
            self.preview_merge_btn.setEnabled(self.execute_merge_btn.isEnabled()) # Match merge button state
            self.execute_merge_btn.setEnabled(self.selected_cohort_table is not None and len(self.item_list.selectedItems()) > 0 and self._validate_column_name(self.new_column_name_input.text())[0])
            self.refresh_btn.setEnabled(True)
            self.table_combo.setEnabled(True)
            self.filter_items_btn.setEnabled(True)
            self.item_list.setEnabled(True)
            self.condition_widget.setEnabled(True)
            self.new_column_name_input.setEnabled(True)
            # Re-enable logic options
            self.lab_options_widget.setEnabled(True)
            self.event_options_widget.setEnabled(True)
            # Disable cancel button
            self.cancel_merge_btn.setEnabled(False)
            # Worker and thread cleanup is handled via deleteLater

    def update_execution_progress(self, value, max_value=None):
        """Updates the progress bar."""
        if max_value is not None and self.execution_progress.maximum() != max_value:
            self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        """Appends a message to the execution log."""
        self.execution_log.append(message)
        QApplication.processEvents() # Ensure log updates are shown

    # --- End Helper Methods for Status ---


    # --- UI Event Handlers ---
    def on_db_connected(self):
        self.refresh_btn.setEnabled(True); self.refresh_cohort_tables()
        self.update_default_col_name()

    def refresh_cohort_tables(self): # Keep as is
        if not self._connect_db(): return
        try:
            self.db_cursor.execute(pgsql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = {} AND table_name LIKE {} ORDER BY table_name")
                                   .format(pgsql.Literal('mimiciv_data'), pgsql.Literal('first_%_admissions')))
            tables = self.db_cursor.fetchall()
            current_selection = self.table_combo.currentText()
            self.table_combo.clear()
            if tables:
                table_names = [table[0] for table in tables]
                self.table_combo.addItems(table_names)
                if current_selection in table_names: self.table_combo.setCurrentText(current_selection)
                elif table_names: self.table_combo.setCurrentIndex(0)
            else: self.table_combo.addItem("未找到符合条件的队列数据表")
            self.on_cohort_table_selected(self.table_combo.currentIndex()) # Update state after refresh
        except Exception as e: QMessageBox.critical(self, "查询失败", f"无法获取队列数据表列表: {str(e)}")

    def on_cohort_table_selected(self, index): # Keep as is
        if index >= 0 and self.table_combo.count() > 0 and "未找到" not in self.table_combo.itemText(index):
            self.selected_cohort_table = self.table_combo.itemText(index)
            print(f"Cohort table selected: {self.selected_cohort_table}")
        else:
            self.selected_cohort_table = None
            print("Cohort table deselected.")
        self.update_action_buttons_state()

    def update_source_config(self, checked, source_table_name, name_col, id_col_for_data): # Modified
        if checked:
            print(f"Source changed to: {source_table_name}, setting search field to: {name_col}")
            self.condition_widget.set_search_field(name_col)
            self._update_logic_options_visibility()
            self.item_list.clear()
            self.item_list.addItem("数据源已更改，请点击 [筛选项目] 按钮。")
            self.sql_preview.clear()
            self.selected_items_label.setText("已选项目: 0")
            self.update_default_col_name()
            self.update_action_buttons_state()

    @Slot()
    def filter_source_items(self): # Keep as is
        print("Filter Items button clicked.")
        if not self._connect_db(): return
        source_event_table, source_dict_table, name_col_for_display, id_col_for_data = self._get_current_source_config()
        if not source_event_table: QMessageBox.warning(self, "错误", "未能确定数据来源表。"); return

        condition_sql_template, condition_params = self.condition_widget.get_condition()

        self.item_list.clear(); self.sql_preview.clear()
        self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False)
        QApplication.processEvents()

        query_template_obj = None
        try:
            # Build query template (same logic as before)
            if not source_dict_table:
                if not condition_sql_template: self.item_list.clear(); self.item_list.addItem("请在上方输入并确认药物关键词后点击 [筛选项目]");
                else: query_template_obj = pgsql.SQL("SELECT DISTINCT {name_col} FROM {event_table} WHERE {condition} ORDER BY {name_col} LIMIT 500").format(name_col=pgsql.Identifier(name_col_for_display), event_table=pgsql.SQL(source_event_table), condition=pgsql.SQL(condition_sql_template))
            else:
                if not condition_sql_template: self.item_list.clear(); self.item_list.addItem("请输入筛选条件后点击 [筛选项目]");
                else: query_template_obj = pgsql.SQL("SELECT {id_col}, {name_col} FROM {dict_table} WHERE {condition} ORDER BY {name_col} LIMIT 500").format(id_col=pgsql.Identifier(id_col_for_data), name_col=pgsql.Identifier(name_col_for_display), dict_table=pgsql.SQL(source_dict_table), condition=pgsql.SQL(condition_sql_template))

            if query_template_obj:
                # Display preview SQL
                final_sql_str_preview = query_template_obj.as_string(self.db_conn)
                try:
                    for p in condition_params: final_sql_str_preview = final_sql_str_preview.replace("%s", pgsql.Literal(p).as_string(self.db_conn), 1)
                except Exception as fmt_err: print(f"Preview formatting error: {fmt_err}"); final_sql_str_preview = query_template_obj.as_string(self.db_conn)
                self.sql_preview.setText(f"-- Item Filter Query (Preview):\n{final_sql_str_preview}\n-- Params: {condition_params}")

                # Execute query
                print("Executing filter query...")
                start_time = time.time()
                self.db_cursor.execute(query_template_obj, condition_params)
                items = self.db_cursor.fetchall()
                end_time = time.time()
                print(f"Query executed in {end_time - start_time:.2f}s. Found {len(items)} items.")

                # Populate list
                self.item_list.clear()
                if items:
                    # ... (population logic remains the same) ...
                    if not source_dict_table:
                         for item_tuple in items:
                            val = str(item_tuple[0]) if item_tuple[0] is not None else "Unknown"
                            list_item = QListWidgetItem(val); list_item.setData(Qt.ItemDataRole.UserRole, (val, val))
                            self.item_list.addItem(list_item)
                    else:
                        for item_id, item_name_disp in items:
                            display_name = str(item_name_disp) if item_name_disp is not None else f"ID_{item_id}"
                            list_item = QListWidgetItem(f"{display_name} (ID: {item_id})")
                            list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id), display_name))
                            self.item_list.addItem(list_item)
                else:
                    self.item_list.addItem("未找到符合条件的项目")

        except Exception as e:
            self.item_list.clear()
            QMessageBox.critical(self, "筛选项目失败", f"查询失败: {str(e)}\n{traceback.format_exc()}")
            self.item_list.addItem("查询出错")
        finally:
            self.filter_items_btn.setEnabled(True)
            self.update_default_col_name()
            self.update_action_buttons_state()

    @Slot()
    def update_selected_items_count(self): # Keep as is
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.update_action_buttons_state()

    @Slot()
    def update_action_buttons_state(self): # Keep as is
        col_name_text = self.new_column_name_input.text()
        is_valid_col_name, _ = self._validate_column_name(col_name_text)
        can_act = (self.selected_cohort_table is not None and
                   len(self.item_list.selectedItems()) > 0 and
                   is_valid_col_name)
        self.preview_merge_btn.setEnabled(can_act)
        self.execute_merge_btn.setEnabled(can_act)

    @Slot()
    def update_default_col_name(self): # Keep as is
        default_name = self._generate_default_column_name()
        if self.new_column_name_input.text() != default_name:
            self.new_column_name_input.blockSignals(True)
            self.new_column_name_input.setText(default_name)
            self.new_column_name_input.blockSignals(False)
        self.update_action_buttons_state()

    def _get_selected_item_ids(self): # Keep as is
        ids = []
        for item in self.item_list.selectedItems():
            data = item.data(Qt.ItemDataRole.UserRole)
            if data: ids.append(data[0])
        return ids

    def _build_merge_query(self, preview_limit=100, for_execution=False): # Keep as is (using the fixed version)
        # --- 1. Initial Checks ---
        if not self.selected_cohort_table: return None, "未选择目标队列数据表.", []
        selected_item_ids = self._get_selected_item_ids()
        if not selected_item_ids: return None, "未选择要合并的项目.", []
        new_col_name_str = self.new_column_name_input.text().strip()
        is_valid_name, name_error = self._validate_column_name(new_col_name_str)
        if not is_valid_name: return None, name_error, []
        # --- 2. Get Configuration and Identifiers ---
        source_event_table, _, name_col_display, id_col_data = self._get_current_source_config()
        if not source_event_table: return None, "无法确定数据来源表.", []
        is_lab = self.rb_lab.isChecked()
        target_table_ident = pgsql.Identifier('mimiciv_data', self.selected_cohort_table)
        new_col_ident = pgsql.Identifier(new_col_name_str)
        cohort_alias = pgsql.Identifier("cohort"); event_alias = pgsql.Identifier("evt")
        fe_alias = pgsql.Identifier("fe"); md_alias = pgsql.Identifier("md"); target_alias = pgsql.Identifier("target")
        # --- 3. Build WHERE Conditions and Parameters ---
        params = []; where_conditions = []
        ids_tuple = tuple(selected_item_ids); id_col_ident = pgsql.Identifier(id_col_data)
        if len(ids_tuple) == 1: where_conditions.append(pgsql.SQL("{}.{} = %s").format(event_alias, id_col_ident)); params.append(ids_tuple[0])
        else: where_conditions.append(pgsql.SQL("{}.{} IN %s").format(event_alias, id_col_ident)); params.append(ids_tuple)
        # --- 4. Determine Time Window Conditions and Columns to Select in CTE ---
        select_event_cols_defs = []; event_value_col = pgsql.Identifier("valuenum"); event_time_col = pgsql.Identifier("charttime")
        select_event_cols_defs.append(pgsql.SQL("{}.hadm_id AS hadm_id").format(event_alias))
        cohort_icu_intime = pgsql.SQL("{}.icu_intime").format(cohort_alias); cohort_icu_outtime = pgsql.SQL("{}.icu_outtime").format(cohort_alias)
        cohort_admittime = pgsql.SQL("{}.admittime").format(cohort_alias); cohort_dischtime = pgsql.SQL("{}.dischtime").format(cohort_alias)
        if is_lab:
            time_option = self.lab_time_window_combo.currentText(); agg_option = self.lab_agg_combo.currentText()
            select_event_cols_defs.append(pgsql.SQL("{}.{} AS {}").format(event_alias, event_value_col, event_value_col))
            select_event_cols_defs.append(pgsql.SQL("{}.{} AS {}").format(event_alias, event_time_col, event_time_col))
            if "24小时" in time_option: where_conditions.append(pgsql.SQL("{}.charttime BETWEEN {} AND {} + interval '24 hours'").format(event_alias, cohort_icu_intime, cohort_icu_intime))
            elif "48小时" in time_option: where_conditions.append(pgsql.SQL("{}.charttime BETWEEN {} AND {} + interval '48 hours'").format(event_alias, cohort_icu_intime, cohort_icu_intime))
            elif "ICU期间" in time_option: where_conditions.append(pgsql.SQL("{}.charttime BETWEEN {} AND {}").format(event_alias, cohort_icu_intime, cohort_icu_outtime))
            elif "住院期间" in time_option: where_conditions.append(pgsql.SQL("{}.charttime BETWEEN {} AND {}").format(event_alias, cohort_admittime, cohort_dischtime))
        else:
            time_option = self.event_time_window_combo.currentText(); output_option = self.event_output_combo.currentText()
            if source_event_table == "mimiciv_hosp.prescriptions": current_event_time_col_name = 'starttime'
            elif source_event_table == "mimiciv_hosp.procedures_icd": current_event_time_col_name = 'chartdate'
            elif source_event_table == "mimiciv_hosp.diagnoses_icd": current_event_time_col_name = None
            else: current_event_time_col_name = 'charttime'
            if current_event_time_col_name:
                event_time_col = pgsql.Identifier(current_event_time_col_name)
                select_event_cols_defs.append(pgsql.SQL("{}.{} AS {}").format(event_alias, event_time_col, event_time_col))
                if "ICU期间" in time_option: where_conditions.append(pgsql.SQL("{}.{} BETWEEN {} AND {}").format(event_alias, event_time_col, cohort_icu_intime, cohort_icu_outtime))
                elif "住院期间" in time_option: where_conditions.append(pgsql.SQL("{}.{} BETWEEN {} AND {}").format(event_alias, event_time_col, cohort_admittime, cohort_dischtime))
        # --- 5. Build the FilteredEvents CTE ---
        filtered_events_cte = pgsql.SQL("FilteredEvents AS (SELECT {select_list} FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.hadm_id = {coh_alias}.hadm_id WHERE {conditions})").format(
            select_list=pgsql.SQL(', ').join(select_event_cols_defs), event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
            cohort_table=target_table_ident, coh_alias=cohort_alias, conditions=pgsql.SQL(' AND ').join(where_conditions))
        # --- 6. Build the Main Aggregation/Selection Query ---
        main_select_query = pgsql.SQL(""); cte_hadm_id = pgsql.Identifier("hadm_id"); group_by_hadm_id_sql = pgsql.SQL(" GROUP BY {}").format(cte_hadm_id)
        if is_lab:
            fe_val = event_value_col; fe_time = event_time_col
            if "首次" in agg_option: main_select_query = pgsql.SQL("SELECT {hadm}, (array_agg({val} ORDER BY {time} ASC))[1] AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, time=fe_time, alias=new_col_ident, gb=group_by_hadm_id_sql)
            elif "末次" in agg_option: main_select_query = pgsql.SQL("SELECT {hadm}, (array_agg({val} ORDER BY {time} DESC))[1] AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, time=fe_time, alias=new_col_ident, gb=group_by_hadm_id_sql)
            elif "最小值" in agg_option: main_select_query = pgsql.SQL("SELECT {hadm}, MIN({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
            elif "最大值" in agg_option: main_select_query = pgsql.SQL("SELECT {hadm}, MAX({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
            elif "平均值" in agg_option: main_select_query = pgsql.SQL("SELECT {hadm}, AVG({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
            elif "计数" in agg_option: main_select_query = pgsql.SQL("SELECT {hadm}, COUNT({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
            else: main_select_query = pgsql.SQL("SELECT {hadm}, (array_agg({val} ORDER BY {time} ASC))[1] AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, time=fe_time, alias=new_col_ident, gb=group_by_hadm_id_sql)
        else:
             if "是否存在" in output_option: main_select_query = pgsql.SQL("SELECT DISTINCT {hadm}, TRUE AS {alias} FROM FilteredEvents").format(hadm=cte_hadm_id, alias=new_col_ident)
             elif "发生次数" in output_option: main_select_query = pgsql.SQL("SELECT {hadm}, COUNT(*) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, alias=new_col_ident, gb=group_by_hadm_id_sql)
             else: main_select_query = pgsql.SQL("SELECT DISTINCT {hadm}, TRUE AS {alias} FROM FilteredEvents").format(hadm=cte_hadm_id, alias=new_col_ident)
        final_select_query_with_cte = pgsql.SQL("WITH {cte} {select_query}").format(cte=filtered_events_cte, select_query=main_select_query)
        # --- 7. Build Final Execution or Preview SQL ---
        if for_execution:
            col_type = pgsql.SQL("NUMERIC")
            if not is_lab:
                 if "是否存在" in output_option: col_type = pgsql.SQL("BOOLEAN")
                 elif "发生次数" in output_option: col_type = pgsql.SQL("INTEGER")
            alter_sql = pgsql.SQL("ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS {col_name} {col_type};").format(target_table=target_table_ident, col_name=new_col_ident, col_type=col_type)
            update_sql = pgsql.SQL("UPDATE {target_table} {tgt_alias} SET {col_name} = {md_alias}.{alias_in_select} FROM ({final_select_query_cte_based}) {md_alias} WHERE {tgt_alias}.hadm_id = {md_alias}.hadm_id;").format(
                target_table=target_table_ident, tgt_alias=target_alias, col_name=new_col_ident, alias_in_select=new_col_ident, final_select_query_cte_based=final_select_query_with_cte, md_alias=md_alias)
            full_execution_sql = alter_sql + pgsql.SQL("\n\n") + update_sql
            return full_execution_sql, None, params
        else:
            preview_sql = pgsql.SQL("WITH MergeData AS ({final_select_query_cte_based}) SELECT {coh_alias}.subject_id, {coh_alias}.hadm_id, {coh_alias}.stay_id, {md_alias}.{alias_in_select} as {new_col_name_preview} FROM {target_table} {coh_alias} LEFT JOIN MergeData {md_alias} ON {coh_alias}.hadm_id = {md_alias}.hadm_id ORDER BY {coh_alias}.subject_id, {coh_alias}.hadm_id LIMIT {limit};").format(
                final_select_query_cte_based=final_select_query_with_cte, alias_in_select=new_col_ident, new_col_name_preview=new_col_ident, target_table=target_table_ident, coh_alias=cohort_alias, md_alias=md_alias, limit=pgsql.Literal(preview_limit))
            return preview_sql, None, params


    def preview_merge_data(self): # Keep as is
        if not self._connect_db(): return
        preview_sql_obj, error_msg, params_list = self._build_merge_query(preview_limit=100, for_execution=False)
        if error_msg: QMessageBox.warning(self, "无法预览", error_msg); return
        if not preview_sql_obj: QMessageBox.warning(self, "无法预览", "未能生成预览SQL。"); return
        sql_string_for_display = "Error generating SQL string for display"
        try:
            sql_string_for_display = preview_sql_obj.as_string(self.db_conn)
            self.sql_preview.setText(f"-- Preview Query:\n{sql_string_for_display}\n-- Params: {params_list}")
            print("Executing Preview SQL:", sql_string_for_display, "Params:", params_list)
            df = pd.read_sql_query(sql_string_for_display, self.db_conn, params=params_list if params_list else None)
            self.preview_table.setRowCount(df.shape[0]); self.preview_table.setColumnCount(df.shape[1])
            self.preview_table.setHorizontalHeaderLabels(df.columns)
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    self.preview_table.setItem(i, j, QTableWidgetItem(str(df.iloc[i, j]) if pd.notna(df.iloc[i, j]) else ""))
            self.preview_table.resizeColumnsToContents()
            QMessageBox.information(self, "预览成功", f"已生成预览数据 ({df.shape[0]} 条)。")
        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"执行预览查询失败: {str(e)}\nSQL: {sql_string_for_display}")
            print(f"Error executing preview query: {str(e)}")

    def execute_merge(self): # MODIFIED to use Worker
        if not self.selected_cohort_table or not self.item_list.selectedItems():
            QMessageBox.warning(self, "选择不完整", "请选择目标表和要合并的项目。"); return
        new_col_name_str = self.new_column_name_input.text().strip()
        is_valid_name, name_error = self._validate_column_name(new_col_name_str)
        if not is_valid_name: QMessageBox.warning(self, "列名无效", name_error); return

        reply = QMessageBox.question(self, '确认操作',
                                     f"确定要向表 '{self.selected_cohort_table}' 中添加/更新列 '{new_col_name_str}' 吗？\n此操作将直接修改数据库表。",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No: return

        # Check DB connection *before* building query (build requires it for as_string sometimes)
        if not self._connect_db(): return # _connect_db handles messages

        execution_sql_obj, error_msg, params_list = self._build_merge_query(for_execution=True)
        if error_msg: QMessageBox.critical(self, "合并失败", f"无法构建SQL: {error_msg}"); return
        if not execution_sql_obj: QMessageBox.critical(self, "合并失败", "未能生成执行SQL。"); return

        db_params = self.get_db_params() # Get fresh params for the worker
        if not db_params: QMessageBox.critical(self, "合并失败", "无法获取数据库连接参数。"); return

        # --- Start Worker Thread ---
        self.prepare_for_long_operation(True) # Prepare UI for long operation

        self.merge_worker = MergeSQLWorker(db_params, execution_sql_obj, params_list,
                                           self.selected_cohort_table, new_col_name_str)
        self.worker_thread = QThread()
        self.merge_worker.moveToThread(self.worker_thread)

        # Connect signals
        self.worker_thread.started.connect(self.merge_worker.run)
        self.merge_worker.finished.connect(self.on_merge_finished)
        self.merge_worker.error.connect(self.on_merge_error)
        self.merge_worker.progress.connect(self.update_execution_progress)
        self.merge_worker.log.connect(self.update_execution_log)

        # Cleanup
        self.merge_worker.finished.connect(self.worker_thread.quit)
        self.merge_worker.error.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.merge_worker.finished.connect(self.merge_worker.deleteLater)
        self.merge_worker.error.connect(self.merge_worker.deleteLater) # Ensure cleanup on error too

        print("Starting merge worker thread...")
        self.worker_thread.start()
        # --- End Worker Thread ---

    def cancel_merge(self):
        """Requests cancellation of the merge operation."""
        if self.merge_worker:
            self.update_execution_log("正在请求取消合并操作...")
            self.merge_worker.cancel()
            self.cancel_merge_btn.setEnabled(False) # Disable after requesting

    # --- Slots for Worker Signals ---
    @Slot()
    def on_merge_finished(self):
        """Handles successful completion of the merge worker."""
        self.update_execution_log(f"成功向表 {self.selected_cohort_table} 添加/更新列 {self.new_column_name_input.text()}。")
        QMessageBox.information(self, "合并成功", f"已成功向表 {self.selected_cohort_table} 添加/更新列 {self.new_column_name_input.text()}。")
        self.prepare_for_long_operation(False) # Reset UI
        # Request preview refresh in export tab
        self.request_preview_signal.emit('mimiciv_data', self.selected_cohort_table)
        self.merge_worker = None # Clear worker reference
        self.worker_thread = None

    @Slot(str)
    def on_merge_error(self, error_message):
        """Handles errors reported by the merge worker."""
        self.update_execution_log(f"合并失败: {error_message}")
        if "操作已取消" not in error_message:
             QMessageBox.critical(self, "合并失败", f"执行合并SQL失败: {error_message}")
        else:
             QMessageBox.information(self, "操作取消", "数据合并操作已取消。")
        self.prepare_for_long_operation(False) # Reset UI
        self.merge_worker = None # Clear worker reference
        self.worker_thread = None
    # --- End Slots for Worker Signals ---


    def closeEvent(self, event):
        # Attempt graceful thread shutdown if running
        if self.worker_thread and self.worker_thread.isRunning():
             self.update_execution_log("正在尝试停止合并操作...")
             if self.merge_worker: self.merge_worker.cancel()
             self.worker_thread.quit()
             if not self.worker_thread.wait(1000): # Wait 1 sec
                 self.update_execution_log("合并线程未能及时停止。")
                 # Consider terminating if necessary, but quit/wait is preferred
                 # self.worker_thread.terminate()
        self._close_db()
        super().closeEvent(event)

# --- END OF FILE tab_combine_special_info.py ---