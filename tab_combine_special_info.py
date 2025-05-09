# --- START OF FILE tab_combine_special_info.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QFormLayout, QRadioButton, QButtonGroup,
                          QLineEdit, QSpinBox, QListWidget, QListWidgetItem, QAbstractItemView,
                          QApplication, QProgressBar)
from PySide6.QtCore import Qt, Signal, Slot, QObject, QThread
import psycopg2
import psycopg2.sql as pgsql
import re
import pandas as pd
import time
import traceback
from conditiongroup import ConditionGroupWidget

# --- Worker Class for Asynchronous Merging (MergeSQLWorker) ---
class MergeSQLWorker(QObject):
    finished = Signal()
    error = Signal(str)
    progress = Signal(int, int)
    log = Signal(str)

    def __init__(self, db_params, execution_steps, target_table_name, new_col_name_str):
        super().__init__()
        self.db_params = db_params
        self.execution_steps = execution_steps
        self.target_table_name = target_table_name
        self.new_col_name_str = new_col_name_str
        self.is_cancelled = False

    def cancel(self):
        self.log.emit("合并操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn_merge = None
        total_actual_steps = len(self.execution_steps)
        current_step_num = 0
        
        self.log.emit(f"开始为表 '{self.target_table_name}' 添加/更新列 '{self.new_col_name_str}' ({total_actual_steps} 个数据库步骤)...")
        self.progress.emit(current_step_num, total_actual_steps)

        try:
            self.log.emit("连接数据库...")
            conn_merge = psycopg2.connect(**self.db_params)
            conn_merge.autocommit = False # Ensure transaction control
            cur = conn_merge.cursor()
            self.log.emit("数据库已连接。")

            for i, (sql_obj_or_str, params_for_step) in enumerate(self.execution_steps):
                current_step_num += 1
                step_description = f"执行数据库步骤 {current_step_num}/{total_actual_steps}"
                
                sql_str_for_log_peek = ""
                current_sql_for_debug = "" # For full SQL if needed
                if isinstance(sql_obj_or_str, (pgsql.Composed, pgsql.SQL)):
                    try:
                        # Try to get a more complete SQL string for debugging if it's complex
                        temp_conn_for_as_string = conn_merge if conn_merge and not conn_merge.closed else psycopg2.connect(**self.db_params)
                        current_sql_for_debug = sql_obj_or_str.as_string(temp_conn_for_as_string)
                        if temp_conn_for_as_string != conn_merge: temp_conn_for_as_string.close()

                        sql_str_for_log_peek = current_sql_for_debug[:200].split('\n')[0] # Peek more
                    except Exception as e_as_string:
                        self.log.emit(f"DEBUG: Error getting SQL as_string: {e_as_string}")
                        sql_str_for_log_peek = str(sql_obj_or_str)[:100].split('\n')[0]
                        current_sql_for_debug = str(sql_obj_or_str) # Fallback
                else: # It's a string
                    current_sql_for_debug = sql_obj_or_str
                    sql_str_for_log_peek = sql_obj_or_str[:100].split('\n')[0]

                if "ALTER TABLE" in sql_str_for_log_peek.upper(): step_description += " (ALTER)"
                elif "CREATE TEMPORARY TABLE" in sql_str_for_log_peek.upper(): step_description += " (CREATE TEMP)"
                elif "UPDATE" in sql_str_for_log_peek.upper(): step_description += " (UPDATE)"
                elif "DROP TABLE" in sql_str_for_log_peek.upper(): step_description += " (DROP TEMP)"

                self.log.emit(f"{step_description}: {sql_str_for_log_peek}...")
                # Log parameters for this step for better debugging
                self.log.emit(f"参数: {params_for_step}")
                
                if self.is_cancelled:
                    raise InterruptedError("操作在执行步骤前被取消。")

                self.log.emit(f"DEBUG: 即将执行步骤 {current_step_num}...") # ADD THIS
                start_time = time.time()
                cur.execute(sql_obj_or_str, params_for_step if params_for_step else None)
                end_time = time.time()
                
                self.log.emit(f"步骤 {current_step_num} 执行成功 (耗时: {end_time - start_time:.2f} 秒)。")
                self.progress.emit(current_step_num, total_actual_steps)

            if self.is_cancelled:
                raise InterruptedError("操作在提交前被取消，正在回滚...")

            self.log.emit("所有数据库步骤完成，正在提交事务...")
            start_commit_time = time.time()
            conn_merge.commit()
            end_commit_time = time.time()
            self.log.emit(f"事务提交成功 (耗时: {end_commit_time - start_commit_time:.2f} 秒)。")
            
            self.finished.emit()

        # ... (exception handling as before, but maybe add current_sql_for_debug to error logs) ...
        except psycopg2.Error as db_err:
            # ...
            self.log.emit(f"相关SQL (完整): {current_sql_for_debug if 'current_sql_for_debug' in locals() else 'N/A'}")
            # ...
        except Exception as e:
            # ...
            self.log.emit(f"相关SQL (完整): {current_sql_for_debug if 'current_sql_for_debug' in locals() else 'N/A'}")
            # ...
        finally:
            if conn_merge and not conn_merge.closed:
                self.log.emit("关闭数据库连接。")
                conn_merge.close()

class SpecialInfoDataExtractionTab(QWidget):
    request_preview_signal = Signal(str, str)

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_cohort_table = None
        self.db_conn = None
        self.db_cursor = None
        self.worker_thread = None
        self.merge_worker = None
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)
        config_widget = QWidget()
        config_layout = QVBoxLayout(config_widget)
        config_layout.setContentsMargins(10, 10, 10, 10); config_layout.setSpacing(10)
        splitter.addWidget(config_widget)
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
        self.execution_status_group = QGroupBox("合并执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,4); self.execution_progress.setValue(0) # Now up to 4 steps
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit(); self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(100)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False)
        config_layout.addWidget(self.execution_status_group)
        action_layout = QHBoxLayout()
        self.preview_merge_btn = QPushButton("预览待合并数据"); self.preview_merge_btn.clicked.connect(self.preview_merge_data); self.preview_merge_btn.setEnabled(False)
        action_layout.addWidget(self.preview_merge_btn)
        self.execute_merge_btn = QPushButton("执行合并到表"); self.execute_merge_btn.clicked.connect(self.execute_merge); self.execute_merge_btn.setEnabled(False)
        action_layout.addWidget(self.execute_merge_btn)
        self.cancel_merge_btn = QPushButton("取消合并"); self.cancel_merge_btn.clicked.connect(self.cancel_merge); self.cancel_merge_btn.setEnabled(False)
        action_layout.addWidget(self.cancel_merge_btn)
        config_layout.addLayout(action_layout)
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
        splitter.setSizes([750, 250])
        self._update_logic_options_visibility()
        self.update_default_col_name()

    def _connect_db(self):
        if self.db_conn and self.db_conn.closed == 0:
            try:
                if not self.db_cursor or self.db_cursor.closed:
                    self.db_cursor = self.db_conn.cursor()
                self.db_conn.isolation_level
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                self.db_conn = None
                self.db_cursor = None
            else:
                return True
        db_params = self.get_db_params()
        if not db_params:
            if not self.isHidden():
                QMessageBox.warning(self, "未连接", "请先在“数据库连接”页面连接数据库。")
            return False
        try:
            self.db_conn = psycopg2.connect(**db_params)
            self.db_cursor = self.db_conn.cursor()
            return True
        except Exception as e:
            if not self.isHidden():
                 QMessageBox.critical(self, "数据库连接失败", f"UI Thread: 无法连接数据库: {str(e)}")
            self.db_conn = None
            self.db_cursor = None
            return False

    def _close_db(self):
        if self.db_cursor:
            try: self.db_cursor.close()
            except Exception as e: print(f"Error closing UI cursor: {e}")
            self.db_cursor = None
        if self.db_conn:
            try: self.db_conn.close()
            except Exception as e: print(f"Error closing UI connection: {e}")
            self.db_conn = None

    def _get_current_source_config(self):
        checked_id = self.direction_group.checkedId()
        if checked_id == 1: return "mimiciv_hosp.labevents", "mimiciv_hosp.d_labitems", "label", "itemid"
        elif checked_id == 2: return "mimiciv_hosp.prescriptions", None, "drug", "drug"
        elif checked_id == 3: return "mimiciv_hosp.procedures_icd", "mimiciv_hosp.d_icd_procedures", "long_title", "icd_code"
        elif checked_id == 4: return "mimiciv_hosp.diagnoses_icd", "mimiciv_hosp.d_icd_diagnoses", "long_title", "icd_code"
        return None, None, None, None

    def _update_logic_options_visibility(self):
        is_lab = self.rb_lab.isChecked()
        self.lab_options_widget.setVisible(is_lab)
        self.event_options_widget.setVisible(not is_lab)

    def _validate_column_name(self, name):
        if not name: return False, "列名不能为空。"
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
             if re.match(r'^_[a-zA-Z0-9_]+$', name): pass
             else: return False, "列名只能包含字母、数字和下划线，且以字母或下划线开头。"
        keywords = {"SELECT", "TABLE", "UPDATE", "COLUMN", "ADD", "ALTER", "WHERE", "FROM"}
        if name.upper() in keywords: return False, f"列名 '{name}' 是 SQL 关键字。"
        if len(name) > 63: return False, f"列名 '{name}' 过长 (最多63个字符)。"
        return True, ""

    def _sanitize_name(self, name):
        if not name: return ""
        name = re.sub(r'[ /\\:,;()\[\]{}"\'.\-+*?^$|<>%]+', '_', name)
        name = re.sub(r'_+', '_', name)
        name = re.sub(r'[^\w]+', '', name)
        name = name.lower().strip('_')
        if name and name[0].isdigit(): name = '_' + name
        return name[:60] if name else "item"

    def _generate_default_column_name(self):
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

    def prepare_for_long_operation(self, starting=True):
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行合并操作...")
            self.preview_merge_btn.setEnabled(False); self.execute_merge_btn.setEnabled(False)
            self.refresh_btn.setEnabled(False); self.table_combo.setEnabled(False)
            self.filter_items_btn.setEnabled(False); self.item_list.setEnabled(False)
            self.condition_widget.setEnabled(False); self.new_column_name_input.setEnabled(False)
            self.lab_options_widget.setEnabled(False); self.event_options_widget.setEnabled(False)
            # Disable radio buttons in the group
            for button in self.direction_group.buttons(): # Iterate through buttons in the group
                button.setEnabled(False)
            self.cancel_merge_btn.setEnabled(True)
        else:
            can_act = (self.selected_cohort_table is not None and
                       len(self.item_list.selectedItems()) > 0 and
                       self._validate_column_name(self.new_column_name_input.text())[0])
            self.preview_merge_btn.setEnabled(can_act)
            self.execute_merge_btn.setEnabled(can_act)
            self.refresh_btn.setEnabled(True); self.table_combo.setEnabled(True)
            self.filter_items_btn.setEnabled(True); self.item_list.setEnabled(True)
            self.condition_widget.setEnabled(True); self.new_column_name_input.setEnabled(True)
            self.lab_options_widget.setEnabled(True); self.event_options_widget.setEnabled(True)
            # Re-enable radio buttons
            for button in self.direction_group.buttons():
                button.setEnabled(True)
            self.cancel_merge_btn.setEnabled(False)

    def update_execution_progress(self, value, max_value=None):
        if max_value is not None and self.execution_progress.maximum() != max_value:
            self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        self.execution_log.append(message)
        QApplication.processEvents()

    def on_db_connected(self):
        self.refresh_btn.setEnabled(True); self.refresh_cohort_tables()
        self.update_default_col_name()

    def refresh_cohort_tables(self):
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
            self.on_cohort_table_selected(self.table_combo.currentIndex())
        except Exception as e: QMessageBox.critical(self, "查询失败", f"无法获取队列数据表列表: {str(e)}")

    def on_cohort_table_selected(self, index):
        if index >= 0 and self.table_combo.count() > 0 and "未找到" not in self.table_combo.itemText(index):
            self.selected_cohort_table = self.table_combo.itemText(index)
        else: self.selected_cohort_table = None
        self.update_action_buttons_state()

    def update_source_config(self, checked, source_table_name, name_col, id_col_for_data):
        if checked:
            self.condition_widget.set_search_field(name_col)
            self._update_logic_options_visibility()
            self.item_list.clear(); self.item_list.addItem("数据源已更改，请点击 [筛选项目] 按钮。")
            self.sql_preview.clear(); self.selected_items_label.setText("已选项目: 0")
            self.update_default_col_name(); self.update_action_buttons_state()

    def _get_readable_sql(self, sql_obj_or_str, params_list):
        if not self._connect_db():
             sql_template_str = str(sql_obj_or_str)
             try:
                 if params_list:
                     # This basic replacement is very limited
                     # It won't correctly format tuples for IN clauses, for example.
                     # It's best to show template and params separately if mogrify isn't possible.
                     # For a single %s and a single param, this might work for simple types.
                     if sql_template_str.count('%s') == len(params_list):
                        safe_params = []
                        for p in params_list:
                            if isinstance(p, str):
                                safe_params.append(f"'{p.replace('%', '%%').replaceSingleQuote()}'") # Basic escape
                            elif isinstance(p, (list, tuple)): # For IN clauses
                                safe_params.append(str(tuple(f"'{sp}'" if isinstance(sp, str) else sp for sp in p)))
                            else:
                                safe_params.append(str(p))
                        return sql_template_str % tuple(safe_params)
                     else: # Mismatch in placeholders and params
                        return f"{sql_template_str} -- PARAMS: {params_list} (Mismatch for basic format)"

                 return sql_template_str
             except Exception as basic_fmt_err:
                print(f"Basic preview formatting error: {basic_fmt_err}")
                return f"{sql_template_str} -- PARAMS: {params_list} (Preview formatting failed)"
        try:
            sql_string_template = sql_obj_or_str.as_string(self.db_conn) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
            return self.db_cursor.mogrify(sql_string_template, params_list).decode(self.db_conn.encoding or 'utf-8')
        except Exception as e:
            print(f"Error using mogrify for readable SQL: {e}")
            base_sql_str = sql_obj_or_str.as_string(self.db_conn) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
            return f"{base_sql_str} -- PARAMS: {params_list} (Mogrify failed)"

    @Slot()
    def filter_source_items(self):
        print("Filter Items button clicked.")
        if not self._connect_db(): return
        source_event_table, source_dict_table, name_col_for_display, id_col_for_data = self._get_current_source_config()
        if not source_event_table: QMessageBox.warning(self, "错误", "未能确定数据来源表。"); return
        condition_sql_template, condition_params = self.condition_widget.get_condition()
        self.item_list.clear(); self.sql_preview.clear(); self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False); QApplication.processEvents()
        query_template_obj = None
        try:
            if not source_dict_table:
                if not condition_sql_template: self.item_list.clear(); self.item_list.addItem("请在上方输入并确认药物关键词后点击 [筛选项目]");
                else: query_template_obj = pgsql.SQL("SELECT DISTINCT {name_col} FROM {event_table} WHERE {condition} ORDER BY {name_col} LIMIT 500").format(name_col=pgsql.Identifier(name_col_for_display), event_table=pgsql.SQL(source_event_table), condition=pgsql.SQL(condition_sql_template))
            else:
                if not condition_sql_template: self.item_list.clear(); self.item_list.addItem("请输入筛选条件后点击 [筛选项目]");
                else: query_template_obj = pgsql.SQL("SELECT {id_col}, {name_col} FROM {dict_table} WHERE {condition} ORDER BY {name_col} LIMIT 500").format(id_col=pgsql.Identifier(id_col_for_data), name_col=pgsql.Identifier(name_col_for_display), dict_table=pgsql.SQL(source_dict_table), condition=pgsql.SQL(condition_sql_template))
            if query_template_obj:
                readable_sql = self._get_readable_sql(query_template_obj, condition_params)
                self.sql_preview.setText(f"-- Item Filter Query (Display Only):\n{readable_sql}")
                print("Executing filter query (template):", query_template_obj.as_string(self.db_conn), "Params:", condition_params)
                start_time = time.time()
                self.db_cursor.execute(query_template_obj, condition_params)
                items = self.db_cursor.fetchall()
                end_time = time.time()
                print(f"Query executed in {end_time - start_time:.2f}s. Found {len(items)} items.")
                self.item_list.clear()
                if items:
                    if not source_dict_table:
                         for item_tuple in items:
                            val = str(item_tuple[0]) if item_tuple[0] is not None else "Unknown"; list_item = QListWidgetItem(val); list_item.setData(Qt.ItemDataRole.UserRole, (val, val)); self.item_list.addItem(list_item)
                    else:
                        for item_id, item_name_disp in items:
                            display_name = str(item_name_disp) if item_name_disp is not None else f"ID_{item_id}"; list_item = QListWidgetItem(f"{display_name} (ID: {item_id})"); list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id), display_name)); self.item_list.addItem(list_item)
                else: self.item_list.addItem("未找到符合条件的项目")
        except Exception as e:
            self.item_list.clear(); QMessageBox.critical(self, "筛选项目失败", f"查询失败: {str(e)}\n{traceback.format_exc()}"); self.item_list.addItem("查询出错")
        finally:
            self.filter_items_btn.setEnabled(True); self.update_default_col_name(); self.update_action_buttons_state()

    @Slot()
    def update_selected_items_count(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.update_action_buttons_state()

    @Slot()
    def update_action_buttons_state(self):
        col_name_text = self.new_column_name_input.text()
        is_valid_col_name, _ = self._validate_column_name(col_name_text)
        can_act = (self.selected_cohort_table is not None and
                   len(self.item_list.selectedItems()) > 0 and
                   is_valid_col_name)
        self.preview_merge_btn.setEnabled(can_act)
        self.execute_merge_btn.setEnabled(can_act)

    @Slot()
    def update_default_col_name(self):
        default_name = self._generate_default_column_name()
        if self.new_column_name_input.text() != default_name:
            self.new_column_name_input.blockSignals(True)
            self.new_column_name_input.setText(default_name)
            self.new_column_name_input.blockSignals(False)
        self.update_action_buttons_state()

    def _get_selected_item_ids(self):
        ids = [];
        for item in self.item_list.selectedItems():
            data = item.data(Qt.ItemDataRole.UserRole)
            if data: ids.append(data[0])
        return ids

    def _build_merge_query(self, preview_limit=100, for_execution=False):
        if not self.selected_cohort_table: return None, "未选择目标队列数据表.", []
        selected_item_ids = self._get_selected_item_ids()
        if not selected_item_ids: return None, "未选择要合并的项目.", []
        new_col_name_str = self.new_column_name_input.text().strip()
        is_valid_name, name_error = self._validate_column_name(new_col_name_str)
        if not is_valid_name: return None, name_error, []
        source_event_table, _, name_col_display, id_col_data = self._get_current_source_config()
        if not source_event_table: return None, "无法确定数据来源表.", []
        is_lab = self.rb_lab.isChecked()
        target_table_ident = pgsql.Identifier('mimiciv_data', self.selected_cohort_table)
        new_col_ident = pgsql.Identifier(new_col_name_str)
        cohort_alias = pgsql.Identifier("cohort"); event_alias = pgsql.Identifier("evt")
        md_alias = pgsql.Identifier("md"); target_alias = pgsql.Identifier("target")
        params = []; where_conditions = []
        ids_tuple = tuple(selected_item_ids); id_col_ident = pgsql.Identifier(id_col_data)
        if len(ids_tuple) == 1: where_conditions.append(pgsql.SQL("{}.{} = %s").format(event_alias, id_col_ident)); params.append(ids_tuple[0])
        else: where_conditions.append(pgsql.SQL("{}.{} IN %s").format(event_alias, id_col_ident)); params.append(ids_tuple)
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
            current_event_time_col_name = None
            if source_event_table == "mimiciv_hosp.prescriptions": current_event_time_col_name = 'starttime'
            elif source_event_table == "mimiciv_hosp.procedures_icd": current_event_time_col_name = 'chartdate'
            elif source_event_table == "mimiciv_hosp.diagnoses_icd": current_event_time_col_name = None
            else: current_event_time_col_name = 'charttime'
            if current_event_time_col_name:
                event_time_col = pgsql.Identifier(current_event_time_col_name)
                select_event_cols_defs.append(pgsql.SQL("{}.{} AS {}").format(event_alias, event_time_col, event_time_col))
                if "ICU期间" in time_option: where_conditions.append(pgsql.SQL("{}.{} BETWEEN {} AND {}").format(event_alias, event_time_col, cohort_icu_intime, cohort_icu_outtime))
                elif "住院期间" in time_option: where_conditions.append(pgsql.SQL("{}.{} BETWEEN {} AND {}").format(event_alias, event_time_col, cohort_admittime, cohort_dischtime))
        
        filtered_events_cte_sql = pgsql.SQL("FilteredEvents AS (SELECT {select_list} FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.hadm_id = {coh_alias}.hadm_id WHERE {conditions})").format(
            select_list=pgsql.SQL(', ').join(select_event_cols_defs), event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
            cohort_table=target_table_ident, coh_alias=cohort_alias, conditions=pgsql.SQL(' AND ').join(where_conditions))
        
        main_aggregation_select_sql = pgsql.SQL(""); cte_hadm_id = pgsql.Identifier("hadm_id"); group_by_hadm_id_sql = pgsql.SQL(" GROUP BY {}").format(cte_hadm_id)
        
        if is_lab:
            agg_option = self.lab_agg_combo.currentText()
            fe_val = event_value_col; fe_time = event_time_col
            if "首次" in agg_option or "末次" in agg_option:
                order_direction = pgsql.SQL("ASC") if "首次" in agg_option else pgsql.SQL("DESC")
                main_aggregation_select_sql = pgsql.SQL(
                    "SELECT {hadm}, (array_agg({val} ORDER BY {time} {direction}))[1] AS {alias} "
                    "FROM FilteredEvents {gb}"
                ).format(hadm=cte_hadm_id, val=fe_val, time=fe_time, direction=order_direction, alias=new_col_ident, gb=group_by_hadm_id_sql)
            else: # Min, Max, Avg, Count
                if "最小值" in agg_option: main_aggregation_select_sql = pgsql.SQL("SELECT {hadm}, MIN({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
                elif "最大值" in agg_option: main_aggregation_select_sql = pgsql.SQL("SELECT {hadm}, MAX({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
                elif "平均值" in agg_option: main_aggregation_select_sql = pgsql.SQL("SELECT {hadm}, AVG({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
                elif "计数" in agg_option: main_aggregation_select_sql = pgsql.SQL("SELECT {hadm}, COUNT({val}) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, val=fe_val, alias=new_col_ident, gb=group_by_hadm_id_sql)
                if not main_aggregation_select_sql: return None, "未知聚合选项 (lab)。", params
        else:
            output_option = self.event_output_combo.currentText()
            if "是否存在" in output_option: main_aggregation_select_sql = pgsql.SQL("SELECT DISTINCT {hadm}, TRUE AS {alias} FROM FilteredEvents").format(hadm=cte_hadm_id, alias=new_col_ident)
            elif "发生次数" in output_option: main_aggregation_select_sql = pgsql.SQL("SELECT {hadm}, COUNT(*) AS {alias} FROM FilteredEvents {gb}").format(hadm=cte_hadm_id, alias=new_col_ident, gb=group_by_hadm_id_sql)
            else: main_aggregation_select_sql = pgsql.SQL("SELECT DISTINCT {hadm}, TRUE AS {alias} FROM FilteredEvents").format(hadm=cte_hadm_id, alias=new_col_ident)

        if not main_aggregation_select_sql: return None, "未能构建主聚合/选择查询。", params

        data_generation_query_part = pgsql.SQL("WITH {filtered_cte} {main_agg_select}").format(
            filtered_cte=filtered_events_cte_sql, main_agg_select=main_aggregation_select_sql)

        if for_execution:
            temp_table_data_name_str = f"temp_merge_data_{new_col_name_str.lower()}_{int(time.time()) % 100000}"
            if len(temp_table_data_name_str) > 63: temp_table_data_name_str = temp_table_data_name_str[:63]
            temp_table_data_ident = pgsql.Identifier(temp_table_data_name_str)
            col_type = pgsql.SQL("NUMERIC")
            if not is_lab:
                 output_option = self.event_output_combo.currentText()
                 if "是否存在" in output_option: col_type = pgsql.SQL("BOOLEAN")
                 elif "发生次数" in output_option: col_type = pgsql.SQL("INTEGER")
            alter_sql = pgsql.SQL("ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS {col_name} {col_type};").format(target_table=target_table_ident, col_name=new_col_ident, col_type=col_type)
            create_temp_table_sql = pgsql.SQL("CREATE TEMPORARY TABLE {temp_table} AS ({data_gen_query});").format(temp_table=temp_table_data_ident, data_gen_query=data_generation_query_part)
            update_sql = pgsql.SQL("UPDATE {target_table} {tgt_alias} SET {col_name} = {tmp_alias}.{alias_in_select} FROM {temp_table} {tmp_alias} WHERE {tgt_alias}.hadm_id = {tmp_alias}.hadm_id;").format(
                target_table=target_table_ident, tgt_alias=target_alias, col_name=new_col_ident, alias_in_select=new_col_ident, temp_table=temp_table_data_ident, tmp_alias=md_alias)
            drop_temp_table_sql = pgsql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_data_ident)
            execution_steps = [(alter_sql, None), (create_temp_table_sql, params), (update_sql, None), (drop_temp_table_sql, None)]
            return execution_steps, "execution_list", None
        else: # Preview
            preview_sql = pgsql.SQL("WITH MergeData AS ({data_gen_query}) SELECT {coh_alias}.subject_id, {coh_alias}.hadm_id, {coh_alias}.stay_id, {md_alias}.{alias_in_select} as {new_col_name_preview} FROM {target_table} {coh_alias} LEFT JOIN MergeData {md_alias} ON {coh_alias}.hadm_id = {md_alias}.hadm_id ORDER BY {coh_alias}.subject_id, {coh_alias}.hadm_id LIMIT {limit};").format(
                data_gen_query=data_generation_query_part, alias_in_select=new_col_ident, new_col_name_preview=new_col_ident, target_table=target_table_ident, coh_alias=cohort_alias, md_alias=md_alias, limit=pgsql.Literal(preview_limit))
            return preview_sql, None, params


    def preview_merge_data(self):
        # if not self._connect_db(): return # self._connect_db() 可能会返回一个新连接或重用 self.db_conn
        
        # === 获取连接，关键在于如何管理这个连接的事务 ===
        conn_for_preview = None # 使用一个独立的连接变量
        owns_connection = False # 标记是否在此方法中创建了连接

        if self.db_conn and self.db_conn.closed == 0:
            # 选项1：如果重用 self.db_conn，需要确保其事务状态
            # 假设 self.db_conn.autocommit 可能为 False
            # 最好是为预览操作使用它自己的事务上下文或一个有 autocommit=True 的连接
            # 为了简单和安全，这里倾向于为只读操作创建一个新连接或确保 autocommit
            
            # 简单粗暴的方式（如果 self.db_conn 可能有未提交的事务，这能避免干扰）：
            # self.db_conn.rollback() # 回滚任何可能存在的之前的事务状态
            
            # 或者，为预览创建一个新的连接，这样它有自己的事务空间
            db_params = self.get_db_params()
            if not db_params:
                QMessageBox.warning(self, "未连接", "请先在“数据库连接”页面连接数据库。")
                return
            try:
                conn_for_preview = psycopg2.connect(**db_params)
                conn_for_preview.autocommit = True # 对于只读预览，可以设置 autocommit 为 True
                owns_connection = True
            except Exception as e:
                QMessageBox.critical(self, "数据库连接失败", f"无法为预览连接数据库: {str(e)}")
                return
        else: # self.db_conn 不可用或已关闭，尝试 _connect_db
            if not self._connect_db(): # _connect_db 应该会设置 self.db_conn
                return # _connect_db 内部会显示错误消息
            conn_for_preview = self.db_conn # 使用 _connect_db 设置的连接
            # 假设 _connect_db 返回的连接，如果不是 autocommit，需要我们管理
            # 如果 _connect_db 创建新连接，它应该处理其 autocommit 或我们在这里处理
            # 为安全起见，如果不是自己创建的，最好不要修改它的 autocommit 状态
            # 更好的做法是 _connect_db 明确返回连接，而不是只设置 self.db_conn

        # === 如果仍然没有有效的 conn_for_preview，则使用 self.db_conn ===
        # 这是一个回退，但需要更仔细的事务管理
        if conn_for_preview is None:
            if not self._connect_db(): return # 确保 self.db_conn 是活的
            conn_for_preview = self.db_conn
            # 如果 self.db_conn.autocommit is False, 我们需要在 finally 中 rollback
            # 因为这是一个只读操作，所以 rollback 是安全的，不会丢失数据。

        preview_sql_obj, error_msg, params_list = self._build_merge_query(preview_limit=100, for_execution=False)
        if error_msg: QMessageBox.warning(self, "无法预览", error_msg); return
        if not preview_sql_obj: QMessageBox.warning(self, "无法预览", "未能生成预览SQL。"); return

        # _get_readable_sql 可能也需要一个连接，确保它使用正确的连接
        # readable_sql = self._get_readable_sql(preview_sql_obj, params_list) # 这行可以移到 try 块之后，如果预览失败就不需要了

        try:
            # 确保使用 conn_for_preview 来执行
            # _get_readable_sql 内部如果也用 self.db_cursor.mogrify，它也需要一个活动的连接和游标
            # 我们需要确保 _get_readable_sql 使用的是 conn_for_preview
            sql_for_execution = ""
            readable_sql_for_display = ""

            if hasattr(preview_sql_obj, 'as_string'):
                sql_for_execution = preview_sql_obj.as_string(conn_for_preview) # 使用 conn_for_preview
            else:
                sql_for_execution = str(preview_sql_obj)
            
            # 对于 mogrify 获取可读 SQL
            try:
                with conn_for_preview.cursor() as temp_cur:
                    readable_sql_for_display = temp_cur.mogrify(sql_for_execution, params_list if params_list else None).decode(conn_for_preview.encoding or 'utf-8')
            except Exception as e_mogrify:
                readable_sql_for_display = f"{sql_for_execution} -- PARAMS: {params_list} (Mogrify failed: {e_mogrify})"

            self.sql_preview.setText(f"-- Preview Query (Display Only):\n{readable_sql_for_display}")
            print("Executing Preview SQL (template):", sql_for_execution, "Params:", params_list)

            # 使用 conn_for_preview 执行查询
            df = pd.read_sql_query(sql_for_execution, conn_for_preview, params=params_list if params_list else None)
            
            self.preview_table.setRowCount(df.shape[0]); self.preview_table.setColumnCount(df.shape[1])
            self.preview_table.setHorizontalHeaderLabels(df.columns)
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    self.preview_table.setItem(i, j, QTableWidgetItem(str(df.iloc[i, j]) if pd.notna(df.iloc[i, j]) else ""))
            self.preview_table.resizeColumnsToContents()
            QMessageBox.information(self, "预览成功", f"已生成预览数据 ({df.shape[0]} 条)。")

            # 如果 conn_for_preview 不是 autocommit=True，并且不是自己拥有的，则不应 commit
            # 如果是只读，并且不是 autocommit，理论上应该 rollback 来释放资源，尽管 SELECT 不会改数据
            if not conn_for_preview.autocommit and not owns_connection:
                conn_for_preview.rollback() # 确保事务结束，释放可能的锁

        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"执行预览查询失败: {str(e)}\nSQL (Readable Approx.):\n{readable_sql_for_display}")
            print(f"Error executing preview query: {str(e)}")
            if conn_for_preview and not conn_for_preview.autocommit and not owns_connection :
                try:
                    conn_for_preview.rollback() # 发生错误也要回滚
                except Exception as rb_err:
                    print(f"Rollback after error failed: {rb_err}")
        finally:
            # 如果是这个方法创建的连接，由它负责关闭
            if owns_connection and conn_for_preview:
                try:
                    conn_for_preview.close()
                except Exception as e_close:
                    print(f"Error closing preview-specific connection: {e_close}")
            # 如果使用的是 self.db_conn，不要在这里关闭它，除非 _connect_db 的逻辑变了
            # 但至少要确保事务结束了 (通过 autocommit=True，或显式 rollback/commit)

    def execute_merge(self):
        if not self.selected_cohort_table or not self.item_list.selectedItems():
            QMessageBox.warning(self, "选择不完整", "请选择目标表和要合并的项目。"); return
        new_col_name_str = self.new_column_name_input.text().strip()
        is_valid_name, name_error = self._validate_column_name(new_col_name_str)
        if not is_valid_name: QMessageBox.warning(self, "列名无效", name_error); return
        reply = QMessageBox.question(self, '确认操作',
                                     f"确定要向表 '{self.selected_cohort_table}' 中添加/更新列 '{new_col_name_str}' 吗？\n此操作将直接修改数据库表。",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No: return
        if not self._connect_db(): return

        execution_steps_list, signal_type, _ = self._build_merge_query(for_execution=True)
        if signal_type != "execution_list" or not execution_steps_list :
            error_msg_from_build = execution_steps_list if isinstance(execution_steps_list, str) else "未能生成执行SQL步骤。"
            QMessageBox.critical(self, "合并失败", f"无法构建SQL: {error_msg_from_build}"); return
        
        db_params = self.get_db_params()
        if not db_params: QMessageBox.critical(self, "合并失败", "无法获取数据库连接参数。"); return
        
        preview_text = "-- 准备执行多个SQL步骤 --"
        if len(execution_steps_list) > 1:
            # The second step is CREATE TEMP TABLE which has the params for FilteredEvents
            create_temp_sql_obj, create_temp_params = execution_steps_list[1] 
            preview_text = f"-- 主数据生成步骤 (预览):\n{self._get_readable_sql(create_temp_sql_obj, create_temp_params)}"
        self.sql_preview.setText(preview_text)
        QApplication.processEvents()

        self.prepare_for_long_operation(True)
        self.merge_worker = MergeSQLWorker(db_params, execution_steps_list,
                                           self.selected_cohort_table, new_col_name_str)
        self.worker_thread = QThread()
        self.merge_worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.merge_worker.run)
        self.merge_worker.finished.connect(self.on_merge_finished)
        self.merge_worker.error.connect(self.on_merge_error)
        self.merge_worker.progress.connect(self.update_execution_progress)
        self.merge_worker.log.connect(self.update_execution_log)
        self.merge_worker.finished.connect(self.worker_thread.quit)
        self.merge_worker.error.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.merge_worker.finished.connect(self.merge_worker.deleteLater)
        self.merge_worker.error.connect(self.merge_worker.deleteLater)
        print("Starting merge worker thread...")
        self.worker_thread.start()

    def cancel_merge(self):
        if self.merge_worker:
            self.update_execution_log("正在请求取消合并操作...")
            self.merge_worker.cancel()
            self.cancel_merge_btn.setEnabled(False)

    @Slot()
    def on_merge_finished(self):
        self.update_execution_log(f"成功向表 {self.selected_cohort_table} 添加/更新列 {self.new_column_name_input.text()}。")
        QMessageBox.information(self, "合并成功", f"已成功向表 {self.selected_cohort_table} 添加/更新列 {self.new_column_name_input.text()}。")
        self.prepare_for_long_operation(False)
        self.request_preview_signal.emit('mimiciv_data', self.selected_cohort_table)
        self.merge_worker = None; self.worker_thread = None

    @Slot(str)
    def on_merge_error(self, error_message):
        self.update_execution_log(f"合并失败: {error_message}")
        if "操作已取消" not in error_message:
             QMessageBox.critical(self, "合并失败", f"执行合并SQL失败: {error_message}")
        else: QMessageBox.information(self, "操作取消", "数据合并操作已取消。")
        self.prepare_for_long_operation(False)
        self.merge_worker = None; self.worker_thread = None

    def closeEvent(self, event):
        if self.worker_thread and self.worker_thread.isRunning():
             self.update_execution_log("正在尝试停止合并操作...")
             if self.merge_worker: self.merge_worker.cancel()
             self.worker_thread.quit()
             if not self.worker_thread.wait(1000): self.update_execution_log("合并线程未能及时停止。")
        self._close_db()
        super().closeEvent(event)

# --- END OF FILE tab_combine_special_info.py ---