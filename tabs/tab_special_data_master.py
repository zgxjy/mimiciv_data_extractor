# --- START OF FILE tab_special_data_master.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox,
                          QRadioButton, QButtonGroup, QStackedWidget,
                          QLineEdit, QProgressBar, QAbstractItemView, QApplication)
# QCheckBox 不再直接在主Tab中使用，它们在UI组件里
from PySide6.QtCore import Qt, Signal, Slot, QObject, QThread
import psycopg2
import psycopg2.sql as pgsql
import re
import pandas as pd
import time
import traceback
from source_panels.base_panel import BaseSourceConfigPanel # 仍然需要，但主要是为了类型提示
from source_panels.chartevents_panel import CharteventsConfigPanel
from source_panels.labevents_panel import LabeventsConfigPanel
from source_panels.medication_panel import MedicationConfigPanel
from source_panels.procedure_panel import ProcedureConfigPanel
from source_panels.diagnosis_panel import DiagnosisConfigPanel
from sql_logic.sql_builder_special import build_special_data_sql # 导入SQL构建函数
from utils import sanitize_name_part

# (MergeSQLWorker 类代码保持不变，直接复制过来)
class MergeSQLWorker(QObject):
    finished = Signal()
    error = Signal(str)
    progress = Signal(int, int)
    log = Signal(str)
    def __init__(self, db_params, execution_steps, target_table_name, new_cols_description_str):
        super().__init__()
        self.db_params = db_params
        self.execution_steps = execution_steps
        self.target_table_name = target_table_name
        self.new_cols_description_str = new_cols_description_str
        self.is_cancelled = False
        self.current_sql_for_debug = ""
    def cancel(self): self.log.emit("合并操作被请求取消..."); self.is_cancelled = True
    def run(self):
        conn_merge = None
        total_actual_steps = len(self.execution_steps)
        current_step_num = 0
        self.log.emit(f"开始为表 '{self.target_table_name}' 添加/更新列 (基于: {self.new_cols_description_str})，共 {total_actual_steps} 个数据库步骤...")
        self.progress.emit(current_step_num, total_actual_steps)
        try:
            self.log.emit("连接数据库..."); conn_merge = psycopg2.connect(**self.db_params); conn_merge.autocommit = False; cur = conn_merge.cursor(); self.log.emit("数据库已连接。")
            for i, (sql_obj_or_str, params_for_step) in enumerate(self.execution_steps):
                current_step_num += 1; step_description = f"执行数据库步骤 {current_step_num}/{total_actual_steps}"
                sql_str_for_log_peek = ""; self.current_sql_for_debug = "" 
                if isinstance(sql_obj_or_str, (pgsql.Composed, pgsql.SQL)):
                    try: self.current_sql_for_debug = sql_obj_or_str.as_string(conn_merge); sql_str_for_log_peek = self.current_sql_for_debug[:200].split('\n')[0]
                    except Exception as e_as_string: self.log.emit(f"DEBUG: Error getting SQL as_string: {e_as_string}"); sql_str_for_log_peek = str(sql_obj_or_str)[:100].split('\n')[0]; self.current_sql_for_debug = str(sql_obj_or_str) 
                else: self.current_sql_for_debug = sql_obj_or_str; sql_str_for_log_peek = sql_obj_or_str[:100].split('\n')[0]
                if "ALTER TABLE" in sql_str_for_log_peek.upper(): step_description += " (ALTER)"
                elif "CREATE TEMPORARY TABLE" in sql_str_for_log_peek.upper(): step_description += " (CREATE TEMP)"
                elif "UPDATE" in sql_str_for_log_peek.upper(): step_description += " (UPDATE)"
                elif "DROP TABLE" in sql_str_for_log_peek.upper(): step_description += " (DROP TEMP)"
                self.log.emit(f"{step_description}: {sql_str_for_log_peek}..."); 
                if self.is_cancelled: raise InterruptedError("操作在执行步骤前被取消。")
                start_time = time.time(); cur.execute(sql_obj_or_str, params_for_step if params_for_step else None); end_time = time.time()
                self.log.emit(f"步骤 {current_step_num} 执行成功 (耗时: {end_time - start_time:.2f} 秒)。"); self.progress.emit(current_step_num, total_actual_steps)
            if self.is_cancelled: raise InterruptedError("操作在提交前被取消，正在回滚...")
            self.log.emit("所有数据库步骤完成，正在提交事务..."); start_commit_time = time.time(); conn_merge.commit(); end_commit_time = time.time(); self.log.emit(f"事务提交成功 (耗时: {end_commit_time - start_commit_time:.2f} 秒)。")
            self.finished.emit()
        except InterruptedError as ie: 
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            self.log.emit(f"操作已取消: {str(ie)}"); self.error.emit("操作已取消")
        except psycopg2.Error as db_err:
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            err_msg = f"数据库错误: {db_err}\n相关SQL (完整): {self.current_sql_for_debug}"
            self.log.emit(err_msg); self.log.emit(f"Traceback: {traceback.format_exc()}"); self.error.emit(err_msg)
        except Exception as e:
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            err_msg = f"发生意外错误: {e}\n相关SQL (完整): {self.current_sql_for_debug}"
            self.log.emit(err_msg); self.log.emit(f"Traceback: {traceback.format_exc()}"); self.error.emit(err_msg)
        finally:
            if conn_merge and not conn_merge.closed: self.log.emit("关闭数据库连接。"); conn_merge.close()


class SpecialDataMasterTab(QWidget):
    request_preview_signal = Signal(str, str)

    SOURCE_LABEVENTS = 1; SOURCE_MEDICATIONS = 2; SOURCE_PROCEDURES = 3
    SOURCE_DIAGNOSES = 4; SOURCE_CHARTEVENTS = 5

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_cohort_table = None
        self.worker_thread = None
        self.merge_worker = None
        self.config_panels = {}
        self.init_ui()
        self.rb_chartevents.setChecked(True) # 默认选中并触发初始更新

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Vertical); main_layout.addWidget(splitter)
        config_widget = QWidget(); config_layout = QVBoxLayout(config_widget)
        config_layout.setContentsMargins(10,10,10,10); config_layout.setSpacing(10)
        splitter.addWidget(config_widget)

        # 1. 目标队列表
        cohort_group = QGroupBox("1. 选择目标队列数据表")
        cohort_layout = QHBoxLayout(cohort_group)
        cohort_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox(); self.table_combo.setMinimumWidth(250)
        self.table_combo.currentIndexChanged.connect(self.on_cohort_table_selected)
        cohort_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新列表"); self.refresh_btn.clicked.connect(self.refresh_cohort_tables); self.refresh_btn.setEnabled(False)
        cohort_layout.addWidget(self.refresh_btn); cohort_layout.addStretch()
        config_layout.addWidget(cohort_group)

        # 2. 数据来源选择 和 特定配置面板
        source_and_panel_group = QGroupBox("2. 选择数据来源并配置提取项")
        source_main_layout = QVBoxLayout(source_and_panel_group)
        source_select_layout = QHBoxLayout()
        source_select_layout.addWidget(QLabel("数据来源:"))
        self.source_selection_group = QButtonGroup(self)
        self.rb_lab = QRadioButton("化验 (labevents)"); self.rb_med = QRadioButton("用药 (prescriptions)")
        self.rb_proc = QRadioButton("操作/手术 (proc.)"); self.rb_diag = QRadioButton("诊断 (diag.)")
        self.rb_chartevents = QRadioButton("监测指标 (chartevents)")
        self.source_radio_buttons_map = {
            self.SOURCE_LABEVENTS: self.rb_lab, self.SOURCE_MEDICATIONS: self.rb_med,
            self.SOURCE_PROCEDURES: self.rb_proc, self.SOURCE_DIAGNOSES: self.rb_diag,
            self.SOURCE_CHARTEVENTS: self.rb_chartevents,
        }
        for sid, rb in self.source_radio_buttons_map.items():
            self.source_selection_group.addButton(rb, sid); source_select_layout.addWidget(rb)
        source_select_layout.addStretch(); source_main_layout.addLayout(source_select_layout)
        self.search_field_hint_label = QLabel()
        self.search_field_hint_label.setStyleSheet("font-style: italic; color: gray;")
        source_main_layout.addWidget(self.search_field_hint_label)
        self.config_panel_stack = QStackedWidget()
        source_main_layout.addWidget(self.config_panel_stack)
        self._create_config_panels()
        config_layout.addWidget(source_and_panel_group)

        # 3. 新列基础名 (这是唯一保留在主Tab的“通用逻辑”UI元素)
        column_name_group = QGroupBox("3. 定义新列基础名")
        column_name_layout = QHBoxLayout(column_name_group)
        column_name_layout.addWidget(QLabel("新列基础名 (可修改):"))
        self.new_column_name_input = QLineEdit()
        self.new_column_name_input.setPlaceholderText("根据选择自动生成或手动输入...")
        self.new_column_name_input.textChanged.connect(self._on_master_config_changed)
        column_name_layout.addWidget(self.new_column_name_input, 1)
        config_layout.addWidget(column_name_group)

        # 执行状态组 和 操作按钮 (保持不变)
        self.execution_status_group = QGroupBox("合并执行状态") # ... (内部结构不变)
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,4); self.execution_progress.setValue(0)
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

        # 下方结果预览区域 (保持不变)
        result_widget = QWidget() # ... (内部结构不变)
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(10, 10, 10, 10); result_layout.setSpacing(10)
        splitter.addWidget(result_widget)
        result_layout.addWidget(QLabel("SQL预览 (仅供参考):"))
        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(120)
        result_layout.addWidget(self.sql_preview)
        result_layout.addWidget(QLabel("数据预览 (最多100条):"))
        self.preview_table = QTableWidget(); self.preview_table.setAlternatingRowColors(True); self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        result_layout.addWidget(self.preview_table)
        splitter.setSizes([650, 350]) # 调整初始比例

        self.source_selection_group.buttonToggled.connect(self._on_source_type_changed)
        # self.rb_chartevents.setChecked(True) # 移到 __init__ 末尾确保所有UI都已创建

    def _create_config_panels(self): # (与上一个回复版本相同)
        panels_to_create = [
            (self.SOURCE_CHARTEVENTS, CharteventsConfigPanel),
            (self.SOURCE_LABEVENTS, LabeventsConfigPanel),
            (self.SOURCE_MEDICATIONS, MedicationConfigPanel),
            (self.SOURCE_PROCEDURES, ProcedureConfigPanel),
            (self.SOURCE_DIAGNOSES, DiagnosisConfigPanel),
        ]
        for source_id, PanelClass in panels_to_create:
            panel = PanelClass(self.get_db_params, self)
            panel.config_changed_signal.connect(self._on_panel_config_changed)
            self.config_panel_stack.addWidget(panel)
            self.config_panels[source_id] = panel
            
    def _update_active_panel(self): # (与上一个回复版本相同)
        current_id = self.source_selection_group.checkedId()
        active_panel = self.config_panels.get(current_id)
        if active_panel:
            self.config_panel_stack.setCurrentWidget(active_panel)
            active_panel.populate_panel_if_needed()
            details = active_panel.get_item_filtering_details()
            hint = details[3] if details and len(details) > 3 else "配置筛选条件。"
            self.search_field_hint_label.setText(hint)
            self._generate_and_set_default_col_name() 
            self.update_master_action_buttons_state()
        else:
            self.search_field_hint_label.setText("请选择一个数据来源。")

    @Slot(int, bool) # 确保槽函数签名与信号匹配
    def _on_source_type_changed(self, id, checked): # (与上一个回复版本相同)
        if checked: self._update_active_panel()

    @Slot()
    def _on_panel_config_changed(self): # (与上一个回复版本相同)
        self._generate_and_set_default_col_name()
        self.update_master_action_buttons_state()

    @Slot()
    def _on_master_config_changed(self): # (与上一个回复版本相同)
        self._generate_and_set_default_col_name()
        self.update_master_action_buttons_state()
        
    # _update_common_logic_ui_visibility 方法可以移除了，因为通用UI已移到面板的子组件
    # def _update_common_logic_ui_visibility(self): ...

    def _generate_and_set_default_col_name(self): # (与上一个回复版本相同，它现在从活动面板的子组件获取信息)
        parts = []
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if not active_panel: self.new_column_name_input.setText("new_col"); return

        logic_code = "data" 
        if hasattr(active_panel, 'value_agg_widget') and active_panel.value_agg_widget:
            selected_methods = active_panel.value_agg_widget.get_selected_methods()
            is_text = False
            if hasattr(active_panel, 'value_type_combo') and active_panel.value_type_combo.currentData() == "value": is_text = True
            map_num = { "first": "first", "last": "last", "min": "min", "max": "max", "mean": "mean", "countval": "countval" }
            map_text = { "first": "first_text", "last": "last_text", "min": "min_text", "max": "max_text", "countval": "counttext" }
            current_map = map_text if is_text else map_num
            found = False
            for key, chk in selected_methods.items():
                if chk: logic_code = current_map.get(key, "val"); found = True; break
            if not found: logic_code = "val"
        elif hasattr(active_panel, 'event_output_widget') and active_panel.event_output_widget:
            selected_outputs = active_panel.event_output_widget.get_selected_outputs()
            map_evt = {"exists": "has", "countevt": "count"}
            found = False
            for key, chk in selected_outputs.items():
                if chk: logic_code = map_evt.get(key, "evt"); found = True; break
            if not found: logic_code = "evt"
        parts.append(logic_code)

        item_name_part = "item"
        if hasattr(active_panel, 'item_list') and active_panel.item_list and active_panel.item_list.selectedItems():
            try: item_name_part = sanitize_name_part(active_panel.item_list.selectedItems()[0].data(Qt.ItemDataRole.UserRole)[1])
            except: item_name_part = sanitize_name_part(active_panel.item_list.selectedItems()[0].text().split('(')[0].strip())
        parts.append(item_name_part)
        
        time_code = ""
        if hasattr(active_panel, 'time_window_widget') and active_panel.time_window_widget:
            current_time_text = active_panel.time_window_widget.get_current_time_window_text()
            if current_time_text:
                time_map_val = {"ICU入住后24小时": "icu24h", "ICU入住后48小时": "icu48h", "整个ICU期间": "icuall", "整个住院期间": "hospall"}
                time_map_evt = {"整个住院期间 (当前入院)": "hosp", "整个ICU期间 (当前入院)": "icu", "住院以前 (既往史)": "prior"}
                current_time_map = time_map_val if hasattr(active_panel, 'value_agg_widget') else time_map_evt
                time_code = current_time_map.get(current_time_text, sanitize_name_part(current_time_text.split(" ")[0]))
        if time_code: parts.append(time_code)
        
        default_name = "_".join(filter(None, parts)); final_name = (default_name if default_name else "new_col")[:50]
        if final_name and final_name[0].isdigit(): final_name = "_" + final_name 
        if self.new_column_name_input.text() != final_name:
            self.new_column_name_input.blockSignals(True); self.new_column_name_input.setText(final_name); self.new_column_name_input.blockSignals(False)
        self.update_master_action_buttons_state()

    def _are_configs_valid_for_action(self) -> bool: # (与上一个回复版本相同)
        if not self.selected_cohort_table: return False
        col_name_text = self.new_column_name_input.text()
        is_valid_col_name, _ = self._validate_column_name(col_name_text)
        if not is_valid_col_name: return False
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if not active_panel or not active_panel.get_selected_item_ids(): return False
        any_method_selected = False
        if hasattr(active_panel, 'value_agg_widget') and hasattr(active_panel.value_agg_widget, 'get_selected_methods'): # Check if widget and method exist
            any_method_selected = any(active_panel.value_agg_widget.get_selected_methods().values())
        elif hasattr(active_panel, 'event_output_widget') and hasattr(active_panel.event_output_widget, 'get_selected_outputs'):
            any_method_selected = any(active_panel.event_output_widget.get_selected_outputs().values())
        panel_specific_methods = active_panel.get_specific_aggregation_methods() if active_panel else None
        any_panel_method_selected = any(panel_specific_methods.values()) if panel_specific_methods else False
        return any_method_selected or any_panel_method_selected

    # _build_merge_query, execute_merge, preview_merge_data, 等方法与上一个回复中调用 build_special_data_sql 的版本保持一致
    def _build_merge_query(self, preview_limit=100, for_execution=False):
        if not self.selected_cohort_table: return None, "未选择目标队列数据表.", [], []
        base_new_col_name = self.new_column_name_input.text().strip()
        is_valid_base_name, name_error = self._validate_column_name(base_new_col_name)
        if not is_valid_base_name: return None, f"基础列名错误: {name_error}", [], []
        active_panel_id = self.source_selection_group.checkedId()
        active_panel = self.config_panels.get(active_panel_id)
        if not active_panel: return None, "未选择有效的数据来源或未找到配置面板.", [], []
        panel_config_dict = active_panel.get_panel_config()
        
        try:
            return build_special_data_sql(
                target_cohort_table_name=f"mimiciv_data.{self.selected_cohort_table}",
                base_new_column_name=base_new_col_name,
                panel_specific_config=panel_config_dict,
                for_execution=for_execution,
                preview_limit=preview_limit
            )
        except Exception as e:
            error_msg = f"构建SQL时发生内部错误: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            return None, error_msg, [], []

    # 其他方法 (prepare_for_long_operation, on_db_connected, refresh_cohort_tables, etc.)
    # 与上一个回复中的 SpecialDataMasterTab 版本保持一致
    def prepare_for_long_operation(self, starting=True):
        is_enabled = not starting
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行合并操作...")
        self.table_combo.setEnabled(is_enabled); self.refresh_btn.setEnabled(is_enabled)
        for rb_button in self.source_selection_group.buttons(): rb_button.setEnabled(is_enabled) # Corrected iteration
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel: active_panel.setEnabled(is_enabled)
        self.new_column_name_input.setEnabled(is_enabled)
        self.cancel_merge_btn.setEnabled(starting)
        if not starting: self.update_master_action_buttons_state()
        else: self.preview_merge_btn.setEnabled(False); self.execute_merge_btn.setEnabled(False)

    def update_execution_progress(self, value, max_value=None):
        if max_value is not None and self.execution_progress.maximum() != max_value: self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        self.execution_log.append(message); QApplication.processEvents()

    @Slot()
    def on_db_connected(self):
        self.refresh_btn.setEnabled(True); self.refresh_cohort_tables()
        # self._generate_and_set_default_col_name() # on_cohort_table_selected -> update_master_action_buttons_state会触发

    def refresh_cohort_tables(self):
        db_params = self.get_db_params(); conn = None
        if not db_params: return
        try:
            conn = psycopg2.connect(**db_params); cur = conn.cursor()
            cur.execute(pgsql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'mimiciv_data' AND (table_name LIKE 'first_%_admissions' OR table_name LIKE 'all_%_admissions') ORDER BY table_name"))
            tables = [r[0] for r in cur.fetchall()]
            current_sel = self.table_combo.currentText(); self.table_combo.blockSignals(True); self.table_combo.clear()
            if tables: self.table_combo.addItems(tables); idx = self.table_combo.findText(current_sel); self.table_combo.setCurrentIndex(idx if idx!=-1 else (0 if tables else -1))
            else: self.table_combo.addItem("未找到队列表")
            self.table_combo.blockSignals(False); self.on_cohort_table_selected(self.table_combo.currentIndex())
        except Exception as e: QMessageBox.critical(self, "查询失败", f"无法获取队列表: {str(e)}")
        finally:
            if conn: conn.close()
        
    def on_cohort_table_selected(self, index):
        current_text = self.table_combo.itemText(index)
        if index >= 0 and current_text and "未找到" not in current_text: self.selected_cohort_table = current_text
        else: self.selected_cohort_table = None
        self.update_master_action_buttons_state()
        
    @Slot()
    def update_master_action_buttons_state(self): # (与上一个回复版本相同)
        is_valid = self._are_configs_valid_for_action()
        self.preview_merge_btn.setEnabled(is_valid)
        self.execute_merge_btn.setEnabled(is_valid)
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel:
            general_ok = bool(self.get_db_params()) 
            active_panel.update_panel_action_buttons_state(general_ok) # 让面板更新自己的按钮（如筛选按钮）

    def execute_merge(self): # (与上一个回复版本相同)
        if not self._are_configs_valid_for_action(): QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写。"); return
        build_result = self._build_merge_query(for_execution=True)
        if build_result is None or len(build_result) < 4: QMessageBox.critical(self, "内部错误", "构建合并查询时未能返回预期结果。"); return
        execution_steps_list, signal_type, new_cols_desc_for_worker, column_details_for_dialog = build_result
        if signal_type != "execution_list": QMessageBox.critical(self, "合并准备失败", f"无法构建SQL: {signal_type if isinstance(signal_type, str) else '未知错误'}"); return
        if not column_details_for_dialog: QMessageBox.warning(self, "无法继续", "未能生成列详情以供确认。"); return
        column_preview_message = f"确定要向表 '{self.selected_cohort_table}' 中添加/更新以下列吗？\n" + "\n".join([f" - {name} (类型: {type_str})" for name, type_str in column_details_for_dialog]) + "\n\n此操作将直接修改数据库表。"
        if QMessageBox.question(self, '确认操作', column_preview_message, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No: return
        db_params = self.get_db_params()
        if not db_params: QMessageBox.critical(self, "合并失败", "无法获取数据库连接参数。"); return
        preview_text = f"-- 准备为表 {self.selected_cohort_table} 添加列: {new_cols_desc_for_worker} --"; self.sql_preview.setText(preview_text); QApplication.processEvents() # Simplified preview for brevity
        self.prepare_for_long_operation(True)
        self.merge_worker = MergeSQLWorker(db_params, execution_steps_list, self.selected_cohort_table, new_cols_desc_for_worker)
        self.worker_thread = QThread(); self.merge_worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.merge_worker.run)
        self.merge_worker.finished.connect(self.on_merge_worker_finished_actions); self.merge_worker.error.connect(self.on_merge_error_actions)
        self.merge_worker.progress.connect(self.update_execution_progress); self.merge_worker.log.connect(self.update_execution_log)
        self.merge_worker.finished.connect(self.worker_thread.quit); self.merge_worker.error.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.trigger_preview_after_thread_finish); self.worker_thread.finished.connect(self.worker_thread.deleteLater); self.worker_thread.finished.connect(lambda: setattr(self, 'merge_worker', None))
        self.worker_thread.start()

    def preview_merge_data(self): # (与上一个回复版本相同)
        if not self._are_configs_valid_for_action(): QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写以进行预览。"); return
        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库。"); return
        conn_for_preview = None
        try:
            conn_for_preview = psycopg2.connect(**db_params); conn_for_preview.autocommit = True
            preview_sql_obj, error_msg, params_list, _ = self._build_merge_query(preview_limit=100, for_execution=False)
            if error_msg: QMessageBox.warning(self, "无法预览", error_msg); return
            if not preview_sql_obj: QMessageBox.warning(self, "无法预览", "未能生成预览SQL。"); return
            readable_sql_for_display = self._get_readable_sql_with_conn(preview_sql_obj, params_list, conn_for_preview)
            self.sql_preview.setText(f"-- Preview Query (Display Only):\n{readable_sql_for_display}")
            df = pd.read_sql_query(preview_sql_obj.as_string(conn_for_preview), conn_for_preview, params=params_list if params_list else None)
            self.preview_table.clearContents(); self.preview_table.setRowCount(df.shape[0]); self.preview_table.setColumnCount(df.shape[1])
            self.preview_table.setHorizontalHeaderLabels(df.columns)
            for i in range(df.shape[0]):
                for j in range(df.shape[1]): self.preview_table.setItem(i, j, QTableWidgetItem(str(df.iloc[i, j]) if pd.notna(df.iloc[i, j]) else ""))
            self.preview_table.resizeColumnsToContents(); QMessageBox.information(self, "预览成功", f"已生成预览数据 ({df.shape[0]} 条)。")
        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"执行预览查询失败: {str(e)}\nTraceback:\n{traceback.format_exc()}")
            if 'readable_sql_for_display' in locals(): self.sql_preview.append(f"\n-- ERROR DURING PREVIEW: {str(e)}")
        finally:
            if conn_for_preview: conn_for_preview.close()
    
    def _get_readable_sql_with_conn(self, sql_obj_or_str, params_list, conn): # (与上一个回复版本相同)
        if not conn or conn.closed:
            try: sql_template = str(sql_obj_or_str.as_string(psycopg2.connect(dbname='dummy')) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str))
            except: sql_template = str(sql_obj_or_str) # Fallback if dummy connection fails
            try: return sql_template % tuple(f"'{p}'" if isinstance(p, str) else p for p in params_list) if params_list else sql_template
            except: return f"{sql_template} -- Parameters: {params_list} (basic formatting failed)"
        try:
            with conn.cursor() as cur:
                sql_string_template = sql_obj_or_str.as_string(conn) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
                return cur.mogrify(sql_string_template, params_list).decode(conn.encoding or 'utf-8')
        except Exception as e:
            print(f"Error using mogrify with provided conn: {e}")
            base_sql_str = sql_obj_or_str.as_string(conn) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
            return f"{base_sql_str}\n-- PARAMETERS: {params_list} (Mogrify failed with conn)"

    def cancel_merge(self): # (与上一个回复版本相同)
        if self.merge_worker: self.update_execution_log("正在请求取消合并操作..."); self.merge_worker.cancel(); self.cancel_merge_btn.setEnabled(False) 

    @Slot()
    def on_merge_worker_finished_actions(self): # (与上一个回复版本相同)
        base_name_for_log = self.new_column_name_input.text()
        self.update_execution_log(f"成功向表 {self.selected_cohort_table} 添加/更新与 '{base_name_for_log}' 相关的列。")
        QMessageBox.information(self, "合并成功", f"已成功向表 {self.selected_cohort_table} 添加/更新与 '{base_name_for_log}' 相关的列。")
        self.prepare_for_long_operation(False)

    @Slot()
    def trigger_preview_after_thread_finish(self): # (与上一个回复版本相同)
        if self.selected_cohort_table: self.request_preview_signal.emit('mimiciv_data', self.selected_cohort_table)
        else: print("Worker thread finished, but no table selected for preview.")
        
    @Slot(str)
    def on_merge_error_actions(self, error_message): # (与上一个回复版本相同)
        self.update_execution_log(f"合并失败: {error_message}")
        if "操作已取消" not in error_message: QMessageBox.critical(self, "合并失败", f"执行合并SQL失败: {error_message}")
        else: QMessageBox.information(self, "操作取消", "数据合并操作已取消。")
        self.prepare_for_long_operation(False)
        
    def closeEvent(self, event): # (与上一个回复版本相同)
        if self.worker_thread and self.worker_thread.isRunning():
             self.update_execution_log("正在尝试停止合并操作...")
             if self.merge_worker: self.merge_worker.cancel()
             self.worker_thread.quit()
             if not self.worker_thread.wait(1000): self.update_execution_log("合并线程未能及时停止。")
        for panel in self.config_panels.values():
            if hasattr(panel, '_close_panel_db'): panel._close_panel_db()
        super().closeEvent(event)

# --- END OF FILE tab_special_data_master.py ---