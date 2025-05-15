# --- START OF FILE tab_special_data_master.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox,
                          QRadioButton, QButtonGroup, QStackedWidget, # 新增 QStackedWidget
                          QLineEdit, QProgressBar, QCheckBox, QAbstractItemView, QApplication)
from PySide6.QtCore import Qt, Signal, Slot, QObject, QThread
import psycopg2
import psycopg2.sql as pgsql
import re
import pandas as pd
import time
import traceback

# 导入配置面板 (假设 source_panels 目录与此文件在同一父目录下, 或者已添加到PYTHONPATH)
from source_panels.base_panel import BaseSourceConfigPanel
from source_panels.chartevents_panel import CharteventsConfigPanel
from source_panels.labevents_panel import LabeventsConfigPanel
from source_panels.medication_panel import MedicationConfigPanel
from source_panels.procedure_panel import ProcedureConfigPanel
from source_panels.diagnosis_panel import DiagnosisConfigPanel


# --- Worker Class for Asynchronous Merging (MergeSQLWorker) ---
# (MergeSQLWorker 类代码与你提供的版本相同，这里不再重复，直接复制过来即可)
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

    def cancel(self):
        self.log.emit("合并操作被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn_merge = None
        total_actual_steps = len(self.execution_steps)
        current_step_num = 0
        
        self.log.emit(f"开始为表 '{self.target_table_name}' 添加/更新列 (基于: {self.new_cols_description_str})，共 {total_actual_steps} 个数据库步骤...")
        self.progress.emit(current_step_num, total_actual_steps)

        try:
            self.log.emit("连接数据库...")
            conn_merge = psycopg2.connect(**self.db_params)
            conn_merge.autocommit = False 
            cur = conn_merge.cursor()
            self.log.emit("数据库已连接。")

            for i, (sql_obj_or_str, params_for_step) in enumerate(self.execution_steps):
                current_step_num += 1
                step_description = f"执行数据库步骤 {current_step_num}/{total_actual_steps}"
                
                sql_str_for_log_peek = ""
                self.current_sql_for_debug = "" 
                if isinstance(sql_obj_or_str, (pgsql.Composed, pgsql.SQL)):
                    try:
                        # 使用主连接进行 as_string, 避免在循环中重复连接
                        self.current_sql_for_debug = sql_obj_or_str.as_string(conn_merge)
                        sql_str_for_log_peek = self.current_sql_for_debug[:200].split('\n')[0]
                    except Exception as e_as_string:
                        self.log.emit(f"DEBUG: Error getting SQL as_string: {e_as_string}")
                        sql_str_for_log_peek = str(sql_obj_or_str)[:100].split('\n')[0]
                        self.current_sql_for_debug = str(sql_obj_or_str) 
                else: 
                    self.current_sql_for_debug = sql_obj_or_str
                    sql_str_for_log_peek = sql_obj_or_str[:100].split('\n')[0]

                if "ALTER TABLE" in sql_str_for_log_peek.upper(): step_description += " (ALTER)"
                elif "CREATE TEMPORARY TABLE" in sql_str_for_log_peek.upper(): step_description += " (CREATE TEMP)"
                elif "UPDATE" in sql_str_for_log_peek.upper(): step_description += " (UPDATE)"
                elif "DROP TABLE" in sql_str_for_log_peek.upper(): step_description += " (DROP TEMP)"

                self.log.emit(f"{step_description}: {sql_str_for_log_peek}...")
                # self.log.emit(f"参数: {params_for_step if params_for_step else '无'}") # 参数可能过长，暂时注释
                
                if self.is_cancelled:
                    raise InterruptedError("操作在执行步骤前被取消。")

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

        except InterruptedError as ie: 
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            self.log.emit(f"操作已取消: {str(ie)}")
            self.error.emit("操作已取消")
        except psycopg2.Error as db_err:
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            err_msg = f"数据库错误: {db_err}\n相关SQL (完整): {self.current_sql_for_debug}"
            self.log.emit(err_msg)
            self.log.emit(f"Traceback: {traceback.format_exc()}")
            self.error.emit(err_msg)
        except Exception as e:
            if conn_merge and not conn_merge.closed: conn_merge.rollback()
            err_msg = f"发生意外错误: {e}\n相关SQL (完整): {self.current_sql_for_debug}"
            self.log.emit(err_msg)
            self.log.emit(f"Traceback: {traceback.format_exc()}")
            self.error.emit(err_msg)
        finally:
            if conn_merge and not conn_merge.closed:
                self.log.emit("关闭数据库连接。")
                conn_merge.close()


# --- 主专项数据提取标签页 ---
class SpecialDataMasterTab(QWidget):
    request_preview_signal = Signal(str, str) # schema_name, table_name

    # 定义数据源常量以便引用
    SOURCE_LABEVENTS = 1
    SOURCE_MEDICATIONS = 2
    SOURCE_PROCEDURES = 3
    SOURCE_DIAGNOSES = 4
    SOURCE_CHARTEVENTS = 5 # 新增

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_cohort_table = None
        self.worker_thread = None
        self.merge_worker = None
        
        # 通用时间窗口选项 (用于非lab/chartevents的事件类型)
        self.general_event_time_window_options = [
            "整个住院期间 (当前入院)", "整个ICU期间 (当前入院)", "住院以前 (既往史)"
        ]
        self.diag_event_time_window_options = ["住院以前 (既往史)"]
        # 通用数值聚合时间窗口选项 (用于labevents, chartevents)
        self.value_agg_time_window_options = [
            "ICU入住后24小时", "ICU入住后48小时", "整个ICU期间", "整个住院期间"
        ]

        self.config_panels = {} # 存储数据源ID到对应配置面板的映射

        self.init_ui()
        self._update_active_panel() # 初始时激活默认面板

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        # Splitter for top (config) and bottom (results)
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        # --- 上方配置区域 ---
        config_widget = QWidget()
        config_layout = QVBoxLayout(config_widget)
        config_layout.setContentsMargins(10, 10, 10, 10)
        config_layout.setSpacing(10)
        splitter.addWidget(config_widget)

        # 1. 目标队列表选择
        cohort_group = QGroupBox("1. 选择目标队列数据表")
        cohort_layout = QHBoxLayout(cohort_group)
        cohort_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox(); self.table_combo.setMinimumWidth(250)
        self.table_combo.currentIndexChanged.connect(self.on_cohort_table_selected)
        cohort_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新列表"); self.refresh_btn.clicked.connect(self.refresh_cohort_tables); self.refresh_btn.setEnabled(False)
        cohort_layout.addWidget(self.refresh_btn); cohort_layout.addStretch()
        config_layout.addWidget(cohort_group)

        # 2. 数据来源选择 和 特定配置面板区域
        source_and_panel_group = QGroupBox("2. 选择数据来源并配置提取项")
        source_and_panel_main_layout = QVBoxLayout(source_and_panel_group)

        source_select_layout = QHBoxLayout()
        source_select_layout.addWidget(QLabel("数据来源:"))
        self.source_selection_group = QButtonGroup(self)
        
        self.rb_lab = QRadioButton("化验 (labevents)")
        self.rb_med = QRadioButton("用药 (prescriptions)")
        self.rb_proc = QRadioButton("操作/手术 (procedures_icd)")
        self.rb_diag = QRadioButton("诊断 (diagnoses_icd)")
        self.rb_chartevents = QRadioButton("监测指标 (chartevents)")

        # 关联 RadioButton 和 ID
        self.source_radio_buttons_map = {
            self.SOURCE_LABEVENTS: self.rb_lab,
            self.SOURCE_MEDICATIONS: self.rb_med,
            self.SOURCE_PROCEDURES: self.rb_proc,
            self.SOURCE_DIAGNOSES: self.rb_diag,
            self.SOURCE_CHARTEVENTS: self.rb_chartevents,
        }
        for source_id, rb in self.source_radio_buttons_map.items():
            self.source_selection_group.addButton(rb, source_id)
            source_select_layout.addWidget(rb)
        source_select_layout.addStretch()
        source_and_panel_main_layout.addLayout(source_select_layout)
        
        self.search_field_hint_label = QLabel("提示：上方选择数据来源后，下方将显示对应的筛选配置区域。") # 通用提示
        self.search_field_hint_label.setStyleSheet("font-style: italic; color: gray;")
        source_and_panel_main_layout.addWidget(self.search_field_hint_label)

        # QStackedWidget 用于动态显示配置面板
        self.config_panel_stack = QStackedWidget()
        source_and_panel_main_layout.addWidget(self.config_panel_stack)
        
        # 初始化并添加配置面板到 QStackedWidget
        self._create_config_panels()

        config_layout.addWidget(source_and_panel_group)

        # 3. 通用提取逻辑和列名定义
        common_logic_group = QGroupBox("3. 定义通用提取逻辑和列名")
        common_logic_layout = QGridLayout(common_logic_group)
        common_logic_layout.addWidget(QLabel("新列基础名 (可修改):"), 0, 0)
        self.new_column_name_input = QLineEdit(); self.new_column_name_input.setPlaceholderText("根据选择自动生成或手动输入...")
        self.new_column_name_input.textChanged.connect(self.update_master_action_buttons_state)
        common_logic_layout.addWidget(self.new_column_name_input, 0, 1, 1, 3)

        # 通用聚合选项 (用于 labevents, chartevents, 或其他需要数值聚合的源)
        self.common_agg_options_widget = QWidget()
        common_agg_layout = QVBoxLayout(self.common_agg_options_widget)
        common_agg_layout.setContentsMargins(0,0,0,0)
        self.common_agg_group = QGroupBox("提取方式 (数值型数据)") # 例如 valuenum
        common_agg_inner_layout = QGridLayout(self.common_agg_group)
        self.cb_common_first = QCheckBox("首次 (First)"); self.cb_common_last = QCheckBox("末次 (Last)")
        self.cb_common_min = QCheckBox("最小值 (Min)");   self.cb_common_max = QCheckBox("最大值 (Max)")
        self.cb_common_mean = QCheckBox("平均值 (Mean)"); self.cb_common_count_val = QCheckBox("有效值计数 (Count Value)")
        self.common_agg_checkboxes = [self.cb_common_first, self.cb_common_last, self.cb_common_min, self.cb_common_max, self.cb_common_mean, self.cb_common_count_val]
        common_agg_inner_layout.addWidget(self.cb_common_first, 0, 0); common_agg_inner_layout.addWidget(self.cb_common_last, 0, 1)
        common_agg_inner_layout.addWidget(self.cb_common_min, 1, 0);   common_agg_inner_layout.addWidget(self.cb_common_max, 1, 1)
        common_agg_inner_layout.addWidget(self.cb_common_mean, 2, 0);  common_agg_inner_layout.addWidget(self.cb_common_count_val, 2, 1)
        for cb in self.common_agg_checkboxes: cb.stateChanged.connect(self._on_common_config_changed)
        common_agg_layout.addWidget(self.common_agg_group)
        common_logic_layout.addWidget(self.common_agg_options_widget, 1, 0, 1, 2) # 跨两列

        # 通用事件输出选项 (用于 prescriptions, procedures, diagnoses 等)
        self.common_event_output_widget = QWidget()
        common_event_layout = QVBoxLayout(self.common_event_output_widget)
        common_event_layout.setContentsMargins(0,0,0,0)
        self.common_event_group = QGroupBox("输出类型 (事件型数据)")
        common_event_inner_layout = QGridLayout(self.common_event_group)
        self.cb_common_exists = QCheckBox("是否存在 (Boolean)"); self.cb_common_count_event = QCheckBox("发生次数 (Count Event)")
        self.common_event_checkboxes = [self.cb_common_exists, self.cb_common_count_event]
        common_event_inner_layout.addWidget(self.cb_common_exists, 0, 0); common_event_inner_layout.addWidget(self.cb_common_count_event, 0, 1)
        for cb in self.common_event_checkboxes: cb.stateChanged.connect(self._on_common_config_changed)
        common_event_layout.addWidget(self.common_event_group)
        common_logic_layout.addWidget(self.common_event_output_widget, 1, 0, 1, 2) # 与上面重叠，稍后通过显示/隐藏控制

        # 通用时间窗口选择
        time_window_layout = QHBoxLayout()
        time_window_layout.addWidget(QLabel("时间窗口:"))
        self.common_time_window_combo = QComboBox()
        self.common_time_window_combo.currentTextChanged.connect(self._on_common_config_changed)
        time_window_layout.addWidget(self.common_time_window_combo)
        common_logic_layout.addLayout(time_window_layout, 1, 2, 1, 2) # 放在右边

        config_layout.addWidget(common_logic_group)

        # 执行状态
        self.execution_status_group = QGroupBox("合并执行状态")
        # ... (与之前版本相同的执行状态UI)
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,4); self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit(); self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(100)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False)
        config_layout.addWidget(self.execution_status_group)

        # 操作按钮
        action_layout = QHBoxLayout()
        # ... (与之前版本相同的按钮)
        self.preview_merge_btn = QPushButton("预览待合并数据"); self.preview_merge_btn.clicked.connect(self.preview_merge_data); self.preview_merge_btn.setEnabled(False)
        action_layout.addWidget(self.preview_merge_btn)
        self.execute_merge_btn = QPushButton("执行合并到表"); self.execute_merge_btn.clicked.connect(self.execute_merge); self.execute_merge_btn.setEnabled(False)
        action_layout.addWidget(self.execute_merge_btn)
        self.cancel_merge_btn = QPushButton("取消合并"); self.cancel_merge_btn.clicked.connect(self.cancel_merge); self.cancel_merge_btn.setEnabled(False)
        action_layout.addWidget(self.cancel_merge_btn)
        config_layout.addLayout(action_layout)

        # --- 下方结果预览区域 ---
        result_widget = QWidget()
        # ... (与之前版本相同的结果预览UI)
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(10, 10, 10, 10); result_layout.setSpacing(10)
        splitter.addWidget(result_widget)
        result_layout.addWidget(QLabel("SQL预览 (仅供参考):"))
        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(120)
        result_layout.addWidget(self.sql_preview)
        result_layout.addWidget(QLabel("数据预览 (最多100条):"))
        self.preview_table = QTableWidget(); self.preview_table.setAlternatingRowColors(True); self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        result_layout.addWidget(self.preview_table)
        
        splitter.setSizes([700, 300]) # 调整比例

        # 连接信号
        self.source_selection_group.buttonToggled.connect(self._on_source_type_changed)
        
        # 初始状态
        self.rb_chartevents.setChecked(True) # 默认选中 Chartevents
        self._update_common_logic_ui_visibility()
        self.update_master_action_buttons_state()
        self._update_active_panel() # 确保初始面板被加载

    def _create_config_panels(self):
        # Chartevents Panel
        chartevents_panel = CharteventsConfigPanel(self.get_db_params, self) # Pass self as parent
        chartevents_panel.config_changed_signal.connect(self._on_panel_config_changed)
        self.config_panel_stack.addWidget(chartevents_panel)
        self.config_panels[self.SOURCE_CHARTEVENTS] = chartevents_panel

        # Labevents Panel
        labevents_panel = LabeventsConfigPanel(self.get_db_params, self)
        labevents_panel.config_changed_signal.connect(self._on_panel_config_changed)
        self.config_panel_stack.addWidget(labevents_panel)
        self.config_panels[self.SOURCE_LABEVENTS] = labevents_panel
        
        # Medication Panel
        medication_panel = MedicationConfigPanel(self.get_db_params, self)
        medication_panel.config_changed_signal.connect(self._on_panel_config_changed)
        self.config_panel_stack.addWidget(medication_panel)
        self.config_panels[self.SOURCE_MEDICATIONS] = medication_panel

        # Procedure Panel
        procedure_panel = ProcedureConfigPanel(self.get_db_params, self)
        procedure_panel.config_changed_signal.connect(self._on_panel_config_changed)
        self.config_panel_stack.addWidget(procedure_panel)
        self.config_panels[self.SOURCE_PROCEDURES] = procedure_panel

        # Diagnosis Panel
        diagnosis_panel = DiagnosisConfigPanel(self.get_db_params, self)
        diagnosis_panel.config_changed_signal.connect(self._on_panel_config_changed)
        self.config_panel_stack.addWidget(diagnosis_panel)
        self.config_panels[self.SOURCE_DIAGNOSES] = diagnosis_panel

    def _update_active_panel(self):
        current_id = self.source_selection_group.checkedId()
        active_panel = self.config_panels.get(current_id)

        if active_panel:
            self.config_panel_stack.setCurrentWidget(active_panel)
            active_panel.populate_panel_if_needed() # 让面板进行初始化 (包括设置其ConditionGroupWidget的可用字段)
            
            # 更新主Tab的 search_field_hint_label，从活动面板获取提示信息
            # get_item_filtering_details 返回: (dict_table, name_col_in_dict, id_col_in_dict, friendly_hint, event_table_if_no_dict)
            details = active_panel.get_item_filtering_details()
            hint = details[3] if details and len(details) > 3 else "配置对应数据源的筛选条件。"
            self.search_field_hint_label.setText(hint) # 更新提示
            
            self._update_common_logic_ui_visibility()
            self._generate_and_set_default_col_name() # 这个方法会读取面板的选中项等，确保在面板populate后调用
            self.update_master_action_buttons_state()
        else:
            self.search_field_hint_label.setText("请选择一个数据来源。")

    def _on_source_type_changed(self, button, checked):
        if checked:
            self._update_active_panel()
            # 清理之前可能存在的特定面板状态（例如列表内容）
            # for panel_id, panel_widget in self.config_panels.items():
            #     if panel_id != self.source_selection_group.checkedId():
            #         panel_widget.clear_panel_state() # 确保非活动面板状态被清空或重置

    def _on_panel_config_changed(self):
        """当活动面板的配置改变时调用。"""
        self._generate_and_set_default_col_name()
        self.update_master_action_buttons_state()

    def _on_common_config_changed(self):
        """当通用配置（聚合方法、时间窗口）改变时调用。"""
        self._generate_and_set_default_col_name()
        self.update_master_action_buttons_state()

    def _update_common_logic_ui_visibility(self):
        current_id = self.source_selection_group.checkedId()
        active_panel = self.config_panels.get(current_id)

        use_value_aggregation = False
        time_options_for_combo = []

        if active_panel:
            # 如果面板自己提供聚合UI或时间窗口，则隐藏通用的
            # (chartevents panel 现在配置为使用通用的)
            if active_panel.get_aggregation_config_widget() is not None:
                self.common_agg_options_widget.hide()
                self.common_event_output_widget.hide()
            else: # 面板使用通用聚合逻辑
                if active_panel.get_value_column_for_aggregation() is not None: # 如 chartevents, labevents
                    self.common_agg_options_widget.show()
                    self.common_event_output_widget.hide()
                    use_value_aggregation = True
                else: # 如 prescriptions, procedures, diagnoses
                    self.common_agg_options_widget.hide()
                    self.common_event_output_widget.show()
                    use_value_aggregation = False
            
            panel_time_options = active_panel.get_time_window_options()
            if panel_time_options is not None: # 面板有自己特定的时间窗口
                time_options_for_combo = panel_time_options
            else: # 面板使用通用的时间窗口
                if use_value_aggregation: # chartevents, labevents
                    time_options_for_combo = self.value_agg_time_window_options
                else: # prescriptions, procedures, diagnoses
                    if current_id == self.SOURCE_DIAGNOSES:
                        time_options_for_combo = self.diag_event_time_window_options
                    else:
                        time_options_for_combo = self.general_event_time_window_options
        else: # 没有活动面板
            self.common_agg_options_widget.hide()
            self.common_event_output_widget.hide()

        current_time_text = self.common_time_window_combo.currentText()
        self.common_time_window_combo.blockSignals(True)
        self.common_time_window_combo.clear()
        if time_options_for_combo:
            self.common_time_window_combo.addItems(time_options_for_combo)
            if current_time_text in time_options_for_combo:
                self.common_time_window_combo.setCurrentText(current_time_text)
            elif self.common_time_window_combo.count() > 0:
                self.common_time_window_combo.setCurrentIndex(0)
        self.common_time_window_combo.blockSignals(False)


    def _validate_column_name(self, name): # 从旧代码复制
        if not name: return False, "列名不能为空。"
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
             if re.match(r'^_[a-zA-Z0-9_]+$', name): pass 
             else: return False, "列名只能包含字母、数字和下划线，且以字母或下划线开头。"
        keywords = {"SELECT", "TABLE", "UPDATE", "COLUMN", "ADD", "ALTER", "WHERE", "FROM"} 
        if name.upper() in keywords: return False, f"列名 '{name}' 是 SQL 关键字。"
        if len(name) > 63: return False, f"列名 '{name}' 过长 (最多63个字符)。" 
        return True, ""

    def _sanitize_name(self, name_part): # 从旧代码复制，稍作修改
        if not name_part: return ""
        name_part = re.sub(r'[ /\\:,;()\[\]{}"\'.\-+*?^$|<>%]+', '_', name_part) 
        name_part = re.sub(r'_+', '_', name_part) 
        name_part = re.sub(r'[^\w]+', '', name_part) # 移除非单词字符
        name_part = name_part.lower().strip('_')
        if name_part and name_part[0].isdigit(): name_part = '_' + name_part 
        return name_part[:25] if name_part else "item" # 限制单个部分的长度

    def _generate_and_set_default_col_name(self):
        parts = []
        logic_code = ""
        first_selected_method_text = ""
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        
        is_value_aggregation_source = False
        if active_panel and active_panel.get_value_column_for_aggregation():
            is_value_aggregation_source = True

        if is_value_aggregation_source:
            # 使用通用的数值聚合复选框
            logic_map = {
                "首次 (First)": "first", "末次 (Last)": "last", 
                "最小值 (Min)": "min", "最大值 (Max)": "max", 
                "平均值 (Mean)": "mean", "有效值计数 (Count Value)": "countval"
            }
            for cb in self.common_agg_checkboxes: # 使用主Tab的通用复选框
                if cb.isChecked():
                    first_selected_method_text = cb.text()
                    break 
            logic_code = logic_map.get(first_selected_method_text, "val")
        else: # 事件型数据源
            logic_map = {"是否存在 (Boolean)": "has", "发生次数 (Count Event)": "count"}
            for cb in self.common_event_checkboxes: # 使用主Tab的通用复选框
                if cb.isChecked():
                    first_selected_method_text = cb.text()
                    break
            logic_code = logic_map.get(first_selected_method_text, "evt")
        
        if first_selected_method_text: 
            parts.append(logic_code)
        else: 
            parts.append("val" if is_value_aggregation_source else "evt")

        item_name_part = "item" 
        if active_panel and hasattr(active_panel, 'item_list'):
            selected_list_items = active_panel.item_list.selectedItems()
            if selected_list_items:
                try:
                    # data[1] 是显示名 (label/drug/long_title)
                    item_name_part = self._sanitize_name(selected_list_items[0].data(Qt.ItemDataRole.UserRole)[1])
                except Exception: 
                    item_name_part = self._sanitize_name(selected_list_items[0].text().split('(')[0].strip())
        parts.append(item_name_part)
        
        time_code = ""
        current_time_text = self.common_time_window_combo.currentText() # 总是使用通用的时间窗口组合框
        if is_value_aggregation_source:
            time_map = {"ICU入住后24小时": "icu24h", "ICU入住后48小时": "icu48h", "整个ICU期间": "icu_all", "整个住院期间": "hosp_all"}
            time_code = time_map.get(current_time_text, "")
        else: 
            time_map = { 
                "整个住院期间 (当前入院)": "hosp", # 简化
                "整个ICU期间 (当前入院)": "icu",   # 简化
                "住院以前 (既往史)": "prior"     # 简化
            }
            time_code = time_map.get(current_time_text, "") 
        if time_code: parts.append(time_code)
        
        default_name = "_".join(filter(None, parts))
        if not default_name: default_name = "new_col" 
        if default_name and default_name[0].isdigit(): default_name = "_" + default_name 
        
        final_name = default_name[:50] # 最终截断
        # 只有当自动生成的名称与当前输入框内容不同时才更新，以允许用户手动修改
        if self.new_column_name_input.text() != final_name:
            self.new_column_name_input.blockSignals(True)
            self.new_column_name_input.setText(final_name)
            self.new_column_name_input.blockSignals(False)
        
        self.update_master_action_buttons_state() # 确保按钮状态随之更新

    def prepare_for_long_operation(self, starting=True): # 与旧版类似，但操作的控件不同
        is_enabled = not starting
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行合并操作...")
        
        self.table_combo.setEnabled(is_enabled)
        self.refresh_btn.setEnabled(is_enabled)
        for rb in self.source_selection_group.buttons(): rb.setEnabled(is_enabled)
        
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel: active_panel.setEnabled(is_enabled) # 禁用整个活动面板

        self.new_column_name_input.setEnabled(is_enabled)
        self.common_agg_options_widget.setEnabled(is_enabled)
        self.common_event_output_widget.setEnabled(is_enabled)
        self.common_time_window_combo.setEnabled(is_enabled)

        self.preview_merge_btn.setEnabled(is_enabled and self._are_configs_valid_for_action())
        self.execute_merge_btn.setEnabled(is_enabled and self._are_configs_valid_for_action())
        self.cancel_merge_btn.setEnabled(starting)

        if not starting: # 操作结束
            self.update_master_action_buttons_state() # 重新评估按钮状态

    def update_execution_progress(self, value, max_value=None): # 与旧版相同
        if max_value is not None and self.execution_progress.maximum() != max_value:
            self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message): # 与旧版相同
        self.execution_log.append(message)
        QApplication.processEvents() 

    @Slot()
    def on_db_connected(self): # 与旧版类似
        self.refresh_btn.setEnabled(True)
        self.refresh_cohort_tables()
        self._generate_and_set_default_col_name()

    def refresh_cohort_tables(self): # 与旧版相同
        db_params = self.get_db_params()
        if not db_params: return # on_db_connected 会处理消息
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute(pgsql.SQL("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = {schema_name}
                AND (table_name LIKE {pattern_first} OR table_name LIKE {pattern_all})
                ORDER BY table_name
                """)
                .format(
                    schema_name=pgsql.Literal('mimiciv_data'),
                    pattern_first=pgsql.Literal('first_%_admissions'),
                    pattern_all=pgsql.Literal('all_%_admissions')
                )
            )
            tables = cur.fetchall()
            current_selection = self.table_combo.currentText()
            self.table_combo.blockSignals(True)
            self.table_combo.clear()
            if tables:
                table_names = [table[0] for table in tables]
                self.table_combo.addItems(table_names)
                if current_selection in table_names: self.table_combo.setCurrentText(current_selection)
                elif table_names: self.table_combo.setCurrentIndex(0)
            else: self.table_combo.addItem("未找到符合条件的队列数据表")
            self.table_combo.blockSignals(False)
            self.on_cohort_table_selected(self.table_combo.currentIndex()) # 手动触发一次
        except Exception as e: QMessageBox.critical(self, "查询失败", f"无法获取队列数据表列表: {str(e)}")
        finally:
            if conn: conn.close()
        
    def on_cohort_table_selected(self, index): # 与旧版类似
        if index >= 0 and self.table_combo.count() > 0 and "未找到" not in self.table_combo.itemText(index):
            self.selected_cohort_table = self.table_combo.itemText(index)
        else: self.selected_cohort_table = None
        self.update_master_action_buttons_state()

    def _are_configs_valid_for_action(self) -> bool:
        """检查所有必要的配置是否有效，以便启用预览/执行按钮。"""
        if not self.selected_cohort_table: return False
        
        col_name_text = self.new_column_name_input.text()
        is_valid_col_name, _ = self._validate_column_name(col_name_text)
        if not is_valid_col_name: return False

        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if not active_panel or not active_panel.get_selected_item_ids():
            return False # 没有活动面板或面板未选择项目

        # 检查通用聚合/事件选项是否至少选择了一个
        any_common_method_selected = False
        if self.common_agg_options_widget.isVisible():
            any_common_method_selected = any(cb.isChecked() for cb in self.common_agg_checkboxes)
        elif self.common_event_output_widget.isVisible():
            any_common_method_selected = any(cb.isChecked() for cb in self.common_event_checkboxes)
        
        # 如果面板自己提供聚合方法，则检查面板的
        panel_specific_methods = active_panel.get_specific_aggregation_methods() if active_panel else None
        any_panel_method_selected = False
        if panel_specific_methods:
            any_panel_method_selected = any(panel_specific_methods.values())

        return any_common_method_selected or any_panel_method_selected


    @Slot()
    def update_master_action_buttons_state(self):
        is_valid = self._are_configs_valid_for_action()
        self.preview_merge_btn.setEnabled(is_valid)
        self.execute_merge_btn.setEnabled(is_valid)
        
        # 更新活动面板的按钮状态
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel:
            general_ok = bool(self.get_db_params()) # 简化：只要DB已连接，通用配置就OK
            active_panel.update_panel_action_buttons_state(general_ok)


    def _build_merge_query(self, preview_limit=100, for_execution=False):
        generated_column_details_for_preview = [] # [(col_name, col_type_str), ...]

        # 1. 获取通用配置
        if not self.selected_cohort_table:
            return None, "未选择目标队列数据表.", [], generated_column_details_for_preview
        
        base_new_col_name_str = self.new_column_name_input.text().strip()
        is_valid_base_name, name_error = self._validate_column_name(base_new_col_name_str)
        if not is_valid_base_name:
            return None, f"基础列名错误: {name_error}", [], generated_column_details_for_preview

        # 2. 获取活动面板及其配置
        active_panel_id = self.source_selection_group.checkedId()
        active_panel = self.config_panels.get(active_panel_id)
        if not active_panel:
            return None, "未选择有效的数据来源或未找到配置面板.", [], generated_column_details_for_preview
        
        panel_config = active_panel.get_panel_config()
        # panel_config 应该包含:
        # source_event_table, source_dict_table (optional), item_id_column_in_event_table,
        # item_filter_conditions (sql, params), selected_item_ids
        
        selected_item_ids_values = panel_config.get("selected_item_ids", [])
        if not selected_item_ids_values:
            return None, "未在当前数据来源面板中选择要合并的项目.", [], generated_column_details_for_preview

        source_event_table = panel_config["source_event_table"]
        id_col_in_event_table = panel_config["item_id_column_in_event_table"]
        
        # 3. 确定聚合值列和时间列 (从面板获取)
        val_col_for_agg = active_panel.get_value_column_for_aggregation() # e.g., "valuenum"
        time_col_for_window = active_panel.get_time_column_for_windowing() # e.g., "charttime"

        # 4. 定义SQL构建中常用的标识符
        target_table_ident = pgsql.Identifier('mimiciv_data', self.selected_cohort_table)
        cohort_alias = pgsql.Identifier("cohort")
        event_alias = pgsql.Identifier("evt")
        event_admission_alias = pgsql.Identifier("adm_evt") # 用于诊断等的历史追溯
        md_alias = pgsql.Identifier("md") # MergedData alias
        target_alias = pgsql.Identifier("target") # Target table alias for UPDATE

        # 5. 构建 FilteredEvents CTE 的参数和条件
        #    a. 项目ID条件 (来自 panel_config)
        params_for_cte = list(panel_config["item_filter_conditions"][1]) # 复制一份，因为后面可能追加
        item_id_conditions_sql_template = panel_config["item_filter_conditions"][0] # 这是针对字典表的筛选
        
        #  需要修改：item_id_conditions 应该是针对 event_table 的 id_col_in_event_table
        item_id_filter_on_event_table_parts = []
        event_table_item_id_col_ident = pgsql.Identifier(id_col_in_event_table)
        
        # 如果字典表被用来筛选ID，那么 selected_item_ids_values 就是从字典表筛选出来的ID
        # 这些ID需要用来过滤事件表
        if selected_item_ids_values:
            if len(selected_item_ids_values) == 1:
                item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} = %s").format(event_alias, event_table_item_id_col_ident))
                params_for_cte.append(selected_item_ids_values[0])
            else:
                item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} IN %s").format(event_alias, event_table_item_id_col_ident))
                params_for_cte.append(tuple(selected_item_ids_values))
        else: # 如果selected_item_ids_values为空，理论上不应该到这里，因为按钮状态会阻止
            return None, "未选择任何项目进行提取。", [], generated_column_details_for_preview


        #    b. 时间窗口条件 (来自通用时间窗口UI)
        time_filter_conditions_sql_parts = []
        cohort_icu_intime = pgsql.SQL("{}.icu_intime").format(cohort_alias)
        cohort_icu_outtime = pgsql.SQL("{}.icu_outtime").format(cohort_alias)
        cohort_admittime = pgsql.SQL("{}.admittime").format(cohort_alias)
        cohort_dischtime = pgsql.SQL("{}.dischtime").format(cohort_alias)
        
        current_time_window_text = self.common_time_window_combo.currentText()
        actual_event_time_col_ident = pgsql.Identifier(time_col_for_window) if time_col_for_window else None

        is_value_source = bool(val_col_for_agg) # 是否是提取数值的源 (lab, chartevents)

        if is_value_source: # labevents, chartevents
            if not actual_event_time_col_ident:
                return None, f"{source_event_table} 需要时间列进行窗口化提取，但未配置。", params_for_cte, generated_column_details_for_preview
            if current_time_window_text == "ICU入住后24小时":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '24 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
            elif current_time_window_text == "ICU入住后48小时":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '48 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
            elif current_time_window_text == "整个ICU期间":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
            elif current_time_window_text == "整个住院期间":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
        else: # 事件型 (med, proc, diag)
            if current_time_window_text == "住院以前 (既往史)":
                from_join_clause_for_cte_override = pgsql.SQL(
                    "FROM {event_table} {evt_alias} "
                    "JOIN mimiciv_hosp.admissions {adm_evt} ON {evt_alias}.hadm_id = {adm_evt}.hadm_id "
                    "JOIN {cohort_table} {coh_alias} ON {evt_alias}.subject_id = {coh_alias}.subject_id "
                ).format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias, adm_evt=event_admission_alias,
                         cohort_table=target_table_ident, coh_alias=cohort_alias)
                time_filter_conditions_sql_parts.append(pgsql.SQL("{adm_evt}.admittime < {compare_ts}").format(adm_evt=event_admission_alias, compare_ts=cohort_admittime))
            elif current_time_window_text == "整个住院期间 (当前入院)":
                if actual_event_time_col_ident:
                    time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
            elif current_time_window_text == "整个ICU期间 (当前入院)":
                if actual_event_time_col_ident:
                     time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
                elif active_panel_id == self.SOURCE_CHARTEVENTS: # Chartevents 可能用 stay_id
                     pass # Chartevents JOIN on stay_id already filters by ICU stay implicitly for "整个ICU期间" if cohort.stay_id is the one.
                          # If cohort.stay_id is null, this won't work well for "整个ICU期间"
            # else: return None, f"未知时间窗口: {current_time_window_text}", params_for_cte, generated_column_details_for_preview


        # 6. 构建 FilteredEvents CTE 的 SELECT 列, FROM/JOIN, WHERE
        select_event_cols_defs = [
            pgsql.SQL("{}.subject_id AS subject_id").format(cohort_alias),
            pgsql.SQL("{}.hadm_id AS hadm_id_cohort").format(cohort_alias)
        ]
        if val_col_for_agg: # 对于lab/chartevents，需要值列
            select_event_cols_defs.append(pgsql.SQL("{}.{} AS event_value").format(event_alias, pgsql.Identifier(val_col_for_agg)))
        if time_col_for_window: # 对于需要时间排序或窗口的，需要时间列
            select_event_cols_defs.append(pgsql.SQL("{}.{} AS event_time").format(event_alias, pgsql.Identifier(time_col_for_window)))

        # 默认 JOIN 逻辑
        from_join_clause_for_cte = pgsql.SQL(
            "FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.hadm_id = {coh_alias}.hadm_id"
        ).format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias, cohort_table=target_table_ident, coh_alias=cohort_alias)

        # 特殊 JOIN 逻辑 for chartevents (用 stay_id) 和 "住院以前"
        if active_panel_id == self.SOURCE_CHARTEVENTS:
            # Chartevents 最好用 stay_id (如果队列表有的话)
            # 假设队列表的 stay_id 是有效的
            from_join_clause_for_cte = pgsql.SQL(
                "FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.stay_id = {coh_alias}.stay_id"
            ).format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias, cohort_table=target_table_ident, coh_alias=cohort_alias)
        
        if hasattr(self, 'from_join_clause_for_cte_override') and self.from_join_clause_for_cte_override: # 如果时间窗口设置了覆盖
            from_join_clause_for_cte = self.from_join_clause_for_cte_override
            del self.from_join_clause_for_cte_override # 用完即删

        all_where_conditions_sql_parts = item_id_filter_on_event_table_parts + time_filter_conditions_sql_parts
        
        filtered_events_cte_sql = pgsql.SQL(
            "FilteredEvents AS (SELECT DISTINCT {select_list} {from_join_clause} WHERE {conditions})"
        ).format(
            select_list=pgsql.SQL(', ').join(select_event_cols_defs),
            from_join_clause=from_join_clause_for_cte,
            conditions=pgsql.SQL(' AND ').join(all_where_conditions_sql_parts) if all_where_conditions_sql_parts else pgsql.SQL("TRUE")
        )

        # 7. 构建聚合逻辑
        selected_methods_details = [] # (final_col_name_str, final_col_ident, agg_template_str, pgsql_col_type_obj)
        type_map_pgsql_to_str = { "NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text" }
        
        # 使用通用UI控件的状态
        if self.common_agg_options_widget.isVisible(): # 数值型聚合
            method_configs = [
                (self.cb_common_first, "first", "(array_agg({val} ORDER BY {time} ASC NULLS LAST))[1]", pgsql.SQL("NUMERIC")),
                (self.cb_common_last, "last", "(array_agg({val} ORDER BY {time} DESC NULLS LAST))[1]", pgsql.SQL("NUMERIC")),
                (self.cb_common_min, "min", "MIN({val})", pgsql.SQL("NUMERIC")),
                (self.cb_common_max, "max", "MAX({val})", pgsql.SQL("NUMERIC")),
                (self.cb_common_mean, "mean", "AVG({val})", pgsql.SQL("NUMERIC")),
                (self.cb_common_count_val, "countval", "COUNT({val})", pgsql.SQL("INTEGER")),
            ]
            for cb, suffix, agg_template, col_type_sql_obj in method_configs:
                if cb.isChecked():
                    final_col_name_str = f"{base_new_col_name_str}_{suffix}" # suffix 已经是小写
                    is_valid_final_name, final_name_error = self._validate_column_name(final_col_name_str)
                    if not is_valid_final_name: return None, f"列名错误 ({final_col_name_str}): {final_name_error}", params_for_cte, generated_column_details_for_preview
                    selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), agg_template, col_type_sql_obj))
                    col_type_str_raw = col_type_sql_obj.as_string(self._db_conn if self._connect_panel_db() else pgsql.SQL("TEXT")) # 尝试获取连接
                    self._close_panel_db() # 关闭临时连接
                    generated_column_details_for_preview.append((final_col_name_str, type_map_pgsql_to_str.get(col_type_str_raw.upper(), col_type_str_raw)))

        elif self.common_event_output_widget.isVisible(): # 事件型输出
            method_configs = [
                (self.cb_common_exists, "exists", "TRUE", pgsql.SQL("BOOLEAN")),
                (self.cb_common_count_event, "countevt", "COUNT(*)", pgsql.SQL("INTEGER")),
            ]
            for cb, suffix, agg_template, col_type_sql_obj in method_configs:
                if cb.isChecked():
                    final_col_name_str = f"{base_new_col_name_str}_{suffix}"
                    is_valid_final_name, final_name_error = self._validate_column_name(final_col_name_str)
                    if not is_valid_final_name: return None, f"列名错误 ({final_col_name_str}): {final_name_error}", params_for_cte, generated_column_details_for_preview
                    selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), agg_template, col_type_sql_obj))
                    col_type_str_raw = col_type_sql_obj.as_string(self._db_conn if self._connect_panel_db() else pgsql.SQL("TEXT"))
                    self._close_panel_db()
                    generated_column_details_for_preview.append((final_col_name_str, type_map_pgsql_to_str.get(col_type_str_raw.upper(), col_type_str_raw)))
        
        if not selected_methods_details:
            return None, "请至少选择一种提取方式或输出类型。", params_for_cte, generated_column_details_for_preview

        # 构建聚合SELECT列表
        aggregated_columns_sql_list = []
        fe_val_ident = pgsql.Identifier("event_value")
        fe_time_ident = pgsql.Identifier("event_time")
        cte_hadm_id_for_grouping = pgsql.Identifier("hadm_id_cohort") 
        group_by_hadm_id_sql = pgsql.SQL(" GROUP BY {}").format(cte_hadm_id_for_grouping)


        for _, final_col_ident, agg_template_str, _ in selected_methods_details:
            sql_expr = None
            if self.common_agg_options_widget.isVisible(): # 数值型聚合
                if "{val}" not in agg_template_str and "{time}" not in agg_template_str:
                     sql_expr = pgsql.SQL(agg_template_str) # e.g. COUNT(*) if template was just COUNT(*)
                elif "{val}" in agg_template_str and "{time}" in agg_template_str:
                    if not time_col_for_window: return None, f"提取方式 '{agg_template_str}' 需要时间列。", params_for_cte, generated_column_details_for_preview
                    sql_expr = pgsql.SQL(agg_template_str).format(val=fe_val_ident, time=fe_time_ident)
                elif "{val}" in agg_template_str:
                    sql_expr = pgsql.SQL(agg_template_str).format(val=fe_val_ident)
                else: # Should not happen
                    return None, f"数值聚合模板 '{agg_template_str}' 格式无法识别。", params_for_cte, generated_column_details_for_preview
            else: # 事件型聚合
                sql_expr = pgsql.SQL(agg_template_str)
            aggregated_columns_sql_list.append(pgsql.SQL("{} AS {}").format(sql_expr, final_col_ident))
        
        # 8. 组装最终查询
        main_aggregation_select_sql = pgsql.SQL("SELECT {hadm_grp_col}, {agg_cols} FROM FilteredEvents {gb}").format(
            hadm_grp_col=cte_hadm_id_for_grouping,
            agg_cols=pgsql.SQL(', ').join(aggregated_columns_sql_list),
            gb=group_by_hadm_id_sql
        )
        data_generation_query_part = pgsql.SQL("WITH {filtered_cte} {main_agg_select}").format(
            filtered_cte=filtered_events_cte_sql, main_agg_select=main_aggregation_select_sql)

        # ... (后续的 for_execution 和 for_preview 逻辑与之前版本基本一致, 使用这里的 data_generation_query_part 和 params_for_cte)
        if for_execution:
            alter_clauses = []
            for _, final_col_ident, _, col_type_sql_obj in selected_methods_details:
                alter_clauses.append(pgsql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(final_col_ident, col_type_sql_obj))
            alter_sql = pgsql.SQL("ALTER TABLE {target_table} ").format(target_table=target_table_ident) + pgsql.SQL(', ').join(alter_clauses) + pgsql.SQL(";")
            
            temp_table_data_name_str = f"temp_merge_data_{base_new_col_name_str.lower()}_{int(time.time()) % 100000}"
            if len(temp_table_data_name_str) > 63: temp_table_data_name_str = temp_table_data_name_str[:63]
            temp_table_data_ident = pgsql.Identifier(temp_table_data_name_str)
            create_temp_table_sql = pgsql.SQL("CREATE TEMPORARY TABLE {temp_table} AS ({data_gen_query});").format(temp_table=temp_table_data_ident, data_gen_query=data_generation_query_part)
            
            update_set_clauses = []
            for _, final_col_ident, _, _ in selected_methods_details:
                update_set_clauses.append(pgsql.SQL("{col_to_set} = {tmp_alias}.{col_from_tmp}").format(col_to_set=final_col_ident, tmp_alias=md_alias, col_from_tmp=final_col_ident))
            
            update_sql = pgsql.SQL("UPDATE {target_table} {tgt_alias} SET {set_clauses} FROM {temp_table} {tmp_alias} WHERE {tgt_alias}.hadm_id = {tmp_alias}.{hadm_col_in_temp};").format(
                target_table=target_table_ident, 
                tgt_alias=target_alias, 
                set_clauses=pgsql.SQL(', ').join(update_set_clauses), 
                temp_table=temp_table_data_ident, 
                tmp_alias=md_alias, 
                hadm_col_in_temp=cte_hadm_id_for_grouping 
            )
            drop_temp_table_sql = pgsql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_data_ident)
            
            execution_steps = [(alter_sql, None), (create_temp_table_sql, params_for_cte), (update_sql, None), (drop_temp_table_sql, None)]
            return execution_steps, "execution_list", base_new_col_name_str, generated_column_details_for_preview
        else: # for_preview
            preview_select_cols = [
                pgsql.SQL("{}.subject_id").format(cohort_alias),
                pgsql.SQL("{}.hadm_id").format(cohort_alias)
            ]
            if hasattr(self.config_panels.get(active_panel_id), 'stay_id_present_in_cohort') and self.config_panels[active_panel_id].stay_id_present_in_cohort: # 假设有方法检查队列表是否有stay_id
                 preview_select_cols.append(pgsql.SQL("{}.stay_id").format(cohort_alias))


            for _, final_col_ident, _, _ in selected_methods_details: 
                preview_select_cols.append(pgsql.SQL("{md_alias}.{col_ident} AS {col_ident_preview}").format(
                    md_alias=md_alias, col_ident=final_col_ident, col_ident_preview=final_col_ident
                ))

            preview_sql = pgsql.SQL(
                "WITH MergedDataCTE AS ({data_gen_query}) "
                "SELECT {select_cols_list} "
                "FROM {target_table} {coh_alias} "
                "LEFT JOIN MergedDataCTE {md_alias} ON {coh_alias}.hadm_id = {md_alias}.{hadm_col_in_temp} " # 主键通常是 hadm_id
                "ORDER BY RANDOM() LIMIT {limit};" # 修改为随机预览
            ).format(
                data_gen_query=data_generation_query_part,
                select_cols_list=pgsql.SQL(', ').join(preview_select_cols),
                target_table=target_table_ident,
                coh_alias=cohort_alias,
                md_alias=md_alias,
                hadm_col_in_temp=cte_hadm_id_for_grouping, # MergedDataCTE中用于join的hadm_id
                limit=pgsql.Literal(preview_limit)
            )
            return preview_sql, None, params_for_cte, generated_column_details_for_preview


    # --- 数据库连接管理 (主Tab层面) ---
    def _connect_main_db(self):
        # 这个方法用于主Tab执行可能需要的、非面板特定的数据库操作
        # （目前主要是 MergeSQLWorker 和 preview_merge_data 使用）
        # 面板有自己的 _connect_panel_db
        # 为避免混淆，这里暂时不实现，让需要的地方各自按需连接
        pass

    def _close_main_db(self):
        # 对应 _connect_main_db
        pass

    # execute_merge, preview_merge_data, _get_readable_sql_with_conn, cancel_merge,
    # on_merge_worker_finished_actions, trigger_preview_after_thread_finish, on_merge_error
    # 这些方法基本保持不变，因为它们调用 _build_merge_query 并与 MergeSQLWorker 交互。
    # 主要变化在 _build_merge_query 如何从新的UI结构中收集参数。
    def execute_merge(self):
        if not self._are_configs_valid_for_action():
            QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写。")
            return
        
        # 调用 _build_merge_query 获取执行步骤
        build_result = self._build_merge_query(for_execution=True)
        if build_result is None or len(build_result) < 4:
            QMessageBox.critical(self, "内部错误", "构建合并查询时未能返回预期结果。"); return
        
        execution_steps_list, signal_type, new_cols_desc_for_worker, column_details_for_dialog = build_result

        if signal_type != "execution_list": # An error message was returned
            error_msg_from_build = signal_type if isinstance(signal_type, str) else "未能生成SQL执行步骤。"
            QMessageBox.critical(self, "合并准备失败", f"无法构建SQL: {error_msg_from_build}"); return
        
        if not column_details_for_dialog:
            QMessageBox.warning(self, "无法继续", "未能生成列详情以供确认。"); return

        column_preview_message = f"确定要向表 '{self.selected_cohort_table}' 中添加/更新以下列吗？\n"
        for name, type_str in column_details_for_dialog:
            column_preview_message += f"\n - {name} (类型: {type_str})"
        column_preview_message += "\n\n此操作将直接修改数据库表。"

        reply_confirm_cols = QMessageBox.question(self, '确认操作', column_preview_message,
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply_confirm_cols == QMessageBox.StandardButton.No: return
        
        db_params = self.get_db_params()
        if not db_params: QMessageBox.critical(self, "合并失败", "无法获取数据库连接参数。"); return
        
        # SQL预览（可选，可以简化或移除，因为完整的SQL预览在_build_merge_query中）
        preview_text = f"-- 准备为表 {self.selected_cohort_table} 添加列: {new_cols_desc_for_worker} --"
        if execution_steps_list and len(execution_steps_list) > 1:
            # 可以尝试显示第一个ALTER TABLE或CREATE TEMP TABLE语句的预览
            first_step_sql, first_step_params = execution_steps_list[0] # ALTER
            if len(execution_steps_list) > 1 : 
                first_step_sql, first_step_params = execution_steps_list[1] # CREATE TEMP
            
            conn_temp_readable = None
            try:
                conn_temp_readable = psycopg2.connect(**db_params)
                preview_text += f"\n-- 部分SQL预览 (步骤1或2):\n{self._get_readable_sql_with_conn(first_step_sql, first_step_params, conn_temp_readable)}"
            except Exception as e:
                preview_text += f"\n-- 部分SQL预览 (模板):\n{str(first_step_sql)}"
            finally:
                if conn_temp_readable: conn_temp_readable.close()

        self.sql_preview.setText(preview_text)
        QApplication.processEvents()

        self.prepare_for_long_operation(True)
        self.merge_worker = MergeSQLWorker(db_params, execution_steps_list,
                                           self.selected_cohort_table, new_cols_desc_for_worker)
        self.worker_thread = QThread()
        self.merge_worker.moveToThread(self.worker_thread)
        
        self.worker_thread.started.connect(self.merge_worker.run)
        self.merge_worker.finished.connect(self.on_merge_worker_finished_actions)
        self.merge_worker.error.connect(self.on_merge_error_actions) # 修改连接到新的错误处理槽
        self.merge_worker.progress.connect(self.update_execution_progress)
        self.merge_worker.log.connect(self.update_execution_log)
        
        self.merge_worker.finished.connect(self.worker_thread.quit)
        self.merge_worker.error.connect(self.worker_thread.quit)
        
        self.worker_thread.finished.connect(self.trigger_preview_after_thread_finish)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.finished.connect(lambda: setattr(self, 'merge_worker', None))
        
        self.worker_thread.start()

    def preview_merge_data(self):
        if not self._are_configs_valid_for_action():
            QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写以进行预览。")
            return

        db_params = self.get_db_params()
        if not db_params: QMessageBox.warning(self, "未连接", "请先连接数据库。"); return
        
        conn_for_preview = None
        try:
            conn_for_preview = psycopg2.connect(**db_params)
            conn_for_preview.autocommit = True # 预览通常是只读的

            preview_sql_obj, error_msg, params_list, gen_cols = self._build_merge_query(preview_limit=100, for_execution=False)
            
            if error_msg:
                QMessageBox.warning(self, "无法预览", error_msg); return
            if not preview_sql_obj:
                QMessageBox.warning(self, "无法预览", "未能生成预览SQL。"); return

            readable_sql_for_display = self._get_readable_sql_with_conn(preview_sql_obj, params_list, conn_for_preview)
            self.sql_preview.setText(f"-- Preview Query (Display Only):\n{readable_sql_for_display}")
            
            df = pd.read_sql_query(preview_sql_obj.as_string(conn_for_preview), conn_for_preview, params=params_list if params_list else None)
            
            self.preview_table.clearContents()
            self.preview_table.setRowCount(df.shape[0]); self.preview_table.setColumnCount(df.shape[1])
            self.preview_table.setHorizontalHeaderLabels(df.columns)
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    self.preview_table.setItem(i, j, QTableWidgetItem(str(df.iloc[i, j]) if pd.notna(df.iloc[i, j]) else ""))
            self.preview_table.resizeColumnsToContents()
            QMessageBox.information(self, "预览成功", f"已生成预览数据 ({df.shape[0]} 条)。")

        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"执行预览查询失败: {str(e)}\nTraceback:\n{traceback.format_exc()}")
            if 'readable_sql_for_display' in locals():
                 self.sql_preview.append(f"\n-- ERROR DURING PREVIEW: {str(e)}")
        finally:
            if conn_for_preview: conn_for_preview.close()
    
    def _get_readable_sql_with_conn(self, sql_obj_or_str, params_list, conn):
        # (与之前版本相同)
        if not conn or conn.closed:
            # Fallback if connection is not available for mogrify
            sql_template = str(sql_obj_or_str.as_string(psycopg2.connect(dbname='dummy')) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)) # Dummy conn for as_string
            # Basic formatting, not SQL injection safe for display, but it's for display only
            try:
                return sql_template % tuple(f"'{p}'" if isinstance(p, str) else p for p in params_list) if params_list else sql_template
            except:
                return f"{sql_template} -- Parameters: {params_list} (basic formatting failed)"

        try:
            # Use a cursor from the provided connection
            with conn.cursor() as cur:
                sql_string_template = sql_obj_or_str.as_string(conn) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
                return cur.mogrify(sql_string_template, params_list).decode(conn.encoding or 'utf-8')
        except Exception as e:
            print(f"Error using mogrify with provided conn: {e}")
            # Fallback if mogrify fails
            base_sql_str = sql_obj_or_str.as_string(conn) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
            return f"{base_sql_str}\n-- PARAMETERS: {params_list} (Mogrify failed with conn)"


    def cancel_merge(self): # 与旧版相同
        if self.merge_worker:
            self.update_execution_log("正在请求取消合并操作...")
            self.merge_worker.cancel()
            self.cancel_merge_btn.setEnabled(False) 

    @Slot()
    def on_merge_worker_finished_actions(self): # 与旧版相同
        base_name_for_log = self.new_column_name_input.text()
        self.update_execution_log(f"成功向表 {self.selected_cohort_table} 添加/更新与 '{base_name_for_log}' 相关的列。")
        QMessageBox.information(self, "合并成功", f"已成功向表 {self.selected_cohort_table} 添加/更新与 '{base_name_for_log}' 相关的列。")
        self.prepare_for_long_operation(False)

    @Slot()
    def trigger_preview_after_thread_finish(self): # 与旧版相同
        if self.selected_cohort_table:
            print(f"Worker thread finished. Requesting preview for: mimiciv_data.{self.selected_cohort_table}")
            self.request_preview_signal.emit('mimiciv_data', self.selected_cohort_table)
        else:
            print("Worker thread finished, but no table selected for preview.")
        
    @Slot(str)
    def on_merge_error_actions(self, error_message): # 重命名以区分旧的 on_merge_error
        self.update_execution_log(f"合并失败: {error_message}")
        if "操作已取消" not in error_message:
             QMessageBox.critical(self, "合并失败", f"执行合并SQL失败: {error_message}")
        else: QMessageBox.information(self, "操作取消", "数据合并操作已取消。")
        self.prepare_for_long_operation(False)
        # if self.worker_thread and not self.worker_thread.isFinished(): # Defensive quit, 已在worker的finally中处理
        #     self.worker_thread.quit()
        
    def closeEvent(self, event): # 与旧版相同
        if self.worker_thread and self.worker_thread.isRunning():
             self.update_execution_log("正在尝试停止合并操作...")
             if self.merge_worker: self.merge_worker.cancel()
             self.worker_thread.quit()
             if not self.worker_thread.wait(1000): 
                 self.update_execution_log("合并线程未能及时停止。")
        # self._close_main_db() # 如果有主Tab级别的连接
        super().closeEvent(event)

# --- END OF FILE tab_special_data_master.py ---