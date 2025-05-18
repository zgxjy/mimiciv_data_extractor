# --- START OF FILE tab_special_data_master.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QTextEdit, QComboBox, QGroupBox,
                          QRadioButton, QButtonGroup, QStackedWidget,
                          QLineEdit, QProgressBar, QAbstractItemView, QApplication,
                          QScrollArea,QSizePolicy)
from PySide6.QtCore import Qt, Signal, Slot, QObject, QThread, QTimer
from typing import Optional

import psycopg2
import psycopg2.sql as pgsql
import re
import pandas as pd
import time
import traceback
import numpy as np # <--- 确保导入 numpy

from source_panels.base_panel import BaseSourceConfigPanel
from source_panels.chartevents_panel import CharteventsConfigPanel
from source_panels.labevents_panel import LabeventsConfigPanel
from source_panels.medication_panel import MedicationConfigPanel
from source_panels.procedure_panel import ProcedureConfigPanel
from source_panels.diagnosis_panel import DiagnosisConfigPanel
from sql_logic.sql_builder_special import build_special_data_sql
from utils import sanitize_name_part, validate_column_name
from app_config import SQL_BUILDER_DUMMY_DB_FOR_AS_STRING

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
                    try: self.current_sql_for_debug = sql_obj_or_str.as_string(conn_merge.cursor()); sql_str_for_log_peek = self.current_sql_for_debug[:200].split('\n')[0]
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
        self.config_panels: dict[int, BaseSourceConfigPanel] = {}
        self.user_manually_edited_col_name = False
        self.init_ui()
        QTimer.singleShot(0, lambda: self.rb_chartevents.setChecked(True))

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        main_layout.addWidget(scroll_area)
        content_widget = QWidget()
        scroll_area.setWidget(content_widget)
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(10,10,10,10)
        content_layout.setSpacing(10)
        cohort_group = QGroupBox("1. 选择目标队列数据表")
        cohort_layout = QHBoxLayout(cohort_group)
        cohort_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox(); self.table_combo.setMinimumWidth(250)
        self.table_combo.currentIndexChanged.connect(self.on_cohort_table_selected)
        cohort_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新列表"); self.refresh_btn.clicked.connect(self.refresh_cohort_tables); self.refresh_btn.setEnabled(False)
        cohort_layout.addWidget(self.refresh_btn); cohort_layout.addStretch()
        content_layout.addWidget(cohort_group)
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
        self.config_panel_stack.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        source_main_layout.addWidget(self.config_panel_stack)
        self._create_config_panels()
        content_layout.addWidget(source_and_panel_group)
        column_name_group = QGroupBox("3. 定义新列基础名")
        column_name_layout = QHBoxLayout(column_name_group)
        column_name_layout.addWidget(QLabel("新列基础名 (可修改):"))
        self.new_column_name_input = QLineEdit()
        self.new_column_name_input.setPlaceholderText("根据选择自动生成或手动输入...")
        self.new_column_name_input.textEdited.connect(self._on_new_column_name_manually_edited)
        self.new_column_name_input.editingFinished.connect(self._on_new_column_name_editing_finished)
        column_name_layout.addWidget(self.new_column_name_input, 1)
        content_layout.addWidget(column_name_group)
        self.execution_status_group = QGroupBox("合并执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,4); self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit(); self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(100)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False)
        content_layout.addWidget(self.execution_status_group)
        action_layout = QHBoxLayout()
        self.preview_merge_btn = QPushButton("预览待合并数据"); self.preview_merge_btn.clicked.connect(self.preview_merge_data); self.preview_merge_btn.setEnabled(False)
        action_layout.addWidget(self.preview_merge_btn)
        self.execute_merge_btn = QPushButton("执行合并到表"); self.execute_merge_btn.clicked.connect(self.execute_merge); self.execute_merge_btn.setEnabled(False)
        action_layout.addWidget(self.execute_merge_btn)
        self.cancel_merge_btn = QPushButton("取消合并"); self.cancel_merge_btn.clicked.connect(self.cancel_merge); self.cancel_merge_btn.setEnabled(False)
        action_layout.addWidget(self.cancel_merge_btn)
        content_layout.addLayout(action_layout)
        content_layout.addWidget(QLabel("SQL预览 (仅供参考):"))
        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True)
        self.sql_preview.setMinimumHeight(100); self.sql_preview.setMaximumHeight(200)
        content_layout.addWidget(self.sql_preview)
        content_layout.addWidget(QLabel("数据预览 (最多100条):"))
        self.preview_table = QTableWidget(); self.preview_table.setAlternatingRowColors(True); self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.preview_table.setMinimumHeight(200)
        content_layout.addWidget(self.preview_table)
        self.source_selection_group.buttonToggled.connect(self._on_source_type_changed)
        self.setLayout(main_layout)

    def _create_config_panels(self):
        panels_to_create = [
            (self.SOURCE_CHARTEVENTS, CharteventsConfigPanel),
            (self.SOURCE_LABEVENTS, LabeventsConfigPanel),
            (self.SOURCE_MEDICATIONS, MedicationConfigPanel),
            (self.SOURCE_PROCEDURES, ProcedureConfigPanel),
            (self.SOURCE_DIAGNOSES, DiagnosisConfigPanel),
        ]
        for source_id, PanelClass in panels_to_create:
            panel = PanelClass(self.get_db_params, self)
            if hasattr(panel, 'config_changed_signal'):
                panel.config_changed_signal.connect(self._on_panel_config_changed)
            else:
                print(f"警告: 面板 {PanelClass.__name__} 没有 config_changed_signal 信号。")
            self.config_panel_stack.addWidget(panel)
            self.config_panels[source_id] = panel

    def _update_active_panel(self, force_col_name_update=False):
        current_id = self.source_selection_group.checkedId()
        active_panel = self.config_panels.get(current_id)
        if active_panel:
            self.config_panel_stack.setCurrentWidget(active_panel)
            if hasattr(active_panel, 'populate_panel_if_needed'):
                active_panel.populate_panel_if_needed()
            QTimer.singleShot(0, lambda: self._finish_update_active_panel(active_panel, force_col_name_update))
        else:
            self.search_field_hint_label.setText("请选择一个数据来源。")
            self._generate_and_set_default_col_name(force_update=True)
            self.update_master_action_buttons_state()

    def _finish_update_active_panel(self, active_panel: BaseSourceConfigPanel, force_col_name_update: bool):
        details = ("-", "-", "-", "配置筛选条件。", "-")
        if hasattr(active_panel, 'get_item_filtering_details'):
                panel_details = active_panel.get_item_filtering_details()
                if panel_details and isinstance(panel_details, tuple) and len(panel_details) > 3:
                    details = panel_details
        hint = details[3] if details[3] else "配置筛选条件。"
        self.search_field_hint_label.setText(hint)
        self._generate_and_set_default_col_name(force_update=force_col_name_update)
        self.update_master_action_buttons_state()

    @Slot(int, bool)
    def _on_source_type_changed(self, id, checked):
        if checked:
            self._update_active_panel(force_col_name_update=True)

    @Slot()
    def _on_panel_config_changed(self):
        self.user_manually_edited_col_name = False
        QTimer.singleShot(0, lambda: self._generate_and_set_default_col_name(force_update=False))
        QTimer.singleShot(0, self.update_master_action_buttons_state)

    @Slot()
    def _on_new_column_name_manually_edited(self):
        self.user_manually_edited_col_name = True

    @Slot()
    def _on_new_column_name_editing_finished(self):
        self.update_master_action_buttons_state()

    def _generate_and_set_default_col_name(self, force_update=False):
        parts = []
        active_panel: Optional[BaseSourceConfigPanel] = self.config_panels.get(self.source_selection_group.checkedId())
        panel_config = {}
        if active_panel and hasattr(active_panel, 'get_panel_config'):
            panel_config = active_panel.get_panel_config() or {}
        logic_code = "data"; found_logic = False
        agg_methods = panel_config.get("aggregation_methods")
        if agg_methods and isinstance(agg_methods, dict):
            for key, selected in agg_methods.items():
                if selected: logic_code = key.lower(); found_logic = True; break
        if not found_logic:
            evt_outputs = panel_config.get("event_outputs")
            if evt_outputs and isinstance(evt_outputs, dict):
                for key, selected in evt_outputs.items():
                    if selected: logic_code = key.lower(); found_logic = True; break
        if not found_logic and active_panel:
            if hasattr(active_panel, 'value_agg_widget') and active_panel.value_agg_widget:
                selected_old_methods = active_panel.value_agg_widget.get_selected_methods()
                for key, chk_val in selected_old_methods.items():
                    if chk_val: logic_code = key; break
            elif hasattr(active_panel, 'event_output_widget') and active_panel.event_output_widget:
                selected_old_outputs = active_panel.event_output_widget.get_selected_outputs()
                for key, chk_val in selected_old_outputs.items():
                    if chk_val: logic_code = key; break
        parts.append(sanitize_name_part(logic_code))
        item_name_part = "item"
        primary_label_from_config = panel_config.get("primary_item_label_for_naming")
        selected_ids_from_config = panel_config.get("selected_item_ids")
        if primary_label_from_config:
            item_name_part = sanitize_name_part(primary_label_from_config)
        elif selected_ids_from_config and isinstance(selected_ids_from_config, list) and selected_ids_from_config:
            item_text_for_name = str(selected_ids_from_config[0])
            if active_panel and hasattr(active_panel, 'item_list') and active_panel.item_list and active_panel.item_list.selectedItems():
                try:
                    user_data = active_panel.item_list.selectedItems()[0].data(Qt.ItemDataRole.UserRole)
                    if isinstance(user_data, (list, tuple)) and len(user_data) > 1 and user_data[1]:
                        item_text_for_name = user_data[1]
                    else:
                         item_text_for_name = active_panel.item_list.selectedItems()[0].text().split(' (ID:')[0].strip()
                except Exception: pass
            item_name_part = sanitize_name_part(item_text_for_name)
        parts.append(item_name_part)
        time_code = ""
        time_window_text_from_config = panel_config.get("time_window_text")
        if time_window_text_from_config:
            time_map = {
                "ICU入住后24小时": "icu24h", "ICU入住后48小时": "icu48h",
                "整个ICU期间": "icuall", "整个住院期间": "hospall",
                "整个住院期间 (当前入院)": "hosp",
                "整个ICU期间 (当前入院)": "icu",
                "住院以前 (既往史)": "prior"
            }
            time_code = time_map.get(time_window_text_from_config,
                                     sanitize_name_part(time_window_text_from_config.split(" ")[0]))
        elif active_panel and hasattr(active_panel, 'time_window_widget') and active_panel.time_window_widget:
            current_time_text_old = active_panel.time_window_widget.get_current_time_window_text()
            if current_time_text_old:
                time_map_val = {"ICU入住后24小时": "icu24h", "ICU入住后48小时": "icu48h", "整个ICU期间": "icuall", "整个住院期间": "hospall"}
                time_map_evt = {"整个住院期间 (当前入院)": "hosp", "整个ICU期间 (当前入院)": "icu", "住院以前 (既往史)": "prior"}
                current_time_map_old = time_map_val if hasattr(active_panel, 'value_agg_widget') else time_map_evt
                time_code = current_time_map_old.get(current_time_text_old, sanitize_name_part(current_time_text_old.split(" ")[0]))
        if time_code: parts.append(time_code)
        default_name = "_".join(filter(None, parts))
        final_name = default_name if default_name and default_name.strip() else "new_col"
        final_name = final_name[:50]
        if final_name and final_name[0].isdigit():
            final_name = "_" + final_name
        if force_update or not self.user_manually_edited_col_name:
            current_text = self.new_column_name_input.text()
            if current_text != final_name:
                self.new_column_name_input.blockSignals(True)
                self.new_column_name_input.setText(final_name)
                self.new_column_name_input.blockSignals(False)
                if force_update:
                     self.user_manually_edited_col_name = False
            elif force_update and current_text == final_name:
                self.user_manually_edited_col_name = False

    def _are_configs_valid_for_action(self) -> bool:
        if not self.selected_cohort_table: return False
        col_name_text = self.new_column_name_input.text().strip()
        is_valid_col_name, name_err = validate_column_name(col_name_text)
        if not is_valid_col_name: return False
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if not active_panel or not hasattr(active_panel, 'get_panel_config'): return False
        panel_config = active_panel.get_panel_config()
        if not panel_config: return False
        agg_methods = panel_config.get("aggregation_methods")
        evt_outputs = panel_config.get("event_outputs")
        any_method_selected = False
        if agg_methods and isinstance(agg_methods, dict) and any(agg_methods.values()):
            any_method_selected = True
        elif evt_outputs and isinstance(evt_outputs, dict) and any(evt_outputs.values()):
            any_method_selected = True
        return any_method_selected

    def _build_merge_query(self, preview_limit=100, for_execution=False):
        if not self.selected_cohort_table:
            return None, "未选择目标队列数据表.", [], []
        base_new_col_name = self.new_column_name_input.text().strip()
        is_valid_base_name, name_error = validate_column_name(base_new_col_name)
        if not is_valid_base_name:
            return None, f"基础列名 '{base_new_col_name}' 无效: {name_error}", [], []
        active_panel_id = self.source_selection_group.checkedId()
        active_panel = self.config_panels.get(active_panel_id)
        if not active_panel or not hasattr(active_panel, 'get_panel_config'):
            return None, "未选择有效的数据来源或未找到其配置面板.", [], []
        panel_config_dict = active_panel.get_panel_config()
        if not panel_config_dict:
            panel_name = active_panel.__class__.__name__
            return None, f"来自 {panel_name} 的配置不完整或无效，无法构建查询。", [], []
        try:
            return build_special_data_sql(
                target_cohort_table_name=f"mimiciv_data.{self.selected_cohort_table}",
                base_new_column_name=base_new_col_name,
                panel_specific_config=panel_config_dict,
                for_execution=for_execution,
                preview_limit=preview_limit
            )
        except Exception as e:
            error_msg = f"构建SQL时发生内部错误: {str(e)}\n详细信息:\n{traceback.format_exc()}"
            return None, error_msg, [], []

    def prepare_for_long_operation(self, starting=True):
        is_enabled = not starting
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行合并操作...")
        self.table_combo.setEnabled(is_enabled)
        self.refresh_btn.setEnabled(is_enabled and bool(self.get_db_params()))
        for rb_button in self.source_selection_group.buttons():
            rb_button.setEnabled(is_enabled)
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel: active_panel.setEnabled(is_enabled)
        self.new_column_name_input.setEnabled(is_enabled)
        self.cancel_merge_btn.setEnabled(starting)
        if not starting: self.update_master_action_buttons_state()
        else:
            self.preview_merge_btn.setEnabled(False)
            self.execute_merge_btn.setEnabled(False)

    def update_execution_progress(self, value, max_value=None):
        if max_value is not None and self.execution_progress.maximum() != max_value:
            self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        self.execution_log.append(message)
        QApplication.processEvents()

    @Slot()
    def on_db_connected(self):
        self.refresh_btn.setEnabled(True)
        self.refresh_cohort_tables()

    def refresh_cohort_tables(self):
        db_params = self.get_db_params()
        conn = None
        self.table_combo.blockSignals(True)
        current_sel_text = self.table_combo.currentText()
        self.table_combo.clear()
        if not db_params:
            self.table_combo.addItem("数据库未连接")
            self.selected_cohort_table = None
            self.table_combo.blockSignals(False)
            self.on_cohort_table_selected(self.table_combo.currentIndex())
            return
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute(pgsql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'mimiciv_data' AND (table_name LIKE 'first_%_admissions' OR table_name LIKE 'all_%_admissions' OR table_name LIKE 'cohort_%') ORDER BY table_name"))
            tables = [r[0] for r in cur.fetchall()]
            if tables:
                self.table_combo.addItems(tables)
                idx = self.table_combo.findText(current_sel_text)
                self.table_combo.setCurrentIndex(idx if idx != -1 else 0)
            else:
                self.table_combo.addItem("未找到队列表")
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取队列表: {str(e)}")
            self.table_combo.addItem("获取列表失败")
        finally:
            if conn: conn.close()
            self.table_combo.blockSignals(False)
            self.on_cohort_table_selected(self.table_combo.currentIndex())

    def on_cohort_table_selected(self, index):
        current_text = self.table_combo.itemText(index)
        if index >= 0 and current_text and "未找到" not in current_text and "失败" not in current_text and "未连接" not in current_text:
            self.selected_cohort_table = current_text
        else:
            self.selected_cohort_table = None
        self.update_master_action_buttons_state()

    @Slot()
    def update_master_action_buttons_state(self):
        is_valid_for_action = self._are_configs_valid_for_action()
        self.preview_merge_btn.setEnabled(is_valid_for_action)
        self.execute_merge_btn.setEnabled(is_valid_for_action)
        active_panel = self.config_panels.get(self.source_selection_group.checkedId())
        if active_panel and hasattr(active_panel, 'update_panel_action_buttons_state'):
            db_connected = bool(self.get_db_params())
            cohort_table_selected = bool(self.selected_cohort_table)
            general_ok_for_panel_filter = db_connected and cohort_table_selected
            active_panel.update_panel_action_buttons_state(general_ok_for_panel_filter)

    def execute_merge(self):
        if not self._are_configs_valid_for_action():
            QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写，并且基础列名有效。")
            return
        build_result = self._build_merge_query(for_execution=True)
        if build_result is None or len(build_result) < 4:
            QMessageBox.critical(self, "内部错误", "构建合并查询时未能返回预期结果结构。")
            return
        execution_steps_list, signal_type_or_error, new_cols_desc_for_worker, column_details_for_dialog = build_result
        if signal_type_or_error != "execution_list":
            QMessageBox.critical(self, "合并准备失败", f"无法构建SQL: {signal_type_or_error if isinstance(signal_type_or_error, str) else '未知构建错误'}")
            return
        if not column_details_for_dialog:
            QMessageBox.warning(self, "无法继续", "未能生成列详情以供确认。")
            return
        column_preview_message = f"确定要向表 '{self.selected_cohort_table}' 中添加/更新以下列吗？\n" + \
                                 "\n".join([f" - {name} (类型: {type_str})" for name, type_str in column_details_for_dialog]) + \
                                 "\n\n此操作将直接修改数据库表。"
        if QMessageBox.question(self, '确认操作', column_preview_message,
                                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No:
            return
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.critical(self, "合并失败", "无法获取数据库连接参数。")
            return
        self.sql_preview.clear()
        self.sql_preview.append(f"-- 准备为表 {self.selected_cohort_table} 添加/更新列 ({new_cols_desc_for_worker}) --\n")
        temp_conn_for_display = None; readable_sql_steps = []
        try:
            if db_params: temp_conn_for_display = psycopg2.connect(**db_params)
            for i, (sql_obj_or_str, params_for_step) in enumerate(execution_steps_list):
                step_header = f"\n-- 执行步骤 {i+1} --"
                try:
                    readable_step = self._get_readable_sql_with_conn(sql_obj_or_str, params_for_step, temp_conn_for_display)
                    readable_sql_steps.append(f"{step_header}\n{readable_step};")
                except Exception as e_format_step:
                    readable_sql_steps.append(f"{step_header}\n-- (无法格式化步骤: {e_format_step}) --\n{str(sql_obj_or_str)}")
            self.sql_preview.append("\n".join(readable_sql_steps))
        except Exception as e_conn_preview:
            self.sql_preview.append(f"\n-- 无法连接数据库以完整预览执行步骤: {e_conn_preview} --")
            for i, (sql_obj_or_str, _) in enumerate(execution_steps_list):
                 self.sql_preview.append(f"\n-- 原始执行步骤 {i+1} --\n{str(sql_obj_or_str)};")
        finally:
            if temp_conn_for_display and not temp_conn_for_display.closed:
                temp_conn_for_display.close()
        QApplication.processEvents()
        self.prepare_for_long_operation(True)
        self.merge_worker = MergeSQLWorker(db_params, execution_steps_list, self.selected_cohort_table, new_cols_desc_for_worker)
        self.worker_thread = QThread()
        self.merge_worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.merge_worker.run)
        self.merge_worker.finished.connect(self.on_merge_worker_finished_actions)
        self.merge_worker.error.connect(self.on_merge_error_actions)
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
            QMessageBox.warning(self, "配置不完整", "请确保所有必要的选项已选择或填写以进行预览，并且基础列名有效。")
            return
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库。")
            return
        conn_for_preview = None
        try:
            conn_for_preview = psycopg2.connect(**db_params)
            conn_for_preview.autocommit = True
            preview_sql_obj, error_msg, params_list, _ = self._build_merge_query(preview_limit=100, for_execution=False)

            if error_msg:
                QMessageBox.warning(self, "无法预览", error_msg)
                self.sql_preview.setText(f"-- BUILD ERROR: {error_msg}")
                if conn_for_preview: conn_for_preview.close() # Ensure connection is closed
                return
            if not preview_sql_obj:
                QMessageBox.warning(self, "无法预览", "未能生成预览SQL。")
                if conn_for_preview: conn_for_preview.close() # Ensure connection is closed
                return

            readable_sql_for_display = self._get_readable_sql_with_conn(preview_sql_obj, params_list, conn_for_preview)
            self.sql_preview.setText(f"-- Preview Query (Display Only):\n{readable_sql_for_display}")
            QApplication.processEvents()
            sql_string_for_pandas = preview_sql_obj.as_string(conn_for_preview.cursor())

            final_params_tuple_for_pandas = None
            if params_list:
                if len(params_list) == 1:
                    final_params_tuple_for_pandas = (params_list[0],)
                # No explicit 'else if len(params_list) > 1:' because build_special_data_sql for preview
                # should only return a list with one item (or an empty list if no params).
                # If it's empty, final_params_tuple_for_pandas remains None.
            
            print(f"DEBUG: SQL for pandas: {sql_string_for_pandas}")
            print(f"DEBUG: Params for pandas: {final_params_tuple_for_pandas}")

            df = pd.read_sql_query(sql_string_for_pandas, conn_for_preview, params=final_params_tuple_for_pandas)
            
            self.preview_table.clearContents()
            self.preview_table.setRowCount(df.shape[0])
            self.preview_table.setColumnCount(df.shape[1])
            self.preview_table.setHorizontalHeaderLabels(df.columns)
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    value = df.iloc[i, j]
                    item_text = ""
                    if isinstance(value, (list, dict)) or \
                       (hasattr(value, 'dtype') and isinstance(value, np.ndarray)):
                        item_text = str(value)
                    elif pd.isna(value):
                        item_text = ""
                    else:
                        item_text = str(value)
                    self.preview_table.setItem(i, j, QTableWidgetItem(item_text))

            self.preview_table.resizeColumnsToContents()
            QMessageBox.information(self, "预览成功", f"已生成预览数据 ({df.shape[0]} 条)。")
        except NameError as ne: # Should be caught if params_list was mistyped
            QMessageBox.critical(self, "代码错误", f"发生变量名错误: {str(ne)}\nTraceback:\n{traceback.format_exc()}")
            self.sql_preview.append(f"\n-- CODING ERROR (NameError): {str(ne)}")
        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"执行预览查询失败: {str(e)}\nTraceback:\n{traceback.format_exc()}")
            self.sql_preview.append(f"\n-- ERROR DURING PREVIEW: {str(e)}")
        finally:
            if conn_for_preview: conn_for_preview.close()

    def _get_readable_sql_with_conn(self, sql_obj_or_str, params_list, conn):
        if not conn or conn.closed:
            dummy_conn_for_string = None
            try:
                dummy_conn_for_string = psycopg2.connect(SQL_BUILDER_DUMMY_DB_FOR_AS_STRING)
                sql_template = sql_obj_or_str.as_string(dummy_conn_for_string.cursor()) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
            except Exception:
                sql_template = str(sql_obj_or_str)
            finally:
                if dummy_conn_for_string: dummy_conn_for_string.close()
            try:
                if params_list and '%s' in sql_template:
                    # This display formatting for params_list might need refinement if params_list contains tuples for IN clauses
                    # For now, it directly uses the elements.
                    # Ensure all elements in params_list are suitable for direct string formatting into the SQL template.
                    # If params_list = [('a', 'b')], then sql_template % (('a','b'),) would be needed.
                    # The current logic `sql_template % tuple(display_params)` assumes params_list maps 1:1 to %s.
                    # If params_list = [('a','b')] and sql_template is "IN %s", then tuple(display_params) becomes (("('a'", "'b'") ,)
                    # which is incorrect for "IN %s". psycopg2 handles IN %s with a single tuple param.
                    # For display with mogrify (below), this is handled correctly by psycopg2.
                    # For this fallback, it's tricky.
                    
                    # Simplification for fallback display: show template and params separately if complex.
                    if len(params_list) == 1 and isinstance(params_list[0], tuple): # Likely for IN clause
                         return f"{sql_template} -- Params (for IN clause): {params_list}"
                    else:
                         formatted_params = tuple(f"'{str(p).replace('%', '%%')}'" if isinstance(p, str) else str(p) for p in params_list)
                         return sql_template % formatted_params

                return f"{sql_template} -- Params: {params_list if params_list else 'None'}"
            except TypeError:
                return f"{sql_template} -- Params (display formatting error): {params_list}"
        try:
            with conn.cursor() as cur:
                sql_string_template = sql_obj_or_str.as_string(cur) if hasattr(sql_obj_or_str, 'as_string') else str(sql_obj_or_str)
                return cur.mogrify(sql_string_template, params_list if params_list else None).decode(conn.encoding or 'utf-8')
        except Exception as e_mogrify:
            base_sql_str = str(sql_obj_or_str)
            return f"{base_sql_str}\n-- PARAMETERS (mogrify failed with conn): {params_list}"

    def cancel_merge(self):
        if self.merge_worker:
            self.update_execution_log("正在请求取消合并操作...")
            self.merge_worker.cancel()
            self.cancel_merge_btn.setEnabled(False)

    @Slot()
    def on_merge_worker_finished_actions(self):
        desc_for_log = self.merge_worker.new_cols_description_str if self.merge_worker else self.new_column_name_input.text()
        self.update_execution_log(f"成功向表 {self.selected_cohort_table} 添加/更新与 '{desc_for_log}' 相关的列。")
        QMessageBox.information(self, "合并成功", f"已成功向表 {self.selected_cohort_table} 添加/更新与 '{desc_for_log}' 相关的列。")
        self.prepare_for_long_operation(False)

    @Slot()
    def trigger_preview_after_thread_finish(self):
        if self.selected_cohort_table:
            self.request_preview_signal.emit('mimiciv_data', self.selected_cohort_table)

    @Slot(str)
    def on_merge_error_actions(self, error_message):
        self.update_execution_log(f"合并失败: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "合并失败", f"执行合并SQL失败: {error_message}")
        else:
            QMessageBox.information(self, "操作取消", "数据合并操作已取消。")
        self.prepare_for_long_operation(False)

    def closeEvent(self, event):
        if self.worker_thread and self.worker_thread.isRunning():
             self.update_execution_log("正在尝试停止合并操作...")
             if self.merge_worker: self.merge_worker.cancel()
             self.worker_thread.quit()
             if not self.worker_thread.wait(1500):
                 self.update_execution_log("合并线程未能及时停止。")
        for panel in self.config_panels.values():
            if hasattr(panel, '_close_panel_db') and callable(panel._close_panel_db):
                panel._close_panel_db()
        super().closeEvent(event)

# --- END OF FILE tab_special_data_master.py ---