# --- START OF FILE source_panels/diagnosis_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                               QListWidget, QListWidgetItem, QAbstractItemView,QTextEdit,
                               QApplication, QGroupBox, QLabel, QMessageBox, QScrollArea,QFrame)
from PySide6.QtCore import Qt, Slot

from .base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.event_output_widget import EventOutputWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

import psycopg2.sql as pgsql
import traceback

class DiagnosisConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)

        filter_group = QGroupBox("筛选诊断 (来自 mimc_hosp.d_icd_diagnoses)")
        filter_group_layout = QVBoxLayout(filter_group) # 主垂直布局
        filter_group_layout.setSpacing(8)

        # 1. 条件构建区 (ConditionGroupWidget in QScrollArea)
        self.condition_widget = ConditionGroupWidget(is_root=True)
        self.condition_widget.condition_changed.connect(self.config_changed_signal.emit)

        cg_scroll_area_panel = QScrollArea()
        cg_scroll_area_panel.setWidgetResizable(True)
        cg_scroll_area_panel.setWidget(self.condition_widget)
        cg_scroll_area_panel.setMinimumHeight(200) # 调整一个合适的最小高度
        filter_group_layout.addWidget(cg_scroll_area_panel, 2) # Stretch factor 2 (使其优先扩展)

        # 2. 操作按钮区
        filter_action_layout = QHBoxLayout() 
        filter_action_layout.addStretch() 
        self.filter_items_btn = QPushButton("筛选指标项目") 
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        filter_action_layout.addWidget(self.filter_items_btn)
        # filter_action_layout.addStretch() # 如果想让按钮靠右，取消注释这个，并注释上面的 addStretch()
        filter_group_layout.addLayout(filter_action_layout) # 这个布局高度会比较固定

        # 可选的分隔线，增加视觉分离
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        filter_group_layout.addWidget(separator)

        # --- 新增：筛选SQL预览区 ---
        self.filter_sql_preview_label = QLabel("最近筛选SQL预览:") # 可选的标签
        filter_group_layout.addWidget(self.filter_sql_preview_label)
        self.filter_sql_preview_textedit = QTextEdit()
        self.filter_sql_preview_textedit.setReadOnly(True)
        self.filter_sql_preview_textedit.setFixedHeight(60) # 设置一个合适的高度，例如3-4行
        self.filter_sql_preview_textedit.setPlaceholderText("执行“筛选指标项目”后将在此显示SQL...")
        filter_group_layout.addWidget(self.filter_sql_preview_textedit)
        # --- 结束新增 ---

        # 可选的分隔线，增加视觉分离
        separator = QFrame()
        # ... (separator setup) ...
        filter_group_layout.addWidget(separator)
        
        # 3. 筛选结果显示区 (QListWidget in QScrollArea, 和 QLabel)
        self.item_list = QListWidget()
        self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.item_list.itemSelectionChanged.connect(self._on_item_selection_changed)

        item_list_scroll_area = QScrollArea() # 将 QListWidget 放入 QScrollArea
        item_list_scroll_area.setWidgetResizable(True)
        item_list_scroll_area.setWidget(self.item_list)
        item_list_scroll_area.setMinimumHeight(100) # 调整一个合适的最小高度
        filter_group_layout.addWidget(item_list_scroll_area, 1) # Stretch factor 1

        self.selected_items_label = QLabel("已选项目: 0")
        self.selected_items_label.setAlignment(Qt.AlignmentFlag.AlignRight) # 标签靠右
        filter_group_layout.addWidget(self.selected_items_label)

        # 将 filter_group 添加到主面板布局
        panel_layout.addWidget(filter_group) 

        logic_group = QGroupBox("提取逻辑")
        logic_group_layout = QVBoxLayout(logic_group)

        self.event_output_widget = EventOutputWidget() 
        self.event_output_widget.output_type_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.event_output_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口:")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)

    def populate_panel_if_needed(self):
        available_fields = [
            ("long_title", "诊断描述 (Long Title)"),
            ("icd_code", "诊断代码 (ICD Code 精确)"),("icd_version", "ICD 版本 (精确)")
        ]
        self.condition_widget.set_available_search_fields(available_fields)
        if self.condition_widget.keywords and available_fields:
             first_kw_field_combo = self.condition_widget.keywords[0].get("field_combo")
             if first_kw_field_combo and first_kw_field_combo.count() > 0:
                 first_kw_field_combo.setCurrentIndex(0)
        
        # 诊断通常只关注 "住院以前"
        diag_event_time_options = ["住院以前 (既往史)"]
        # 也可以允许其他，但 "住院以前" 是最常见的
        # general_event_time_options = ["整个住院期间 (当前入院)", "整个ICU期间 (当前入院)", "住院以前 (既往史)"]
        self.time_window_widget.set_options(diag_event_time_options)
        
    def get_friendly_source_name(self) -> str:
        return "诊断 (Diagnoses - d_icd_diagnoses)"

    def get_item_filtering_details(self) -> tuple:
        return "mimiciv_hosp.d_icd_diagnoses", "long_title", "icd_code", "筛选字段: d_icd_diagnoses.long_title", None

    # def get_value_column_for_aggregation(self) -> str | None: return None
    # def get_time_column_for_windowing(self) -> str | None: return None # 诊断事件没有自己的时间戳

    def get_panel_config(self) -> dict:
        # ... (获取 condition_sql, condition_params, selected_item_ids) ...
        condition_sql, condition_params = self.condition_widget.get_condition()
        selected_items = self.get_selected_item_ids()

        current_time_window = self.time_window_widget.get_current_time_window_text()
        
        join_override_sql = None
        # diagnoses_icd 表没有直接的事件时间戳。其时间上下文来自其关联的 admissions 表。
        # 对于“当前住院/ICU”，JOIN on hadm_id 已经限定了。
        # 对于“住院以前”，JOIN on subject_id + admissions.admittime < cohort.admittime 是核心。
        event_time_column = None # diagnoses_icd 没有自己的 charttime 或类似列

        if current_time_window == "住院以前 (既往史)":
            join_override_sql = pgsql.SQL(
                "FROM {event_table} {evt_alias} " # e.g., mimiciv_hosp.diagnoses_icd "evt"
                "JOIN {cohort_table} {coh_alias} ON {evt_alias}.subject_id = {coh_alias}.subject_id "
                "JOIN mimiciv_hosp.admissions {adm_evt} ON {evt_alias}.hadm_id = {adm_evt}.hadm_id"
            )
        
        # 对于 "整个住院期间 (当前入院)" 或 "整个ICU期间 (当前入院)"
        # diagnoses_icd 本身没有时间戳，它的时间上下文是其关联的 hadm_id。
        # 默认的 JOIN (evt.hadm_id = cohort.hadm_id) 已经将诊断限定在了正确的入院事件。
        # sql_builder_special.py 中针对非 is_value_source 且非 "住院以前" 的时间窗口逻辑
        # 可能需要调整，因为它们期望一个 actual_event_time_col_ident。
        # 或者，对于这类事件，这些时间窗口的意义不大，因为诊断通常是针对某次入院的，
        # "住院以前" 才需要特殊处理时间比较。

        return {
            "source_event_table": "mimiciv_hosp.diagnoses_icd",
            # "source_dict_table": "mimiciv_hosp.d_icd_diagnoses",
            "item_id_column_in_event_table": "icd_code",
            "item_filter_conditions": (condition_sql, condition_params),
            "selected_item_ids": selected_items,
            "value_column_to_extract": None,
            "time_column_in_event_table": event_time_column, # 设为None，因为诊断表本身无时间
            "event_outputs": self.event_output_widget.get_selected_outputs(),
            "time_window_text": current_time_window,
            "cte_join_on_cohort_override": join_override_sql
        }

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.filter_sql_preview_textedit.clear()
        self.event_output_widget.clear_selections()
        self.time_window_widget.clear_selection()
        # self.config_changed_signal.emit()

    # _on_item_selection_changed, _filter_items_action, update_panel_action_buttons_state 与 ProcedureConfigPanel 类似
    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit()
    @Slot()
    def _filter_items_action(self):
        if not self._connect_panel_db(): QMessageBox.warning(self, "数据库连接失败", "无法连接到数据库以筛选项目。"); return
        dict_table, name_col, id_col, _, _ = self.get_item_filtering_details()
        condition_sql_template, condition_params = self.condition_widget.get_condition()
        self.item_list.clear(); self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False); QApplication.processEvents()
        if not condition_sql_template:
            self.item_list.clear(); self.item_list.addItem("请输入筛选条件。")
            self.filter_items_btn.setEnabled(True); self._close_panel_db(); return
        try:
            query_template_obj = pgsql.SQL("SELECT {id_col_ident}, {name_col_ident} FROM {dict_table_ident} WHERE {condition} ORDER BY {name_col_ident} LIMIT 500").format(id_col_ident=pgsql.Identifier(id_col),name_col_ident=pgsql.Identifier(name_col),dict_table_ident=pgsql.SQL(dict_table),condition=pgsql.SQL(condition_sql_template))
            # --- 更新SQL预览文本框 ---
            try:
                # 尝试使用数据库连接来“美化”SQL（如果参数是元组，会替换 %s）
                # 注意：mogrify 用于调试，它会处理参数替换，但可能不完全等同于服务器执行时的绑定
                if self._db_conn and not self._db_conn.closed:
                    # Mogrify expects a query string, not a Composed object directly for params
                    # We need to get the string version of query_template_obj first
                    base_sql_str = query_template_obj.as_string(self._db_conn)
                    if condition_params: # mogrify needs params as a tuple or dict
                        mogrified_sql = self._db_conn.cursor().mogrify(base_sql_str, condition_params).decode(self._db_conn.encoding or 'utf-8')
                    else:
                        mogrified_sql = base_sql_str
                    self.filter_sql_preview_textedit.setText(mogrified_sql)
                else: # 如果没有连接，就显示模板和参数
                    self.filter_sql_preview_textedit.setText(
                        f"-- SQL Template --\n{str(query_template_obj)}\n-- Params --\n{condition_params}"
                    )
            except Exception as e_preview:
                # 如果生成预览失败，显示原始模板和参数
                self.filter_sql_preview_textedit.setText(
                    f"-- Error generating SQL preview: {e_preview}\n"
                    f"-- SQL Template --\n{str(query_template_obj)}\n-- Params --\n{condition_params}"
                )
            # --- 结束更新 ---
            
            
            self._db_cursor.execute(query_template_obj, condition_params)
            items = self._db_cursor.fetchall()
            self.item_list.clear()
            if items:
                for item_id_val, item_name_disp_val in items:
                    display_name = str(item_name_disp_val) if item_name_disp_val is not None else f"ID_{item_id_val}"
                    list_item = QListWidgetItem(f"{display_name} (ICD Code: {item_id_val})")
                    list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id_val), display_name))
                    self.item_list.addItem(list_item)
            else: self.item_list.addItem("未找到符合条件的项目")
        except Exception as e:
            self.item_list.clear(); self.item_list.addItem("查询项目出错!")
            QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
        finally:
            self.filter_items_btn.setEnabled(True); self._close_panel_db(); self.config_changed_signal.emit()
    def update_panel_action_buttons_state(self, general_config_ok: bool):
        # general_config_ok: 表示主 Tab 的通用配置是否OK（数据库已连接，队列表已选择）
        # has_valid_conditions_in_panel: 表示此面板内的 ConditionGroupWidget 是否有有效输入
        has_valid_conditions_in_panel = self.condition_widget.has_valid_input()
        
        # 筛选按钮的可用性取决于通用配置OK 并且 面板内的条件组有有效输入
        can_filter = general_config_ok and has_valid_conditions_in_panel
        
        # print(f"DEBUG Panel {self.__class__.__name__}: general_ok={general_config_ok}, panel_conditions_ok={has_valid_conditions_in_panel}, can_filter={can_filter}")
        self.filter_items_btn.setEnabled(can_filter)

# --- END OF FILE source_panels/diagnosis_panel.py ---