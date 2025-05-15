# --- START OF MODIFIED source_panels/chartevents_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                               QListWidget, QListWidgetItem, QAbstractItemView,
                               QApplication, QGroupBox, QLabel, QMessageBox, 
                               QComboBox, QScrollArea)
from PySide6.QtCore import Qt, Slot

from .base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.value_aggregation_widget import ValueAggregationWidget
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

import psycopg2.sql as pgsql
import traceback


class CharteventsConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)

        filter_group = QGroupBox("筛选监测指标 (来自 mimc_hosp.d_items)")
        filter_group_layout = QVBoxLayout(filter_group)

        self.condition_widget = ConditionGroupWidget(is_root=True)
        cg_scroll_area_panel = QScrollArea()
        cg_scroll_area_panel.setWidgetResizable(True)
        cg_scroll_area_panel.setWidget(self.condition_widget)
        cg_scroll_area_panel.setMinimumHeight(200)
        filter_group_layout.addWidget(cg_scroll_area_panel)
        
        self.filter_items_btn = QPushButton("筛选指标项目")
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        filter_action_layout = QHBoxLayout()
        filter_action_layout.addStretch()
        filter_action_layout.addWidget(self.filter_items_btn)
        filter_group_layout.addLayout(filter_action_layout)
        
        self.item_list = QListWidget()
        self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.item_list.itemSelectionChanged.connect(self._on_item_selection_changed)
        filter_group_layout.addWidget(self.item_list)

        self.selected_items_label = QLabel("已选项目: 0")
        filter_group_layout.addWidget(self.selected_items_label)
        panel_layout.addWidget(filter_group)

        logic_group = QGroupBox("提取逻辑")
        logic_group_layout = QVBoxLayout(logic_group)

        value_type_layout = QHBoxLayout()
        value_type_layout.addWidget(QLabel("提取值列:"))
        self.value_type_combo = QComboBox()
        self.value_type_combo.addItem("数值 (valuenum)", "valuenum")
        self.value_type_combo.addItem("文本 (value)", "value")
        # _on_value_type_combo_changed 内部会 self.config_changed_signal.emit()
        self.value_type_combo.currentIndexChanged.connect(self._on_value_type_combo_changed) 
        value_type_layout.addWidget(self.value_type_combo)
        value_type_layout.addStretch()
        logic_group_layout.addLayout(value_type_layout)

        self.value_agg_widget = ValueAggregationWidget()
        # ValueAggregationWidget.aggregation_changed 是无参数信号，可以直接连接或用lambda
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.value_agg_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口:")
        # TimeWindowSelectorWidget.time_window_changed 是带参数 Signal(str)
        # 使用 lambda 忽略参数 text_param
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)

        self._on_value_type_combo_changed(self.value_type_combo.currentIndex())


    @Slot(int)
    def _on_value_type_combo_changed(self, index):
        is_text_mode = (self.value_type_combo.itemData(index) == "value")
        self.value_agg_widget.set_text_mode(is_text_mode)
        self.config_changed_signal.emit() 

    def populate_panel_if_needed(self):
        available_fields = [
            ("label", "项目名 (Label)"), ("abbreviation", "缩写 (Abbreviation)"),
            ("category", "类别 (Category)"), ("param_type", "参数类型 (Param Type)"),
            ("unitname", "单位 (Unit Name)"), ("linksto", "关联表 (Links To)"),
            ("dbsource", "数据源 (DB Source)"), ("itemid", "ItemID (精确)") 
        ]
        self.condition_widget.set_available_search_fields(available_fields)
        if self.condition_widget.keywords and available_fields:
             first_kw_field_combo = self.condition_widget.keywords[0].get("field_combo")
             if first_kw_field_combo and first_kw_field_combo.count() > 0:
                 first_kw_field_combo.setCurrentIndex(0)
        
        value_agg_time_window_options = [
            "ICU入住后24小时", "ICU入住后48小时", "整个ICU期间", "整个住院期间"
        ]
        self.time_window_widget.set_options(value_agg_time_window_options)

    def get_friendly_source_name(self) -> str:
        return "监测指标 (Chartevents - d_items)"

    def get_item_filtering_details(self) -> tuple:
        return "mimiciv_hosp.d_items", "label", "itemid", "筛选字段: d_items.label", None
        
    def get_panel_config(self) -> dict:
        condition_sql, condition_params = self.condition_widget.get_condition()
        return {
            "source_event_table": "mimiciv_icu.chartevents",
            "source_dict_table": "mimiciv_hosp.d_items",    
            "item_id_column_in_event_table": "itemid",      
            "item_filter_conditions": (condition_sql, condition_params), 
            "selected_item_ids": self.get_selected_item_ids(),
            "value_column_to_extract": self.value_type_combo.currentData(), 
            "time_column_in_event_table": "charttime", 
            "aggregation_methods": self.value_agg_widget.get_selected_methods(), 
            "time_window_text": self.time_window_widget.get_current_time_window_text(), 
        }

    def clear_panel_state(self): 
        self.condition_widget.clear_all() 
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.value_type_combo.setCurrentIndex(0) 
        self.value_agg_widget.clear_selections()
        self.time_window_widget.clear_selection()
        self.config_changed_signal.emit()
        
    # _on_item_selection_changed, _filter_items_action, update_panel_action_buttons_state 保持不变
    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit()

    @Slot()
    def _filter_items_action(self): 
         if not self._connect_panel_db():
             QMessageBox.warning(self, "数据库连接失败", "无法连接到数据库以筛选项目。")
             return
         dict_table, name_col, id_col, _, _ = self.get_item_filtering_details()
         condition_sql_template, condition_params = self.condition_widget.get_condition()
         self.item_list.clear(); self.item_list.addItem("正在查询...")
         self.filter_items_btn.setEnabled(False); QApplication.processEvents()
         if not condition_sql_template:
             self.item_list.clear(); self.item_list.addItem("请输入筛选条件。")
             self.filter_items_btn.setEnabled(True); self._close_panel_db(); return
         try:
             query_template_obj = pgsql.SQL("SELECT {id_col_ident}, {name_col_ident} FROM {dict_table_ident} WHERE {condition} ORDER BY {name_col_ident} LIMIT 500") \
                                 .format(id_col_ident=pgsql.Identifier(id_col),
                                         name_col_ident=pgsql.Identifier(name_col),
                                         dict_table_ident=pgsql.SQL(dict_table),
                                         condition=pgsql.SQL(condition_sql_template))
             self._db_cursor.execute(query_template_obj, condition_params)
             items = self._db_cursor.fetchall()
             self.item_list.clear()
             if items:
                 for item_id_val, item_name_disp_val in items:
                     display_name = str(item_name_disp_val) if item_name_disp_val is not None else f"ID_{item_id_val}"
                     list_item = QListWidgetItem(f"{display_name} (ID: {item_id_val})")
                     list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id_val), display_name))
                     self.item_list.addItem(list_item)
             else:
                 self.item_list.addItem("未找到符合条件的项目")
         except Exception as e:
             self.item_list.clear(); self.item_list.addItem("查询项目出错!")
             QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
         finally:
             self.filter_items_btn.setEnabled(True)
             self._close_panel_db()
             self.config_changed_signal.emit()
            
    def update_panel_action_buttons_state(self, general_config_ok: bool): 
        can_filter = general_config_ok and self.condition_widget.has_valid_input()
        self.filter_items_btn.setEnabled(can_filter)

# --- END OF MODIFIED source_panels/chartevents_panel.py ---