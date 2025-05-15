# --- START OF FILE source_panels/medication_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                               QListWidget, QListWidgetItem, QAbstractItemView,
                               QApplication, QGroupBox, QLabel, QMessageBox, QScrollArea)
from PySide6.QtCore import Qt, Slot

from .base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.event_output_widget import EventOutputWidget # 使用事件输出组件
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget

import psycopg2
import psycopg2.sql as pgsql
import traceback

class MedicationConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)

        filter_group = QGroupBox("筛选药物 (来自 mimiciv_hosp.prescriptions)")
        filter_group_layout = QVBoxLayout(filter_group)
        
        # search_field_hint_label 和 condition_widget 由主面板创建和管理
        self.condition_widget = ConditionGroupWidget(is_root=True) # search_field 会在 populate_panel_if_needed 中设置
        filter_group_layout.addWidget(self.condition_widget)
        # 将 ConditionGroupWidget 放入 QScrollArea
        cg_scroll_area_panel = QScrollArea()
        cg_scroll_area_panel.setWidgetResizable(True)
        cg_scroll_area_panel.setWidget(self.condition_widget)
        # 通常面板内的 ConditionGroupWidget 不需要设置固定的最小/最大高度，
        # 因为它会填充 QStackedWidget 中的可用空间，而 QStackedWidget 的大小由主Tab的布局决定。
        # 如果需要，可以设置: 
        cg_scroll_area_panel.setMinimumHeight(200)
        filter_group_layout.addWidget(cg_scroll_area_panel) # 添加滚动区域 
        
        self.filter_items_btn = QPushButton("筛选药物项目")
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

        self.event_output_widget = EventOutputWidget() 
        self.event_output_widget.output_type_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.event_output_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口:")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)

    def populate_panel_if_needed(self):
        available_fields = [("drug", "药物名称 (Drug)")]
        self.condition_widget.set_available_search_fields(available_fields)
        if self.condition_widget.keywords and available_fields:
            first_kw_field_combo = self.condition_widget.keywords[0].get("field_combo")
            if first_kw_field_combo and first_kw_field_combo.count() > 0:
                first_kw_field_combo.setCurrentIndex(0)

        general_event_time_options = [
            "整个住院期间 (当前入院)", "整个ICU期间 (当前入院)", "住院以前 (既往史)"
        ]
        self.time_window_widget.set_options(general_event_time_options)
        
    def get_friendly_source_name(self) -> str: return "用药 (Prescriptions)"
    def get_item_filtering_details(self) -> tuple: return None, "drug", "drug", "筛选字段: prescriptions.drug", "mimiciv_hosp.prescriptions"
    
    # MedicationPanel 不直接聚合数值，所以这些返回 None
    # def get_value_column_for_aggregation(self) -> str | None: return None 
    # def get_time_column_for_windowing(self) -> str | None: return "starttime" # prescriptions 有 starttime

    def get_panel_config(self) -> dict:
        condition_sql, condition_params = self.condition_widget.get_condition()
        return {
            "source_event_table": "mimiciv_hosp.prescriptions",
            "source_dict_table": None,
            "item_id_column_in_event_table": "drug", 
            "item_filter_conditions": (condition_sql, condition_params), 
            "selected_item_ids": self.get_selected_item_ids(),
            "value_column_to_extract": None, # 没有直接的值列用于通用聚合
            "time_column_in_event_table": "starttime", # 用于时间窗口
            "event_outputs": self.event_output_widget.get_selected_outputs(),
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
        }

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.event_output_widget.clear_selections()
        self.time_window_widget.clear_selection()
        self.config_changed_signal.emit()

    # _on_item_selection_changed, _filter_items_action, update_panel_action_buttons_state 与 LabeventsConfigPanel 类似
    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit()

    @Slot()
    def _filter_items_action(self):
        if not self._connect_panel_db(): QMessageBox.warning(self, "数据库连接失败", "无法连接到数据库以筛选项目。"); return
        _, name_col, id_col, _, event_table = self.get_item_filtering_details()
        condition_sql_template, condition_params = self.condition_widget.get_condition()
        self.item_list.clear(); self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False); QApplication.processEvents()
        if not condition_sql_template:
            self.item_list.clear(); self.item_list.addItem("请输入筛选条件。")
            self.filter_items_btn.setEnabled(True); self._close_panel_db(); return
        try:
            query_template_obj = pgsql.SQL("SELECT DISTINCT {name_col_ident} FROM {event_table_ident} WHERE {condition} ORDER BY {name_col_ident} LIMIT 500").format(name_col_ident=pgsql.Identifier(name_col), event_table_ident=pgsql.SQL(event_table), condition=pgsql.SQL(condition_sql_template))
            self._db_cursor.execute(query_template_obj, condition_params)
            items = self._db_cursor.fetchall()
            self.item_list.clear()
            if items:
                for item_tuple in items:
                    drug_name = str(item_tuple[0]) if item_tuple[0] is not None else "Unknown Drug"
                    list_item = QListWidgetItem(drug_name)
                    list_item.setData(Qt.ItemDataRole.UserRole, (drug_name, drug_name))
                    self.item_list.addItem(list_item)
            else: self.item_list.addItem("未找到符合条件的药物")
        except Exception as e:
            self.item_list.clear(); self.item_list.addItem("查询项目出错!")
            QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
        finally:
            self.filter_items_btn.setEnabled(True); self._close_panel_db(); self.config_changed_signal.emit()
            
    def update_panel_action_buttons_state(self, general_config_ok: bool):
        can_filter = general_config_ok and self.condition_widget.has_valid_input()
        self.filter_items_btn.setEnabled(can_filter)

# --- END OF FILE source_panels/medication_panel.py ---