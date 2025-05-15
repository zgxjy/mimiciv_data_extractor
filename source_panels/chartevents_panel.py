# --- START OF PROPOSED MODIFICATIONS FOR source_panels/chartevents_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                               QListWidget, QListWidgetItem, QAbstractItemView,
                               QApplication, QGroupBox, QLabel, QMessageBox, QCheckBox,QComboBox,QScrollArea) # 新增 QComboBox
from PySide6.QtCore import Qt, Slot

from .base_panel import BaseSourceConfigPanel
from conditiongroup import ConditionGroupWidget
import psycopg2
import psycopg2.sql as pgsql
import traceback


class CharteventsConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)

        # 1. 项目筛选部分
        filter_group = QGroupBox("筛选监测指标 (来自 mimc_hosp.d_items)")
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

        # 新增：选择提取值类型的 QComboBox
        value_type_layout = QHBoxLayout()
        value_type_layout.addWidget(QLabel("提取值列:"))
        self.value_type_combo = QComboBox()
        self.value_type_combo.addItem("数值 (valuenum)", "valuenum")
        self.value_type_combo.addItem("文本 (value)", "value")
        self.value_type_combo.currentIndexChanged.connect(lambda: self.config_changed_signal.emit())
        value_type_layout.addWidget(self.value_type_combo)
        value_type_layout.addStretch()
        panel_layout.addLayout(value_type_layout) # 将此布局添加到面板

        # 2. 提取逻辑部分 - 这个面板使用主Tab的通用提取逻辑UI
        # get_aggregation_config_widget() 和 get_time_window_options() 会返回 None

        self.setLayout(panel_layout)

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
        # 可以在这里根据选中的 itemid 的 param_type 动态设置 value_type_combo 的默认值
        # 但为了简单起见，先让用户手动选择

    def get_friendly_source_name(self) -> str:
        return "监测指标 (Chartevents - d_items)"

    def get_item_filtering_details(self) -> tuple:
        return "mimiciv_hosp.d_items", "label", "itemid", "筛选字段: d_items.label", None
        
    def get_value_column_for_aggregation(self) -> str | None:
        # 现在由 value_type_combo 决定
        if hasattr(self, 'value_type_combo'):
            return self.value_type_combo.currentData()
        return "valuenum" # 默认或回退

    def get_time_column_for_windowing(self) -> str | None:
        return "charttime"

    def get_aggregation_config_widget(self) -> QWidget | None:
        return None 
    
    def get_time_window_options(self) -> list | None:
        return None 
        
    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        # 当选中项改变时，可以尝试根据选中项的 param_type 更新 value_type_combo 的默认值
        # 例如，如果选中的都是数字型itemid，则默认选 valuenum
        # 这里暂时不实现这个自动逻辑，让用户自己选 value_type_combo
        self.config_changed_signal.emit()

    @Slot()
    def _filter_items_action(self): # (此方法逻辑不变，保持原样)
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


    def get_panel_config(self) -> dict:
        condition_sql, condition_params = self.condition_widget.get_condition()
        
        value_col_to_extract = "valuenum" # 默认
        if hasattr(self, 'value_type_combo'):
            value_col_to_extract = self.value_type_combo.currentData()

        return {
            "source_event_table": "mimiciv_icu.chartevents",
            "source_dict_table": "mimiciv_hosp.d_items",    
            "item_id_column_in_event_table": "itemid",      
            "item_filter_conditions": (condition_sql, condition_params), 
            "selected_item_ids": self.get_selected_item_ids(),
            "value_column_to_extract": value_col_to_extract, # 新增：指明提取 valuenum 还是 value
        }

    def clear_panel_state(self): # (与之前版本相同)
        self.condition_widget.clear_all() 
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        if hasattr(self, 'value_type_combo'): self.value_type_combo.setCurrentIndex(0) # 重置为valuenum
        self.config_changed_signal.emit()
        
    def update_panel_action_buttons_state(self, general_config_ok: bool): # (与之前版本相同)
        can_filter = general_config_ok and self.condition_widget.has_valid_input()
        self.filter_items_btn.setEnabled(can_filter)
# --- END OF PROPOSED MODIFICATIONS FOR source_panels/chartevents_panel.py ---