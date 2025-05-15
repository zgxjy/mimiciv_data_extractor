# --- START OF FILE source_panels/chartevents_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                               QListWidget, QListWidgetItem, QAbstractItemView,
                               QApplication, QGroupBox, QLabel, QMessageBox, QCheckBox)
from PySide6.QtCore import Qt, Slot

from .base_panel import BaseSourceConfigPanel # 从同级目录的base_panel导入
from conditiongroup import ConditionGroupWidget # 假设conditiongroup.py在项目根目录或PYTHONPATH中
import psycopg2
import psycopg2.sql as pgsql


class CharteventsConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0) # 面板通常不需要自己的边距

        # 1. 项目筛选部分
        filter_group = QGroupBox("筛选监测指标 (来自 mimc_hosp.d_items)") # 明确字典表
        filter_group_layout = QVBoxLayout(filter_group)

        # search_field_hint_label 和 condition_widget 由主面板创建和管理
        self.condition_widget = ConditionGroupWidget(is_root=True) # search_field 会在 populate_panel_if_needed 中设置
        filter_group_layout.addWidget(self.condition_widget)
        
        self.filter_items_btn = QPushButton("筛选指标项目")
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        # 将按钮放在 ConditionGroupWidget 的右边或下方
        # 这里先简单地放在下方，可以在主面板中用 QHBoxLayout 调整
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

        # 2. 提取逻辑部分 - 这个面板使用主Tab的通用提取逻辑UI
        # 因此 get_aggregation_config_widget() 和 get_time_window_options() 会返回 None
        # 以告知主Tab使用它的通用控件

        self.setLayout(panel_layout)

    def populate_panel_if_needed(self):
        # 当此面板显示时，设置 ConditionGroupWidget 的 search_field
        dict_table, name_col, id_col, hint, _ = self.get_item_filtering_details()
        self.condition_widget.set_search_field(name_col)
        # 主Tab的 search_field_hint_label 需要由主Tab自己更新
        # self.parentWidget().update_search_field_hint(hint) # 不太好，让主Tab管理

    def get_friendly_source_name(self) -> str:
        return "监测指标 (Chartevents - d_items)"

    def get_item_filtering_details(self) -> tuple:
        # (dict_table, name_col_in_dict, id_col_in_dict, friendly_hint, event_table_if_no_dict)
        return "mimiciv_hosp.d_items", "label", "itemid", "筛选字段: label (mimiciv_hosp.d_items)", None

    def get_value_column_for_aggregation(self) -> str | None:
        return "valuenum" # Chartevents 主要聚合 valuenum

    def get_time_column_for_windowing(self) -> str | None:
        return "charttime"

    # Chartevents 使用主Tab的通用聚合UI和时间窗口UI
    def get_aggregation_config_widget(self) -> QWidget | None:
        return None 
    
    def get_time_window_options(self) -> list | None:
        return None # 返回None表示使用主Tab的通用时间窗口
        
    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit() # 通知主Tab配置已变，可能需要更新按钮状态

    @Slot()
    def _filter_items_action(self):
        if not self._connect_panel_db():
            QMessageBox.warning(self, "数据库连接失败", "无法连接到数据库以筛选项目。")
            return

        dict_table, name_col, id_col, _, _ = self.get_item_filtering_details()
        condition_sql_template, condition_params = self.condition_widget.get_condition()

        self.item_list.clear()
        self.item_list.addItem("正在查询...")
        self.filter_items_btn.setEnabled(False)
        QApplication.processEvents()

        if not condition_sql_template:
            self.item_list.clear()
            self.item_list.addItem("请输入筛选条件。")
            self.filter_items_btn.setEnabled(True)
            self._close_panel_db()
            return
        
        try:
            # 确保SQL对象中的表名和列名是正确引用的
            query_template_obj = pgsql.SQL("SELECT {id_col_ident}, {name_col_ident} FROM {dict_table_ident} WHERE {condition} ORDER BY {name_col_ident} LIMIT 500") \
                                .format(id_col_ident=pgsql.Identifier(id_col),
                                        name_col_ident=pgsql.Identifier(name_col),
                                        dict_table_ident=pgsql.SQL(dict_table), # dict_table 是字符串 "schema.table"
                                        condition=pgsql.SQL(condition_sql_template))
            
            # print(f"Debug SQL: {self._db_cursor.mogrify(query_template_obj, condition_params)}")
            self._db_cursor.execute(query_template_obj, condition_params)
            items = self._db_cursor.fetchall()
            self.item_list.clear()
            if items:
                for item_id_val, item_name_disp_val in items:
                    display_name = str(item_name_disp_val) if item_name_disp_val is not None else f"ID_{item_id_val}"
                    # item_id 可能是数值类型，需要转为字符串
                    list_item = QListWidgetItem(f"{display_name} (ID: {item_id_val})")
                    list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id_val), display_name)) # 存储 (id, name)
                    self.item_list.addItem(list_item)
            else:
                self.item_list.addItem("未找到符合条件的项目")
        except Exception as e:
            self.item_list.clear()
            self.item_list.addItem("查询项目出错!")
            QMessageBox.critical(self, "筛选项目失败", f"查询项目时出错: {str(e)}\n{traceback.format_exc()}")
        finally:
            self.filter_items_btn.setEnabled(True)
            self._close_panel_db()
            self.config_changed_signal.emit()

    def get_panel_config(self) -> dict:
        condition_sql, condition_params = self.condition_widget.get_condition()
        
        return {
            "source_event_table": "mimiciv_icu.chartevents", # 事件表
            "source_dict_table": "mimiciv_hosp.d_items",    # 字典表
            "item_id_column_in_event_table": "itemid",      # 事件表中与字典表ID关联的列
            "item_filter_conditions": (condition_sql, condition_params), # ConditionGroupWidget的输出
            "selected_item_ids": self.get_selected_item_ids(), # QListWidget中选中的ID
            # Chartevents 使用主Tab的通用聚合和时间窗口配置，所以这里不需要返回
            # "value_column_to_extract": self.get_value_column_for_aggregation(), # "valuenum"
            # "time_column_in_event_table": self.get_time_column_for_windowing(), # "charttime"
        }

    def clear_panel_state(self):
        self.condition_widget.clear_all() # ConditionGroupWidget 需要一个 clear_all 方法
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        # Checkboxes for aggregation methods are managed by the master tab for chartevents
        self.config_changed_signal.emit()
        
    def update_panel_action_buttons_state(self, general_config_ok: bool):
        # 筛选按钮的可用性取决于通用配置（如数据库连接）和自身是否有输入
        can_filter = general_config_ok and self.condition_widget.has_valid_input()
        self.filter_items_btn.setEnabled(can_filter)


# --- END OF FILE source_panels/chartevents_panel.py ---