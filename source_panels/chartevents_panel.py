# --- START OF MODIFIED source_panels/chartevents_panel.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                               QListWidget, QListWidgetItem, QAbstractItemView,
                               QApplication, QGroupBox, QLabel, QMessageBox, QTextEdit,
                               QComboBox, QScrollArea,QFrame)
from PySide6.QtCore import Qt, Slot

from .base_panel import BaseSourceConfigPanel
from ui_components.conditiongroup import ConditionGroupWidget
from ui_components.value_aggregation_widget import ValueAggregationWidget # 使用更新后的
from ui_components.time_window_selector_widget import TimeWindowSelectorWidget
from app_config import DEFAULT_VALUE_COLUMN, DEFAULT_TEXT_VALUE_COLUMN, DEFAULT_TIME_COLUMN # 导入默认列名

import psycopg2.sql as pgsql
import traceback
from typing import Optional

class CharteventsConfigPanel(BaseSourceConfigPanel):
    def init_panel_ui(self):
        panel_layout = QVBoxLayout(self)
        panel_layout.setContentsMargins(0,0,0,0)
        panel_layout.setSpacing(10)

        # --- “筛选...” GroupBox ---
        filter_group = QGroupBox("筛选监测指标 (来自 mimiciv_icu.d_items)")
        filter_group_layout = QVBoxLayout(filter_group)
        filter_group_layout.setSpacing(8)

        self.condition_widget = ConditionGroupWidget(is_root=True)
        self.condition_widget.condition_changed.connect(self.config_changed_signal.emit)
        cg_scroll_area_panel = QScrollArea()
        cg_scroll_area_panel.setWidgetResizable(True)
        cg_scroll_area_panel.setWidget(self.condition_widget)
        cg_scroll_area_panel.setMinimumHeight(200)
        filter_group_layout.addWidget(cg_scroll_area_panel, 2)

        filter_action_layout = QHBoxLayout()
        filter_action_layout.addStretch()
        self.filter_items_btn = QPushButton("筛选指标项目")
        self.filter_items_btn.clicked.connect(self._filter_items_action)
        filter_action_layout.addWidget(self.filter_items_btn)
        filter_group_layout.addLayout(filter_action_layout)

        separator1 = QFrame()
        separator1.setFrameShape(QFrame.Shape.HLine); separator1.setFrameShadow(QFrame.Shadow.Sunken)
        filter_group_layout.addWidget(separator1)

        self.filter_sql_preview_label = QLabel("最近筛选SQL预览:")
        filter_group_layout.addWidget(self.filter_sql_preview_label)
        self.filter_sql_preview_textedit = QTextEdit()
        self.filter_sql_preview_textedit.setReadOnly(True)
        self.filter_sql_preview_textedit.setFixedHeight(60)
        self.filter_sql_preview_textedit.setPlaceholderText("执行“筛选指标项目”后将在此显示SQL...")
        filter_group_layout.addWidget(self.filter_sql_preview_textedit)

        separator2 = QFrame()
        separator2.setFrameShape(QFrame.Shape.HLine); separator2.setFrameShadow(QFrame.Shadow.Sunken)
        filter_group_layout.addWidget(separator2)
        
        self.item_list = QListWidget()
        self.item_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.item_list.itemSelectionChanged.connect(self._on_item_selection_changed)
        item_list_scroll_area = QScrollArea()
        item_list_scroll_area.setWidgetResizable(True)
        item_list_scroll_area.setWidget(self.item_list)
        item_list_scroll_area.setMinimumHeight(100)
        filter_group_layout.addWidget(item_list_scroll_area, 1)

        self.selected_items_label = QLabel("已选项目: 0")
        self.selected_items_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        filter_group_layout.addWidget(self.selected_items_label)
        panel_layout.addWidget(filter_group)

        # --- “提取逻辑” GroupBox ---
        logic_group = QGroupBox("提取逻辑")
        logic_group_layout = QVBoxLayout(logic_group)
        logic_group_layout.setSpacing(8)

        value_type_layout = QHBoxLayout()
        value_type_layout.addWidget(QLabel("提取值列:"))
        self.value_type_combo = QComboBox()
        self.value_type_combo.addItem(f"数值 ({DEFAULT_VALUE_COLUMN})", DEFAULT_VALUE_COLUMN)
        self.value_type_combo.addItem(f"文本 ({DEFAULT_TEXT_VALUE_COLUMN})", DEFAULT_TEXT_VALUE_COLUMN)
        self.value_type_combo.currentIndexChanged.connect(self._on_value_type_combo_changed)
        value_type_layout.addWidget(self.value_type_combo)
        value_type_layout.addStretch()
        logic_group_layout.addLayout(value_type_layout)

        self.value_agg_widget = ValueAggregationWidget() # 使用更新后的 Widget
        self.value_agg_widget.aggregation_changed.connect(self.config_changed_signal.emit)
        logic_group_layout.addWidget(self.value_agg_widget)

        self.time_window_widget = TimeWindowSelectorWidget(label_text="时间窗口:")
        self.time_window_widget.time_window_changed.connect(lambda: self.config_changed_signal.emit())
        logic_group_layout.addWidget(self.time_window_widget)
        
        panel_layout.addWidget(logic_group)
        self.setLayout(panel_layout)

        # 初始化 value_agg_widget 的文本模式
        self._on_value_type_combo_changed(self.value_type_combo.currentIndex())


    @Slot(int)
    def _on_value_type_combo_changed(self, index):
        # DEFAULT_TEXT_VALUE_COLUMN 是在 app_config 中定义的 "value"
        is_text_mode = (self.value_type_combo.currentData() == DEFAULT_TEXT_VALUE_COLUMN)
        self.value_agg_widget.set_text_mode(is_text_mode)
        self.config_changed_signal.emit() # 模式改变也算配置改变

    def populate_panel_if_needed(self):
        available_fields = [
            ("label", "项目名 (Label)"), ("abbreviation", "缩写 (Abbreviation)"),
            ("category", "类别 (Category)"), ("param_type", "参数类型 (Param Type)"),
            ("unitname", "单位 (Unit Name)"), ("linksto", "关联表 (Links To)"),("itemid", "ItemID (精确)")
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
        # (dict_table, name_col_in_dict, id_col_in_dict, hint, event_table_if_no_dict)
        return "mimiciv_icu.d_items", "label", "itemid", "筛选字段: d_items.label (label, category, itemid)", None
        
    def get_panel_config(self) -> dict:
        condition_sql, condition_params = self.condition_widget.get_condition()
        selected_ids = self.get_selected_item_ids()

        # 如果没有选择任何 itemid，根据业务逻辑，可能返回空字典或特定错误指示
        # if not selected_ids:
        #     QMessageBox.warning(self, "配置不完整", "请至少选择一个监测指标项目。")
        #     return {}

        aggregation_methods_from_widget = self.value_agg_widget.get_selected_methods()
        
        # 确保至少选择了一个聚合方法
        if not any(aggregation_methods_from_widget.values()):
            # QMessageBox.warning(self, "配置不完整", "请至少选择一种聚合方法。")
            return {} # 返回空字典表示配置不完整，主Tab会处理

        config = {
            "source_event_table": "mimiciv_icu.chartevents",
            "source_dict_table": "mimiciv_icu.d_items", # 用于UI筛选，builder不直接用
            "item_id_column_in_event_table": "itemid",
            "item_filter_conditions": (condition_sql, condition_params), # 用于从字典表筛选ID
            "selected_item_ids": selected_ids, # 筛选后选中的ID列表
            "value_column_to_extract": self.value_type_combo.currentData(), # "valuenum" or "value"
            "time_column_in_event_table": DEFAULT_TIME_COLUMN, # "charttime"
            "aggregation_methods": aggregation_methods_from_widget, # 来自 ValueAggregationWidget
            "event_outputs": {}, # Chartevents 使用 aggregation_methods，所以 event_outputs 为空
            "time_window_text": self.time_window_widget.get_current_time_window_text(),
            "primary_item_label_for_naming": self._get_primary_item_label_for_naming(), # 用于主Tab列名生成
            "cte_join_on_cohort_override": None # Chartevents 通常不需要覆盖默认JOIN (stay_id)
        }
        return config
        
    def _get_primary_item_label_for_naming(self) -> Optional[str]:
        """尝试获取一个用于主Tab列名生成的代表性项目标签。"""
        if self.item_list.selectedItems():
            # 使用第一个选中项的显示名（不含ID部分）
            first_selected_item_text = self.item_list.selectedItems()[0].text()
            return first_selected_item_text.split(' (ID:')[0].strip()
        return None

    def clear_panel_state(self):
        self.condition_widget.clear_all()
        self.item_list.clear()
        self.selected_items_label.setText("已选项目: 0")
        self.filter_sql_preview_textedit.clear()
        self.value_type_combo.setCurrentIndex(0) # 恢复默认值 "valuenum"
        self._on_value_type_combo_changed(0) # 确保 value_agg_widget 更新为数值模式
        self.value_agg_widget.clear_selections()
        if self.time_window_widget.combo_box.count() > 0: # 确保有选项才设置
            self.time_window_widget.combo_box.setCurrentIndex(0) # 或者使用 clear_selection
        # self.config_changed_signal.emit()

    def _on_item_selection_changed(self):
        count = len(self.item_list.selectedItems())
        self.selected_items_label.setText(f"已选项目: {count}")
        self.config_changed_signal.emit() # 选择变化也通知主Tab

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
            # 构建查询字典表的SQL
            query_template_obj = pgsql.SQL("SELECT {id_col_ident}, {name_col_ident} FROM {dict_table_ident} WHERE {condition} ORDER BY {name_col_ident} LIMIT 500") \
                                 .format(id_col_ident=pgsql.Identifier(id_col),
                                         name_col_ident=pgsql.Identifier(name_col),
                                         dict_table_ident=pgsql.SQL(dict_table), # dict_table 是字符串 "mimiciv_icu.d_items"
                                         condition=pgsql.SQL(condition_sql_template))
            
            # 更新SQL预览文本框
            try:
                if self._db_conn and not self._db_conn.closed:
                    base_sql_str = query_template_obj.as_string(self._db_conn) # 使用连接获取字符串
                    if condition_params:
                        mogrified_sql = self._db_cursor.mogrify(base_sql_str, condition_params).decode(self._db_conn.encoding or 'utf-8')
                    else:
                        mogrified_sql = base_sql_str
                    self.filter_sql_preview_textedit.setText(mogrified_sql)
                else:
                    self.filter_sql_preview_textedit.setText(f"-- SQL Template --\n{str(query_template_obj)}\n-- Params --\n{condition_params}")
            except Exception as e_preview:
                self.filter_sql_preview_textedit.setText(f"-- Error generating SQL preview: {e_preview}\n-- SQL Template --\n{str(query_template_obj)}\n-- Params --\n{condition_params}")

            self._db_cursor.execute(query_template_obj, condition_params)
            items = self._db_cursor.fetchall()
            self.item_list.clear() # 清除 "正在查询..."
            if items:
                for item_id_val, item_name_disp_val in items:
                    display_name = str(item_name_disp_val) if item_name_disp_val is not None else f"ID_{item_id_val}"
                    list_item = QListWidgetItem(f"{display_name} (ID: {item_id_val})")
                    # 存储 (itemid, display_name_for_column_base)
                    list_item.setData(Qt.ItemDataRole.UserRole, (str(item_id_val), display_name)) 
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
            self.config_changed_signal.emit() # 筛选完成也通知

    def update_panel_action_buttons_state(self, general_config_ok: bool):
        has_valid_conditions_in_panel = self.condition_widget.has_valid_input()
        can_filter = general_config_ok and has_valid_conditions_in_panel
        self.filter_items_btn.setEnabled(can_filter)

# --- END OF MODIFIED source_panels/chartevents_panel.py ---