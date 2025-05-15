# --- START OF FILE tab_data_dictionary.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                               QComboBox, QTableWidget, QTableWidgetItem, QLabel,
                               QMessageBox, QApplication, QHeaderView, QAbstractItemView,
                               QScrollArea,QGroupBox) # QScrollArea for ConditionGroupWidget
from PySide6.QtCore import Qt, Slot
import psycopg2
import psycopg2.sql as pgsql
import traceback

from ui_components.conditiongroup import ConditionGroupWidget # 导入增强版的 ConditionGroupWidget

class DataDictionaryTab(QWidget):
    DICT_D_ITEMS = "mimiciv_hosp.d_items"
    DICT_D_LABITEMS = "mimiciv_hosp.d_labitems"
    DICT_D_ICD_DIAGNOSES = "mimiciv_hosp.d_icd_diagnoses"
    DICT_D_ICD_PROCEDURES = "mimiciv_hosp.d_icd_procedures"

    TABLE_COLUMN_CONFIG = {
        DICT_D_ITEMS: [
            ("itemid", "ItemID"), 
            ("label", "Label"), 
            ("abbreviation", "Abbreviation"),
            ("category", "Category"), 
            ("param_type", "Param Type"), 
            ("unitname", "Unit Name"), 
            ("linksto", "Links To"),
            ("dbsource", "DB Source"),
            # ("conceptid", "ConceptID"), # 通常为空，可以暂时不显示
            # ("row_id", "Row ID") # 通常对用户意义不大
        ],
        DICT_D_LABITEMS: [
            ("itemid", "ItemID"), 
            ("label", "Label"), 
            ("fluid", "Fluid"),
            ("category", "Category"), 
            ("loinc_code", "LOINC Code")
            # ("row_id", "Row ID")
        ],
        DICT_D_ICD_DIAGNOSES: [ # 假设这些是实际列名
            ("icd_code", "ICD Code"), 
            ("icd_version", "ICD Version"), 
            ("long_title", "Long Title"),
            ("short_title", "Short Title") # 根据你的信息添加
            # ("row_id", "Row ID")
        ],
        DICT_D_ICD_PROCEDURES: [ # 假设这些是实际列名
            ("icd_code", "ICD Code"), 
            ("icd_version", "ICD Version"), 
            ("long_title", "Long Title"),
            ("short_title", "Short Title") # 根据你的信息添加
            # ("row_id", "Row ID")
        ]
    }

    AVAILABLE_SEARCH_FIELDS_FOR_CONDITIONS = {
        DICT_D_ITEMS: [
            ("label", "项目名 (Label)"), 
            ("abbreviation", "缩写 (Abbreviation)"),
            ("category", "类别 (Category)"), 
            ("param_type", "参数类型 (Param Type)"),
            ("unitname", "单位 (Unit Name)"), 
            ("linksto", "关联表 (Links To)"),
            ("dbsource", "数据源 (DB Source)"),
            ("itemid", "ItemID (精确)") 
        ],
        DICT_D_LABITEMS: [
            ("label", "项目名 (Label)"),
            ("category", "类别 (Category)"),
            ("fluid", "体液类型 (Fluid)"),
            ("loinc_code", "LOINC 代码"),
            ("itemid", "ItemID (精确)")
        ],
        DICT_D_ICD_DIAGNOSES: [
            ("long_title", "诊断描述 (Long Title)"),
            ("short_title", "诊断缩写 (Short Title)"), # 新增
            ("icd_code", "诊断代码 (ICD Code 精确)"),
            ("icd_version", "ICD 版本 (精确)")
        ],
        DICT_D_ICD_PROCEDURES: [
            ("long_title", "操作描述 (Long Title)"),
            ("short_title", "操作缩写 (Short Title)"), # 新增
            ("icd_code", "操作代码 (ICD Code 精确)"),
            ("icd_version", "ICD 版本 (精确)")
        ]
    }

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # --- 搜索控制区域 ---
        search_controls_layout = QHBoxLayout() # 改为 QHBoxLayout 容纳字典选择和搜索按钮

        search_controls_layout.addWidget(QLabel("搜索字典表:"))
        self.dict_table_combo = QComboBox()
        self.dict_table_combo.addItem("监测/输出/操作项 (d_items)", self.DICT_D_ITEMS)
        self.dict_table_combo.addItem("化验项 (d_labitems)", self.DICT_D_LABITEMS)
        self.dict_table_combo.addItem("诊断代码 (d_icd_diagnoses)", self.DICT_D_ICD_DIAGNOSES)
        self.dict_table_combo.addItem("操作代码 (d_icd_procedures)", self.DICT_D_ICD_PROCEDURES)
        self.dict_table_combo.currentIndexChanged.connect(self._on_dict_table_changed)
        search_controls_layout.addWidget(self.dict_table_combo, 1) # 给下拉框更多空间

        self.search_button = QPushButton("执行搜索")
        self.search_button.clicked.connect(self.perform_search)
        self.search_button.setEnabled(False)
        search_controls_layout.addWidget(self.search_button)
        
        main_layout.addLayout(search_controls_layout)

        # --- 条件组区域 ---
        condition_group_box = QGroupBox("构建搜索条件") # 给条件组一个标题
        condition_layout = QVBoxLayout(condition_group_box)
        self.condition_group_widget = ConditionGroupWidget(is_root=True)
        self.condition_group_widget.condition_changed.connect(self._update_search_button_state)

        scroll_area = QScrollArea() # 包裹在 QScrollArea 中
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(self.condition_group_widget)
        scroll_area.setMinimumHeight(200) # 增加最小高度以便显示更多条件
        condition_layout.addWidget(scroll_area)
        main_layout.addWidget(condition_group_box)


        # --- 结果显示区域 ---
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.result_table.horizontalHeader().setStretchLastSection(True)
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        main_layout.addWidget(self.result_table, 1)
        
        self.status_label = QLabel("请连接数据库并选择字典表进行搜索。")
        main_layout.addWidget(self.status_label)

        self.setLayout(main_layout)
        self._on_dict_table_changed() # 初始化

    @Slot()
    def on_db_connected(self):
        # self.search_button.setEnabled(True) # 按钮状态由 _update_search_button_state 控制
        self._update_search_button_state()
        self.status_label.setText("数据库已连接。请选择字典表并构建搜索条件。")

    @Slot()
    def _on_dict_table_changed(self):
        selected_table_key = self.dict_table_combo.currentData()
        
        # 1. 更新结果表格的表头
        column_config = self.TABLE_COLUMN_CONFIG.get(selected_table_key, [])
        self.result_table.clearContents()
        self.result_table.setRowCount(0)
        self.result_table.setColumnCount(len(column_config))
        headers = [col_info[1] for col_info in column_config]
        self.result_table.setHorizontalHeaderLabels(headers)

        # 2. 更新 ConditionGroupWidget 的可用搜索字段
        available_fields = self.AVAILABLE_SEARCH_FIELDS_FOR_CONDITIONS.get(selected_table_key, [])
        self.condition_group_widget.set_available_search_fields(available_fields)
        
        self.condition_group_widget.clear_all() # 清空之前的条件
        self.status_label.setText(f"当前字典表: {self.dict_table_combo.currentText()}。请构建搜索条件。")
        self._update_search_button_state()


    @Slot()
    def _update_search_button_state(self):
        """根据是否有有效输入和数据库连接来更新搜索按钮的状态。"""
        db_ok = bool(self.get_db_params())
        conditions_ok = self.condition_group_widget.has_valid_input()
        self.search_button.setEnabled(db_ok and conditions_ok)

    @Slot()
    def perform_search(self):
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库。"); return

        selected_table_key = self.dict_table_combo.currentData()
        if not selected_table_key:
            QMessageBox.warning(self, "未选择", "请选择一个要搜索的字典表。"); return
            
        condition_sql_template, query_params = self.condition_group_widget.get_condition()

        if not self.condition_group_widget.has_valid_input():
            # 如果允许无条件搜索，可以在这里设置默认条件
            # condition_sql_template = "TRUE"
            # query_params = []
            # self.status_label.setText("未指定条件，将显示所有记录 (最多500条，可能较慢)。")
            # QMessageBox.information(self, "提示", "未指定条件，将显示所有记录 (最多500条，可能较慢)。")
            # 也可以直接返回，要求用户必须输入条件
            QMessageBox.information(self, "提示", "请输入至少一个有效的搜索条件。")
            return

        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self.status_label.setText(f"正在从 {selected_table_key} 中搜索...")
        self.result_table.setRowCount(0)

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()

            column_config = self.TABLE_COLUMN_CONFIG.get(selected_table_key, [])
            if not column_config:
                raise ValueError("无效的字典表选择或未配置列信息。")

            db_cols_to_select = [pgsql.Identifier(col_info[0]) for col_info in column_config]
            
            query = pgsql.SQL("SELECT {cols} FROM {table}").format(
                cols=pgsql.SQL(', ').join(db_cols_to_select),
                table=pgsql.SQL(selected_table_key) # schema.table
            )

            if condition_sql_template:
                query += pgsql.SQL(" WHERE ") + pgsql.SQL(condition_sql_template)
            
            # 智能排序：如果 'label' 或 'long_title' 在选择的列中，优先用它们排序
            order_by_col_ident = db_cols_to_select[0] # 默认按第一列
            for col_ident in db_cols_to_select:
                if col_ident.strings[0].lower() in ["label", "long_title"]:
                    order_by_col_ident = col_ident
                    break
            query += pgsql.SQL(" ORDER BY {order_col} LIMIT 500").format(order_col=order_by_col_ident)

            # print("Executing SQL:", cur.mogrify(query, query_params).decode()) # 调试用
            cur.execute(query, query_params if query_params else None)
            rows = cur.fetchall()

            if rows:
                self.result_table.setRowCount(len(rows))
                for i, row_data in enumerate(rows):
                    for j, value in enumerate(row_data):
                        self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
                self.result_table.resizeColumnsToContents()
                # ... (列宽调整逻辑与之前相同) ...
                if "label" in [c[0] for c in column_config]:
                    label_idx = [c[0] for c in column_config].index("label")
                    self.result_table.setColumnWidth(label_idx, 300)
                if "long_title" in [c[0] for c in column_config]:
                    title_idx = [c[0] for c in column_config].index("long_title")
                    self.result_table.setColumnWidth(title_idx, 400)
                self.status_label.setText(f"在 {selected_table_key} 中找到 {len(rows)} 条符合条件的记录 (最多显示500条)。")
            else:
                self.status_label.setText(f"在 {selected_table_key} 中未找到符合条件的记录。")

        except psycopg2.Error as db_err:
            self.status_label.setText(f"数据库查询错误: {db_err}")
            QMessageBox.critical(self, "查询错误", f"数据库查询时发生错误:\n{db_err}\n\n{traceback.format_exc()}")
        except ValueError as val_err: 
            self.status_label.setText(f"配置错误: {val_err}")
            QMessageBox.warning(self, "配置错误", str(val_err))
        except Exception as e:
            self.status_label.setText(f"发生意外错误: {e}")
            QMessageBox.critical(self, "意外错误", f"执行搜索时发生意外错误:\n{e}\n\n{traceback.format_exc()}")
        finally:
            if conn:
                conn.close()
            QApplication.restoreOverrideCursor()

    def lookup_and_display_item(self, dict_table_name: str, item_identifier_column: str, item_identifier_value: str):
        """
        供外部调用的方法，用于查找特定字典表中的特定项目并显示。
        """
        # 1. 设置 QComboBox 到对应的字典表
        combo_idx = -1
        for i in range(self.dict_table_combo.count()):
            if self.dict_table_combo.itemData(i) == dict_table_name:
                combo_idx = i
                break
        if combo_idx != -1:
            self.dict_table_combo.setCurrentIndex(combo_idx) # 这会触发 _on_dict_table_changed
                                                            # _on_dict_table_changed 会调用 set_available_search_fields 和 clear_all
        else:
            QMessageBox.warning(self, "字典表未找到", f"未能在下拉列表中找到字典表 '{dict_table_name}'。")
            return

        # 2. 构建 ConditionGroupWidget 的状态以进行精确匹配
        #    确保 _available_search_fields 已经根据新的 dict_table_name 更新了
        #    _on_dict_table_changed 应该已经处理了 self.condition_group_widget.set_available_search_fields()
        
        # 检查 item_identifier_column 是否在当前可用字段中
        current_available_fields = self.AVAILABLE_SEARCH_FIELDS_FOR_CONDITIONS.get(dict_table_name, [])
        field_exists = any(col_info[0] == item_identifier_column for col_info in current_available_fields)

        if not field_exists:
            QMessageBox.warning(self, "搜索字段不适用", f"字段 '{item_identifier_column}' 不适用于当前选择的字典表 '{dict_table_name}'。")
            # 可以在这里选择一个默认字段，或者直接返回
            # 例如，如果 dict_table_name 是 d_items，但 item_identifier_column 是 long_title (不适用)
            # 就不能直接用它来构建条件。
            return

        # 构建精确匹配的状态
        # 假设 itemid 和 icd_code 通常需要精确匹配
        operator_type = "等于"
        # 对于 label, long_title 等，可能外部调用时也希望是精确的，这里统一用“等于”
        # 如果外部希望是“包含”，则外部调用者需要传入不同的操作符或使用不同的联动方法

        condition_state = {
            "logic": "AND",
            "keywords": [
                {
                    "field_db_name": item_identifier_column,
                    "type": operator_type, # 使用“等于”进行精确查找
                    "text": str(item_identifier_value)
                }
            ],
            "child_groups": []
        }
        
        self.condition_group_widget.set_state(condition_state, current_available_fields) # 传入可用字段以正确初始化
        
        # 3. 执行搜索
        self.perform_search()

# --- END OF FILE tab_data_dictionary.py ---