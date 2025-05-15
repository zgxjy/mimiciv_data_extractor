# --- START OF FILE tab_data_dictionary.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                               QComboBox, QTableWidget, QTableWidgetItem, QLabel,
                               QMessageBox, QApplication, QHeaderView, QAbstractItemView,
                               QScrollArea,QGroupBox, QTextEdit, QProgressBar) 
from PySide6.QtCore import Qt, Slot
import psycopg2
import psycopg2.sql as pgsql 
import traceback
import re 

from ui_components.conditiongroup import ConditionGroupWidget

class DataDictionaryTab(QWidget):
    # ... (常量和 TABLE_COLUMN_CONFIG, AVAILABLE_SEARCH_FIELDS_FOR_CONDITIONS 保持不变) ...
    DICT_D_ITEMS = "mimiciv_icu.d_items" 
    DICT_D_LABITEMS = "mimiciv_hosp.d_labitems"
    DICT_D_ICD_DIAGNOSES = "mimiciv_hosp.d_icd_diagnoses"
    DICT_D_ICD_PROCEDURES = "mimiciv_hosp.d_icd_procedures"
    

    TABLE_COLUMN_CONFIG = {
        DICT_D_ITEMS: [
            ("itemid", "ItemID"), ("label", "Label"), ("abbreviation", "Abbreviation"),
            ("category", "Category"), ("param_type", "Param Type"), 
            ("unitname", "Unit Name"), ("linksto", "Links To")
        ],
        DICT_D_LABITEMS: [
            ("itemid", "ItemID"), ("label", "Label"), ("fluid", "Fluid"),
            ("category", "Category")
        ],
        DICT_D_ICD_DIAGNOSES: [
            ("icd_code", "ICD Code"), ("icd_version", "ICD Version"), 
            ("long_title", "Long Title")
        ],
        DICT_D_ICD_PROCEDURES: [
            ("icd_code", "ICD Code"), ("icd_version", "ICD Version"), 
            ("long_title", "Long Title")
        ]
    }

    AVAILABLE_SEARCH_FIELDS_FOR_CONDITIONS = {
        DICT_D_ITEMS: [ 
            ("label", "项目名 (Label)"), ("abbreviation", "缩写 (Abbreviation)"),
            ("category", "类别 (Category)"), ("param_type", "参数类型 (Param Type)"),
            ("unitname", "单位 (Unit Name)"), ("linksto", "关联表 (Links To)"),("itemid", "ItemID (精确)") 
        ],
        DICT_D_LABITEMS: [
            ("label", "项目名 (Label)"),("category", "类别 (Category)"),
            ("fluid", "体液类型 (Fluid)"),("itemid", "ItemID (精确)")
        ],
        DICT_D_ICD_DIAGNOSES: [
            ("long_title", "诊断描述 (Long Title)"),
            ("icd_code", "诊断代码 (ICD Code 精确)"),("icd_version", "ICD 版本 (精确)")
        ],
        DICT_D_ICD_PROCEDURES: [
            ("long_title", "操作描述 (Long Title)"),
            ("icd_code", "操作代码 (ICD Code 精确)"),("icd_version", "ICD 版本 (精确)")
        ]
    }

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()

    def init_ui(self):
        # ... (大部分 init_ui 保持不变) ...
        main_layout = QVBoxLayout(self)
        dict_select_layout = QHBoxLayout()
        dict_select_layout.addWidget(QLabel("搜索字典表:"))
        self.dict_table_combo = QComboBox()
        self.dict_table_combo.addItem("监测/输出/操作项 (d_items)", self.DICT_D_ITEMS)
        self.dict_table_combo.addItem("化验项 (d_labitems)", self.DICT_D_LABITEMS)
        self.dict_table_combo.addItem("诊断代码 (d_icd_diagnoses)", self.DICT_D_ICD_DIAGNOSES)
        self.dict_table_combo.addItem("操作代码 (d_icd_procedures)", self.DICT_D_ICD_PROCEDURES)
        self.dict_table_combo.currentIndexChanged.connect(self._on_dict_table_changed)
        dict_select_layout.addWidget(self.dict_table_combo, 1)
        dict_select_layout.addStretch()
        main_layout.addLayout(dict_select_layout)

        condition_group_box = QGroupBox("构建搜索条件")
        condition_layout = QVBoxLayout(condition_group_box)
        self.condition_group_widget = ConditionGroupWidget(is_root=True)
        self.condition_group_widget.condition_changed.connect(self._on_condition_changed_update_preview)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True); scroll_area.setWidget(self.condition_group_widget)
        scroll_area.setMinimumHeight(150); scroll_area.setMaximumHeight(300) 
        condition_layout.addWidget(scroll_area)
        main_layout.addWidget(condition_group_box)

        sql_preview_group = QGroupBox("SQL 预览 (只读)")
        sql_preview_layout = QVBoxLayout(sql_preview_group)
        self.sql_preview_textedit = QTextEdit()
        self.sql_preview_textedit.setReadOnly(True)
        self.sql_preview_textedit.setMaximumHeight(80) 
        sql_preview_layout.addWidget(self.sql_preview_textedit)
        main_layout.addWidget(sql_preview_group)

        search_button_layout = QHBoxLayout()
        search_button_layout.addStretch()
        self.search_button = QPushButton("执行搜索")
        self.search_button.clicked.connect(self.perform_search)
        self.search_button.setEnabled(False)
        search_button_layout.addWidget(self.search_button)
        search_button_layout.addStretch()
        main_layout.addLayout(search_button_layout)

        self.execution_status_group = QGroupBox("搜索执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar()
        self.execution_progress.setRange(0, 100) 
        self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit()
        self.execution_log.setReadOnly(True)
        self.execution_log.setMaximumHeight(100) 
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False) 
        main_layout.addWidget(self.execution_status_group)
        
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.result_table.horizontalHeader().setStretchLastSection(True)
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        main_layout.addWidget(self.result_table, 1) 
        
        self.setLayout(main_layout)
        self._on_dict_table_changed() 


    def _get_sql_string_from_composed(self, sql_object, params):
        # ... (此方法保持不变) ...
        if not sql_object: return ""
        current_sql_parts = []
        
        def to_string_recursive(obj_to_convert):
            if isinstance(obj_to_convert, pgsql.Composed):
                for sub_item in obj_to_convert:
                    to_string_recursive(sub_item)
            elif isinstance(obj_to_convert, pgsql.SQL):
                current_sql_parts.append(obj_to_convert.string)
            elif isinstance(obj_to_convert, pgsql.Identifier):
                processed_parts = []
                for s_part in obj_to_convert.strings: 
                    escaped_s_part = s_part.replace('"', '""') 
                    processed_parts.append(f'"{escaped_s_part}"') 
                current_sql_parts.append(".".join(processed_parts))
            elif isinstance(obj_to_convert, pgsql.Literal):
                current_sql_parts.append(obj_to_convert.adapted) 
            else:
                current_sql_parts.append(str(obj_to_convert))

        to_string_recursive(sql_object)
        base_template = "".join(current_sql_parts)

        if params:
            formatted_params = []
            for p_val in params:
                if isinstance(p_val, str):
                    escaped_p = p_val.replace("'", "''")
                    formatted_params.append(f"'{escaped_p}'")
                elif isinstance(p_val, (list, tuple)): 
                    inner_parts = []
                    for v_item in p_val:
                        if isinstance(v_item, str):
                            escaped_v_item = str(v_item).replace("'", "''")
                            inner_parts.append(f"'{escaped_v_item}'")
                        else:
                            inner_parts.append(str(v_item))
                    formatted_params.append("(" + ", ".join(inner_parts) + ")")
                else:
                    formatted_params.append(str(p_val) if p_val is not None else "NULL")
            
            try:
                if base_template.count("%s") == len(formatted_params):
                    return base_template % tuple(formatted_params)
                else:
                    temp_sql = base_template
                    for param_str_val in formatted_params: 
                        placeholder = "%s"
                        if placeholder in temp_sql:
                            temp_sql = temp_sql.replace(placeholder, param_str_val, 1)
                        else: 
                            break 
                    return temp_sql
            except TypeError as te_format:
                return f"{base_template}\n-- Parameters: {params} (Preview formatting error)"
        else:
            return base_template


    @Slot() 
    def _on_condition_changed_update_preview(self):
        self._update_sql_preview()
        self._update_search_button_state()
        # 当条件改变时，可以考虑清除上一次的日志和进度
        if self.execution_status_group.isVisible():
            self.execution_log.clear()
            self.execution_progress.setValue(0)
            # 可以选择隐藏状态组，直到下一次搜索
            # self.execution_status_group.setVisible(False) 
            # 或者只是清空内容，让用户知道新的预览对应新的搜索
            self._update_execution_log("条件已更改，请重新执行搜索以查看结果。")

    def _update_sql_preview(self):
        # ... (此方法保持不变) ...
        if not self.condition_group_widget.has_valid_input():
            self.sql_preview_textedit.setText("-- 请构建有效的搜索条件以生成SQL预览 --"); return
        selected_table_key = self.dict_table_combo.currentData()
        if not selected_table_key:
            self.sql_preview_textedit.setText("-- 请先选择一个字典表 --"); return

        condition_sql_template_str, query_params = self.condition_group_widget.get_condition()
        
        column_config = self.TABLE_COLUMN_CONFIG.get(selected_table_key, [])
        if not column_config: self.sql_preview_textedit.setText("-- 字典表列配置错误 --"); return
        db_cols_to_select_idents = [pgsql.Identifier(col_info[0]) for col_info in column_config]

        query_base_obj = pgsql.SQL("SELECT {cols} FROM {table}").format(
            cols=pgsql.SQL(', ').join(db_cols_to_select_idents),
            table=pgsql.SQL(selected_table_key) 
        )
        
        full_query_obj_for_preview = query_base_obj
        if condition_sql_template_str:
            full_query_obj_for_preview = pgsql.Composed([
                query_base_obj, 
                pgsql.SQL(" WHERE "), 
                pgsql.SQL(condition_sql_template_str) 
            ])
        
        readable_sql = ""
        try:
            readable_sql = self._get_sql_string_from_composed(full_query_obj_for_preview, query_params)
            
            order_by_col_ident = db_cols_to_select_idents[0] 
            for col_ident in db_cols_to_select_idents:
                if col_ident.strings[0].lower() in ["label", "long_title"]: order_by_col_ident = col_ident; break
            
            order_by_obj = pgsql.SQL(" ORDER BY {} LIMIT 500").format(order_by_col_ident)
            order_by_str = self._get_sql_string_from_composed(order_by_obj, []) 

            self.sql_preview_textedit.setText(readable_sql + order_by_str)

        except Exception as e:
            self.sql_preview_textedit.setText(f"-- 生成SQL预览失败: {e}\n{traceback.format_exc()} --")


    @Slot()
    def on_db_connected(self):
        self._update_search_button_state()
        self._update_execution_log("数据库已连接。请选择字典表并构建搜索条件。")
        self._on_condition_changed_update_preview() 

    @Slot()
    def _on_dict_table_changed(self):
        selected_table_key = self.dict_table_combo.currentData()
        column_config = self.TABLE_COLUMN_CONFIG.get(selected_table_key, [])
        self.result_table.clearContents(); self.result_table.setRowCount(0)
        self.result_table.setColumnCount(len(column_config))
        self.result_table.setHorizontalHeaderLabels([c[1] for c in column_config])
        available_fields = self.AVAILABLE_SEARCH_FIELDS_FOR_CONDITIONS.get(selected_table_key, [])
        self.condition_group_widget.set_available_search_fields(available_fields)
        self.condition_group_widget.clear_all()
        
        # 当字典表改变时，清除上一次的日志和进度
        self.execution_log.clear()
        self.execution_progress.setValue(0)
        # 可以选择隐藏状态组，或者只是清空
        # self.execution_status_group.setVisible(False) 
        self._update_execution_log(f"当前字典表: {self.dict_table_combo.currentText()}。请构建搜索条件。")
        self._on_condition_changed_update_preview()

    @Slot()
    def _update_search_button_state(self):
        db_ok = bool(self.get_db_params())
        conditions_ok = self.condition_group_widget.has_valid_input()
        self.search_button.setEnabled(db_ok and conditions_ok)

    def _prepare_for_search(self, starting=True):
        """准备UI以进行搜索操作或恢复UI。"""
        # 总是显示状态组，除非明确在其他地方隐藏
        self.execution_status_group.setVisible(True) 
        
        if starting:
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self._update_execution_log("准备开始搜索...") # 添加初始日志
        
        self.dict_table_combo.setEnabled(not starting)
        self.condition_group_widget.setEnabled(not starting)
        self.search_button.setEnabled(not starting and self._is_search_ready())

    def _is_search_ready(self):
        """检查搜索按钮是否应该启用（用于 _prepare_for_search 结束时）。"""
        db_ok = bool(self.get_db_params())
        conditions_ok = self.condition_group_widget.has_valid_input()
        return db_ok and conditions_ok

    def _update_execution_log(self, message):
        """更新执行日志。"""
        self.execution_log.append(message)
        QApplication.processEvents() 

    def _update_execution_progress(self, value):
        """更新进度条。"""
        self.execution_progress.setValue(value)
        QApplication.processEvents()


    @Slot()
    def perform_search(self):
        # ... (方法体上半部分，到 _prepare_for_search(True) 保持不变) ...
        db_params = self.get_db_params()
        if not db_params: 
            QMessageBox.warning(self, "未连接", "请先连接数据库。")
            self._update_execution_log("错误: 数据库未连接。") # 确保日志可见
            self.execution_status_group.setVisible(True)
            return
        selected_table_key = self.dict_table_combo.currentData()
        if not selected_table_key: 
            QMessageBox.warning(self, "未选择", "请选择一个要搜索的字典表。")
            self._update_execution_log("错误: 未选择字典表。")
            self.execution_status_group.setVisible(True)
            return
        if not self.condition_group_widget.has_valid_input():
            QMessageBox.information(self, "提示", "请输入至少一个有效的搜索条件。")
            self._update_execution_log("提示: 请输入有效的搜索条件。")
            self.execution_status_group.setVisible(True)
            return

        self._prepare_for_search(True) # 这会显示状态组并清空日志
        self._update_execution_log(f"开始从 {selected_table_key} 中搜索...") # 确保这条日志在清空后
        self._update_execution_progress(10)
        
        condition_sql_template, query_params = self.condition_group_widget.get_condition()
        self._update_sql_preview() 
        
        self.result_table.setRowCount(0)
        conn = None
        try:
            self._update_execution_log("正在连接数据库...")
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            self._update_execution_progress(25)

            column_config = self.TABLE_COLUMN_CONFIG.get(selected_table_key, [])
            if not column_config: 
                raise ValueError("无效的字典表选择或未配置列信息。")
            
            db_cols_to_select = [pgsql.Identifier(col_info[0]) for col_info in column_config]
            query = pgsql.SQL("SELECT {cols} FROM {table}").format(
                cols=pgsql.SQL(', ').join(db_cols_to_select),
                table=pgsql.SQL(selected_table_key)
            )
            if condition_sql_template: 
                query += pgsql.SQL(" WHERE ") + pgsql.SQL(condition_sql_template)
            
            order_by_col_ident = db_cols_to_select[0] 
            for col_ident in db_cols_to_select:
                if col_ident.strings[0].lower() in ["label", "long_title"]: 
                    order_by_col_ident = col_ident
                    break
            query += pgsql.SQL(" ORDER BY {order_col} LIMIT 500").format(order_col=order_by_col_ident)

            self._update_execution_log(f"正在执行SQL查询: {self.sql_preview_textedit.toPlainText()}") 
            cur.execute(query, query_params if query_params else None)
            self._update_execution_progress(60)
            rows = cur.fetchall()
            self._update_execution_log(f"查询完成，获取到 {len(rows)} 条记录。正在填充表格...")
            
            if rows:
                self.result_table.setRowCount(len(rows))
                for i, row_data in enumerate(rows):
                    for j, value in enumerate(row_data): 
                        self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
                self.result_table.resizeColumnsToContents()
                try:
                    if "label" in [c[0] for c in column_config]: self.result_table.setColumnWidth([c[0] for c in column_config].index("label"), 300)
                    if "long_title" in [c[0] for c in column_config]: self.result_table.setColumnWidth([c[0] for c in column_config].index("long_title"), 400)
                except ValueError: pass 
                self._update_execution_log(f"在 {selected_table_key} 中找到 {len(rows)} 条符合条件的记录 (最多显示500条)。")
            else: 
                self._update_execution_log(f"在 {selected_table_key} 中未找到符合条件的记录。")
            self._update_execution_progress(100)

        except psycopg2.Error as db_err:
            log_msg = f"数据库查询错误: {db_err}\nSQL: {self.sql_preview_textedit.toPlainText()}\nParams: {query_params}"
            self._update_execution_log(log_msg)
            self._update_execution_log(f"Traceback: {traceback.format_exc()}")
            QMessageBox.critical(self, "查询错误", f"数据库查询时发生错误:\n{db_err}\n\n{traceback.format_exc()}")
            self._update_execution_progress(0) 
        except ValueError as val_err: 
            self._update_execution_log(f"配置错误: {val_err}")
            QMessageBox.warning(self, "配置错误", str(val_err))
            self._update_execution_progress(0)
        except Exception as e:
            self._update_execution_log(f"发生意外错误: {e}")
            self._update_execution_log(f"Traceback: {traceback.format_exc()}")
            QMessageBox.critical(self, "意外错误", f"执行搜索时发生意外错误:\n{e}\n\n{traceback.format_exc()}")
            self._update_execution_progress(0)
        finally:
            if conn: conn.close()
            self._update_execution_log("数据库连接已关闭。搜索操作完成。") # 更新完成日志
            self._prepare_for_search(False) # 恢复UI，但保持日志和进度可见


    def lookup_and_display_item(self, dict_table_name: str, item_identifier_column: str, item_identifier_value: str):
        # ... (此方法保持不变) ...
        combo_idx = -1
        for i in range(self.dict_table_combo.count()):
            if self.dict_table_combo.itemData(i) == dict_table_name: combo_idx = i; break
        if combo_idx != -1: self.dict_table_combo.setCurrentIndex(combo_idx)
        else: QMessageBox.warning(self, "字典表未找到", f"未能在下拉列表中找到字典表 '{dict_table_name}'。"); return
        current_available_fields = self.AVAILABLE_SEARCH_FIELDS_FOR_CONDITIONS.get(dict_table_name, [])
        field_exists = any(col_info[0] == item_identifier_column for col_info in current_available_fields)
        if not field_exists: QMessageBox.warning(self, "搜索字段不适用", f"字段 '{item_identifier_column}' 不适用于当前选择的字典表 '{dict_table_name}'。"); return
        operator_type = "等于"
        condition_state = {"logic": "AND", "keywords": [{"field_db_name": item_identifier_column, "type": operator_type, "text": str(item_identifier_value)}], "child_groups": []}
        self.condition_group_widget.set_state(condition_state, current_available_fields)
        self.perform_search()

# --- END OF FILE tab_data_dictionary.py ---