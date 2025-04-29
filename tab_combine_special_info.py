# --- START OF FILE tab_combine_special_info.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QGridLayout,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QFormLayout, QRadioButton, QButtonGroup,
                          QLineEdit, QSpinBox, QListWidget, QListWidgetItem, QAbstractItemView)
from PySide6.QtCore import Qt, Signal, Slot # Import Slot
import psycopg2
import psycopg2.sql as pgsql # Import for safe SQL identifiers
import re
import pandas as pd # Import Pandas for preview
from conditiongroup import ConditionGroupWidget

class SpecialInfoDataExtractionTab(QWidget):
    request_preview_signal = Signal(str, str) # Signal to request preview in export tab

    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_cohort_table = None
        # No longer need self.selected_source_items here, get directly from list widget
        self.db_conn = None # Store connection for reuse
        self.db_cursor = None # Store cursor
        self.init_ui() # Initialize UI elements first

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(splitter)

        # --- Top Configuration Panel ---
        config_widget = QWidget()
        config_layout = QVBoxLayout(config_widget)
        config_layout.setContentsMargins(10, 10, 10, 10)
        config_layout.setSpacing(10)
        splitter.addWidget(config_widget)

        # 1. Cohort Selection
        # ... (rest of cohort selection UI remains the same) ...
        cohort_group = QGroupBox("1. 选择目标队列数据表")
        cohort_layout = QHBoxLayout(cohort_group)
        cohort_layout.addWidget(QLabel("队列表:"))
        self.table_combo = QComboBox()
        self.table_combo.setMinimumWidth(250)
        self.table_combo.currentIndexChanged.connect(self.on_cohort_table_selected)
        cohort_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新列表")
        self.refresh_btn.clicked.connect(self.refresh_cohort_tables)
        self.refresh_btn.setEnabled(False)
        cohort_layout.addWidget(self.refresh_btn)
        cohort_layout.addStretch()
        config_layout.addWidget(cohort_group)

        # 2. Data Source Selection
        # ... (rest of source selection UI remains the same) ...
        source_group = QGroupBox("2. 选择数据来源和筛选项目")
        source_main_layout = QVBoxLayout(source_group)

        source_select_layout = QHBoxLayout()
        source_select_layout.addWidget(QLabel("数据来源:"))
        self.direction_group = QButtonGroup(self)
        self.rb_lab = QRadioButton("化验 (labevents)")
        self.rb_med = QRadioButton("用药 (prescriptions)") # Changed from pharmacy
        self.rb_proc = QRadioButton("操作/手术 (procedures_icd)") # Changed from surgery
        self.rb_diag = QRadioButton("诊断 (diagnoses_icd)") # Changed from diagnosis
        self.rb_lab.setChecked(True)
        self.direction_group.addButton(self.rb_lab, 1)
        self.direction_group.addButton(self.rb_med, 2)
        self.direction_group.addButton(self.rb_proc, 3)
        self.direction_group.addButton(self.rb_diag, 4)
        source_select_layout.addWidget(self.rb_lab)
        source_select_layout.addWidget(self.rb_med)
        source_select_layout.addWidget(self.rb_proc)
        source_select_layout.addWidget(self.rb_diag)
        source_select_layout.addStretch()
        source_main_layout.addLayout(source_select_layout)

        # Item Filtering (Splitter for Condition and List)
        filter_splitter = QSplitter(Qt.Horizontal)
        source_main_layout.addWidget(filter_splitter)

        # Condition Widget (Left side of filter_splitter)
        self.condition_widget = ConditionGroupWidget(is_root=True, search_field="label") # Default for labs
        self.condition_widget.condition_changed.connect(self.filter_source_items) # Connect signal
        filter_splitter.addWidget(self.condition_widget)

        # Item List (Right side of filter_splitter)
        item_list_widget = QWidget()
        item_list_layout = QVBoxLayout(item_list_widget)
        item_list_layout.addWidget(QLabel("筛选出的项目 (多选合并时，列名基于首选项):")) # Clarify selection effect
        self.item_list = QListWidget()
        self.item_list.setSelectionMode(QAbstractItemView.ExtendedSelection) # Allow multi-select
        # Connect selection change to update counts AND default name
        self.item_list.itemSelectionChanged.connect(self.update_selected_items_count)
        self.item_list.itemSelectionChanged.connect(self.update_default_col_name) # <-- Connect here
        item_list_layout.addWidget(self.item_list)
        self.selected_items_label = QLabel("已选项目: 0")
        item_list_layout.addWidget(self.selected_items_label)
        filter_splitter.addWidget(item_list_widget)
        filter_splitter.setSizes([300, 200]) # Adjust initial sizes

        config_layout.addWidget(source_group)

        # Signal connections for radio buttons AFTER condition_widget is created
        self.rb_lab.toggled.connect(lambda checked: self.update_source_config(checked, "labevents", "label", "itemid"))
        self.rb_med.toggled.connect(lambda checked: self.update_source_config(checked, "prescriptions", "drug", "drug"))
        self.rb_proc.toggled.connect(lambda checked: self.update_source_config(checked, "procedures_icd", "long_title", "icd_code"))
        self.rb_diag.toggled.connect(lambda checked: self.update_source_config(checked, "diagnoses_icd", "long_title", "icd_code"))
        # Connect radio button toggle also to default name update
        self.direction_group.buttonToggled.connect(self.update_default_col_name) # <-- Connect group toggle


        # 3. Extraction Logic Definition
        logic_group = QGroupBox("3. 定义提取逻辑和列名")
        logic_layout = QGridLayout(logic_group)

        # Column Name
        logic_layout.addWidget(QLabel("新列名 (自动生成, 可修改):"), 0, 0) # Changed label
        self.new_column_name_input = QLineEdit()
        # Placeholder text guides the user
        self.new_column_name_input.setPlaceholderText("自动生成或手动输入列名...")
        # Update button state when text changes
        self.new_column_name_input.textChanged.connect(self.update_action_buttons_state)
        logic_layout.addWidget(self.new_column_name_input, 0, 1, 1, 3) # Span 3 columns

        # --- Lab Specific Options ---
        self.lab_options_widget = QWidget()
        lab_logic_layout = QFormLayout(self.lab_options_widget)
        lab_logic_layout.setContentsMargins(0,0,0,0)
        self.lab_agg_combo = QComboBox()
        self.lab_agg_combo.addItems(["首次 (First)", "末次 (Last)", "最小值 (Min)", "最大值 (Max)", "平均值 (Mean)", "计数 (Count)"])
        lab_logic_layout.addRow("提取方式:", self.lab_agg_combo)
        self.lab_time_window_combo = QComboBox()
        self.lab_time_window_combo.addItems(["ICU入住后24小时", "ICU入住后48小时", "整个ICU期间", "首次住院期间"])
        lab_logic_layout.addRow("时间窗口:", self.lab_time_window_combo)
        logic_layout.addWidget(self.lab_options_widget, 1, 0, 1, 4) # Span 4 columns
        # Connect combo box changes to default name update
        self.lab_agg_combo.currentTextChanged.connect(self.update_default_col_name) # <-- Connect here
        self.lab_time_window_combo.currentTextChanged.connect(self.update_default_col_name) # <-- Connect here

        # --- Med/Proc/Diag Specific Options ---
        self.event_options_widget = QWidget()
        event_logic_layout = QFormLayout(self.event_options_widget)
        event_logic_layout.setContentsMargins(0,0,0,0)
        self.event_output_combo = QComboBox()
        self.event_output_combo.addItems(["是否存在 (Boolean)", "发生次数 (Count)"])
        event_logic_layout.addRow("输出类型:", self.event_output_combo)
        self.event_time_window_combo = QComboBox()
        self.event_time_window_combo.addItems(["首次住院期间", "整个ICU期间"])
        event_logic_layout.addRow("时间窗口:", self.event_time_window_combo)
        logic_layout.addWidget(self.event_options_widget, 2, 0, 1, 4) # Span 4 columns
        self.event_options_widget.setVisible(False) # Initially hidden
        # Connect combo box changes to default name update
        self.event_output_combo.currentTextChanged.connect(self.update_default_col_name) # <-- Connect here
        self.event_time_window_combo.currentTextChanged.connect(self.update_default_col_name) # <-- Connect here

        config_layout.addWidget(logic_group)


        # 4. Actions (Preview & Merge)
        # ... (rest of actions UI remains the same) ...
        action_layout = QHBoxLayout()
        self.preview_merge_btn = QPushButton("预览待合并数据")
        self.preview_merge_btn.clicked.connect(self.preview_merge_data)
        self.preview_merge_btn.setEnabled(False)
        action_layout.addWidget(self.preview_merge_btn)

        self.execute_merge_btn = QPushButton("执行合并到表")
        self.execute_merge_btn.clicked.connect(self.execute_merge)
        self.execute_merge_btn.setEnabled(False)
        action_layout.addWidget(self.execute_merge_btn)
        config_layout.addLayout(action_layout)

        # --- Bottom Results Panel ---
        # ... (rest of results UI remains the same) ...
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(10, 10, 10, 10)
        result_layout.setSpacing(10)
        splitter.addWidget(result_widget)

        # SQL Preview Textbox
        self.sql_preview = QTextEdit()
        self.sql_preview.setReadOnly(True)
        self.sql_preview.setMaximumHeight(100) # Smaller height
        result_layout.addWidget(QLabel("SQL预览 (仅供参考):"))
        result_layout.addWidget(self.sql_preview)


        # Data Preview Table
        result_layout.addWidget(QLabel("数据预览 (最多100条):"))
        self.preview_table = QTableWidget()
        self.preview_table.setAlternatingRowColors(True)
        self.preview_table.setEditTriggers(QAbstractItemView.NoEditTriggers) # Read-only
        result_layout.addWidget(self.preview_table)

        splitter.setSizes([600, 400]) # Adjust initial splitter sizes

        # Initial state update
        self._update_logic_options_visibility()
        # Call default name generation after UI is built
        self.update_default_col_name() # <-- Call initially


    # --- Helper Methods ---

    def _connect_db(self):
        # ... (code remains the same) ...
        """Establishes or returns existing DB connection."""
        if self.db_conn and self.db_conn.closed == 0:
             # Check if cursor is valid, create if not
             try:
                 if not self.db_cursor or self.db_cursor.closed:
                     self.db_cursor = self.db_conn.cursor()
             except psycopg2.InterfaceError: # Handle cases where cursor is invalidated
                 self.db_cursor = self.db_conn.cursor()
             return True

        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先在“数据库连接”页面连接数据库")
            return False
        try:
            self.db_conn = psycopg2.connect(**db_params)
            self.db_cursor = self.db_conn.cursor()
            return True
        except Exception as e:
            QMessageBox.critical(self, "数据库连接失败", f"无法连接到数据库: {str(e)}")
            self.db_conn = None
            self.db_cursor = None
            return False

    def _close_db(self):
         # ... (code remains the same) ...
         """Closes DB connection and cursor."""
         if self.db_cursor:
             self.db_cursor.close()
             self.db_cursor = None
         if self.db_conn:
             self.db_conn.close()
             self.db_conn = None

    def _get_current_source_config(self):
        # ... (code remains the same) ...
        """Gets table, name column, id column based on radio button."""
        checked_id = self.direction_group.checkedId()
        # MIMIC IV v2.0 schema names are lowercase, adjust if needed
        if checked_id == 1: # Lab
            return "mimiciv_hosp.labevents", "mimiciv_hosp.d_labitems", "label", "itemid"
        elif checked_id == 2: # Med
            # Using prescriptions.drug as both name and "ID" for simplicity
            return "mimiciv_hosp.prescriptions", None, "drug", "drug"
        elif checked_id == 3: # Proc
            return "mimiciv_hosp.procedures_icd", "mimiciv_hosp.d_icd_procedures", "long_title", "icd_code"
        elif checked_id == 4: # Diag
            return "mimiciv_hosp.diagnoses_icd", "mimiciv_hosp.d_icd_diagnoses", "long_title", "icd_code"
        return None, None, None, None

    def _update_logic_options_visibility(self):
         # ... (code remains the same) ...
         """Show/hide logic options based on data source type."""
         is_lab = self.rb_lab.isChecked()
         self.lab_options_widget.setVisible(is_lab)
         self.event_options_widget.setVisible(not is_lab)

    def _validate_column_name(self, name):
        # ... (code remains the same) ...
        """Basic validation for a new column name."""
        if not name:
            return False, "列名不能为空。"
        # Allow starting with underscore now
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
             if re.match(r'^_[a-zA-Z0-9_]+$', name): # Check if starts with underscore then valid chars
                  pass # Allow starting with underscore
             else:
                  return False, "列名只能包含字母、数字和下划线，且通常以字母或下划线开头。"

        # Optional: Check against SQL keywords
        if name.upper() in ["SELECT", "TABLE", "UPDATE", "INSERT", "DELETE", "WHERE", "FROM", "GROUP", "ORDER", "INDEX", "COLUMN", "ADD", "ALTER"]:
             return False, f"列名 '{name}' 可能是 SQL 关键字，建议使用其他名称。"
        return True, ""

    def _sanitize_name(self, name):
        """Converts a string into a potentially valid SQL identifier part."""
        if not name:
            return ""
        # Replace common symbols and spaces with underscore
        name = re.sub(r'[ /\\:,;()\[\]{}"\'.\-+*?^$|<>]+', '_', name)
        # Remove any characters that are not alphanumeric or underscore
        name = re.sub(r'[^\w]+', '', name)
        # Convert to lowercase
        name = name.lower()
        # Remove leading/trailing underscores that might result
        name = name.strip('_')
        # If it starts with a digit after sanitization, prefix with underscore
        if name and name[0].isdigit():
            name = '_' + name
        # If the name became empty, return a default
        if not name:
             return "item"
        return name

    def _generate_default_column_name(self):
        """Generates a default column name based on current selections."""
        parts = []

        # 1. Logic/Aggregation
        logic_code = ""
        if self.rb_lab.isChecked():
            logic_text = self.lab_agg_combo.currentText()
            logic_map = {
                "首次 (First)": "first", "末次 (Last)": "last",
                "最小值 (Min)": "min", "最大值 (Max)": "max",
                "平均值 (Mean)": "mean", "计数 (Count)": "count"
            }
            logic_code = logic_map.get(logic_text, "value")
        else:
            logic_text = self.event_output_combo.currentText()
            logic_map = {"是否存在 (Boolean)": "has", "发生次数 (Count)": "count"}
            logic_code = logic_map.get(logic_text, "event")
        parts.append(logic_code)

        # 2. Item Name (from first selected item)
        selected_list_items = self.item_list.selectedItems()
        item_name_part = "item" # Default if no selection
        if selected_list_items:
            first_item = selected_list_items[0]
            data = first_item.data(Qt.UserRole) # Get (id, name) tuple
            if data and len(data) > 1:
                raw_name = data[1] # Get the name part
                item_name_part = self._sanitize_name(raw_name)
            else: # Fallback if data is weird, use text but sanitize
                 item_name_part = self._sanitize_name(first_item.text().split('(')[0].strip())
        parts.append(item_name_part)


        # 3. Time Window
        time_code = ""
        if self.rb_lab.isChecked():
            time_text = self.lab_time_window_combo.currentText()
            time_map = {
                "ICU入住后24小时": "24h", "ICU入住后48小时": "48h",
                "整个ICU期间": "icu", "首次住院期间": "hosp"
            }
            time_code = time_map.get(time_text, "")
        else:
            time_text = self.event_time_window_combo.currentText()
            time_map = {"首次住院期间": "hosp", "整个ICU期间": "icu"}
            time_code = time_map.get(time_text, "")

        if time_code:
             parts.append(time_code)

        # Combine parts
        default_name = "_".join(filter(None, parts)) # Join non-empty parts with underscore

        # Truncate if too long? (Optional, PostgreSQL limit is typically 63 chars)
        # max_len = 60
        # if len(default_name) > max_len:
        #     default_name = default_name[:max_len]

        return default_name

    # --- UI Event Handlers ---

    def on_db_connected(self):
        # ... (code remains the same) ...
        self.refresh_btn.setEnabled(True)
        self.refresh_cohort_tables()
        # Attempt initial item filtering if conditions are set
        self.filter_source_items()
        # Update name on connect as well, in case defaults changed
        self.update_default_col_name()


    def refresh_cohort_tables(self):
        # ... (code remains the same) ...
        if not self._connect_db(): return
        try:
            # Query for tables matching the pattern in the 'mimiciv_data' schema
            self.db_cursor.execute(pgsql.SQL("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = {}
                AND table_name LIKE {}
                ORDER BY table_name
            """).format(
                pgsql.Literal('mimiciv_data'), # Schema name as literal
                pgsql.Literal('first_%_admissions') # Pattern as literal
                )
            )
            tables = self.db_cursor.fetchall()

            self.table_combo.clear()
            if tables:
                for table in tables:
                    self.table_combo.addItem(table[0])
                self.on_cohort_table_selected(0) # Trigger selection update
            else:
                self.table_combo.addItem("未找到符合条件的队列数据表")
                self.selected_cohort_table = None
                self.preview_merge_btn.setEnabled(False)
                self.execute_merge_btn.setEnabled(False)

        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取队列数据表列表: {str(e)}")
        finally:
             pass # Keep connection open for now


    def on_cohort_table_selected(self, index):
        # ... (code remains the same) ...
        if index >= 0 and self.table_combo.count() > 0 and "未找到" not in self.table_combo.currentText():
            self.selected_cohort_table = self.table_combo.currentText()
            # Enable buttons only if items are also selected
            self.update_action_buttons_state()
        else:
            self.selected_cohort_table = None
            self.preview_merge_btn.setEnabled(False)
            self.execute_merge_btn.setEnabled(False)

    def update_source_config(self, checked, source_table_name, name_col, id_col):
        # ... (code remains the same) ...
        """Update condition widget and filter items when source changes."""
        if checked:
            print(f"Source changed: {source_table_name}, Name Col: {name_col}")
            # Update the search field in the condition widget
            self.condition_widget.set_search_field(name_col)
            # Trigger filtering for the new source
            self.filter_source_items()
            # Update visibility of logic options
            self._update_logic_options_visibility()
            # Update default column name as source changed
            # self.update_default_col_name() # This is handled by the group toggle signal now

    @Slot()
    def filter_source_items(self):
        """根据 ConditionGroupWidget 中的条件筛选源表中的项目。"""
        if not self._connect_db(): return

        source_event_table, source_dict_table, name_col, id_col = self._get_current_source_config()

        condition = self.condition_widget.get_condition() # 获取用户输入的条件

        if not source_dict_table:
             # 处理无字典表的情况 (例如 prescriptions)
             self.item_list.clear() # 先清空列表
             self.sql_preview.clear() # 清空 SQL 预览

             if not condition: # 如果用户没有输入任何关键词
                 self.item_list.addItem("请在上方输入已知药物关键词...")
                 self.update_default_col_name() # 更新列名（可能为空）
                 return # 直接返回

             # 如果用户输入了关键词，构建查询语句查找匹配的药物名称
             query_sql = pgsql.SQL("SELECT DISTINCT {name_col} FROM {event_table} WHERE {condition} ORDER BY {name_col} LIMIT 500").format(
                 name_col=pgsql.Identifier(name_col), # name_col 此时是 'drug'
                 event_table=pgsql.SQL(source_event_table), # source_event_table 是 'mimiciv_hosp.prescriptions'
                 condition=pgsql.SQL(condition) # 使用用户输入的条件
             )

             print("Filtering items (no dict) with SQL:", query_sql.as_string(self.db_conn)) # 调试信息
             self.sql_preview.setText(f"-- Item Filter Query (No Dict):\n{query_sql.as_string(self.db_conn)}") # 显示 SQL 预览

             try:
                 self.db_cursor.execute(query_sql)
                 items = self.db_cursor.fetchall()
                 if items:
                     for item_tuple in items:
                         drug_name = str(item_tuple[0]) if item_tuple[0] is not None else "Unknown Drug"
                         # 对于 prescriptions, name 和 id 都是 drug name
                         list_item = QListWidgetItem(f"{drug_name}") # 只显示名称
                         list_item.setData(Qt.UserRole, (drug_name, drug_name)) # 存储 (id=name, name)
                         self.item_list.addItem(list_item)
                 else:
                     self.item_list.addItem("未找到符合关键词的药物")
             except Exception as e:
                 QMessageBox.critical(self, "筛选药物失败", f"执行查询失败: {str(e)}\nSQL: {query_sql.as_string(self.db_conn)}")
             finally:
                 pass # 保持连接
                 self.update_default_col_name() # 根据列表结果更新默认列名
             # 不再从这里 return，让函数自然结束

        else: # 处理有字典表的情况 (Lab, Proc, Diag)
            # 这部分逻辑保持不变...
            if not condition:
                 self.item_list.clear()
                 self.item_list.addItem("请输入筛选条件...")
                 self.sql_preview.clear() # 清空预览
                 self.update_default_col_name()
                 return

            query_sql = pgsql.SQL("SELECT {id_col}, {name_col} FROM {dict_table} WHERE {condition} ORDER BY {name_col} LIMIT 500").format(
                 id_col=pgsql.Identifier(id_col),
                 name_col=pgsql.Identifier(name_col),
                 dict_table=pgsql.SQL(source_dict_table),
                 condition=pgsql.SQL(condition)
            )

            print("Filtering items with SQL:", query_sql.as_string(self.db_conn)) # Debug print
            self.sql_preview.setText(f"-- Item Filter Query:\n{query_sql.as_string(self.db_conn)}")

            try:
                self.db_cursor.execute(query_sql)
                items = self.db_cursor.fetchall()
                self.item_list.clear()
                if items:
                    for item_id, item_name in items:
                        display_name = str(item_name) if item_name is not None else f"ID_{item_id}"
                        list_item = QListWidgetItem(f"{display_name} (ID: {item_id})")
                        list_item.setData(Qt.UserRole, (str(item_id), display_name))
                        self.item_list.addItem(list_item)
                else:
                    self.item_list.addItem("未找到符合条件的项目")
            except Exception as e:
                QMessageBox.critical(self, "筛选项目失败", f"执行查询失败: {str(e)}\nSQL: {query_sql.as_string(self.db_conn)}")
            finally:
                 pass # Keep connection open
                 self.update_default_col_name() # Update name based on results


    @Slot()
    def update_selected_items_count(self):
         # ... (code remains the same) ...
         """Updates the label showing the count of selected items."""
         count = len(self.item_list.selectedItems())
         self.selected_items_label.setText(f"已选项目: {count}")
         # Enable/disable action buttons based on selection
         self.update_action_buttons_state()
         # Note: default name update is now handled by itemSelectionChanged directly

    @Slot()
    def update_action_buttons_state(self):
         # ... (code remains the same, validation check included) ...
         """Enable/disable Preview and Merge buttons based on selections."""
         # Also check if column name input is valid
         col_name_text = self.new_column_name_input.text()
         is_valid_col_name, _ = self._validate_column_name(col_name_text)

         can_preview_or_merge = (self.selected_cohort_table is not None and
                                 len(self.item_list.selectedItems()) > 0 and
                                 is_valid_col_name) # Check valid name too

         self.preview_merge_btn.setEnabled(can_preview_or_merge)
         self.execute_merge_btn.setEnabled(can_preview_or_merge)


    @Slot() # 确保有 @Slot()
    def update_default_col_name(self):
        """根据当前选择设置默认列名。"""
        print("Updating default column name...") # 添加调试信息
        default_name = self._generate_default_column_name()
        current_text = self.new_column_name_input.text()

        # 只有当新生成的默认名与当前文本不同时才设置，避免不必要的信号触发
        if current_text != default_name:
            print(f"Setting column name text from '{current_text}' to '{default_name}'") # 调试信息
            # 在程序化设置文本时，临时阻止 new_column_name_input 发送 textChanged 信号
            self.new_column_name_input.blockSignals(True)
            try:
                self.new_column_name_input.setText(default_name)
            finally:
                # 确保即使setText出错（理论上不应发生），也能恢复信号
                self.new_column_name_input.blockSignals(False) # 恢复信号
        else:
            print("Default column name is already up-to-date.") # 调试信息

        # 即使文本没有改变，也可能需要更新按钮状态（因为其他条件如列表选择可能变了）
        # 在设置完文本并恢复信号之后调用
        print("Calling update_action_buttons_state from update_default_col_name") # 调试信息
        self.update_action_buttons_state()

    @Slot()
    def _get_selected_item_ids(self):
        # ... (code remains the same) ...
        """Helper to get IDs from the selected list items."""
        selected_ids = []
        for item in self.item_list.selectedItems():
            data = item.data(Qt.UserRole)
            if data:
                selected_ids.append(str(data[0])) # Ensure ID is string for IN clause
        return selected_ids

    def _build_merge_query(self, preview_limit=100, for_execution=False):
        # --- (MAJOR UPDATE HERE for the SQL error) ---
        """Builds the SQL query for previewing or executing the merge."""
        # ... (Initial checks for table, items, column name remain the same) ...
        if not self.selected_cohort_table: return None, "未选择目标队列数据表。"
        selected_item_ids = self._get_selected_item_ids()
        if not selected_item_ids: return None, "未选择要合并的项目。"

        new_col_name_str = self.new_column_name_input.text().strip()
        is_valid_name, name_error = self._validate_column_name(new_col_name_str)
        if not is_valid_name: return None, name_error

        source_event_table, source_dict_table, name_col, id_col = self._get_current_source_config()
        is_lab = self.rb_lab.isChecked()
        target_table_ident = pgsql.Identifier('mimiciv_data', self.selected_cohort_table)
        new_col_ident = pgsql.Identifier(new_col_name_str)

        # --- Common Table Expression (CTE) for filtering events ---
        # ... (Filtering logic for item IDs and cohort remains the same) ...
        cte_parts = []
        where_clauses = []

        ids_tuple = tuple(selected_item_ids)
        params = [] # Initialize params list

        # Use %s for parameters, psycopg2 will handle quoting
        if len(ids_tuple) == 1:
            where_clauses.append(pgsql.SQL("evt.{} = %s").format(pgsql.Identifier(id_col)))
            params.append(ids_tuple[0]) # Add single param
        else:
            where_clauses.append(pgsql.SQL("evt.{} IN %s").format(pgsql.Identifier(id_col))) # 显式指定 evt.
            params.append(ids_tuple) # Add tuple param

        where_clauses.append(pgsql.SQL("evt.hadm_id IN (SELECT hadm_id FROM {target_table})").format(target_table=target_table_ident))


        # --- (Time window logic remains the same) ---
        time_window_col = 'icu_intime'
        join_table = target_table_ident

        if is_lab:
            time_option = self.lab_time_window_combo.currentText()
            agg_option = self.lab_agg_combo.currentText()
            if "24小时" in time_option:
                where_clauses.append(pgsql.SQL("evt.charttime BETWEEN cohort.{time_col} AND cohort.{time_col} + interval '24 hours'").format(time_col=pgsql.Identifier(time_window_col)))
            elif "48小时" in time_option:
                where_clauses.append(pgsql.SQL("evt.charttime BETWEEN cohort.{time_col} AND cohort.{time_col} + interval '48 hours'").format(time_col=pgsql.Identifier(time_window_col)))
            elif "ICU期间" in time_option:
                 where_clauses.append(pgsql.SQL("evt.charttime BETWEEN cohort.icu_intime AND cohort.icu_outtime"))
            # elif "住院期间" in time_option: ...

            select_cols = [pgsql.SQL("evt.hadm_id"), pgsql.SQL("evt.valuenum"), pgsql.SQL("evt.charttime")]
            # Use simple column names for partitioning/ordering later, as they come from the CTE output
            partition_col = pgsql.Identifier("hadm_id")
            order_col = pgsql.Identifier("charttime")
            value_col = pgsql.Identifier("valuenum")


        else: # Med, Proc, Diag
            # ... (Event logic remains similar, adjust select_cols and time cols if needed) ...
            time_option = self.event_time_window_combo.currentText()
            output_option = self.event_output_combo.currentText()
            event_time_col_name = 'starttime' if source_event_table == 'mimiciv_hosp.prescriptions' else 'charttime'
            if source_event_table == 'mimiciv_hosp.procedures_icd': event_time_col_name = 'chartdate'
            if source_event_table == 'mimiciv_hosp.diagnoses_icd': event_time_col_name = None

            event_time_col_ident = pgsql.Identifier(event_time_col_name) if event_time_col_name else None

            if event_time_col_ident:
                 if "ICU期间" in time_option:
                     where_clauses.append(pgsql.SQL("evt.{evt_time} BETWEEN cohort.icu_intime AND cohort.icu_outtime").format(evt_time=event_time_col_ident))
                 # elif "住院期间" in time_option: ...
            else:
                 pass # No time filtering

            select_cols = [pgsql.SQL("evt.hadm_id")]
            if event_time_col_ident: select_cols.append(pgsql.SQL("evt.{evt_time}").format(evt_time=event_time_col_ident))
            partition_col = pgsql.Identifier("hadm_id") # Output column name


        # Build the FilteredEvents CTE SQL
        cte_sql = pgsql.SQL("""
            WITH FilteredEvents AS (
                SELECT {select_list}
                FROM {event_table} evt
                INNER JOIN {join_table} cohort ON evt.hadm_id = cohort.hadm_id
                WHERE {where_conditions}
            )
        """).format(
            select_list=pgsql.SQL(', ').join(select_cols),
            event_table=pgsql.SQL(source_event_table),
            join_table=join_table,
            where_conditions=pgsql.SQL(' AND ').join(where_clauses)
        )
        cte_parts.append(cte_sql)

        # --- Main Query Logic (Aggregation/Selection) ---
        # ... (Result alias remains the same) ...
        result_col_alias = new_col_ident

        if is_lab:
            # Choose the aggregation/window function
            if "首次" in agg_option:
                # Corrected: No evt. prefix here, use names from FilteredEvents output
                select_func = pgsql.SQL("FIRST_VALUE({val_col}) OVER (PARTITION BY {part_col} ORDER BY {order_col} ASC)")
            elif "末次" in agg_option:
                select_func = pgsql.SQL("LAST_VALUE({val_col}) OVER (PARTITION BY {part_col} ORDER BY {order_col} ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)")
            elif "最小值" in agg_option:
                select_func = pgsql.SQL("MIN({val_col})")
            elif "最大值" in agg_option:
                select_func = pgsql.SQL("MAX({val_col})")
            elif "平均值" in agg_option:
                select_func = pgsql.SQL("AVG({val_col})")
            elif "计数" in agg_option:
                select_func = pgsql.SQL("COUNT({val_col})")
            else: # Default to first
                 select_func = pgsql.SQL("FIRST_VALUE({val_col}) OVER (PARTITION BY {part_col} ORDER BY {order_col} ASC)")

            # Apply the function
            if "OVER" in str(select_func):
                 # Window function approach
                 processed_cte_sql = pgsql.SQL("""
                     , ProcessedEvents AS (
                         SELECT
                             fe.hadm_id,
                             fe.charttime, 
                             {select_val_window} as result_value
                         FROM FilteredEvents fe
                     )
                     , RankedEvents AS (
                         SELECT
                             pe.*,
                             ROW_NUMBER() OVER (PARTITION BY {part_col} ORDER BY {order_col} ASC) as rn -- Order by original charttime if needed, or just use partition col
                         FROM ProcessedEvents pe
                     )
                     SELECT hadm_id, result_value as {alias}
                     FROM RankedEvents
                     WHERE rn = 1 -- Select the first row per partition after applying FIRST/LAST_VALUE
                 """).format(
                    # Pass correct column names (without evt.) to the format
                     select_val_window=select_func.format(val_col=value_col, part_col=partition_col, order_col=order_col),
                     part_col=partition_col,
                     order_col=order_col, # If order matters for rn (usually does for FIRST_VALUE tie-breaking)
                     alias=result_col_alias
                 )
            else:
                 # Aggregation function approach
                 processed_cte_sql = pgsql.SQL("""
                     SELECT {part_col}, {agg_func} as {alias}
                     FROM FilteredEvents
                     GROUP BY {part_col}
                 """).format(
                     part_col=partition_col,
                     agg_func=select_func.format(val_col=value_col), # MIN, MAX, AVG, COUNT need the value col
                     alias=result_col_alias
                 )

        else: # Med, Proc, Diag
             # ... (Event aggregation/selection logic remains similar) ...
             if "是否存在" in output_option:
                 select_val = pgsql.SQL("TRUE") # Just existence
                 processed_cte_sql = pgsql.SQL("""
                     SELECT DISTINCT hadm_id, {select_val} as {alias}
                     FROM FilteredEvents
                 """).format(
                     select_val=select_val,
                     alias=result_col_alias
                 )
             elif "发生次数" in output_option:
                 select_val = pgsql.SQL("COUNT(*)")
                 processed_cte_sql = pgsql.SQL("""
                      SELECT hadm_id, {agg_func} as {alias}
                      FROM FilteredEvents
                      GROUP BY hadm_id
                  """).format(
                      agg_func=select_val,
                      alias=result_col_alias
                  )


        # --- Combine CTEs and Main Logic ---
        final_query = pgsql.SQL(" ").join(cte_parts) + processed_cte_sql

        # --- Build Final SQL (Execution or Preview) ---
        if for_execution:
            # ... (ALTER and UPDATE logic remains the same) ...
            col_type = pgsql.SQL("NUMERIC") # Default for labs
            if not is_lab:
                 if "是否存在" in output_option: col_type = pgsql.SQL("BOOLEAN")
                 elif "发生次数" in output_option: col_type = pgsql.SQL("INTEGER")

            alter_sql = pgsql.SQL("ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS {col_name} {col_type};").format(
                 target_table=target_table_ident,
                 col_name=new_col_ident,
                 col_type=col_type
            )

            update_sql = pgsql.SQL("""
                WITH MergeData AS (
                    {final_select_query}
                )
                UPDATE {target_table} target
                SET {col_name} = md.{col_alias}
                FROM MergeData md
                WHERE target.hadm_id = md.hadm_id;
            """).format(
                final_select_query=final_query,
                target_table=target_table_ident,
                col_name=new_col_ident,
                col_alias=result_col_alias
            )
            full_execution_sql = alter_sql + pgsql.SQL("\n") + update_sql
            return full_execution_sql, None, params

        else:
            # Preview Query
            preview_sql = pgsql.SQL("""
                WITH MergeData AS (
                    {final_select_query}
                )
                SELECT cohort.subject_id, cohort.hadm_id, cohort.stay_id, md.{col_alias} as {new_col_name}
                FROM {target_table} cohort
                LEFT JOIN MergeData md ON cohort.hadm_id = md.hadm_id
                LIMIT {limit};
            """).format(
                final_select_query=final_query,
                col_alias=result_col_alias,
                new_col_name=new_col_ident,
                target_table=target_table_ident,
                limit=pgsql.Literal(preview_limit)
            )
            return preview_sql, None, params

    def preview_merge_data(self):
        # Add more detailed error printing
        if not self._connect_db(): return

        preview_sql, error_msg, params = self._build_merge_query(preview_limit=100, for_execution=False)

        if error_msg:
            QMessageBox.warning(self, "无法预览", error_msg)
            return
        if not preview_sql:
             QMessageBox.warning(self, "无法预览", "未能生成预览SQL语句。")
             return

        sql_string_for_display = "Error generating SQL string for display"
        try:
             # Generate string for display safely
             sql_string_for_display = preview_sql.as_string(self.db_conn)
             self.sql_preview.setText(f"-- Preview Query:\n{sql_string_for_display}")
             print("Executing Preview SQL:", sql_string_for_display)
             print("Params:", params)

             # Execute using pandas
             df = pd.read_sql_query(sql_string_for_display, self.db_conn, params=params)

             # Populate QTableWidget
             # ... (rest of the preview population code) ...
             self.preview_table.setRowCount(df.shape[0])
             self.preview_table.setColumnCount(df.shape[1])
             self.preview_table.setHorizontalHeaderLabels(df.columns)

             for i in range(df.shape[0]):
                 for j in range(df.shape[1]):
                     value = df.iloc[i, j]
                     item = QTableWidgetItem(str(value) if pd.notna(value) else "")
                     self.preview_table.setItem(i, j, item)

             self.preview_table.resizeColumnsToContents()
             QMessageBox.information(self, "预览成功", f"已生成预览数据 ({df.shape[0]} 条)。请检查是否符合预期。")

        except Exception as e:
             # Catch pandas UserWarning specifically if needed, otherwise treat as error
             if isinstance(e, UserWarning) and "SQLAlchemy" in str(e):
                  print(f"Pandas UserWarning: {str(e)}")
                  # Attempt to proceed despite warning, but maybe log it
                  # If execution fails below, the main exception handler will catch it
             else:
                  QMessageBox.critical(self, "预览失败", f"执行预览查询失败: {str(e)}\nSQL: {sql_string_for_display}")
                  print(f"Error executing preview query: {str(e)}")
                  print(f"SQL: {sql_string_for_display}")
        finally:
             pass # Keep connection open


    def execute_merge(self):
        # Add more detailed error printing
        # ... (initial checks remain the same) ...
        if not self.selected_cohort_table:
            QMessageBox.warning(self, "未选择表", "请选择目标队列数据表。")
            return
        if not self.item_list.selectedItems():
             QMessageBox.warning(self, "未选择项目", "请筛选并选择要合并的项目。")
             return
        new_col_name_str = self.new_column_name_input.text().strip()
        is_valid_name, name_error = self._validate_column_name(new_col_name_str)
        if not is_valid_name:
            QMessageBox.warning(self, "列名无效", name_error)
            return

        reply = QMessageBox.question(self, '确认操作',
                                     f"确定要向表 '{self.selected_cohort_table}' 中添加/更新列 '{new_col_name_str}' 吗？\n此操作将直接修改数据库表，请确保预览结果正确。",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.No:
            return


        if not self._connect_db(): return

        execution_sql, error_msg, params = self._build_merge_query(for_execution=True)

        if error_msg:
            QMessageBox.critical(self, "合并失败", error_msg)
            return
        if not execution_sql:
             QMessageBox.critical(self, "合并失败", "未能生成执行SQL语句。")
             return

        sql_string_for_display = "Error generating SQL string for display"
        try:
            sql_string_for_display = execution_sql.as_string(self.db_conn)
            self.sql_preview.setText(f"-- Execution SQL:\n{sql_string_for_display}")
            print("Executing Merge SQL:", sql_string_for_display)
            print("Params:", params)

            # Execute ALTER and UPDATE separately
            # Split carefully, assuming ALTER ends with ; and UPDATE starts after newline
            sql_parts = sql_string_for_display.split(';', 1)
            if len(sql_parts) != 2:
                 raise ValueError("无法将SQL拆分为ALTER和UPDATE语句。")

            alter_sql_str = sql_parts[0].strip() + ';'
            update_sql_str = sql_parts[1].strip()

            if not alter_sql_str.upper().startswith("ALTER"):
                 raise ValueError(f"第一部分似乎不是ALTER语句: {alter_sql_str}")
            if not update_sql_str.upper().startswith("WITH") and not update_sql_str.upper().startswith("UPDATE"): # Allow CTE in UPDATE
                  raise ValueError(f"第二部分似乎不是UPDATE语句 (或 WITH...UPDATE): {update_sql_str}")


            print("Executing ALTER:", alter_sql_str)
            self.db_cursor.execute(alter_sql_str)
            print("Executing UPDATE:", update_sql_str)
            # Pass params ONLY to the statement that uses them (UPDATE in this case)
            self.db_cursor.execute(update_sql_str, params)

            self.db_conn.commit() # Commit the transaction

            QMessageBox.information(self, "合并成功", f"已成功向表 {self.selected_cohort_table} 添加/更新列 {new_col_name_str}。")

            # Request preview refresh
            self.request_preview_signal.emit('mimiciv_data', self.selected_cohort_table)

        except Exception as e:
            if self.db_conn: # Check if connection still exists
                try:
                    self.db_conn.rollback() # Rollback on error
                    print("Transaction rolled back.")
                except Exception as rb_e:
                    print(f"Error during rollback: {rb_e}")
            QMessageBox.critical(self, "合并失败", f"执行合并SQL失败: {str(e)}\nSQL: {sql_string_for_display}")
            print(f"Error executing merge SQL: {str(e)}")
            print(f"SQL attempted: {sql_string_for_display}")
        finally:
            pass # Keep connection open


    def closeEvent(self, event):
         # ... (code remains the same) ...
         """Close DB connection when the widget/window is closed."""
         self._close_db()
         super().closeEvent(event)

# --- END OF FILE tab_combine_special_info.py ---