# --- START OF FILE tab_data_export.py ---

# ... (other imports)
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QFormLayout, QFileDialog, QLineEdit, QSpinBox, QGridLayout, QAbstractItemView)
from PySide6.QtCore import Qt, Slot # Import Slot
import psycopg2
import psycopg2.sql as pgsql
import csv
import os
import pandas as pd # Import pandas for export and preview

class DataExportTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_table_schema = 'mimiciv_data' # Default schema
        self.selected_table_name = None
        self.db_conn = None # Store connection
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(splitter)

        # Top Config Widget
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        splitter.addWidget(top_widget)

        # Schema and Table Selection
        schema_table_group = QGroupBox("选择要导出的表")
        schema_table_layout = QGridLayout(schema_table_group)

        schema_table_layout.addWidget(QLabel("Schema:"), 0, 0)
        self.schema_combo = QComboBox()
        # Populate schemas later in refresh_schemas_and_tables
        self.schema_combo.currentIndexChanged.connect(lambda: self.refresh_tables(schema_changed=True))
        schema_table_layout.addWidget(self.schema_combo, 0, 1)

        schema_table_layout.addWidget(QLabel("数据表:"), 1, 0)
        self.table_combo = QComboBox()
        self.table_combo.setMinimumWidth(300)
        self.table_combo.currentIndexChanged.connect(self.on_table_selected)
        schema_table_layout.addWidget(self.table_combo, 1, 1)

        self.refresh_btn = QPushButton("刷新列表")
        self.refresh_btn.clicked.connect(self.refresh_schemas_and_tables)
        self.refresh_btn.setEnabled(False) # Initially disabled
        schema_table_layout.addWidget(self.refresh_btn, 0, 2, 2, 1) # Span 2 rows

        top_layout.addWidget(schema_table_group)


        # Export Options
        export_options_group = QGroupBox("导出选项")
        export_options_layout = QVBoxLayout(export_options_group)

        # File Format
        format_layout = QHBoxLayout()
        format_layout.addWidget(QLabel("导出格式:"))
        self.format_combo = QComboBox()
        self.format_combo.addItems(["CSV (.csv)", "Parquet (.parquet)"]) # Add Parquet
        format_layout.addWidget(self.format_combo)
        format_layout.addStretch()
        export_options_layout.addLayout(format_layout)

        # Path Selection (Combined Label, Input, Button)
        path_layout = QHBoxLayout()
        self.export_path_input = QLineEdit()
        self.export_path_input.setPlaceholderText("选择导出文件路径...")
        self.export_path_input.setReadOnly(False) # Allow manual path entry
        self.browse_btn = QPushButton("浏览...")
        self.browse_btn.clicked.connect(self.browse_export_path)
        path_layout.addWidget(QLabel("导出文件:"))
        path_layout.addWidget(self.export_path_input)
        path_layout.addWidget(self.browse_btn)
        export_options_layout.addLayout(path_layout)

        # Export Limit
        limit_layout = QHBoxLayout()
        limit_layout.addWidget(QLabel("导出最大行数 (0=全部):"))
        self.limit_spinbox = QSpinBox()
        self.limit_spinbox.setRange(0, 20000000) # Increase range
        self.limit_spinbox.setValue(0)
        self.limit_spinbox.setSpecialValueText("全部")
        limit_layout.addWidget(self.limit_spinbox)
        limit_layout.addStretch()
        export_options_layout.addLayout(limit_layout)

        top_layout.addWidget(export_options_group)

        # Actions (Preview & Export)
        action_layout = QHBoxLayout()
        self.preview_btn = QPushButton("预览数据")
        self.preview_btn.clicked.connect(self.preview_data)
        self.preview_btn.setEnabled(False)
        action_layout.addWidget(self.preview_btn)

        self.export_btn = QPushButton("导出数据")
        self.export_btn.clicked.connect(self.export_data)
        self.export_btn.setEnabled(False)
        action_layout.addWidget(self.export_btn)
        top_layout.addLayout(action_layout)


        # Bottom Result Widget
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        splitter.addWidget(result_widget)

        # Preview Row Count
        preview_options_layout = QHBoxLayout()
        preview_options_layout.addWidget(QLabel("预览行数:"))
        self.preview_spinbox = QSpinBox()
        self.preview_spinbox.setRange(10, 1000)
        self.preview_spinbox.setValue(100)
        preview_options_layout.addWidget(self.preview_spinbox)
        preview_options_layout.addStretch()
        result_layout.addLayout(preview_options_layout)


        # Result Table
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        result_layout.addWidget(self.result_table)

        splitter.setSizes([350, 450]) # Adjust sizes

    def _connect_db(self):
        """Establishes or returns existing DB connection."""
        # Simplified: Reconnect each time needed for now
        # A more robust approach would manage a persistent connection
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先在“数据库连接”页面连接数据库")
            return None
        try:
            conn = psycopg2.connect(**db_params)
            return conn
        except Exception as e:
            QMessageBox.critical(self, "数据库连接失败", f"无法连接到数据库: {str(e)}")
            return None

    @Slot()
    def on_db_connected(self):
        self.refresh_btn.setEnabled(True)
        self.refresh_schemas_and_tables()

    def refresh_schemas_and_tables(self):
        """Refreshes both schema and table lists."""
        conn = self._connect_db()
        if not conn: return

        try:
            with conn.cursor() as cur:
                # Get schemas
                cur.execute("""
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                      AND schema_name NOT LIKE 'pg_toast%' AND schema_name NOT LIKE 'pg_temp%'
                    ORDER BY schema_name;
                """)
                schemas = [s[0] for s in cur.fetchall()]

                current_schema = self.schema_combo.currentText()
                self.schema_combo.blockSignals(True)
                self.schema_combo.clear()
                self.schema_combo.addItems(schemas)
                if current_schema in schemas:
                    self.schema_combo.setCurrentText(current_schema)
                elif 'mimiciv_data' in schemas: # Default to mimiciv_data if available
                    self.schema_combo.setCurrentText('mimiciv_data')
                self.schema_combo.blockSignals(False)

                # Refresh tables for the selected schema
                self.refresh_tables(schema_changed=False) # Pass False as schema is now set

        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取 Schemas: {str(e)}")
        finally:
            if conn: conn.close()


    def refresh_tables(self, schema_changed=False):
        """Refreshes only the table list based on the selected schema."""
        selected_schema = self.schema_combo.currentText()
        if not selected_schema:
            self.table_combo.clear()
            self.table_combo.addItem("请先选择 Schema")
            self.preview_btn.setEnabled(False)
            self.export_btn.setEnabled(False)
            return

        conn = self._connect_db()
        if not conn: return

        try:
            with conn.cursor() as cur:
                # Use parameters for schema name
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                """, (selected_schema,))
                tables = cur.fetchall()

                current_table = self.table_combo.currentText() if not schema_changed else None
                self.table_combo.blockSignals(True)
                self.table_combo.clear()
                if tables:
                    table_names = [table[0] for table in tables]
                    self.table_combo.addItems(table_names)
                    if current_table and current_table in table_names:
                        self.table_combo.setCurrentText(current_table)
                    elif table_names:
                         self.table_combo.setCurrentIndex(0) # Select first table if none was selected before

                    self.on_table_selected(self.table_combo.currentIndex()) # Trigger update even if index is 0
                else:
                    self.table_combo.addItem("未找到表")
                    self.selected_table_name = None
                    self.preview_btn.setEnabled(False)
                    self.export_btn.setEnabled(False)
                self.table_combo.blockSignals(False)

                # Manually trigger if index is 0 after clearing/adding
                if self.table_combo.currentIndex() == 0 and table_names:
                     self.on_table_selected(0)


        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取 '{selected_schema}' 中的表列表: {str(e)}")
        finally:
            if conn: conn.close()


    def on_table_selected(self, index):
        if index >= 0 and self.table_combo.count() > 0 and "未找到" not in self.table_combo.currentText():
            self.selected_table_schema = self.schema_combo.currentText()
            self.selected_table_name = self.table_combo.currentText()
            self.preview_btn.setEnabled(True)
            self.export_btn.setEnabled(True)
            # Auto-update export filename suggestion
            self._update_export_path_suggestion()
        else:
            self.selected_table_schema = None
            self.selected_table_name = None
            self.preview_btn.setEnabled(False)
            self.export_btn.setEnabled(False)
            self.export_path_input.clear()

    def _update_export_path_suggestion(self):
        """Suggests a default export filename."""
        if self.selected_table_name:
             current_dir = os.path.dirname(self.export_path_input.text()) if self.export_path_input.text() else "."
             file_ext = ".csv" if "CSV" in self.format_combo.currentText() else ".parquet"
             suggested_path = os.path.join(current_dir, f"{self.selected_table_schema}_{self.selected_table_name}{file_ext}")
             self.export_path_input.setText(suggested_path.replace("\\", "/")) # Use forward slashes


    def browse_export_path(self):
        if not self.selected_table_name:
             QMessageBox.warning(self, "未选定表", "请先选择要导出的表。")
             return

        file_ext = "CSV 文件 (*.csv)" if "CSV" in self.format_combo.currentText() else "Parquet 文件 (*.parquet)"
        default_filename = f"{self.selected_table_schema}_{self.selected_table_name}{'.csv' if 'CSV' in self.format_combo.currentText() else '.parquet'}"

        # Suggest a filename in the dialog
        filePath, _ = QFileDialog.getSaveFileName(self, "选择导出文件", default_filename, file_ext)

        if filePath:
            self.export_path_input.setText(filePath)


    @Slot(str, str) # Slot to receive schema and table name
    def preview_specific_table(self, schema_name, table_name):
         """Previews a table specified by signal (e.g., after merge)."""
         print(f"Received request to preview: {schema_name}.{table_name}")
         # Find the schema and table in the combos and select them
         schema_index = self.schema_combo.findText(schema_name, Qt.MatchFixedString)
         if schema_index >= 0:
             self.schema_combo.setCurrentIndex(schema_index)
             # Refresh tables if necessary (might already be up-to-date)
             # self.refresh_tables() # Be careful about infinite loops if signals connected wrongly
             table_index = self.table_combo.findText(table_name, Qt.MatchFixedString)
             if table_index >= 0:
                 self.table_combo.setCurrentIndex(table_index)
                 # Now call the regular preview function
                 self.preview_data()
             else:
                 QMessageBox.warning(self, "预览失败", f"在 Schema '{schema_name}' 中未找到表 '{table_name}'。")
         else:
             QMessageBox.warning(self, "预览失败", f"未找到 Schema '{schema_name}'。")


    def preview_data(self):
        if not self.selected_table_name or not self.selected_table_schema:
            QMessageBox.warning(self, "未选择表", "请先选择 Schema 和数据表")
            return

        conn = self._connect_db()
        if not conn: return

        try:
            preview_limit = self.preview_spinbox.value()
            table_identifier = pgsql.Identifier(self.selected_table_schema, self.selected_table_name)

            # Use pandas to read data for preview
            query = pgsql.SQL("SELECT * FROM {table} LIMIT {limit}").format(
                table=table_identifier,
                limit=pgsql.Literal(preview_limit)
            )
            print("Preview Query:", query.as_string(conn))

            df = pd.read_sql_query(query.as_string(conn), conn)

            # Populate QTableWidget
            self.result_table.setRowCount(df.shape[0])
            self.result_table.setColumnCount(df.shape[1])
            self.result_table.setHorizontalHeaderLabels(df.columns)

            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    value = df.iloc[i, j]
                    # Handle potential pandas types like Timestamp
                    if pd.isna(value):
                        display_value = ""
                    else:
                        display_value = str(value)
                    item = QTableWidgetItem(display_value)
                    self.result_table.setItem(i, j, item)

            self.result_table.resizeColumnsToContents()

            # Get total row count for info message
            with conn.cursor() as cur:
                 cur.execute(pgsql.SQL("SELECT COUNT(*) FROM {table}").format(table=table_identifier))
                 total_rows = cur.fetchone()[0]

            QMessageBox.information(self, "预览成功", f"表 {self.selected_table_schema}.{self.selected_table_name} (共 {total_rows} 行)\n"
                                                    f"已加载预览 {df.shape[0]} 行。")

        except Exception as e:
            QMessageBox.critical(self, "预览失败", f"无法预览数据: {str(e)}")
        finally:
            if conn: conn.close()


    def export_data(self):
        if not self.selected_table_name or not self.selected_table_schema:
            QMessageBox.warning(self, "未选择表", "请先选择 Schema 和数据表")
            return

        export_file_path = self.export_path_input.text()
        if not export_file_path:
            QMessageBox.warning(self, "未指定文件", "请指定导出文件的完整路径和名称")
            return

        # Ensure directory exists
        export_dir = os.path.dirname(export_file_path)
        if not os.path.exists(export_dir):
            try:
                os.makedirs(export_dir)
            except Exception as e:
                 QMessageBox.critical(self, "创建目录失败", f"无法创建导出目录 '{export_dir}': {str(e)}")
                 return

        export_format = self.format_combo.currentText()
        limit_value = self.limit_spinbox.value()

        conn = self._connect_db()
        if not conn: return

        QApplication.setOverrideCursor(Qt.WaitCursor) # Indicate busy state
        try:
            table_identifier = pgsql.Identifier(self.selected_table_schema, self.selected_table_name)
            query = pgsql.SQL("SELECT * FROM {table}").format(table=table_identifier)
            if limit_value > 0:
                query += pgsql.SQL(" LIMIT {limit}").format(limit=pgsql.Literal(limit_value))

            print("Export Query:", query.as_string(conn))

            # Use Pandas for efficient reading and writing different formats
            row_count = 0
            chunk_size = 10000 # Process in chunks for large tables
            first_chunk = True

            # Read data in chunks using pandas
            for chunk_df in pd.read_sql_query(query.as_string(conn), conn, chunksize=chunk_size):
                if export_format.startswith("CSV"):
                    mode = 'w' if first_chunk else 'a'
                    header = first_chunk
                    chunk_df.to_csv(export_file_path, mode=mode, header=header, index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)
                elif export_format.startswith("Parquet"):
                    # Parquet needs pyarrow or fastparquet installed
                    # Append mode is complex for parquet, usually write all at once
                    # For chunking, would need to collect all chunks or use Dask/PyArrow features
                    # Simplification: If limit is set, read all at once. If no limit, warn about memory.
                     if limit_value > 0 or total_rows < 500000: # Heuristic for direct write
                         if first_chunk: # Write the entire limited result or smaller table
                             chunk_df.to_parquet(export_file_path, index=False)
                         else: # Should not happen with this logic, but as safeguard
                              raise Exception("Parquet chunked writing not fully implemented here. Try limiting rows or ensure 'pyarrow' is installed.")
                     else:
                         # Handle very large tables for Parquet - requires more advanced handling
                         # Option 1: Warn user
                         QMessageBox.warning(self,"内存警告", f"导出大型表 ({total_rows} 行) 为 Parquet 可能消耗大量内存。\n考虑使用行数限制或确保有足够内存。")
                         # Option 2: Attempt to write (might fail)
                         chunk_df.to_parquet(export_file_path, index=False) # Try writing the first chunk anyway
                         if not first_chunk: break # Stop after first chunk for large parquet export warning

                row_count += len(chunk_df)
                first_chunk = False
                print(f"Processed {row_count} rows...") # Progress feedback


            QMessageBox.information(self, "导出成功", f"已成功导出 {row_count} 条记录到:\n{export_file_path}")

        except ImportError:
             if "Parquet" in export_format:
                 QMessageBox.critical(self, "导出失败", "导出为 Parquet 格式需要安装 'pyarrow' 库。\n请运行: pip install pyarrow")
             else:
                 QMessageBox.critical(self, "导出失败", f"发生未知导入错误: {str(e)}")
        except Exception as e:
            QMessageBox.critical(self, "导出失败", f"无法导出数据: {str(e)}")
        finally:
            if conn: conn.close()
            QApplication.restoreOverrideCursor() # Restore cursor

# --- END OF FILE tab_data_export.py ---