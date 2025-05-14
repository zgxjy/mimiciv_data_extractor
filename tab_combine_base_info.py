# --- START OF FILE tab_combine_base_info.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QFormLayout, QProgressBar)
from PySide6.QtCore import Qt, Signal, QThread, QObject
import psycopg2
import re
import time
import pandas as pd
from base_info_sql import (add_demography, add_antecedent, add_vital_sign,
                           add_blood_info, add_cardiovascular_lab, add_medicine,
                           add_surgeries, add_past_diagnostic) # Updated imports


class SQLWorker(QObject):
    finished = Signal(list, list)
    error = Signal(str)
    progress = Signal(int, int)
    log = Signal(str)

    def __init__(self, sql_to_execute, db_params, table_name):
        super().__init__()
        self.sql_to_execute = sql_to_execute
        self.db_params = db_params
        self.table_name = table_name
        self.is_cancelled = False

    def cancel(self):
        self.log.emit("SQL 执行被请求取消...")
        self.is_cancelled = True

    def run(self):
        conn_extract = None
        try:
            self.log.emit(f"准备为表 '{self.table_name}' 执行SQL批处理...")
            self.log.emit("连接数据库...")
            conn_extract = psycopg2.connect(**self.db_params)
            conn_extract.autocommit = False # Important for batch processing
            cur = conn_extract.cursor()

            self.log.emit("开始解析和执行SQL语句...")
            sql_statements = self._parse_sql(self.sql_to_execute)
            total_statements = len(sql_statements)

            if total_statements == 0:
                self.log.emit("没有可执行的SQL语句。")
                self.progress.emit(0, 0)
                self.finished.emit([], []) # Ensure finished signal is emitted
                return

            self.progress.emit(0, total_statements)
            executed_count = 0

            for i, stmt in enumerate(sql_statements):
                if self.is_cancelled:
                    self.log.emit("SQL 执行已取消。正在回滚任何未提交的更改...")
                    if conn_extract: conn_extract.rollback()
                    self.error.emit("操作已取消")
                    return

                stmt_trimmed = stmt.strip()
                if not stmt_trimmed or stmt_trimmed.startswith('--'):
                    self.log.emit(f"跳过空语句或注释: 第 {i+1}/{total_statements} 条")
                    self.progress.emit(i + 1, total_statements) # Still count for progress
                    continue

                max_log_length = 150
                log_stmt_display = stmt_trimmed[:max_log_length] + ("..." if len(stmt_trimmed) > max_log_length else "")
                self.log.emit(f"执行第 {i+1}/{total_statements} 条语句: {log_stmt_display}")

                try:
                    start_time = time.time()
                    cur.execute(stmt_trimmed)
                    end_time = time.time()
                    self.log.emit(f"语句执行成功 (耗时: {end_time - start_time:.2f} 秒)")
                    executed_count +=1
                except psycopg2.Error as db_err:
                    self.log.emit(f"数据库语句执行出错: {db_err}")
                    self.log.emit(f"出错的SQL语句: {stmt_trimmed}") # Log full failing statement
                    if conn_extract: conn_extract.rollback()
                    self.log.emit("事务已回滚。")
                    self.error.emit(f"数据库错误: {db_err}\n问题语句: {log_stmt_display}")
                    return
                except Exception as e: # Catch other unexpected errors
                    self.log.emit(f"发生意外错误: {str(e)}")
                    if conn_extract: conn_extract.rollback()
                    self.log.emit("事务已回滚。")
                    self.error.emit(f"意外错误: {str(e)}\n问题语句: {log_stmt_display}")
                    return

                self.progress.emit(i + 1, total_statements)

            if self.is_cancelled: # Check again before commit
                self.log.emit("SQL 执行在提交前已取消。正在回滚...")
                if conn_extract: conn_extract.rollback()
                self.error.emit("操作已取消")
                return

            if executed_count > 0: # Only commit if something was actually run
                self.log.emit("所有语句执行完毕。正在提交事务...")
                conn_extract.commit()
                self.log.emit("事务已成功提交。")
            else:
                self.log.emit("没有实际执行的修改语句，无需提交。")


            self.log.emit("准备获取更新后的表结构和预览数据...")
            cur.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'mimiciv_data' AND table_name = %s ORDER BY ordinal_position
            """, (self.table_name,))
            columns = cur.fetchall()
            cur.execute(f"SELECT * FROM mimiciv_data.{self.table_name} LIMIT 100") # Use f-string carefully, table_name is controlled
            rows = cur.fetchall()
            self.log.emit("数据提取和预览准备完成。")
            self.finished.emit(columns, rows)

        except psycopg2.OperationalError as op_err: # e.g. connection lost during operation
            self.log.emit(f"数据库操作错误 (如连接问题): {op_err}")
            self.error.emit(f"数据库操作错误: {op_err}")
        except Exception as e:
            self.log.emit(f"在 SQLWorker 主体中发生意外错误: {str(e)}")
            if conn_extract and not conn_extract.closed:
                try:
                    conn_extract.rollback()
                    self.log.emit("因意外错误，事务已回滚。")
                except Exception as rb_err:
                    self.log.emit(f"尝试回滚失败: {rb_err}")
            self.error.emit(f"SQLWorker 意外错误: {str(e)}")
        finally:
            if conn_extract:
                self.log.emit("关闭数据库连接。")
                conn_extract.close()

    def _parse_sql(self, sql_script):
        statements = []
        current_statement = []
        for line in sql_script.splitlines():
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('--'):
                if current_statement:
                     current_statement.append(line)
                continue

            current_statement.append(line)
            if stripped_line.endswith(';'):
                full_stmt = "\n".join(current_statement).strip()
                is_only_comment_or_empty = True
                for sub_line in full_stmt.split('\n'):
                    trimmed_sub_line = sub_line.strip()
                    if trimmed_sub_line and not trimmed_sub_line.startswith('--'):
                        is_only_comment_or_empty = False
                        break
                if not is_only_comment_or_empty:
                    statements.append(full_stmt)
                current_statement = []

        if current_statement:
            full_stmt = "\n".join(current_statement).strip()
            is_only_comment_or_empty = True
            for sub_line in full_stmt.split('\n'):
                trimmed_sub_line = sub_line.strip()
                if trimmed_sub_line and not trimmed_sub_line.startswith('--'):
                    is_only_comment_or_empty = False
                    break
            if not is_only_comment_or_empty:
                 statements.append(full_stmt)

        self.log.emit(f"解析得到 {len(statements)} 条有效SQL语句。")
        return statements


class BaseInfoDataExtractionTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_table = None
        self.sql_confirmed = False
        self.worker = None
        self.worker_thread = None
        self.option_checkboxes = [] # List to hold checkboxes
        self.DIAG_CATEGORY_KEYWORDS = [
            "sleep apnea", "insomnia", "depressive", "anxiety", "anxiolytic",
            "diabetes", "hypertension", "myocardial infarction", "stroke", "asthma", "copd"
        ]
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        instruction_label = QLabel("从数据库中选择病种表，选择要添加的基础数据选项，然后点击“SQL确认预览”生成SQL并确认，最后点击“提取基础数据”。")
        instruction_label.setWordWrap(True)
        top_layout.addWidget(instruction_label)
        table_select_layout = QHBoxLayout()
        table_select_layout.addWidget(QLabel("选择病种表:"))
        self.table_combo = QComboBox()
        self.table_combo.setMinimumWidth(300)
        self.table_combo.currentIndexChanged.connect(self.on_table_selected)
        table_select_layout.addWidget(self.table_combo)
        self.refresh_btn = QPushButton("刷新表列表")
        self.refresh_btn.clicked.connect(self.refresh_tables)
        table_select_layout.addWidget(self.refresh_btn)
        top_layout.addLayout(table_select_layout)

        options_group = QGroupBox("数据提取选项")
        options_layout = QVBoxLayout(options_group)

        # --- Add Select All / Deselect All buttons ---
        select_buttons_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(self.select_all_options)
        select_buttons_layout.addWidget(self.select_all_btn)
        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(self.deselect_all_options)
        select_buttons_layout.addWidget(self.deselect_all_btn)
        select_buttons_layout.addStretch()
        options_layout.addLayout(select_buttons_layout)
        # --- End Add Buttons ---

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        self.cb_demography = QCheckBox("住院及人口学信息"); self.cb_demography.setChecked(True)
        self.cb_antecedent = QCheckBox("患者既往史 (Charlson)"); self.cb_antecedent.setChecked(True)
        self.cb_vital_sign = QCheckBox("患者住院生命体征"); self.cb_vital_sign.setChecked(True) # Changed label
        self.cb_scores = QCheckBox("患者评分 (SOFA, SAPSII, APSIII, LODS, OASIS, SIRS)"); self.cb_scores.setChecked(True) # New checkbox
        self.cb_blood_info = QCheckBox("患者住院红细胞相关指标"); self.cb_blood_info.setChecked(True)
        self.cb_cardiovascular_lab = QCheckBox("患者住院心血管化验指标"); self.cb_cardiovascular_lab.setChecked(True)
        self.cb_medications = QCheckBox("患者住院用药记录"); self.cb_medications.setChecked(True)
        self.cb_surgery = QCheckBox("患者住院手术记录"); self.cb_surgery.setChecked(True)
        self.cb_past_disease = QCheckBox("患者既往病史 (如高血压、糖尿病等)"); self.cb_past_disease.setChecked(True)

        # Store checkboxes and connect stateChanged to reset confirmation
        self.option_checkboxes = [
            self.cb_demography, self.cb_antecedent, self.cb_vital_sign,self.cb_scores,
            self.cb_blood_info, self.cb_cardiovascular_lab, self.cb_medications,
            self.cb_surgery, self.cb_past_disease
        ]
        for cb_obj in self.option_checkboxes:
            scroll_layout.addWidget(cb_obj)
            # Connect to a method that ONLY resets confirmation, doesn't preview
            cb_obj.stateChanged.connect(self._reset_sql_confirmation)

        scroll_area.setWidget(scroll_content)
        options_layout.addWidget(scroll_area)
        top_layout.addWidget(options_group)
        self.execution_status_group = QGroupBox("SQL执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar(); self.execution_progress.setRange(0,100); self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit(); self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(150)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False)
        top_layout.addWidget(self.execution_status_group)
        top_layout.addWidget(QLabel("SQL预览:"))
        self.sql_preview = QTextEdit(); self.sql_preview.setReadOnly(True); self.sql_preview.setMinimumHeight(150) # Increased height
        top_layout.addWidget(self.sql_preview)
        buttons_layout = QHBoxLayout()
        self.confirm_sql_btn = QPushButton("SQL确认预览"); self.confirm_sql_btn.clicked.connect(self.handle_confirm_sql_preview); self.confirm_sql_btn.setEnabled(False)
        buttons_layout.addWidget(self.confirm_sql_btn)
        self.extract_btn = QPushButton("提取基础数据"); self.extract_btn.clicked.connect(self.extract_data); self.extract_btn.setEnabled(False)
        buttons_layout.addWidget(self.extract_btn)
        self.cancel_extraction_btn = QPushButton("取消操作"); self.cancel_extraction_btn.clicked.connect(self.cancel_extraction); self.cancel_extraction_btn.setEnabled(False)
        buttons_layout.addWidget(self.cancel_extraction_btn)
        top_layout.addLayout(buttons_layout)
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(top_widget)
        self.result_table = QTableWidget(); self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)
        splitter.setSizes([700, 200]) # Adjust for taller top part
        main_layout.addWidget(splitter)

    def on_db_connected(self):
        self.refresh_btn.setEnabled(True)
        self.refresh_tables()

    def refresh_tables(self):
        self.selected_table = None
        self.sql_confirmed = False
        self.sql_preview.clear()
        self.confirm_sql_btn.setEnabled(False)
        self.extract_btn.setEnabled(False)
        self.table_combo.clear()
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            self.table_combo.addItem("数据库未连接")
            return
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'mimiciv_data' AND table_name LIKE 'first_%_admissions'
                ORDER BY table_name
            """)
            tables = cur.fetchall()
            if tables:
                for table in tables: self.table_combo.addItem(table[0])
                if self.table_combo.count() > 0:
                     self.table_combo.setCurrentIndex(0) # Auto-select first
                     self.on_table_selected(0) # Explicitly trigger update for index 0
            else: self.table_combo.addItem("未找到病种表")
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取表列表: {str(e)}")
            self.table_combo.addItem("查询表失败")
        finally:
            if conn: conn.close()

    def on_table_selected(self, index):
        # Reset confirmation and buttons, enable confirm SQL button if valid table
        self._reset_sql_confirmation()
        current_item_text = self.table_combo.itemText(index) if index >= 0 else None
        is_valid_table = current_item_text and current_item_text not in ["未找到病种表", "数据库未连接", "查询表失败"]

        if is_valid_table:
            self.selected_table = current_item_text
            self.confirm_sql_btn.setEnabled(True)
            self.sql_preview.setText("-- 请点击 'SQL确认预览' 生成SQL --") # Prompt user
        else:
            self.selected_table = None
            self.confirm_sql_btn.setEnabled(False)
            self.sql_preview.clear()

    def _reset_sql_confirmation(self):
        """Resets SQL confirmation status and disables extract button."""
        # print("SQL confirmation reset due to option change.") # Debug
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)
        # Clear preview only if confirmation is reset? Optional.
        # self.sql_preview.setText("-- 选项已更改，请重新确认SQL --")

    def select_all_options(self):
        """Sets all data extraction checkboxes to checked."""
        for cb in self.option_checkboxes:
            cb.setChecked(True)
        # No need to call _reset_sql_confirmation here, as setChecked triggers stateChanged

    def deselect_all_options(self):
        """Sets all data extraction checkboxes to unchecked."""
        for cb in self.option_checkboxes:
            cb.setChecked(False)
        # No need to call _reset_sql_confirmation here

    def preview_sql(self):
        """Generates SQL based on current selections and displays it."""
        # This method is now primarily called by handle_confirm_sql_preview
        if not self.selected_table:
            self.sql_preview.clear()
            return

        db_params = self.get_db_params()
        conn_preview = None
        generated_sql = ""
        try:
            if self.cb_past_disease.isChecked():
                if db_params:
                    conn_preview = psycopg2.connect(**db_params)
                else:
                    alter_sql, update_sql = self.generate_sql_parts(None)
                    generated_sql = (alter_sql + "\n\n" + update_sql).strip()
                    generated_sql += "\n-- [预览警告] 未连接数据库，无法生成'患者既往病史 (自定义ICD)'部分的SQL。--"
                    self.sql_preview.setText(generated_sql)
                    return # Return early as SQL is incomplete

            # Proceed with generation (with or without connection for ICD)
            final_alter_sql, final_update_sql = self.generate_sql_parts(conn_preview)
            generated_sql = (final_alter_sql + "\n\n" + final_update_sql).strip()

            # Check if any options were selected resulting in SQL
            base_sql_header = f"-- SQL for table mimiciv_data.{self.selected_table} --\n"
            if generated_sql == base_sql_header.strip():
                generated_sql = "-- 没有选择任何数据提取选项，SQL为空。 --"

            self.sql_preview.setText(generated_sql)

        except (Exception, psycopg2.Error) as e:
            error_msg = f"-- 生成SQL预览时出错: {str(e)} --"
            self.sql_preview.setText(error_msg)
            print(f"Error during SQL preview generation: {e}")
        finally:
            if conn_preview: conn_preview.close()


    def generate_sql_parts(self, conn_for_icd_lookup):
        """
        Generates column definitions and update SQL statements separately.
        Returns:
            tuple: (alter_table_sql_string, update_statements_sql_string)
        """
        if not self.selected_table: return "", ""

        qualified_table_name = f"mimiciv_data.{self.selected_table}"
        all_col_defs = []
        all_update_sqls = [f"-- SQL for table {qualified_table_name} --\n"]
        past_diag_data_for_sql = {}

        if self.cb_past_disease.isChecked():
            if conn_for_icd_lookup:
                print("generate_sql_parts: Fetching ICD codes for past diseases...")
                try:
                    for keyword in self.DIAG_CATEGORY_KEYWORDS:
                        if not keyword or not isinstance(keyword, str): continue
                        category_key = keyword.strip().lower().replace(' ', '_')
                        icd_query_template = "SELECT DISTINCT TRIM(icd_code) AS icd_code FROM mimiciv_hosp.d_icd_diagnoses WHERE LOWER(long_title) LIKE %s;"
                        like_param = f'%{keyword.strip().lower()}%'
                        icd_df = pd.read_sql_query(icd_query_template, conn_for_icd_lookup, params=(like_param,))
                        icd_codes_list = [code for code in icd_df['icd_code'].tolist() if code and str(code).strip()]
                        if icd_codes_list:
                            past_diag_data_for_sql[category_key] = icd_codes_list
                except (Exception, psycopg2.Error) as db_err:
                    all_update_sqls.append(f"-- [错误] 查询自定义既往病史ICD码时出错: {db_err} --\n")
                    past_diag_data_for_sql = {}
            # else: # No connection, add_past_diagnostic will handle empty past_diag_data_for_sql

        # Call helper functions which now return (col_defs_list, update_sql_str)
        if self.cb_demography.isChecked():
            defs, updates = add_demography(qualified_table_name, "") # sql_accumulator not used by new funcs
            all_col_defs.extend(defs); all_update_sqls.append(updates)
        if self.cb_antecedent.isChecked():
            defs, updates = add_antecedent(qualified_table_name, "")
            all_col_defs.extend(defs); all_update_sqls.append(updates)
        if self.cb_vital_sign.isChecked(): # For actual vital signs
            defs, updates = add_vital_sign(qualified_table_name, "")
            all_col_defs.extend(defs); all_update_sqls.append(updates)
        if self.cb_scores.isChecked(): # For patient scores
            defs, updates = add_scores(qualified_table_name, "")
            all_col_defs.extend(defs); all_update_sqls.append(updates)
            all_col_defs.extend(defs); all_update_sqls.append(updates)
        if self.cb_blood_info.isChecked():
            defs, updates = add_blood_info(qualified_table_name, "")
            all_col_defs.extend(defs); all_update_sqls.append(updates)
        if self.cb_cardiovascular_lab.isChecked():
            defs, updates = add_cardiovascular_lab(qualified_table_name, "")
            all_col_defs.extend(defs); all_update_sqls.append(updates)
        if self.cb_medications.isChecked():
            defs, updates = add_medicine(qualified_table_name, "")
            all_col_defs.extend(defs); all_update_sqls.append(updates)
        if self.cb_surgery.isChecked():
            defs, updates = add_surgeries(qualified_table_name, "")
            all_col_defs.extend(defs); all_update_sqls.append(updates)

        if self.cb_past_disease.isChecked():
            # This function now also returns (col_defs, update_sql)
            defs, updates = add_past_diagnostic(qualified_table_name, "", past_diag_data_for_sql)
            all_col_defs.extend(defs)
            all_update_sqls.append(updates)
            if not conn_for_icd_lookup and not past_diag_data_for_sql : # if no connection AND no data (meaning lookup wasn't even tried or failed early)
                 # Check if the specific warning was already added by add_past_diagnostic
                 if not any("-- No past diagnoses data provided" in s for s in all_update_sqls):
                    all_update_sqls.append("\n-- [INFO] '患者既往病史 (自定义ICD)' 需要数据库连接才能生成SQL。 --\n")


        # Construct single ALTER TABLE statement
        alter_table_sql = ""
        if all_col_defs:
            # Deduplicate column definitions (name only) before creating ALTER statement
            unique_col_defs_dict = {}
            for col_def_str in all_col_defs:
                col_name = col_def_str.split(' ')[0].strip()
                if col_name not in unique_col_defs_dict:
                    unique_col_defs_dict[col_name] = col_def_str

            if unique_col_defs_dict:
                # Format: ALTER TABLE schema.table ADD COLUMN IF NOT EXISTS col1 type1, ADD COLUMN IF NOT EXISTS col2 type2...;
                add_clauses = [f"ADD COLUMN IF NOT EXISTS {col_def}" for col_def in unique_col_defs_dict.values()]
                alter_table_sql = f"ALTER TABLE {qualified_table_name}\n    " + ",\n    ".join(add_clauses) + ";\n"

        update_statements_sql = "\n\n".join(all_update_sqls)
        return alter_table_sql, update_statements_sql


    def extract_data(self):
        if not self.selected_table:
            QMessageBox.warning(self, "未选择表", "请先选择一个病种表")
            return
        if not self.sql_confirmed:
            QMessageBox.warning(self, "SQL未确认", "请先点击SQL确认预览按钮。")
            return

        db_params = self.get_db_params()
        needs_db_for_generation = self.cb_past_disease.isChecked()

        if not db_params: # General check, then specific for past_disease
            QMessageBox.warning(self, "未连接", "请先连接数据库。")
            return
        if needs_db_for_generation and not db_params: # Should be caught by above, but defensive
            QMessageBox.warning(self, "未连接", "“患者既往病史 (自定义ICD)”选项需要数据库连接。")
            return

        sql_to_execute = ""
        conn_generate = None
        try:
            if needs_db_for_generation:
                conn_generate = psycopg2.connect(**db_params)

            alter_sql, update_sql = self.generate_sql_parts(conn_generate)
            sql_to_execute = (alter_sql + "\n\n" + update_sql).strip()

            base_sql_header = f"-- SQL for table mimiciv_data.{self.selected_table} --"
            if not sql_to_execute or sql_to_execute == base_sql_header:
                 QMessageBox.information(self, "无操作", "没有选择任何数据提取选项，或生成的SQL为空。")
                 return

            # Check for critical errors in generated SQL before proceeding
            if "-- [错误]" in sql_to_execute or "-- [预览警告]" in sql_to_execute:
                 QMessageBox.warning(self, "SQL生成不完整或有误", "生成SQL时遇到问题（详见SQL预览或日志）。请检查数据库连接和选项后重试。")
                 self.sql_preview.setText(sql_to_execute) # Show the problematic SQL
                 self.sql_confirmed = False
                 self.extract_btn.setEnabled(False)
                 return

        except (Exception, psycopg2.Error) as e:
            QMessageBox.critical(self, "SQL生成失败", f"无法生成SQL: {str(e)}")
            return
        finally:
            if conn_generate: conn_generate.close()

        self.prepare_for_long_operation(True)
        self.worker = SQLWorker(sql_to_execute, db_params, self.selected_table)
        self.worker_thread = QThread()
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_sql_execution_finished)
        self.worker.error.connect(self.on_sql_execution_error)
        self.worker.progress.connect(self.update_execution_progress)
        self.worker.log.connect(self.update_execution_log)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.error.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker_thread.start()

    def prepare_for_long_operation(self, starting=True):
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行SQL操作...")
            self.extract_btn.setEnabled(False)
            self.confirm_sql_btn.setEnabled(False)
            self.refresh_btn.setEnabled(False)
            self.table_combo.setEnabled(False)
            self.cancel_extraction_btn.setEnabled(True)
            # Disable option checkboxes during execution
            for cb in self.option_checkboxes: cb.setEnabled(False)
            self.select_all_btn.setEnabled(False)
            self.deselect_all_btn.setEnabled(False)
        else: # Operation finished or cancelled
            self.confirm_sql_btn.setEnabled(bool(self.selected_table))
            self.extract_btn.setEnabled(self.sql_confirmed and bool(self.selected_table))
            self.refresh_btn.setEnabled(True)
            self.table_combo.setEnabled(True)
            self.cancel_extraction_btn.setEnabled(False)
            # Re-enable option checkboxes
            for cb in self.option_checkboxes: cb.setEnabled(True)
            self.select_all_btn.setEnabled(True)
            self.deselect_all_btn.setEnabled(True)

    def update_execution_progress(self, value, max_value=None):
        if max_value is not None and self.execution_progress.maximum() != max_value :
             self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        self.execution_log.append(message)

    def handle_confirm_sql_preview(self):
        """Generates, displays, and validates the SQL for confirmation."""
        if not self.selected_table:
            QMessageBox.warning(self, "无操作", "请先选择一个病种表。")
            return

        # Step 1: Generate the SQL based on current options
        self.preview_sql()

        # Step 2: Validate the generated SQL in the preview area
        current_sql_text = self.sql_preview.toPlainText().strip()
        problematic_phrases = [
            "-- 请先连接数据库", "-- 生成SQL预览时出错", "-- 没有选择任何数据提取选项",
            "-- [预览警告]", "-- [错误]", "-- SQL为空。"
        ]
        base_sql_header = f"-- SQL for table mimiciv_data.{self.selected_table} --"
        is_empty_or_base = not current_sql_text or current_sql_text == base_sql_header

        is_problematic = any(phrase in current_sql_text for phrase in problematic_phrases)

        if is_empty_or_base or is_problematic:
            QMessageBox.warning(self, "SQL预览问题", "SQL预览为空、包含错误/警告，或未选择任何提取选项。\n请检查后重试。")
            self.sql_confirmed = False
            self.extract_btn.setEnabled(False)
        else:
            self.sql_confirmed = True
            self.extract_btn.setEnabled(True) # Enable extraction only if preview is valid
            QMessageBox.information(self, "SQL已确认", "SQL预览已生成并确认。\n您现在可以点击提取基础数据按钮。")

    # on_options_changed is removed as logic moved to _reset_sql_confirmation

    def cancel_extraction(self):
        if self.worker:
            self.update_execution_log("正在请求取消SQL执行...")
            self.worker.cancel()
            self.cancel_extraction_btn.setEnabled(False)

    def on_sql_execution_finished(self, columns, rows):
        self.result_table.setRowCount(len(rows))
        self.result_table.setColumnCount(len(columns))
        column_names = [col[0] for col in columns]
        self.result_table.setHorizontalHeaderLabels(column_names)
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))
        self.result_table.resizeColumnsToContents()
        self.update_execution_log("SQL执行完成！")
        QMessageBox.information(self, "提取成功", f"已成功为表 {self.selected_table} 添加基础数据")
        self.prepare_for_long_operation(False)
        self.worker = None
        self.worker_thread = None

    def on_sql_execution_error(self, error_message):
        self.update_execution_log(f"错误: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "提取失败", f"无法提取基础数据: {error_message}")
        else:
            QMessageBox.information(self, "操作取消", "数据提取操作已取消。")
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)
        self.prepare_for_long_operation(False)
        self.worker = None
        self.worker_thread = None

# --- END OF FILE tab_combine_base_info.py ---