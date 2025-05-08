# --- START OF FILE tab_combine_base_info.py ---
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QFormLayout, QProgressBar)
from PySide6.QtCore import Qt, Signal, QThread, QObject
import psycopg2
import re
import time
import pandas as pd # 需要 pandas 来执行 ICD 查询
from base_info_sql import *


class SQLWorker(QObject):
    """
    SQL执行工作线程类。
    用于在独立线程中执行SQL批处理，避免阻塞主界面。
    信号：
        finished(list, list): 执行完成，返回列信息和数据行。
        error(str): 执行过程中发生错误。
        progress(int, int): 进度更新。
        log(str): 日志信息。
    """
    finished = Signal(list, list)
    error = Signal(str)
    progress = Signal(int, int)
    log = Signal(str)

    def __init__(self, sql_to_execute, db_params, table_name):
        """
        初始化SQLWorker。
        参数：
            sql_to_execute: 待执行的SQL语句字符串。
            db_params: 数据库连接参数（dict）。
            table_name: 目标表名。
        """
        super().__init__()
        self.sql_to_execute = sql_to_execute
        self.db_params = db_params
        self.table_name = table_name
        self.is_cancelled = False

    def cancel(self):
        """
        请求取消SQL执行。
        """
        self.log.emit("SQL 执行被请求取消...")
        self.is_cancelled = True

    def run(self):
        """
        主执行逻辑：连接数据库，依次执行SQL语句，处理进度、异常和日志。
        结束后获取表结构和部分数据预览，发射finished信号。
        """
        conn_extract = None
        try:
            self.log.emit(f"准备为表 '{self.table_name}' 执行SQL批处理...")
            self.log.emit("连接数据库...")
            conn_extract = psycopg2.connect(**self.db_params)
            conn_extract.autocommit = False
            cur = conn_extract.cursor()

            self.log.emit("开始解析和执行SQL语句...")
            sql_statements = self._parse_sql(self.sql_to_execute)
            total_statements = len(sql_statements)

            if total_statements == 0:
                self.log.emit("没有可执行的SQL语句。")
                self.progress.emit(0, 0)
                self.finished.emit([], [])
                return

            self.progress.emit(0, total_statements)
            executed_count = 0

            for i, stmt in enumerate(sql_statements):
                if self.is_cancelled:
                    self.log.emit("SQL 执行已取消。正在回滚任何未提交的更改...")
                    if conn_extract:
                        conn_extract.rollback()
                    self.error.emit("操作已取消")
                    return

                stmt_trimmed = stmt.strip()
                if not stmt_trimmed or stmt_trimmed.startswith('--'):
                    self.log.emit(f"跳过空语句或注释: 第 {i+1}/{total_statements} 条")
                    self.progress.emit(i + 1, total_statements)
                    continue

                max_log_length = 150
                short_stmt = stmt_trimmed[:max_log_length] + ("..." if len(stmt_trimmed) > max_log_length else "")
                self.log.emit(f"执行第 {i+1}/{total_statements} 条语句: {short_stmt}")

                try:
                    start_time = time.time()
                    cur.execute(stmt_trimmed)
                    end_time = time.time()
                    self.log.emit(f"语句执行成功 (耗时: {end_time - start_time:.2f} 秒)")
                    executed_count += 1
                except psycopg2.Error as db_err:
                    self.log.emit(f"数据库语句执行出错: {db_err}")
                    self.log.emit(f"出错的SQL语句: {stmt_trimmed}")
                    if conn_extract: conn_extract.rollback()
                    self.log.emit("事务已回滚。")
                    self.error.emit(f"数据库错误: {db_err}\n问题语句: {short_stmt}")
                    return
                except Exception as e:
                    self.log.emit(f"发生意外错误: {str(e)}")
                    if conn_extract: conn_extract.rollback()
                    self.log.emit("事务已回滚。")
                    self.error.emit(f"意外错误: {str(e)}\n问题语句: {short_stmt}")
                    return

                self.progress.emit(i + 1, total_statements)

            if self.is_cancelled:
                self.log.emit("SQL 执行在提交前已取消。正在回滚...")
                if conn_extract:
                    conn_extract.rollback()
                self.error.emit("操作已取消")
                return

            if executed_count > 0:
                self.log.emit("所有语句执行完毕。正在提交事务...")
                conn_extract.commit()
                self.log.emit("事务已成功提交。")
            else:
                self.log.emit("没有实际执行的修改语句，无需提交。")

            self.log.emit("准备获取更新后的表结构和预览数据...")

            self.log.emit(f"获取表 'mimiciv_data.{self.table_name}' 的结构...")
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                ORDER BY ordinal_position
            """, ('mimiciv_data', self.table_name))
            columns = cur.fetchall()

            self.log.emit(f"获取表 'mimiciv_data.{self.table_name}' 的前100条数据...")
            cur.execute(f"SELECT * FROM mimiciv_data.{self.table_name} LIMIT 100")
            rows = cur.fetchall()

            self.log.emit("数据提取和预览准备完成。")
            self.finished.emit(columns, rows)

        except psycopg2.OperationalError as op_err:
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

    def _parse_sql(self, sql):
        """
        将SQL脚本分割成多个独立可执行的语句（支持多行、注释、空行）。
        参数：
            sql: 原始SQL脚本字符串。
        返回：
            list，每个元素为一条完整的SQL语句（含必要换行和注释）。
        """
        _statements_from_original_parser = []
        _current_statement_orig = ""
        _original_lines = sql.split('\n')

        for line in _original_lines:
            stripped_line = line.strip()

            if not stripped_line:
                if _current_statement_orig.strip():
                    _current_statement_orig += "\n"
                continue
            
            if stripped_line.startswith('--'):
                _current_statement_orig += line + "\n" 
                continue 

            _current_statement_orig += line 
            if stripped_line.endswith(';'):
                # 检查语句是否为纯注释或空
                non_comment_part = ""
                for sub_line in _current_statement_orig.split('\n'): 
                    comment_start_index = sub_line.find('--')
                    if comment_start_index != -1:
                        non_comment_part += sub_line[:comment_start_index].strip()
                    else:
                        non_comment_part += sub_line.strip()
                
                if non_comment_part.strip(): 
                    _statements_from_original_parser.append(_current_statement_orig.strip())
                _current_statement_orig = ""
            else: 
                 _current_statement_orig += "\n"
        
        if _current_statement_orig.strip():
            non_comment_part = ""
            for sub_line in _current_statement_orig.split('\n'):
                comment_start_index = sub_line.find('--')
                if comment_start_index != -1:
                    non_comment_part += sub_line[:comment_start_index].strip()
                else:
                    non_comment_part += sub_line.strip()
            if non_comment_part.strip():
                _statements_from_original_parser.append(_current_statement_orig.strip())
        
        # 过滤掉纯注释或空语句
        final_statements = []
        for s in _statements_from_original_parser:
            is_only_comment_or_empty = True
            for line_in_s in s.split('\n'):
                trimmed_line_in_s = line_in_s.strip()
                if trimmed_line_in_s and not trimmed_line_in_s.startswith('--'):
                    is_only_comment_or_empty = False
                    break
            if not is_only_comment_or_empty:
                final_statements.append(s)
        
        self.log.emit(f"解析得到 {len(final_statements)} 条有效SQL语句。")
        return final_statements


class BaseInfoDataExtractionTab(QWidget):
    """
    基础数据提取Tab页主类。
    用于MIMIC-IV数据的表选择、选项设定、SQL生成与执行、进度反馈和结果预览。
    """
    def __init__(self, get_db_params_func, parent=None):
        """
        初始化Tab。
        参数：
            get_db_params_func: 获取数据库参数的函数。
            parent: Qt父对象。
        """
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_table = None
        self.sql_confirmed = False
        self.worker = None
        self.worker_thread = None
        # 诊断类别关键词，用于ICD查询
        self.DIAG_CATEGORY_KEYWORDS = [
            "sleep apnea", "insomnia", "depressive", "anxiety", "anxiolytic",
            "diabetes", "hypertension", "myocardial infarction", "stroke", "asthma", "copd"
        ]
        self.init_ui()

    def init_ui(self):
        """
        构建Tab的界面，包括表选择、数据选项、执行进度、SQL预览、操作按钮和结果表。
        """
        # 主体布局
        main_layout = QVBoxLayout(self)
        
        # 顶部控件
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        
        # 操作说明
        instruction_label = QLabel("从数据库中选择病种表，并添加基础数据。\n请先点击SQL确认预览以生成并检查SQL语句，然后才能点击提取基础数据。")
        instruction_label.setWordWrap(True)
        top_layout.addWidget(instruction_label)
        
        # 表选择区
        table_select_layout = QHBoxLayout()
        table_select_layout.addWidget(QLabel("选择病种表:"))
        self.table_combo = QComboBox()
        self.table_combo.setMinimumWidth(300)
        self.table_combo.currentIndexChanged.connect(self.on_table_selected)
        table_select_layout.addWidget(self.table_combo)
        
        # 刷新表列表按钮
        self.refresh_btn = QPushButton("刷新表列表")
        self.refresh_btn.clicked.connect(self.refresh_tables)
        table_select_layout.addWidget(self.refresh_btn)
        top_layout.addLayout(table_select_layout)
        
        # 数据提取选项区
        options_group = QGroupBox("数据提取选项")
        options_layout = QVBoxLayout(options_group)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)

        # 各类数据复选框
        self.cb_demography = QCheckBox("住院及人口学信息")
        self.cb_demography.setChecked(True); scroll_layout.addWidget(self.cb_demography)
        self.cb_antecedent = QCheckBox("患者既往史 (Charlson)")
        self.cb_antecedent.setChecked(True); scroll_layout.addWidget(self.cb_antecedent)
        self.cb_vital_sign = QCheckBox("患者住院生命体征")
        self.cb_vital_sign.setChecked(True); scroll_layout.addWidget(self.cb_vital_sign)
        self.cb_blood_info = QCheckBox("患者住院红细胞相关指标")
        self.cb_blood_info.setChecked(True); scroll_layout.addWidget(self.cb_blood_info)
        self.cb_cardiovascular_lab = QCheckBox("患者住院心血管化验指标")
        self.cb_cardiovascular_lab.setChecked(True); scroll_layout.addWidget(self.cb_cardiovascular_lab)
        self.cb_medications = QCheckBox("患者住院用药记录")
        self.cb_medications.setChecked(True); scroll_layout.addWidget(self.cb_medications)
        self.cb_surgery = QCheckBox("患者住院手术记录")
        self.cb_surgery.setChecked(True); scroll_layout.addWidget(self.cb_surgery)
        self.cb_past_disease = QCheckBox("患者既往病史 (自定义ICD)")
        self.cb_past_disease.setChecked(True); scroll_layout.addWidget(self.cb_past_disease)
        
        # 选项变更信号绑定
        for cb in [self.cb_demography, self.cb_antecedent, self.cb_vital_sign, 
                   self.cb_blood_info, self.cb_cardiovascular_lab, self.cb_medications, 
                   self.cb_surgery, self.cb_past_disease]:
            cb.stateChanged.connect(self.on_options_changed)
        
        scroll_area.setWidget(scroll_content)
        options_layout.addWidget(scroll_area)
        top_layout.addWidget(options_group)
        
        # SQL执行状态区
        self.execution_status_group = QGroupBox("SQL执行状态")
        execution_status_layout = QVBoxLayout(self.execution_status_group)
        self.execution_progress = QProgressBar()
        self.execution_progress.setRange(0, 100); self.execution_progress.setValue(0)
        execution_status_layout.addWidget(self.execution_progress)
        self.execution_log = QTextEdit()
        self.execution_log.setReadOnly(True); self.execution_log.setMaximumHeight(150)
        execution_status_layout.addWidget(self.execution_log)
        self.execution_status_group.setVisible(False)
        top_layout.addWidget(self.execution_status_group)
        
        # SQL预览区
        top_layout.addWidget(QLabel("SQL预览:"))
        self.sql_preview = QTextEdit()
        self.sql_preview.setReadOnly(True); self.sql_preview.setMaximumHeight(200)
        top_layout.addWidget(self.sql_preview)

        # 操作按钮区
        buttons_layout = QHBoxLayout()
        self.confirm_sql_btn = QPushButton("SQL确认预览")
        self.confirm_sql_btn.clicked.connect(self.handle_confirm_sql_preview)
        self.confirm_sql_btn.setEnabled(False)
        buttons_layout.addWidget(self.confirm_sql_btn)

        self.extract_btn = QPushButton("提取基础数据")
        self.extract_btn.clicked.connect(self.extract_data)
        self.extract_btn.setEnabled(False)
        buttons_layout.addWidget(self.extract_btn)

        self.cancel_extraction_btn = QPushButton("取消操作") 
        self.cancel_extraction_btn.clicked.connect(self.cancel_extraction)
        self.cancel_extraction_btn.setEnabled(False)
        buttons_layout.addWidget(self.cancel_extraction_btn)
        
        top_layout.addLayout(buttons_layout)
        
        # 主体分割器，分为操作区和结果区
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(top_widget)
        
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)
        splitter.setSizes([600, 250])
        
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
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'mimiciv_data' 
                AND table_name LIKE 'first_%_admissions'
                ORDER BY table_name
            """)
            tables = cur.fetchall()
            if tables:
                for table in tables: self.table_combo.addItem(table[0])
                if self.table_combo.count() > 0: self.table_combo.setCurrentIndex(0)
            else:
                self.table_combo.addItem("未找到病种表")
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取表列表: {str(e)}")
            self.table_combo.addItem("查询表失败")
        finally:
            if conn: conn.close()
            
    def on_table_selected(self, index):
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)
        current_item_text = self.table_combo.itemText(index) if index >=0 else None
        if current_item_text and current_item_text not in ["未找到病种表", "数据库未连接", "查询表失败"]:
            self.selected_table = current_item_text
            self.confirm_sql_btn.setEnabled(True)
            self.preview_sql() # 自动预览
        else:
            self.selected_table = None
            self.confirm_sql_btn.setEnabled(False)
            self.sql_preview.clear()

    def preview_sql(self):
        """预览SQL语句，会尝试连接数据库以获取ICD码（如果需要）"""
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)
        if not self.selected_table:
            self.sql_preview.clear()
            self.confirm_sql_btn.setEnabled(False)
            return

        self.confirm_sql_btn.setEnabled(True) # 只要有表选中，就允许尝试预览/确认

        db_params = self.get_db_params()
        conn_preview = None
        generated_sql = ""

        try:
            # 仅当需要动态查询ICD码并且数据库已配置时才连接
            if self.cb_past_disease.isChecked():
                if db_params:
                    print("Previewing SQL: Connecting to DB for ICD code lookup...")
                    conn_preview = psycopg2.connect(**db_params)
                else:
                    # 如果需要连接但未连接，则生成带提示的SQL
                    print("Previewing SQL: DB connection needed for ICD codes, but not available.")
                    # 先生成不含既往病史的SQL
                    temp_sql = self.generate_sql(None) # Pass None for connection
                    # 附加提示信息
                    generated_sql = temp_sql + "\n-- [预览警告] 未连接数据库，无法生成'患者既往病史 (自定义ICD)'部分的SQL。--"
                    self.sql_preview.setText(generated_sql)
                    # 即使SQL不完整，确认按钮也应可用，以便用户知道问题
                    return # 直接返回，不进行后续生成

            # 如果不需要连接，或者连接已建立，则正常生成SQL
            print(f"Previewing SQL: Calling generate_sql with connection {'present' if conn_preview else 'absent'}")
            generated_sql = self.generate_sql(conn_preview)
            self.sql_preview.setText(generated_sql if generated_sql.strip() else "-- 没有选择任何数据提取选项，SQL为空。 --")

        except (Exception, psycopg2.Error) as e:
            error_msg = f"-- 生成SQL预览时出错: {str(e)} --"
            self.sql_preview.setText(error_msg)
            print(f"Error during SQL preview generation: {e}")
        finally:
            if conn_preview:
                print("Previewing SQL: Closing temporary DB connection.")
                conn_preview.close()

    def generate_sql(self, conn_for_icd_lookup):
        """
        生成SQL语句。
        如果 conn_for_icd_lookup 提供，则会查询ICD码用于 add_past_diagnostic。
        否则，add_past_diagnostic 部分会生成提示信息。
        """
        if not self.selected_table: return ""

        qualified_table_name = f"mimiciv_data.{self.selected_table}"
        sql_accumulator = f"-- 为表 {qualified_table_name} 添加基础数据\n\n"
        past_diag_data_for_sql = {} # 用于存储查询到的ICD数据

        # --- Step 1: (如果需要) 查询 ICD 代码 ---
        if self.cb_past_disease.isChecked():
            if conn_for_icd_lookup:
                print("generate_sql: Fetching ICD codes...")
                try:
                    # 循环查询每个关键词的ICD
                    for keyword in self.DIAG_CATEGORY_KEYWORDS:
                        if not keyword or not isinstance(keyword, str): continue

                        category_key = keyword.strip().lower().replace(' ', '_')
                        # 优化查询：一次性查出所有关键词相关的ICD和原始关键词可能更高效
                        # 但这里保持原逻辑，逐个查询
                        # 使用参数化查询防止SQL注入（尽管这里keyword来自内部列表）
                        icd_query_template = """
                            SELECT DISTINCT TRIM(icd_code) AS icd_code
                            FROM mimiciv_hosp.d_icd_diagnoses
                            WHERE LOWER(long_title) LIKE %s;
                        """
                        like_param = f'%{keyword.strip().lower()}%'

                        # 使用 pandas 读取查询结果
                        icd_df = pd.read_sql_query(icd_query_template, conn_for_icd_lookup, params=(like_param,))
                        icd_codes_list = [code for code in icd_df['icd_code'].tolist() if code and str(code).strip()]

                        if icd_codes_list:
                            past_diag_data_for_sql[category_key] = icd_codes_list
                            print(f"  Found {len(icd_codes_list)} ICD codes for '{keyword}' -> '{category_key}'")
                        else:
                             print(f"  No ICD codes found for '{keyword}'")
                    print(f"generate_sql: Finished fetching ICD codes. Found data for {len(past_diag_data_for_sql)} categories.")
                except (Exception, psycopg2.Error) as db_err:
                     print(f"generate_sql: Error fetching ICD codes: {db_err}. Proceeding without past disease data.")
                     # 如果查询出错，则 past_diag_data_for_sql 可能是空的或不完整的
                     # add_past_diagnostic 会处理空字典的情况
                     # 可以在 sql_accumulator 中添加错误提示
                     sql_accumulator += f"-- [错误] 查询自定义既往病史ICD码时出错: {db_err} --\n"
                     past_diag_data_for_sql = {} # 清空以防部分数据导致问题
            else:
                # 不需要连接，或者连接未提供，跳过查询
                print("generate_sql: Skipping ICD code lookup (no connection provided).")
                pass # add_past_diagnostic 调用时会处理空字典

        # --- Step 2: 生成 SQL 语句 ---
        if self.cb_demography.isChecked(): sql_accumulator = add_demography(qualified_table_name, sql_accumulator)
        if self.cb_antecedent.isChecked(): sql_accumulator = add_antecedent(qualified_table_name, sql_accumulator)
        if self.cb_vital_sign.isChecked(): sql_accumulator = add_vital_sign(qualified_table_name, sql_accumulator)
        if self.cb_blood_info.isChecked(): sql_accumulator = add_blood_info(qualified_table_name, sql_accumulator)
        if self.cb_cardiovascular_lab.isChecked(): sql_accumulator = add_cardiovascular_lab(qualified_table_name, sql_accumulator)
        if self.cb_medications.isChecked(): sql_accumulator = add_medicine(qualified_table_name, sql_accumulator)
        if self.cb_surgery.isChecked(): sql_accumulator = add_surgeries(qualified_table_name, sql_accumulator)

        # 调用修改后的 add_past_diagnostic，传递获取到的数据
        if self.cb_past_disease.isChecked():
            if conn_for_icd_lookup: # 只有在尝试了连接和查询后才调用
                sql_accumulator = add_past_diagnostic(qualified_table_name, sql_accumulator, past_diag_data_for_sql)
            else:
                # 如果没有尝试连接（因为一开始就没连接参数），则添加提示
                sql_accumulator += "\n-- [INFO] '患者既往病史 (自定义ICD)' 需要数据库连接才能生成SQL。 --\n"

        return sql_accumulator


    def prepare_for_long_operation(self, starting=True):
        # ... (代码保持不变)
        if starting:
            self.execution_status_group.setVisible(True)
            self.execution_progress.setValue(0)
            self.execution_log.clear()
            self.update_execution_log("开始执行SQL操作...")
            self.extract_btn.setEnabled(False)
            self.confirm_sql_btn.setEnabled(False)
            self.refresh_btn.setEnabled(False)
            self.table_combo.setEnabled(False)
            self.cancel_extraction_btn.setEnabled(True) # 启用取消按钮
        else: # Operation finished or cancelled
            self.confirm_sql_btn.setEnabled(bool(self.selected_table)) # Re-enable if a table is selected
            self.extract_btn.setEnabled(self.sql_confirmed and bool(self.selected_table)) # Re-enable if SQL confirmed and table selected
            self.refresh_btn.setEnabled(True)
            self.table_combo.setEnabled(True)
            self.cancel_extraction_btn.setEnabled(False) # 禁用取消按钮

            if self.worker_thread and self.worker_thread.isRunning():
                self.worker_thread.quit() # Request thread to finish
                if not self.worker_thread.wait(3000): # Wait up to 3 seconds
                    self.update_execution_log("警告: 工作线程未能正常退出。")
            self.worker = None # Clear worker reference
            self.worker_thread = None # Clear thread reference

    def update_execution_progress(self, value, max_value=None):
        # ... (代码保持不变)
        if max_value is not None: self.execution_progress.setMaximum(max_value)
        self.execution_progress.setValue(value)

    def update_execution_log(self, message):
        # ... (代码保持不变)
        self.execution_log.append(message) # Use append for auto-scrolling

    def extract_data(self):
        """提取基础数据，现在 generate_sql 会在需要时进行 ICD 查询"""
        if not self.selected_table:
            QMessageBox.warning(self, "未选择表", "请先选择一个病种表")
            return
        if not self.sql_confirmed:
            QMessageBox.warning(self, "SQL未确认", "请先点击SQL确认预览按钮生成并确认SQL语句。")
            return

        db_params = self.get_db_params()
        # 检查是否需要数据库连接来生成SQL (取决于选项)
        needs_db_for_generation = self.cb_past_disease.isChecked()

        if needs_db_for_generation and not db_params:
            QMessageBox.warning(self, "未连接", "“患者既往病史 (自定义ICD)”选项需要数据库连接才能生成SQL。请先连接数据库。")
            return
        if not db_params: # 如果其他操作也不需要连接（理论上不太可能，但检查一下）
             QMessageBox.warning(self, "未连接", "请先连接数据库")
             return


        sql_to_execute = ""
        conn_generate = None
        try:
            # 如果需要，建立连接以生成SQL
            if needs_db_for_generation:
                print("Extract Data: Connecting to DB for SQL generation (ICD lookup)...")
                conn_generate = psycopg2.connect(**db_params)

            # 调用 generate_sql，它现在会处理 ICD 查询（如果需要且连接可用）
            sql_to_execute = self.generate_sql(conn_generate)
            print("Extract Data: SQL generated.")

            # 检查生成的SQL是否有效 (generate_sql内部已经处理了无连接的情况)
            if not sql_to_execute.strip():
                 QMessageBox.information(self, "无操作", "没有选择任何数据提取选项，或生成的SQL为空。")
                 return
            # 检查是否包含因连接问题导致的警告信息
            if "-- [错误] 查询自定义既往病史ICD码时出错" in sql_to_execute or \
               "-- [INFO] '患者既往病史 (自定义ICD)' 需要数据库连接才能生成SQL" in sql_to_execute :
                 QMessageBox.warning(self, "SQL生成不完整或有误", "生成SQL时遇到问题（详见SQL预览或日志）。请检查数据库连接和选项后重试。")
                 # 更新预览区域以显示包含错误/提示的SQL
                 self.sql_preview.setText(sql_to_execute)
                 self.sql_confirmed = False # SQL 有问题，取消确认状态
                 self.extract_btn.setEnabled(False)
                 return

        except (Exception, psycopg2.Error) as e:
            QMessageBox.critical(self, "SQL生成失败", f"无法生成SQL: {str(e)}")
            print(f"Error during SQL generation for execution: {e}")
            return
        finally:
            if conn_generate:
                print("Extract Data: Closing DB connection used for SQL generation.")
                conn_generate.close()

        # --- SQL 生成成功，准备执行 ---
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

        self.worker_thread.start()


    def handle_confirm_sql_preview(self):
        # ... (代码保持不变, 它依赖 preview_sql 的输出来判断)
        if not self.selected_table:
            QMessageBox.warning(self, "无操作", "请先选择一个病种表。")
            self.confirm_sql_btn.setEnabled(False)
            return

        # 重新调用 preview_sql 以确保预览是最新的，并处理连接依赖
        self.preview_sql() 
        current_sql_text = self.sql_preview.toPlainText().strip()

        # 更新需要检查的问题短语
        problematic_phrases = [
            "-- 请先连接数据库", 
            "-- 生成SQL预览时出错", 
            "-- 没有选择任何数据提取选项",
            "-- [预览警告] 未连接数据库", # 来自 preview_sql 的新警告
            "-- [错误] 查询自定义既往病史ICD码时出错" # 来自 generate_sql 的错误
        ]

        is_problematic = any(phrase in current_sql_text for phrase in problematic_phrases)
        
        if not current_sql_text or is_problematic :
            QMessageBox.warning(self, "SQL预览问题", "SQL预览为空，包含错误/警告信息，或因数据库未连接而无法生成完整SQL。\n请检查表选择、数据库连接和提取选项后重试。")
            self.sql_confirmed = False
            self.extract_btn.setEnabled(False)
        else:
            self.sql_confirmed = True
            self.extract_btn.setEnabled(True) # 只有在预览成功且无问题时才启用
            QMessageBox.information(self, "SQL已确认", "SQL预览已生成并确认。\n您现在可以点击提取基础数据按钮。")


    def on_options_changed(self):
        # ... (代码保持不变)
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)
        if self.selected_table: self.preview_sql()

    def cancel_extraction(self):
        # ... (代码保持不变)
        if self.worker:
            self.update_execution_log("正在请求取消SQL执行...")
            self.worker.cancel()
            self.cancel_extraction_btn.setEnabled(False)

    def on_sql_execution_finished(self, columns, rows):
        # ... (代码保持不变)
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

    def on_sql_execution_error(self, error_message):
        # ... (代码保持不变)
        self.update_execution_log(f"错误: {error_message}")
        if "操作已取消" not in error_message:
            QMessageBox.critical(self, "提取失败", f"无法提取基础数据: {error_message}")
        else:
            QMessageBox.information(self, "操作取消", "数据提取操作已取消。")

        self.sql_confirmed = False
        self.extract_btn.setEnabled(False)
        self.prepare_for_long_operation(False)

# --- END OF FILE tab_combine_base_info.py ---