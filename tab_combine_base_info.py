from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel, 
                          QSplitter, QTextEdit, QComboBox, QGroupBox, QCheckBox,
                          QScrollArea, QFormLayout)
from PySide6.QtCore import Qt
import psycopg2
import re
from base_info_sql import *

class BaseInfoDataExtractionTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.selected_table = None
        self.sql_confirmed = False # 新增：SQL确认状态标志
        # 定义既往病史的关键词列表
        self.DIAG_CATEGORY_KEYWORDS = [
            "sleep apnea", "insomnia", "depressive", "anxiety", "anxiolytic",
            # 您可以在此处添加更多自定义的疾病关键词
            "diabetes", "hypertension", "myocardial infarction", "stroke", "asthma", "copd"
        ]
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        
        # 顶部控件区域
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        
        # 添加说明标签
        instruction_label = QLabel("从数据库中选择病种表，并添加基础数据。\n请先点击“SQL确认预览”以生成并检查SQL语句，然后才能点击“提取基础数据”。")
        instruction_label.setWordWrap(True)
        top_layout.addWidget(instruction_label)
        
        # 表格选择区域
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
        
        # 创建数据提取选项区域
        options_group = QGroupBox("数据提取选项")
        options_layout = QVBoxLayout(options_group)
        
        # 使用滚动区域来容纳所有选项
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        

        self.cb_demography = QCheckBox("住院及人口学信息")
        self.cb_demography.setChecked(True)
        self.cb_demography.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_demography)

        self.cb_antecedent = QCheckBox("患者既往史") # 注意：这个似乎与下面的“患者既往病史”有重复或相似，请确认
        self.cb_antecedent.setChecked(True)
        self.cb_antecedent.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_antecedent)
    
        self.cb_vital_sign = QCheckBox("患者住院生命体征")
        self.cb_vital_sign.setChecked(True)
        self.cb_vital_sign.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_vital_sign)
        
        self.cb_blood_info = QCheckBox("患者住院红细胞相关指标")
        self.cb_blood_info.setChecked(True)
        self.cb_blood_info.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_blood_info)

        self.cb_cardiovascular_lab = QCheckBox("患者住院心血管化验指标")
        self.cb_cardiovascular_lab.setChecked(True)
        self.cb_cardiovascular_lab.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_cardiovascular_lab)

        self.cb_medications = QCheckBox("患者住院用药记录")
        self.cb_medications.setChecked(True)
        self.cb_medications.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_medications)

        self.cb_surgery = QCheckBox("患者住院手术记录")
        self.cb_surgery.setChecked(True)
        self.cb_surgery.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_surgery)

        self.cb_past_disease = QCheckBox("患者既往病史") # 注意：这个似乎与上面的“患者既往史”有重复或相似，请确认
        self.cb_past_disease.setChecked(True)
        self.cb_past_disease.stateChanged.connect(self.on_options_changed) # 新增连接
        scroll_layout.addWidget(self.cb_past_disease)
        

        scroll_area.setWidget(scroll_content)
        options_layout.addWidget(scroll_area)
        top_layout.addWidget(options_group)
        
        # SQL预览文本框 (移到按钮之前，逻辑上更顺)
        top_layout.addWidget(QLabel("SQL预览:"))
        self.sql_preview = QTextEdit()
        self.sql_preview.setReadOnly(True)
        self.sql_preview.setMaximumHeight(200) # 您可以根据需要调整高度
        top_layout.addWidget(self.sql_preview)

        # 添加按钮区域
        buttons_layout = QHBoxLayout() # 使用水平布局放两个按钮

        self.confirm_sql_btn = QPushButton("SQL确认预览")
        self.confirm_sql_btn.clicked.connect(self.handle_confirm_sql_preview) # 新增连接
        self.confirm_sql_btn.setEnabled(False) # 初始禁用
        buttons_layout.addWidget(self.confirm_sql_btn)

        self.extract_btn = QPushButton("提取基础数据")
        self.extract_btn.clicked.connect(self.extract_data)
        self.extract_btn.setEnabled(False) # 初始禁用
        buttons_layout.addWidget(self.extract_btn)
        
        top_layout.addLayout(buttons_layout) # 将按钮布局添加到主顶部布局
        
        # 创建分割器
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(top_widget)
        
        # 结果表格
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)
        
        # 设置初始分割比例
        splitter.setSizes([500, 300])
        
        main_layout.addWidget(splitter)

    def on_db_connected(self):
        """当数据库连接成功时调用"""
        self.refresh_btn.setEnabled(True)
        self.refresh_tables()

    def refresh_tables(self):
        """刷新表列表"""
        # 重置状态
        self.selected_table = None
        self.sql_confirmed = False
        self.sql_preview.clear()
        self.confirm_sql_btn.setEnabled(False)
        self.extract_btn.setEnabled(False)
        self.table_combo.clear()

        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            self.table_combo.addItem("数据库未连接") # 提示用户
            return
            
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            
            # 查询mimiciv_data schema中的所有表，并筛选出符合first_*_admissions模式的表
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'mimiciv_data' 
                AND table_name LIKE 'first_%_admissions'
                ORDER BY table_name
            """)
            
            tables = cur.fetchall()
            
            if tables:
                for table in tables:
                    self.table_combo.addItem(table[0])
                # 如果有表，则触发第一个表的选择逻辑
                if self.table_combo.count() > 0:
                    self.table_combo.setCurrentIndex(0) # 自动选择第一个表，会触发on_table_selected
                    # on_table_selected 会处理 confirm_sql_btn 的状态
            else:
                self.table_combo.addItem("未找到病种表")
                # self.confirm_sql_btn.setEnabled(False) 已经由初始重置处理
                
            conn.close()
            
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取表列表: {str(e)}")
            self.table_combo.addItem("查询表失败") # 提示用户
            # self.confirm_sql_btn.setEnabled(False) 已经由初始重置处理
            
    def on_table_selected(self, index):
        """当选择表时调用"""
        self.sql_confirmed = False
        self.extract_btn.setEnabled(False) # 每次选择新表，提取按钮都需重新确认SQL
        
        current_item_text = self.table_combo.itemText(index) if index >=0 else None

        if current_item_text and current_item_text not in ["未找到病种表", "数据库未连接", "查询表失败"]:
            self.selected_table = current_item_text
            self.confirm_sql_btn.setEnabled(True) # 有效表被选中，允许预览/确认SQL
            self.preview_sql()
        else:
            self.selected_table = None
            self.confirm_sql_btn.setEnabled(False) # 无效选择，禁用SQL确认
            self.sql_preview.clear()
            
    def preview_sql(self):
        """预览SQL语句"""
        self.sql_confirmed = False # SQL已更改，需要重新确认
        self.extract_btn.setEnabled(False) # 提取按钮也禁用

        if not self.selected_table:
            self.sql_preview.clear()
            self.confirm_sql_btn.setEnabled(False) # 如果没有选中的表，禁用确认按钮
            return
        
        # 如果有选中的表，确保确认按钮是启用的，除非后续生成SQL失败
        self.confirm_sql_btn.setEnabled(True)

        db_params = self.get_db_params()
        if not db_params:
            self.sql_preview.setText("-- 请先连接数据库以生成包含既往病史的完整SQL预览 --")
            # 即便如此，confirm_sql_btn 仍应启用，允许用户“确认”这条消息（如果有必要）
            return

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            sql = self.generate_sql(conn) # 传递连接对象
            self.sql_preview.setText(sql)
            if not sql.strip():
                 self.sql_preview.setText("-- 没有选择任何数据提取选项，SQL为空。 --")

        except Exception as e:
            self.sql_preview.setText(f"-- 生成SQL预览时出错 (可能影响既往病史部分): {str(e)} --")
            # 即使生成预览出错，确认按钮也应该可用，让用户知晓错误
        finally:
            if conn:
                conn.close()
        
    def generate_sql(self, conn): # 添加 conn 参数
        """生成SQL语句"""
        if not self.selected_table:
            return ""
            
        table_name = f"mimiciv_data.{self.selected_table}"
        
        sql_accumulator = f"-- 为表 {table_name} 添加基础数据\n\n"
        
        if self.cb_demography.isChecked():
            sql_accumulator = add_demography(table_name, sql_accumulator)
        if self.cb_antecedent.isChecked():
            sql_accumulator = add_antecedent(table_name, sql_accumulator)
        if self.cb_vital_sign.isChecked():
            sql_accumulator = add_vital_sign(table_name, sql_accumulator)
        if self.cb_blood_info.isChecked():
            sql_accumulator = add_blood_info(table_name, sql_accumulator)
        if self.cb_cardiovascular_lab.isChecked():
            sql_accumulator = add_cardiovascular_lab(table_name, sql_accumulator)
        if self.cb_medications.isChecked():
            sql_accumulator = add_medicine(table_name, sql_accumulator)
        if self.cb_surgery.isChecked():
            sql_accumulator = add_surgeries(table_name, sql_accumulator)
        
        if self.cb_past_disease.isChecked():
            if conn: # 仅当提供了有效的数据库连接时才调用
                sql_accumulator = add_past_diagnostic(table_name, sql_accumulator, conn, self.DIAG_CATEGORY_KEYWORDS)
            else:
                # 如果没有连接 (例如，在某些预览场景下可能不希望连接数据库)
                sql_accumulator += "\n-- [INFO] 既往病史SQL将在执行时动态生成 (需要数据库连接查询ICD码)。\n"
                sql_accumulator += f"--       涉及关键词: {', '.join(self.DIAG_CATEGORY_KEYWORDS)}\n"
        return sql_accumulator
        
    def extract_data(self):
        """提取基础数据"""
        if not self.selected_table:
            QMessageBox.warning(self, "未选择表", "请先选择一个病种表")
            return

        if not self.sql_confirmed: # 新增：检查SQL是否已确认
            QMessageBox.warning(self, "SQL未确认", "请先点击“SQL确认预览”按钮生成并确认SQL语句。")
            return
            
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return
            
        conn_extract = None # 为 extract_data 单独声明连接变量
        try:
            conn_extract = psycopg2.connect(**db_params)
            # 生成SQL时传递连接
            sql_to_execute = self.generate_sql(conn_extract) 
            if not sql_to_execute.strip() or "-- [INFO] 既往病史SQL将在执行时动态生成" in sql_to_execute :
                 # 检查SQL是否有效或是否因缺少连接而未完全生成
                 if not conn_extract and self.cb_past_disease.isChecked():
                     QMessageBox.warning(self, "SQL生成不完整", "无法为既往病史生成SQL，因为数据库连接在预览阶段失败。请重试或检查连接。")
                     return
                 elif not sql_to_execute.strip():
                     QMessageBox.information(self, "无操作", "没有选择任何数据提取选项，或SQL为空。")
                     return

            cur = conn_extract.cursor()
            
            # 执行SQL
            cur.execute(sql_to_execute)
            conn_extract.commit()
            
            # 查询表结构
            table_name = f"mimiciv_data.{self.selected_table}"
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'mimiciv_data' 
                AND table_name = '{self.selected_table}'
                ORDER BY ordinal_position
            """)
            
            columns = cur.fetchall()
            
            # 查询表数据
            cur.execute(f"SELECT * FROM {table_name} LIMIT 100")
            rows = cur.fetchall()
            
            # 设置表格
            self.result_table.setRowCount(len(rows))
            self.result_table.setColumnCount(len(columns))
            
            # 设置列标题
            column_names = [col[0] for col in columns]
            self.result_table.setHorizontalHeaderLabels(column_names)
            
            # 填充数据
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value) if value is not None else "")
                    self.result_table.setItem(i, j, item)
            
            # 自动调整列宽
            self.result_table.resizeColumnsToContents()
            
            QMessageBox.information(self, "提取成功", f"已成功为表 {self.selected_table} 添加基础数据")
            
            conn_extract.close()
            
        except Exception as e:
            QMessageBox.critical(self, "提取失败", f"无法提取基础数据: {str(e)}")
            # 提取失败后，重置确认状态，强制用户重新确认可能已修复的SQL
            self.sql_confirmed = False
            self.extract_btn.setEnabled(False)

    def handle_confirm_sql_preview(self):
        """处理SQL确认预览按钮点击事件"""
        if not self.selected_table:
            QMessageBox.warning(self, "无操作", "请先选择一个病种表。")
            self.confirm_sql_btn.setEnabled(False) # 确保在无表选择时禁用
            return

        # 首先确保SQL预览是最新的
        self.preview_sql() # 这个调用会处理sql_confirmed=False和extract_btn禁用

        current_sql_text = self.sql_preview.toPlainText().strip()

        if not current_sql_text or \
           "-- 请先连接数据库" in current_sql_text or \
           "-- 生成SQL预览时出错" in current_sql_text or \
           "-- 没有选择任何数据提取选项" in current_sql_text:
            QMessageBox.warning(self, "SQL预览问题", "SQL预览为空或包含错误/提示信息。\n请检查表选择、数据库连接和提取选项后重试。")
            self.sql_confirmed = False # 明确未确认
            self.extract_btn.setEnabled(False) # 保持禁用
        else:
            self.sql_confirmed = True
            self.extract_btn.setEnabled(True)
            QMessageBox.information(self, "SQL已确认", "SQL预览已生成并确认。\n您现在可以点击“提取基础数据”按钮。")

    def on_options_changed(self):
        """当数据提取选项改变时调用"""
        self.sql_confirmed = False # 选项已变，SQL需重新确认
        self.extract_btn.setEnabled(False) # 禁用提取按钮
        
        if self.selected_table: # 只有当有表被选中时，选项更改才需要更新预览
            self.preview_sql()
        # 如果没有表被选中，confirm_sql_btn应已在on_table_selected中处理为禁用
        # sql_preview也应已清空或显示提示