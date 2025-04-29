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
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        
        # 顶部控件区域
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        
        # 添加说明标签
        instruction_label = QLabel("从数据库中选择病种表，并添加基础数据")
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
        scroll_layout.addWidget(self.cb_demography)

        self.cb_antecedent = QCheckBox("患者既往史")
        self.cb_antecedent.setChecked(True)
        scroll_layout.addWidget(self.cb_antecedent)
    
        self.cb_vital_sign = QCheckBox("患者住院生命体征")
        self.cb_vital_sign.setChecked(True)
        scroll_layout.addWidget(self.cb_vital_sign)
        
        self.cb_blood_info = QCheckBox("患者住院红细胞相关指标")
        self.cb_blood_info.setChecked(True)
        scroll_layout.addWidget(self.cb_blood_info)

        self.cb_cardiovascular_lab = QCheckBox("患者住院心血管化验指标")
        self.cb_cardiovascular_lab.setChecked(True)
        scroll_layout.addWidget(self.cb_cardiovascular_lab)

        self.cb_medications = QCheckBox("患者住院用药记录")
        self.cb_medications.setChecked(True)
        scroll_layout.addWidget(self.cb_medications)

        self.cb_surgery = QCheckBox("患者住院手术记录")
        self.cb_surgery.setChecked(True)
        scroll_layout.addWidget(self.cb_surgery)

        self.cb_past_disease = QCheckBox("患者既往病史")
        self.cb_past_disease.setChecked(True)
        scroll_layout.addWidget(self.cb_past_disease)
        

        scroll_area.setWidget(scroll_content)
        options_layout.addWidget(scroll_area)
        top_layout.addWidget(options_group)
        
        # 添加提取按钮
        self.extract_btn = QPushButton("提取基础数据")
        self.extract_btn.clicked.connect(self.extract_data)
        self.extract_btn.setEnabled(False)
        top_layout.addWidget(self.extract_btn)
        
        # SQL预览文本框
        self.sql_preview = QTextEdit()
        self.sql_preview.setReadOnly(True)
        self.sql_preview.setMaximumHeight(200)
        top_layout.addWidget(QLabel("SQL预览:"))
        top_layout.addWidget(self.sql_preview)
        
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
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
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
            
            self.table_combo.clear()
            if tables:
                for table in tables:
                    self.table_combo.addItem(table[0])
                self.extract_btn.setEnabled(True)
            else:
                self.table_combo.addItem("未找到病种表")
                self.extract_btn.setEnabled(False)
                
            conn.close()
            
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取表列表: {str(e)}")
            
    def on_table_selected(self, index):
        """当选择表时调用"""
        if index >= 0 and self.table_combo.count() > 0 and self.table_combo.itemText(0) != "未找到病种表":
            self.selected_table = self.table_combo.currentText()
            self.preview_sql()
        else:
            self.selected_table = None
            self.sql_preview.clear()
            
    def preview_sql(self):
        """预览SQL语句"""
        if not self.selected_table:
            return
            
        sql = self.generate_sql()
        self.sql_preview.setText(sql)
        
    def generate_sql(self):
        """生成SQL语句"""
        if not self.selected_table:
            return ""
            
        table_name = f"mimiciv_data.{self.selected_table}"
        
        sql = f"-- 为表 {table_name} 添加基础数据\n\n"
        
        # TODO: 添加sql语句
        if self.cb_demography.isChecked():
            sql = add_demography(table_name, sql)
        if self.cb_antecedent.isChecked():
            sql = add_antecedent(table_name, sql)
        if self.cb_vital_sign.isChecked():
            sql = add_vital_sign(table_name, sql)
        if self.cb_blood_info.isChecked():
            sql = add_blood_info(table_name, sql)
        if self.cb_cardiovascular_lab.isChecked():
            sql = add_cardiovascular_lab(table_name, sql)
        if self.cb_medications.isChecked():
            sql = add_medicine(table_name, sql)
        if self.cb_surgery.isChecked():
            sql = add_surgeries(table_name, sql)
        if self.cb_past_disease.isChecked():
            sql = add_past_diagnostic(table_name, sql)
        return sql
        
    def extract_data(self):
        """提取基础数据"""
        if not self.selected_table:
            QMessageBox.warning(self, "未选择表", "请先选择一个病种表")
            return
            
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return
            
        sql = self.generate_sql()
        if not sql:
            return
            
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            
            # 执行SQL
            cur.execute(sql)
            conn.commit()
            
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
            
            conn.close()
            
        except Exception as e:
            QMessageBox.critical(self, "提取失败", f"无法提取基础数据: {str(e)}")