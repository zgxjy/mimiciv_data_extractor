from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel, 
                          QSplitter, QTextEdit, QDialog, QLineEdit, QFormLayout)
from PySide6.QtCore import Qt
import psycopg2
from conditiongroup import ConditionGroupWidget

class QueryDiseaseTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        
        # 顶部控件区域
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        
        # 添加说明标签
        instruction_label = QLabel("使用下方条件组构建疾病查询条件，可以添加多个关键词和嵌套条件组")
        top_layout.addWidget(instruction_label)
        
        # 添加条件组控件
        self.condition_group = ConditionGroupWidget(is_root=True)
        top_layout.addWidget(self.condition_group)
        
        # 查询按钮和SQL预览
        btn_layout = QHBoxLayout()
        self.query_btn = QPushButton("查询")
        self.query_btn.clicked.connect(self.execute_query)
        self.query_btn.setEnabled(False)  # 初始状态禁用
        btn_layout.addWidget(self.query_btn)
        
        self.preview_btn = QPushButton("预览SQL")
        self.preview_btn.clicked.connect(self.preview_sql)
        self.preview_btn.setEnabled(False)  # 初始状态禁用
        btn_layout.addWidget(self.preview_btn)
        
        self.create_table_btn = QPushButton("创建首次诊断目标病种表")
        self.create_table_btn.clicked.connect(self.create_disease_table)
        self.create_table_btn.setEnabled(False)  # 初始状态禁用
        btn_layout.addWidget(self.create_table_btn)
        
        top_layout.addLayout(btn_layout)
        
        # SQL预览文本框
        self.sql_preview = QTextEdit()
        self.sql_preview.setReadOnly(True)
        self.sql_preview.setMaximumHeight(300)
        top_layout.addWidget(self.sql_preview)
        
        # 创建分割器
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(top_widget)
        
        # 结果表格
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)
        
        # 设置初始分割比例
        splitter.setSizes([300, 500])
        
        main_layout.addWidget(splitter)

    def preview_sql(self):
        """预览生成的SQL语句"""
        condition = self.condition_group.get_condition()
        if not condition:
            condition = "1=1"  # 默认条件，显示所有记录
            
        query = f"""
SELECT *
FROM mimiciv_hosp.d_icd_diagnoses
WHERE {condition}
"""
        self.sql_preview.setText(query)
        return query

    def execute_query(self):
        """执行查询并显示结果"""
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return
            
        query = self.preview_sql()
        
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute(query)
            
            # 获取列名
            columns = [desc[0] for desc in cur.description]
            
            # 获取数据
            rows = cur.fetchall()
            
            # 设置表格
            self.result_table.setRowCount(len(rows))
            self.result_table.setColumnCount(len(columns))
            self.result_table.setHorizontalHeaderLabels(columns)
            
            # 填充数据
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value) if value is not None else "")
                    self.result_table.setItem(i, j, item)
            
            # 自动调整列宽
            self.result_table.resizeColumnsToContents()
            
            # 显示查询结果数量
            QMessageBox.information(self, "查询完成", f"共找到 {len(rows)} 条记录")
            
            # 启用创建表按钮
            if len(rows) > 0:
                self.create_table_btn.setEnabled(True)
            
            conn.close()
            
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法执行查询: {str(e)}")
    
    def create_disease_table(self):
        """创建首次诊断目标病种表"""
        # 获取当前查询条件
        condition = self.condition_group.get_condition()
        if not condition:
            QMessageBox.warning(self, "缺少条件", "请先设置查询条件")
            return
            
        # 创建对话框让用户输入病种名称
        disease_name, ok = self.get_disease_name()
        if not ok or not disease_name:
            return
            
        # 检查病种名称是否为英文
        if not disease_name.isascii():
            QMessageBox.warning(self, "名称错误", "病种名称只能包含英文字符")
            return
            
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return
            
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            
            # 1. 检查并创建 mimiciv_data schema
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS mimiciv_data;
            """)
            
            # 2. 创建 first_diag_ad 表
            cur.execute(f"""
                DROP TABLE IF EXISTS mimiciv_data.first_diag_ad;
                CREATE TABLE mimiciv_data.first_diag_ad AS( 
                  SELECT * FROM (
                    SELECT
                      d.subject_id, d.hadm_id, d.icd_code, d.seq_num, dd.long_title,
                      ROW_NUMBER() OVER(PARTITION BY d.subject_id ORDER BY d.seq_num) AS target_disease_diag_rank
                    FROM mimiciv_hosp.diagnoses_icd d
                    JOIN mimiciv_hosp.d_icd_diagnoses dd ON d.icd_code = dd.icd_code
                    WHERE {condition}
                  ) sub
                  WHERE target_disease_diag_rank = 1);
            """)
            
            # 3. 创建 first_icu_stays 表
            cur.execute("""
                DROP TABLE IF EXISTS mimiciv_data.first_icu_stays;
                CREATE TABLE mimiciv_data.first_icu_stays AS (
                  SELECT * FROM (
                    SELECT 
                      subject_id, hadm_id, stay_id, intime as icu_intime, outtime as icu_outtime,
                      EXTRACT(EPOCH FROM (outtime - intime)) / 3600 AS icu_stay_hours,
                      ROW_NUMBER() OVER (PARTITION BY subject_id, hadm_id ORDER BY intime, stay_id) AS icu_stay_order 
                    FROM mimiciv_icu.icustays
                  ) sub
                  WHERE icu_stay_order = 1
                );
            """)
            
            # 4. 创建 first_{disease_name}_admissions 表
            cur.execute(f"""
                DROP TABLE IF EXISTS mimiciv_data.first_{disease_name}_admissions;
                CREATE TABLE mimiciv_data.first_{disease_name}_admissions AS (
                 SELECT 
                 	st.*,
                	fi.icd_code,fi.long_title,fi.seq_num
                 FROM mimiciv_data.first_diag_ad fi
                 JOIN mimiciv_data.first_icu_stays st
                 ON fi.hadm_id = st.hadm_id
                );
                DROP TABLE IF EXISTS mimiciv_data.first_icu_stays;
                DROP TABLE IF EXISTS mimiciv_data.first_diag_ad;
            """)
            
            conn.commit()
            
            # 查询创建的表中的记录数
            cur.execute(f"SELECT COUNT(*) FROM mimiciv_data.first_{disease_name}_admissions")
            count = cur.fetchone()[0]
            
            QMessageBox.information(self, "创建成功", 
                                   f"已成功创建目标病种表: mimiciv_data.first_{disease_name}_admissions\n"
                                   f"包含 {count} 条记录")
            
            conn.close()
            
        except Exception as e:
            QMessageBox.critical(self, "创建失败", f"无法创建目标病种表: {str(e)}")
    
    def get_disease_name(self):
        """获取用户输入的病种名称"""
        dialog = QDialog(self)
        dialog.setWindowTitle("输入病种名称")
        layout = QVBoxLayout(dialog)
        
        form_layout = QFormLayout()
        name_input = QLineEdit()
        form_layout.addRow("病种名称(英文):", name_input)
        layout.addLayout(form_layout)
        
        # 添加说明标签
        info_label = QLabel("注意: 病种名称只能包含英文字符，将用于创建数据表名称")
        info_label.setWordWrap(True)
        layout.addWidget(info_label)
        
        # 按钮
        btn_layout = QHBoxLayout()
        ok_btn = QPushButton("确定")
        cancel_btn = QPushButton("取消")
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)
        
        ok_btn.clicked.connect(dialog.accept)
        cancel_btn.clicked.connect(dialog.reject)
        
        result = dialog.exec_()
        return name_input.text().strip(), result == QDialog.Accepted
