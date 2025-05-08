# --- START OF FILE tab_query_disease.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                          QTableWidget, QTableWidgetItem, QMessageBox, QLabel,
                          QSplitter, QTextEdit, QDialog, QLineEdit, QFormLayout)
from PySide6.QtCore import Qt
import psycopg2
from psycopg2 import sql as psql # 导入 psql
import re # 确保在文件顶部导入 re 模块
from conditiongroup import ConditionGroupWidget


class QueryDiseaseTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()
        self.last_query_condition_template = None # 保存上次查询的SQL模板
        self.last_query_params = None           # 保存上次查询的参数

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        # 顶部控件区域
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)

        # 添加说明标签
        instruction_label = QLabel("使用下方条件组构建疾病查询条件，可以添加多个关键词（可选择包含或排除）和嵌套条件组")
        top_layout.addWidget(instruction_label)

        # 添加条件组控件
        # 指定 search_field 为 d_icd_diagnoses 表中用于搜索的列
        self.condition_group = ConditionGroupWidget(is_root=True, search_field="long_title") 
        self.condition_group.condition_changed.connect(self.update_button_states)
        top_layout.addWidget(self.condition_group)

        # 查询按钮和SQL预览
        btn_layout = QHBoxLayout()
        self.query_btn = QPushButton("查询")
        self.query_btn.clicked.connect(self.execute_query)
        self.query_btn.setEnabled(False)  # 初始状态禁用
        btn_layout.addWidget(self.query_btn)

        self.preview_btn = QPushButton("预览SQL")
        self.preview_btn.clicked.connect(self.preview_sql_action) # 改为连接到新的 action
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
        self.sql_preview.setMaximumHeight(150) # 调整预览框高度
        top_layout.addWidget(self.sql_preview)

        # 创建分割器
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(top_widget)

        # 结果表格
        self.result_table = QTableWidget()
        self.result_table.setAlternatingRowColors(True)
        splitter.addWidget(self.result_table)

        # 设置初始分割比例
        splitter.setSizes([400, 450]) # 根据您的喜好调整

        main_layout.addWidget(splitter)
        self.update_button_states()

    def update_button_states(self):
        """根据条件组是否有有效输入来更新按钮状态"""
        has_input = self.condition_group.has_valid_input()
        self.query_btn.setEnabled(has_input)
        self.preview_btn.setEnabled(has_input)
        if not has_input:
            self.create_table_btn.setEnabled(False)
            # 当输入无效时，清除上次的查询条件，防止基于旧条件创建表
            self.last_query_condition_template = None
            self.last_query_params = None

    def _build_query_parts(self):
        """构建查询的SQL模板和参数，但不执行"""
        condition_template, params = self.condition_group.get_condition()

        # 基础查询语句，只选择需要的列
        base_query_template = "SELECT icd_code, long_title FROM mimiciv_hosp.d_icd_diagnoses"

        if condition_template:
            full_query_template = f"{base_query_template} WHERE {condition_template}"
        else:
            # 如果没有有效输入条件，理论上不应该能执行查询，但为了预览做个处理
            full_query_template = base_query_template # 预览时显示基础查询
            params = [] # 确保参数为空

        return full_query_template, params

    def preview_sql_action(self):
        """当点击“预览SQL”按钮时，生成并显示更友好的SQL预览"""
        query_template, params = self._build_query_parts()

        preview_sql_filled = query_template
        preview_params_str = f"Parameters: {params}" # 默认显示原始参数

        if params:
            try:
                db_params = self.get_db_params()
                temp_conn = None
                quoted_params = []

                if db_params:
                    try:
                        temp_conn = psycopg2.connect(**db_params)
                        temp_cur = temp_conn.cursor()
                        for p in params:
                            # 使用 mogrify 对每个参数进行安全引用
                            quoted_val = temp_cur.mogrify("%s", (p,)).decode(temp_conn.encoding)
                            quoted_params.append(quoted_val)
                        
                        placeholder_count = preview_sql_filled.count('%s')
                        if placeholder_count == len(quoted_params):
                             # 使用 '{}' 作为 str.format 的占位符替换 %s
                            preview_sql_filled = preview_sql_filled.replace('%s', '{}').format(*quoted_params)
                        else:
                            preview_sql_filled = f"SQL Template:\n{query_template}\n\nParameters:\n{params}\n\n(参数数量与占位符不匹配)"
                            
                    except (Exception, psycopg2.Error) as e:
                        print(f"Error during preview quoting: {e}")
                        preview_sql_filled = f"SQL Template:\n{query_template}\n\nParameters:\n{params}\n\n(无法生成带引用的预览)"
                    finally:
                        if temp_conn:
                            temp_conn.close()
                else:
                    # 回退方法：没有数据库连接，进行基础的字符串处理
                    try:
                        quoted_params_basic = []
                        for p in params:
                            if isinstance(p, str):
                                # 先执行替换操作
                                escaped_p = str(p).replace('%', '%%').replace("'", "''")
                                # 然后使用 f-string 添加引号
                                quoted_params_basic.append(f"'{escaped_p}'")
                            else:
                                quoted_params_basic.append(str(p))
                        
                        placeholder_count = preview_sql_filled.count('%s')
                        if placeholder_count == len(quoted_params_basic):
                             # 使用 '{}' 作为 str.format 的占位符替换 %s
                            preview_sql_filled = preview_sql_filled.replace('%s','{}').format(*quoted_params_basic)
                        else:
                            preview_sql_filled = f"SQL Template:\n{query_template}\n\nParameters:\n{params}\n\n(参数数量与占位符不匹配)"
                    except Exception as format_err:
                        print(f"Error formatting basic preview: {format_err}")
                        preview_sql_filled = f"SQL Template:\n{query_template}\n\nParameters:\n{params}\n\n(无法生成预览)"
                
                # 显示填充后的预览（如果成功）和原始参数
                self.sql_preview.setText(f"-- SQL Preview (仅供参考，实际执行使用参数化查询):\n{preview_sql_filled}\n\n-- Original Parameters:\n{params}")

            except Exception as e: # 捕获其他可能的错误
                print(f"Error creating preview SQL: {e}")
                self.sql_preview.setText(f"SQL Template:\n{query_template}\n\nParameters:\n{params}\n\n(生成预览时出错)")
        else:
            # 没有参数，直接显示模板
            self.sql_preview.setText(f"-- SQL Preview (仅供参考):\n{query_template}")


    def execute_query(self):
        """执行查询并显示结果"""
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return

        query_template, params = self._build_query_parts()

        # 检查是否有有效的条件或参数，防止执行空条件的全表查询（除非明确允许）
        has_meaningful_condition = self.condition_group.has_valid_input()
        if not has_meaningful_condition:
             # 如果需要阻止无条件查询，可以在这里提示用户
             # QMessageBox.warning(self, "查询受限", "请输入有效的查询条件。")
             # return
             # 或者，如果允许无条件查询，则继续，但确保 create_table 按钮被禁用
             print("Warning: Executing query without specific conditions.")
             self.create_table_btn.setEnabled(False)


        # 保存本次查询的条件，供 create_disease_table 使用
        current_condition_template, current_params = self.condition_group.get_condition()
        if not current_condition_template: # 如果条件为空
            self.last_query_condition_template = "1=1" # 使用一个始终为真的条件
            self.last_query_params = []
        else:
            self.last_query_condition_template = current_condition_template
            self.last_query_params = current_params


        # 在日志或预览中显示实际将要执行的模板和参数
        self.sql_preview.setText(f"Executing SQL Template:\n{query_template}\n\nWith Parameters:\n{params}")

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()

            cur.execute(query_template, params) # <--- 使用参数化查询

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            self.result_table.setRowCount(len(rows))
            self.result_table.setColumnCount(len(columns))
            self.result_table.setHorizontalHeaderLabels(columns)

            for i, row_data in enumerate(rows):
                for j, value in enumerate(row_data):
                    self.result_table.setItem(i, j, QTableWidgetItem(str(value) if value is not None else ""))

            self.result_table.resizeColumnsToContents()
            QMessageBox.information(self, "查询完成", f"共找到 {len(rows)} 条记录")

            # 只有在查询有结果且原始输入条件有效时，才启用创建表按钮
            if rows and has_meaningful_condition:
                self.create_table_btn.setEnabled(True)
            else:
                self.create_table_btn.setEnabled(False)

        except (Exception, psycopg2.Error) as error:
            QMessageBox.critical(self, "查询失败", f"无法执行查询: {error}\nTemplate: {query_template}\nParams: {params}")
            self.create_table_btn.setEnabled(False) # 查询失败也禁用
        finally:
            if conn:
                conn.close()

    def create_disease_table(self):
        """创建首次诊断目标病种表"""
        # 确保 self.last_query_condition_template 和 self.last_query_params 已通过 execute_query 设置
        if self.last_query_condition_template is None or not self.condition_group.has_valid_input():
            QMessageBox.warning(self, "缺少有效查询条件", "请先执行一次有效的疾病查询，并确保查询条件不为空。")
            return

        condition_sql_template = self.last_query_condition_template
        condition_params = self.last_query_params

        # --- disease_name 清理逻辑 ---
        raw_disease_name, ok = self.get_disease_name()
        if not ok or not raw_disease_name: return

        disease_name_cleaned = re.sub(r'[^a-z0-9_]+', '_', raw_disease_name.lower()).strip('_')
        if not disease_name_cleaned or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', disease_name_cleaned):
            QMessageBox.warning(self, "名称格式错误",
                                f"提供的病种队列标识符 '{raw_disease_name}' 清理后为 '{disease_name_cleaned}'，不符合命名规则。\n"
                                "请确保只使用英文字母、数字、下划线，并以字母或下划线开头。")
            return
        target_table_name = f"first_{disease_name_cleaned}_admissions"
        if len(target_table_name) > 63: # PostgreSQL 默认限制
             QMessageBox.warning(self, "名称过长",
                                 f"病种队列标识符 '{disease_name_cleaned}' 导致生成的表名过长 (最长63字符)。请缩短。")
             return
        # --- 清理结束 ---

        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return

        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            # 使用事务控制
            conn.autocommit = False

            cur.execute("CREATE SCHEMA IF NOT EXISTS mimiciv_data;")

            target_table_ident = psql.Identifier('mimiciv_data', target_table_name)
            first_diag_ad_ident = psql.Identifier('mimiciv_data', 'first_diag_ad')
            first_icu_stays_ident = psql.Identifier('mimiciv_data', 'first_icu_stays')

            # -- 创建 first_diag_ad 表 --
            create_first_diag_ad_sql_template = psql.SQL("""
                DROP TABLE IF EXISTS {table_ident};
                CREATE TABLE {table_ident} AS (
                  SELECT * FROM (
                    SELECT
                      d.subject_id, d.hadm_id, d.icd_code, d.seq_num, dd.long_title,
                      ROW_NUMBER() OVER(PARTITION BY d.subject_id ORDER BY d.seq_num) AS target_disease_diag_rank
                    FROM mimiciv_hosp.diagnoses_icd d
                    JOIN mimiciv_hosp.d_icd_diagnoses dd ON d.icd_code = dd.icd_code
                    WHERE ({condition_template_placeholder}) -- 使用括号确保优先级
                  ) sub
                  WHERE target_disease_diag_rank = 1
                );
            """).format(
                table_ident=first_diag_ad_ident,
                condition_template_placeholder=psql.SQL(condition_sql_template)
            )
            print("Executing SQL for first_diag_ad:", cur.mogrify(create_first_diag_ad_sql_template, condition_params).decode(conn.encoding)) # 调试日志
            cur.execute(create_first_diag_ad_sql_template, condition_params) # 传递参数

            # -- 创建 first_icu_stays 表 --
            create_first_icu_stays_sql = psql.SQL("""
                DROP TABLE IF EXISTS {table_ident};
                CREATE TABLE {table_ident} AS (
                  SELECT * FROM (
                    SELECT subject_id, hadm_id, stay_id, intime as icu_intime, outtime as icu_outtime,
                           EXTRACT(EPOCH FROM (outtime - intime)) / 3600 AS icu_stay_hours,
                           ROW_NUMBER() OVER (PARTITION BY subject_id, hadm_id ORDER BY intime, stay_id) AS icu_stay_order
                    FROM mimiciv_icu.icustays
                  ) sub WHERE icu_stay_order = 1);
            """).format(table_ident=first_icu_stays_ident)
            print("Executing SQL for first_icu_stays:", create_first_icu_stays_sql.as_string(conn)) # 调试日志
            cur.execute(create_first_icu_stays_sql)

            # -- 创建目标表 --
            create_target_table_sql = psql.SQL("""
                DROP TABLE IF EXISTS {target_ident};
                CREATE TABLE {target_ident} AS (
                 SELECT st.*, fi.icd_code, fi.long_title, fi.seq_num
                 FROM {diag_table} fi JOIN {icu_table} st ON fi.hadm_id = st.hadm_id);
                DROP TABLE IF EXISTS {icu_table};
                DROP TABLE IF EXISTS {diag_table};
            """).format(
                target_ident=target_table_ident,
                diag_table=first_diag_ad_ident,
                icu_table=first_icu_stays_ident
            )
            print("Executing SQL for target table:", create_target_table_sql.as_string(conn)) # 调试日志
            cur.execute(create_target_table_sql)

            conn.commit() # 提交事务

            cur.execute(psql.SQL("SELECT COUNT(*) FROM {}").format(target_table_ident))
            count = cur.fetchone()[0]
            QMessageBox.information(self, "创建成功",
                                   f"已成功创建目标病种表: {target_table_ident.strings[0]}.{target_table_ident.strings[1]}\n"
                                   f"包含 {count} 条记录")
            
            # 创建成功后，可能禁用创建按钮，防止重复创建（或提供覆盖选项）
            self.create_table_btn.setEnabled(False)

        except (Exception, psycopg2.Error) as error:
            if conn: conn.rollback() # 出错时回滚
            QMessageBox.critical(self, "创建失败", f"无法创建目标病种表: {error}")
        finally:
            if conn: conn.close()

    def get_disease_name(self):
        """获取用户输入的病种名称"""
        dialog = QDialog(self)
        dialog.setWindowTitle("输入病种队列标识符") # 修改标题
        layout = QVBoxLayout(dialog)

        form_layout = QFormLayout()
        name_input = QLineEdit()
        form_layout.addRow("队列标识符 (英文,数字,下划线):", name_input) # 修改标签
        layout.addLayout(form_layout)

        info_label = QLabel("注意: 此标识符将用于创建数据库表名 (例如: first_标识符_admissions)。\n"
                            "只能包含英文字母、数字和下划线，且必须以字母或下划线开头。")
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        btn_layout = QHBoxLayout()
        ok_btn = QPushButton("确定"); cancel_btn = QPushButton("取消")
        btn_layout.addWidget(ok_btn); btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        ok_btn.clicked.connect(dialog.accept)
        cancel_btn.clicked.connect(dialog.reject)

        result = dialog.exec_()
        return name_input.text().strip(), result == QDialog.Accepted

# --- END OF FILE tab_query_disease.py ---