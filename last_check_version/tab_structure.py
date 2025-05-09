from PySide6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QTreeWidget, QTreeWidgetItem, QMessageBox

import psycopg2

class StructureTab(QWidget):
    def __init__(self, get_db_params_func, parent=None):
        super().__init__(parent)
        self.get_db_params = get_db_params_func
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        self.view_btn = QPushButton("查看")
        self.view_btn.setEnabled(False)
        self.view_btn.clicked.connect(self.view_db_structure)
        layout.addWidget(self.view_btn)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Schema", "Table"])
        layout.addWidget(self.tree)

    def set_btn_enabled(self, enabled):
        self.view_btn.setEnabled(enabled)

    def view_db_structure(self):
        self.tree.clear()
        db_params = self.get_db_params()
        if not db_params:
            QMessageBox.warning(self, "未连接", "请先连接数据库")
            return
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type='BASE TABLE'
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """)
            rows = cur.fetchall()
            conn.close()
            schema_dict = {}
            for schema, table in rows:
                if schema not in schema_dict:
                    schema_item = QTreeWidgetItem([schema])
                    self.tree.addTopLevelItem(schema_item)
                    schema_dict[schema] = schema_item
                table_item = QTreeWidgetItem([schema, table])
                schema_dict[schema].addChild(table_item)
            self.tree.expandAll()
        except Exception as e:
            QMessageBox.critical(self, "查询失败", f"无法获取数据库结构: {str(e)}")