from PySide6.QtWidgets import QWidget, QVBoxLayout, QFormLayout, QLineEdit, QPushButton, QHBoxLayout, QMessageBox
from PySide6.QtCore import Signal

import psycopg2

class ConnectionTab(QWidget):
    connected_signal = Signal()
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db_params = {}
        self.connected = False
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        self.db_name_input = QLineEdit("mimiciv")
        form_layout.addRow("数据库名称:", self.db_name_input)

        self.db_user_input = QLineEdit("postgres")
        form_layout.addRow("用户名:", self.db_user_input)

        self.db_password_input = QLineEdit()
        self.db_password_input.setEchoMode(QLineEdit.Password)
        form_layout.addRow("密码:", self.db_password_input)

        self.db_host_input = QLineEdit("localhost")
        form_layout.addRow("主机:", self.db_host_input)

        self.db_port_input = QLineEdit("5432")
        form_layout.addRow("端口:", self.db_port_input)

        layout.addLayout(form_layout)

        btn_layout = QHBoxLayout()
        self.test_connection_btn = QPushButton("连接测试")
        self.test_connection_btn.clicked.connect(self.test_connection)
        btn_layout.addWidget(self.test_connection_btn)

        self.connect_btn = QPushButton("数据库连接")
        self.connect_btn.clicked.connect(self.connect_database)
        btn_layout.addWidget(self.connect_btn)

        layout.addLayout(btn_layout)

    def test_connection(self):
        params = {
            'dbname': self.db_name_input.text(),
            'user': self.db_user_input.text(),
            'password': self.db_password_input.text(),
            'host': self.db_host_input.text(),
            'port': self.db_port_input.text()
        }
        try:
            conn = psycopg2.connect(**params)
            conn.close()
            self.db_params = params
            self.connected = True
            self.lock_inputs()
            self.connect_btn.setText("已连接")
            self.connect_btn.setEnabled(False)
            QMessageBox.information(self, "数据库已连接", "数据库参数已锁定，后续操作将使用此连接。")
            self.connected_signal.emit()   # <-- 连接成功后发信号
        except Exception as e:
            QMessageBox.critical(self, "连接失败", f"无法连接到数据库: {str(e)}")

    def connect_database(self):
        if self.connected:
            return
        self.db_params = {
            'dbname': self.db_name_input.text(),
            'user': self.db_user_input.text(),
            'password': self.db_password_input.text(),
            'host': self.db_host_input.text(),
            'port': self.db_port_input.text()
        }
        try:
            conn = psycopg2.connect(**self.db_params)
            conn.close()
            self.connected = True
            self.lock_inputs()
            self.connect_btn.setText("已连接")
            self.connect_btn.setEnabled(False)
            QMessageBox.information(self, "数据库已连接", "数据库参数已锁定，后续操作将使用此连接。")
            self.connected_signal.emit()   # <-- 连接成功后发信号
        except Exception as e:
            QMessageBox.critical(self, "连接失败", f"无法连接到数据库: {str(e)}")

    def lock_inputs(self):
        self.db_name_input.setEnabled(False)
        self.db_user_input.setEnabled(False)
        self.db_password_input.setEnabled(False)
        self.db_host_input.setEnabled(False)
        self.db_port_input.setEnabled(False)
        self.test_connection_btn.setEnabled(False)
