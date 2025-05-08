# --- START OF FILE conditiongroup.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QComboBox, QLabel, QFrame, QGroupBox)
from PySide6.QtCore import Qt, Signal
# 确保导入了 psycopg2.sql，即使在这里不直接用，调用者可能会用
# from psycopg2 import sql as psql # 或者在调用模块中处理

class ConditionGroupWidget(QWidget):
    condition_changed = Signal()

    def __init__(self, is_root=False, search_field="long_title", parent=None):
        super().__init__(parent)
        self.is_root = is_root
        self.search_field = search_field # search_field 仍然假设是安全的列名
        self._block_signals = False
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        main_layout.setSpacing(5)

        if not self.is_root:
            group_box = QGroupBox()
            self.layout = QVBoxLayout(group_box)
            main_layout.addWidget(group_box)
        else:
            self.layout = main_layout

        logic_layout = QHBoxLayout()
        self.logic_combo = QComboBox()
        self.logic_combo.addItems(["AND", "OR"])
        self.logic_combo.currentTextChanged.connect(self._emit_condition_changed)
        logic_layout.addWidget(QLabel("组合方式:"))
        logic_layout.addWidget(self.logic_combo)
        logic_layout.addStretch()

        self.add_keyword_btn = QPushButton("添加关键词")
        self.add_group_btn = QPushButton("添加子组")
        logic_layout.addWidget(self.add_keyword_btn)
        logic_layout.addWidget(self.add_group_btn)

        if not self.is_root:
            self.del_group_btn = QPushButton("删除本组")
            logic_layout.addWidget(self.del_group_btn)
            self.del_group_btn.clicked.connect(lambda: self.delete_self())
        self.layout.addLayout(logic_layout)

        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        self.layout.addWidget(separator)

        self.items_layout = QVBoxLayout()
        self.layout.addLayout(self.items_layout)

        self.add_keyword_btn.clicked.connect(self.add_keyword)
        self.add_group_btn.clicked.connect(self.add_group)

        self.keywords = []
        self.child_groups = []

        if self.is_root:
            self.add_keyword()

    def add_keyword(self):
        kw_widget = QWidget()
        kw_layout = QHBoxLayout(kw_widget)
        kw_layout.setContentsMargins(0, 0, 0, 0)

        kw_type_combo = QComboBox()
        kw_type_combo.addItems(["包含", "排除"])
        kw_type_combo.currentTextChanged.connect(self._emit_condition_changed)

        kw_input = QLineEdit()
        kw_input.setPlaceholderText("输入关键词...")
        kw_input.textChanged.connect(self._emit_condition_changed)
        del_btn = QPushButton("删除")

        kw_layout.addWidget(QLabel("类型:"))
        kw_layout.addWidget(kw_type_combo)
        kw_layout.addWidget(QLabel("关键词:"))
        kw_layout.addWidget(kw_input)
        kw_layout.addWidget(del_btn)
        self.items_layout.addWidget(kw_widget)
        keyword_data = {"widget": kw_widget, "type_combo": kw_type_combo, "input": kw_input, "layout": kw_layout}
        self.keywords.append(keyword_data)
        del_btn.clicked.connect(lambda: self.remove_keyword(keyword_data))
        self._emit_condition_changed()

    def remove_keyword(self, keyword_data):
        if keyword_data in self.keywords:
            keyword_data["widget"].deleteLater()
            # self.items_layout.removeWidget(keyword_data["widget"]) # QWidget.deleteLater() 应该会处理布局移除
            self.keywords.remove(keyword_data)
            self._emit_condition_changed()

    def add_group(self):
        group = ConditionGroupWidget(is_root=False, search_field=self.search_field, parent=self)
        group.condition_changed.connect(self._emit_condition_changed)
        self.child_groups.append(group)
        self.items_layout.addWidget(group)
        self._emit_condition_changed()

    def remove_child_group(self, group):
         if group in self.child_groups:
            self.child_groups.remove(group)
            # group.deleteLater() # 子组自己会调用 delete_self，其中包含 deleteLater
            self._emit_condition_changed()

    def delete_self(self):
        parent_group = self.parent()
        if isinstance(parent_group, ConditionGroupWidget):
             parent_group.remove_child_group(self)
        self.deleteLater()

    def _emit_condition_changed(self):
        if not self._block_signals:
             self.condition_changed.emit()

    def set_search_field(self, field_name):
        self._block_signals = True
        self.search_field = field_name
        for group in self.child_groups:
            group.set_search_field(field_name)
        self._block_signals = False
        self._emit_condition_changed()

    def get_condition(self):
        """
        Generates a parameterized SQL condition.
        Returns:
            tuple: (sql_template_string, params_list)
                   sql_template_string: SQL WHERE clause with %s placeholders.
                   params_list: List of parameters for the placeholders.
                   Returns ("", []) if no valid conditions.
        """
        # self.search_field 仍然假设是一个程序控制的、安全的列名
        # 例如 "long_title", "label", "drug"
        # 如果 search_field 也需要动态和安全处理，需要 psycopg2.sql.Identifier(self.search_field)
        # 但这里我们简化，假设它是安全的。

        cond_parts = []  # List of SQL template parts
        params = []      # List of parameters

        for kw_data in self.keywords:
            kw_text = kw_data["input"].text().strip()
            kw_type = kw_data["type_combo"].currentText()
            if kw_text:
                # search_field 必须是安全的，不能来自用户输入
                # 如果 search_field 可能包含特殊字符或SQL关键字，应该用 Identifier 包装
                # sql_field_part = psql.Identifier(self.search_field).as_string(cursor_or_conn_obj)
                # 但在这里，我们假设 self.search_field 是一个简单的、有效的列名
                
                # 构造 ILIKE 的参数时，SQL 通配符 '%' 应该包含在参数值中
                param_value = f"%{kw_text}%" 
                
                if kw_type == "包含":
                    # 注意：这里直接嵌入 self.search_field 是因为它是程序定义的，不是用户输入
                    cond_parts.append(f"{self.search_field} ILIKE %s")
                elif kw_type == "排除":
                    cond_parts.append(f"{self.search_field} NOT ILIKE %s")
                
                params.append(param_value)

        for group in self.child_groups:
            group_sql_template, group_params = group.get_condition()
            if group_sql_template: # 检查是否有有效的子条件
                cond_parts.append(f"({group_sql_template})")
                params.extend(group_params)

        if not cond_parts:
            return "", [] # 没有条件

        logic_operator = f" {self.logic_combo.currentText()} " # " AND " 或 " OR "
        
        # 将所有部分用逻辑操作符连接起来
        full_sql_template = logic_operator.join(cond_parts)
        
        return full_sql_template, params

    def has_valid_input(self):
        for kw_data in self.keywords:
            if kw_data["input"].text().strip():
                return True
        for group in self.child_groups:
            if group.has_valid_input():
                return True
        return False

# --- END OF FILE conditiongroup.py ---