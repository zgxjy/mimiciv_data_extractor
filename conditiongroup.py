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
            self.layout = QVBoxLayout(group_box) # type: ignore
            main_layout.addWidget(group_box)
        else:
            self.layout = main_layout # type: ignore

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
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        self.layout.addWidget(separator)

        self.items_layout = QVBoxLayout()
        self.layout.addLayout(self.items_layout)

        self.add_keyword_btn.clicked.connect(self.add_keyword)
        self.add_group_btn.clicked.connect(self.add_group)

        self.keywords = [] # type: list[dict]
        self.child_groups = [] # type: list[ConditionGroupWidget]

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
            self.keywords.remove(keyword_data)
            self._emit_condition_changed()

    def add_group(self):
        group = ConditionGroupWidget(is_root=False, search_field=self.search_field, parent=self)
        group.condition_changed.connect(self._emit_condition_changed)
        self.child_groups.append(group)
        self.items_layout.addWidget(group)
        self._emit_condition_changed()

    def remove_child_group(self, group: 'ConditionGroupWidget'):
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
        cond_parts = []
        params = []

        for kw_data in self.keywords:
            kw_text = kw_data["input"].text().strip()
            kw_type = kw_data["type_combo"].currentText()
            if kw_text:
                param_value = f"%{kw_text}%"
                # Here, self.search_field is directly embedded. This is safe if self.search_field
                # is a programmatically controlled, known-safe column name.
                # If self.search_field could be arbitrary or user-influenced,
                # it would need to be escaped using psycopg2.sql.Identifier.
                # For this application's context, it's assumed to be safe.
                if kw_type == "包含":
                    cond_parts.append(f"{self.search_field} ILIKE %s")
                elif kw_type == "排除":
                    cond_parts.append(f"{self.search_field} NOT ILIKE %s")
                params.append(param_value)

        for group in self.child_groups:
            group_sql_template, group_params = group.get_condition()
            if group_sql_template:
                cond_parts.append(f"({group_sql_template})")
                params.extend(group_params)

        if not cond_parts:
            return "", []

        logic_operator = f" {self.logic_combo.currentText()} "
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