# --- START OF FILE conditiongroup.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QComboBox, QLabel, QFrame, QGroupBox)
from PySide6.QtCore import Qt, Signal # Import Signal

class ConditionGroupWidget(QWidget):
    condition_changed = Signal() # Add a signal

    def __init__(self, is_root=False, search_field="long_title", parent=None):
        super().__init__(parent)
        self.is_root = is_root
        self.search_field = search_field
        self._block_signals = False # Flag to prevent recursive signals
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5) # Add some margins
        main_layout.setSpacing(5) # Add spacing

        # Use a QGroupBox for visual grouping if not root
        if not self.is_root:
            group_box = QGroupBox()
            self.layout = QVBoxLayout(group_box)
            main_layout.addWidget(group_box)
        else:
            self.layout = main_layout # Root uses the main layout directly

        logic_layout = QHBoxLayout()
        self.logic_combo = QComboBox()
        self.logic_combo.addItems(["AND", "OR"])
        self.logic_combo.currentTextChanged.connect(self._emit_condition_changed) # Connect signal
        logic_layout.addWidget(QLabel("组合方式:"))
        logic_layout.addWidget(self.logic_combo)
        logic_layout.addStretch() # Add stretch for better alignment

        self.add_keyword_btn = QPushButton("添加关键词")
        self.add_group_btn = QPushButton("添加子组")
        logic_layout.addWidget(self.add_keyword_btn)
        logic_layout.addWidget(self.add_group_btn)

        if not self.is_root:
            self.del_group_btn = QPushButton("删除本组")
            logic_layout.addWidget(self.del_group_btn)
            # Use lambda to ensure the correct object calls delete_self
            self.del_group_btn.clicked.connect(lambda: self.delete_self())
        self.layout.addLayout(logic_layout)

        separator = QFrame() # Add a visual separator
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        self.layout.addWidget(separator)

        self.items_layout = QVBoxLayout()
        self.layout.addLayout(self.items_layout)

        self.add_keyword_btn.clicked.connect(self.add_keyword)
        self.add_group_btn.clicked.connect(self.add_group)

        self.keywords = []
        self.child_groups = []

        # Add a default keyword only for the root group for initial usability
        if self.is_root:
            self.add_keyword()

    def add_keyword(self):
        kw_widget = QWidget() # Use a widget container for the layout
        kw_layout = QHBoxLayout(kw_widget)
        kw_layout.setContentsMargins(0, 0, 0, 0)

        # Add type selection (Include/Exclude)
        kw_type_combo = QComboBox()
        kw_type_combo.addItems(["包含", "排除"])
        kw_type_combo.currentTextChanged.connect(self._emit_condition_changed) # Connect signal

        kw_input = QLineEdit()
        kw_input.setPlaceholderText("输入关键词...")
        kw_input.textChanged.connect(self._emit_condition_changed) # Connect signal
        del_btn = QPushButton("删除")

        kw_layout.addWidget(QLabel("类型:"))
        kw_layout.addWidget(kw_type_combo)
        kw_layout.addWidget(QLabel("关键词:"))
        kw_layout.addWidget(kw_input)
        kw_layout.addWidget(del_btn)
        self.items_layout.addWidget(kw_widget) # Add the container widget
        # Store the type combo along with input and widget
        keyword_data = {"widget": kw_widget, "type_combo": kw_type_combo, "input": kw_input, "layout": kw_layout}
        self.keywords.append(keyword_data)
        # Use lambda to pass the correct keyword_data to remove_keyword
        del_btn.clicked.connect(lambda: self.remove_keyword(keyword_data))
        self._emit_condition_changed() # Emit signal after adding

    def remove_keyword(self, keyword_data):
        if keyword_data in self.keywords:
            # Proper widget removal
            keyword_data["widget"].deleteLater()
            self.items_layout.removeWidget(keyword_data["widget"])
            self.keywords.remove(keyword_data)
            self._emit_condition_changed() # Emit signal after removing

    def add_group(self):
        group = ConditionGroupWidget(is_root=False, search_field=self.search_field, parent=self)
        group.condition_changed.connect(self._emit_condition_changed) # Connect child signal
        self.child_groups.append(group)
        self.items_layout.addWidget(group)
        self._emit_condition_changed() # Emit signal after adding

    def remove_child_group(self, group):
         if group in self.child_groups:
            self.child_groups.remove(group)
            self._emit_condition_changed() # Emit signal after removing child

    def delete_self(self):
        parent_group = self.parent()
        # Ensure parent is a ConditionGroupWidget before calling remove_child_group
        if isinstance(parent_group, ConditionGroupWidget):
             parent_group.remove_child_group(self)
        self.deleteLater() # Proper Qt way to delete widgets

    def _emit_condition_changed(self):
        # Prevent signal loops if changes are programmatic
        if not self._block_signals:
             self.condition_changed.emit()

    def set_search_field(self, field_name):
        self._block_signals = True # Block signals during recursive update
        self.search_field = field_name
        # Recursively update search field in child groups
        for group in self.child_groups:
            group.set_search_field(field_name)
        self._block_signals = False # Unblock signals
        self._emit_condition_changed() # Emit signal after change

    def get_condition(self):
        # Use psycopg2.sql for safe identifier quoting if needed, here simple string formatting
        # Assumes search_field is a valid, safe column name
        conds = []
        for kw_data in self.keywords:
            kw = kw_data["input"].text().strip()
            kw_type = kw_data["type_combo"].currentText() # Get selected type
            if kw:
                # Basic SQL injection prevention: escape single quotes
                kw_escaped = kw.replace("'", "''")
                # Use ILIKE for case-insensitive matching
                if kw_type == "包含":
                    conds.append(f"{self.search_field} ILIKE '%{kw_escaped}%'")
                elif kw_type == "排除":
                    conds.append(f"{self.search_field} NOT ILIKE '%{kw_escaped}%'")


        for group in self.child_groups:
            group_cond = group.get_condition()
            if group_cond:
                conds.append(f"({group_cond})")

        if not conds:
            return "" # Return empty string if no conditions

        logic = f" {self.logic_combo.currentText()} "
        return logic.join(conds)

    def has_valid_input(self):
        """Checks if there is any valid input in this group or its children."""
        for kw_data in self.keywords:
            if kw_data["input"].text().strip():
                return True
        for group in self.child_groups:
            if group.has_valid_input():
                return True
        return False

# --- END OF FILE conditiongroup.py ---