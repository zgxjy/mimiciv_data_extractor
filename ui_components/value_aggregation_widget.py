# --- START OF MODIFIED ui_components/value_aggregation_widget.py ---
from PySide6.QtWidgets import QWidget, QGridLayout, QCheckBox, QGroupBox, QHBoxLayout, QPushButton, QVBoxLayout # 新增 QVBoxLayout
from PySide6.QtCore import Signal, Slot, Qt # 新增 Qt

class ValueAggregationWidget(QWidget):
    aggregation_changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        # 主布局现在是 QVBoxLayout，先放按钮行，再放复选框的 GridLayout
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0,0,0,0) # 通常组件内部不设置外边距，由使用它的地方控制
        main_layout.setSpacing(5) 

        # --- 全选/全不选按钮 ---
        select_buttons_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(self._select_all_methods)
        select_buttons_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(self._deselect_all_methods)
        select_buttons_layout.addWidget(self.deselect_all_btn)
        select_buttons_layout.addStretch() # 让按钮靠左
        main_layout.addLayout(select_buttons_layout)

        # --- 聚合选项复选框 ---
        checkbox_layout = QGridLayout() 
        # checkbox_layout.setContentsMargins(0,0,0,0) # GridLayout本身不需要

        self.cb_first = QCheckBox("首次 (First)")
        self.cb_last = QCheckBox("末次 (Last)")
        self.cb_min = QCheckBox("最小值 (Min)")
        self.cb_max = QCheckBox("最大值 (Max)")
        self.cb_mean = QCheckBox("平均值 (Mean)")
        self.cb_count_val = QCheckBox("有效值计数 (CountVal)")

        self.agg_checkboxes = {
            "first": self.cb_first, "last": self.cb_last,
            "min": self.cb_min, "max": self.cb_max,
            "mean": self.cb_mean, "countval": self.cb_count_val
        }

        # 两列布局复选框
        checkbox_layout.addWidget(self.cb_first, 0, 0, Qt.AlignmentFlag.AlignLeft)
        checkbox_layout.addWidget(self.cb_last, 0, 1, Qt.AlignmentFlag.AlignLeft)
        checkbox_layout.addWidget(self.cb_min, 1, 0, Qt.AlignmentFlag.AlignLeft)
        checkbox_layout.addWidget(self.cb_max, 1, 1, Qt.AlignmentFlag.AlignLeft)
        checkbox_layout.addWidget(self.cb_mean, 2, 0, Qt.AlignmentFlag.AlignLeft)
        checkbox_layout.addWidget(self.cb_count_val, 2, 1, Qt.AlignmentFlag.AlignLeft)
        
        # # 如果希望复选框占据可用宽度，可以给GridLayout的列设置伸展因子
        # checkbox_layout.setColumnStretch(0, 1)
        # checkbox_layout.setColumnStretch(1, 1)
        # 或者让复选框本身横向扩展 (如果它们的 sizePolicy 允许)

        for cb in self.agg_checkboxes.values():
            cb.stateChanged.connect(lambda state: self.aggregation_changed.emit())
        
        main_layout.addLayout(checkbox_layout) 
        self.setLayout(main_layout)

    # ... (其他方法 _select_all_methods, _deselect_all_methods, get_selected_methods, etc. 保持不变) ...
    def _select_all_methods(self):
        for cb in self.agg_checkboxes.values():
            if cb.isEnabled(): 
                cb.setChecked(True)
    def _deselect_all_methods(self):
        for cb in self.agg_checkboxes.values():
            cb.setChecked(False)
    def get_selected_methods(self) -> dict:
        return {key: cb.isChecked() for key, cb in self.agg_checkboxes.items()}
    def set_selected_methods(self, methods_state: dict):
        any_changed = False
        for key, cb in self.agg_checkboxes.items():
            new_state = methods_state.get(key, False)
            if cb.isChecked() != new_state:
                cb.blockSignals(True); cb.setChecked(new_state); cb.blockSignals(False)
                any_changed = True
        if any_changed: self.aggregation_changed.emit()
    def set_text_mode(self, is_text_mode: bool):
        self.cb_min.setEnabled(not is_text_mode); self.cb_max.setEnabled(not is_text_mode); self.cb_mean.setEnabled(not is_text_mode)
        if is_text_mode:
            changed = False
            if self.cb_min.isChecked(): self.cb_min.setChecked(False); changed = True
            if self.cb_max.isChecked(): self.cb_max.setChecked(False); changed = True
            if self.cb_mean.isChecked(): self.cb_mean.setChecked(False); changed = True
            self.cb_count_val.setText("文本计数 (CountText)")
            if changed: self.aggregation_changed.emit()
        else: self.cb_count_val.setText("有效值计数 (CountVal)")
    def clear_selections(self):
        any_changed = False
        for cb in self.agg_checkboxes.values():
            if cb.isChecked():
                cb.blockSignals(True); cb.setChecked(False); cb.blockSignals(False)
                any_changed = True
        if any_changed: self.aggregation_changed.emit()

# --- END OF MODIFIED ui_components/value_aggregation_widget.py ---