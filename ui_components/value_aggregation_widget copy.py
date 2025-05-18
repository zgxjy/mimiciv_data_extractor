# --- START OF MODIFIED ui_components/value_aggregation_widget.py ---
from PySide6.QtWidgets import QWidget, QGridLayout, QCheckBox, QPushButton, QVBoxLayout, QHBoxLayout # 确保 QHBoxLayout 导入
from PySide6.QtCore import Signal, Qt, Slot # 确保 Slot 导入
from app_config import AGGREGATION_METHODS_DISPLAY # 导入配置

class ValueAggregationWidget(QWidget):
    aggregation_changed = Signal()

    NUMERIC_ONLY_METHODS = [
        "MEAN", "MEDIAN", "SUM", "STDDEV_SAMP", "VAR_SAMP",
        "CV", "P25", "P75", "IQR", "RANGE","MIN", "MAX"
    ]
    COUNT_METHOD_KEY = "COUNT"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.agg_checkboxes = {}
        self._block_aggregation_signal = False # 新增：用于在批量操作时临时阻塞信号发射
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(5)

        # --- 全选/全不选按钮 ---
        # 使用 QHBoxLayout 让按钮在一行显示
        select_buttons_layout = QHBoxLayout() # 改为 QHBoxLayout
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(self._select_all_methods)
        select_buttons_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(self._deselect_all_methods)
        select_buttons_layout.addWidget(self.deselect_all_btn)
        select_buttons_layout.addStretch()
        main_layout.addLayout(select_buttons_layout) # 添加按钮布局

        # --- 动态创建聚合选项复选框 ---
        checkbox_layout = QGridLayout()
        row, col = 0, 0
        for display_name, internal_key in AGGREGATION_METHODS_DISPLAY:
            cb = QCheckBox(display_name)
            # 修改连接，使其受 _block_aggregation_signal 控制
            cb.stateChanged.connect(self._emit_aggregation_changed_if_not_blocked)
            checkbox_layout.addWidget(cb, row, col, Qt.AlignmentFlag.AlignLeft)
            self.agg_checkboxes[internal_key] = cb
            
            col += 1
            if col >= 4:  # 你提到的是每行四个选项
                col = 0
                row += 1
        
        main_layout.addLayout(checkbox_layout)
        self.setLayout(main_layout)

    @Slot() # 这个槽函数用于接收来自 QCheckBox 的 stateChanged 信号
    def _emit_aggregation_changed_if_not_blocked(self):
        if not self._block_aggregation_signal:
            self.aggregation_changed.emit()

    def _select_all_methods(self):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for cb in self.agg_checkboxes.values():
                if cb.isEnabled() and not cb.isChecked():
                    cb.setChecked(True)
                    any_checkbox_state_actually_changed = True
        finally:
            self._block_aggregation_signal = False

        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit()

    def _deselect_all_methods(self):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for cb in self.agg_checkboxes.values():
                if cb.isChecked():
                    cb.setChecked(False)
                    any_checkbox_state_actually_changed = True
        finally:
            self._block_aggregation_signal = False

        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit()

    def get_selected_methods(self) -> dict:
        return {key: cb.isChecked() for key, cb in self.agg_checkboxes.items()}

    def set_selected_methods(self, methods_state: dict):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for key, cb in self.agg_checkboxes.items():
                new_state = methods_state.get(key, False)
                if cb.isChecked() != new_state:
                    # cb.blockSignals(True) # 不再需要，因为我们用 _block_aggregation_signal 控制
                    cb.setChecked(new_state)
                    # cb.blockSignals(False)
                    any_checkbox_state_actually_changed = True # 标记状态已改变
        finally:
            self._block_aggregation_signal = False
        
        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit() # 所有更改完成后发射一次信号

    def set_text_mode(self, is_text_mode: bool):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed_due_to_text_mode = False
        try:
            for internal_key, cb in self.agg_checkboxes.items():
                is_numeric_method = internal_key in self.NUMERIC_ONLY_METHODS
                
                original_checked_state = cb.isChecked() # 记录原始选中状态

                if is_text_mode and is_numeric_method:
                    cb.setEnabled(False)
                    if cb.isChecked(): 
                        cb.setChecked(False) # 这会触发 stateChanged -> _emit_... 但被阻塞
                else:
                    cb.setEnabled(True)

                # 检查 setChecked(False) 是否真的改变了状态
                if cb.isChecked() != original_checked_state: 
                    any_checkbox_state_actually_changed_due_to_text_mode = True

                if internal_key == self.COUNT_METHOD_KEY:
                    original_display_name_for_count = "计数 (Count)"
                    for disp, key_in_config in AGGREGATION_METHODS_DISPLAY:
                        if key_in_config == self.COUNT_METHOD_KEY:
                            original_display_name_for_count = disp
                            break
                    if is_text_mode:
                        cb.setText("文本计数 (Count Text)")
                    else:
                        cb.setText(original_display_name_for_count)
        finally:
            self._block_aggregation_signal = False
        
        if any_checkbox_state_actually_changed_due_to_text_mode:
            self.aggregation_changed.emit()

    def clear_selections(self):
        self._block_aggregation_signal = True
        any_checkbox_state_actually_changed = False
        try:
            for cb in self.agg_checkboxes.values():
                if cb.isChecked():
                    # cb.blockSignals(True) # 不再需要
                    cb.setChecked(False)
                    # cb.blockSignals(False)
                    any_checkbox_state_actually_changed = True
        finally:
            self._block_aggregation_signal = False
        
        if any_checkbox_state_actually_changed:
            self.aggregation_changed.emit()

# --- END OF MODIFIED ui_components/value_aggregation_widget.py ---