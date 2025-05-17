# --- START OF MODIFIED ui_components/value_aggregation_widget.py ---
from PySide6.QtWidgets import QWidget, QGridLayout, QCheckBox, QPushButton, QVBoxLayout
from PySide6.QtCore import Signal, Qt
from app_config import AGGREGATION_METHODS_DISPLAY # 导入配置

class ValueAggregationWidget(QWidget):
    aggregation_changed = Signal()

    # 定义只适用于数值型数据的聚合方法内部键
    # 这些键应与 app_config.AGGREGATION_METHODS_DISPLAY 中的内部键一致
    NUMERIC_ONLY_METHODS = [
        "MEAN", "MEDIAN", "SUM", "STDDEV_SAMP", "VAR_SAMP",
        "CV", "P25", "P75", "IQR", "RANGE"
    ]
    # 对于 "COUNT" 方法，在文本模式下标签不同
    COUNT_METHOD_KEY = "COUNT" # 与 app_config 中的键一致


    def __init__(self, parent=None):
        super().__init__(parent)
        self.agg_checkboxes = {}  # 存储: {"INTERNAL_KEY": QCheckBox_instance}
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(5)

        # --- 全选/全不选按钮 ---
        select_buttons_layout = QVBoxLayout()
        self.select_all_btn = QPushButton("全选")
        self.select_all_btn.clicked.connect(self._select_all_methods)
        select_buttons_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("全不选")
        self.deselect_all_btn.clicked.connect(self._deselect_all_methods)
        select_buttons_layout.addWidget(self.deselect_all_btn)
        select_buttons_layout.addStretch()
        main_layout.addLayout(select_buttons_layout)

        # --- 动态创建聚合选项复选框 ---
        checkbox_layout = QGridLayout()
        
        row, col = 0, 0
        for display_name, internal_key in AGGREGATION_METHODS_DISPLAY:
            cb = QCheckBox(display_name)
            cb.stateChanged.connect(lambda state, cb_ref=cb: self.aggregation_changed.emit()) # 使用 lambda 忽略 state 参数 # 连接信号
            checkbox_layout.addWidget(cb, row, col, Qt.AlignmentFlag.AlignLeft)
            self.agg_checkboxes[internal_key] = cb # 使用内部键存储
            
            col += 1
            if col >= 4:  # 每行最多2个复选框，可根据需要调整
                col = 0
                row += 1
        
        main_layout.addLayout(checkbox_layout)
        self.setLayout(main_layout)

    def _select_all_methods(self):
        any_changed = False
        for cb in self.agg_checkboxes.values():
            if cb.isEnabled() and not cb.isChecked():
                cb.setChecked(True)
                any_changed = True # 实际只有在状态改变时才算数，但setChecked会触发stateChanged
        # if any_changed: # setChecked 会自动触发 stateChanged -> aggregation_changed
        #     self.aggregation_changed.emit()

    def _deselect_all_methods(self):
        any_changed = False
        for cb in self.agg_checkboxes.values():
            if cb.isChecked(): # 只对已选中的操作
                cb.setChecked(False)
                any_changed = True
        # if any_changed:
        #     self.aggregation_changed.emit()

    def get_selected_methods(self) -> dict:
        """
        返回一个字典，键是聚合方法的内部键 (如 "MEAN", "MEDIAN")，
        值是布尔类型 (True 表示选中, False 表示未选中)。
        """
        return {key: cb.isChecked() for key, cb in self.agg_checkboxes.items()}

    def set_selected_methods(self, methods_state: dict):
        """
        根据传入的字典设置复选框的选中状态。
        methods_state: 字典，键是内部聚合方法键，值是布尔。
        """
        any_changed = False
        for key, cb in self.agg_checkboxes.items():
            new_state = methods_state.get(key, False) # 如果键不存在，默认为False
            if cb.isChecked() != new_state:
                cb.blockSignals(True)
                cb.setChecked(new_state)
                cb.blockSignals(False)
                any_changed = True
        if any_changed:
            self.aggregation_changed.emit()

    def set_text_mode(self, is_text_mode: bool):
        """
        设置聚合选项是否为文本提取模式。
        is_text_mode: True 表示文本模式，False 表示数值模式。
        """
        emit_signal_due_to_state_change = False
        for internal_key, cb in self.agg_checkboxes.items():
            is_numeric_method = internal_key in self.NUMERIC_ONLY_METHODS
            
            if is_text_mode and is_numeric_method:
                cb.setEnabled(False)
                if cb.isChecked():
                    cb.blockSignals(True) # 避免重复触发信号
                    cb.setChecked(False)
                    cb.blockSignals(False)
                    emit_signal_due_to_state_change = True
            else:
                cb.setEnabled(True)

            # 特殊处理 COUNT 标签
            if internal_key == self.COUNT_METHOD_KEY:
                # 从 AGGREGATION_METHODS_DISPLAY 中找到 COUNT 的原始显示名
                original_display_name_for_count = "计数 (Count)" # 默认值
                for disp, key_in_config in AGGREGATION_METHODS_DISPLAY:
                    if key_in_config == self.COUNT_METHOD_KEY:
                        original_display_name_for_count = disp
                        break
                
                # 这里可以做得更通用，比如有一个 TEXT_MODE_LABELS 的字典
                if is_text_mode:
                    cb.setText("文本计数 (Count Text)") # 或者其他您希望的文本模式标签
                else:
                    cb.setText(original_display_name_for_count)
        
        if emit_signal_due_to_state_change:
            self.aggregation_changed.emit()

    def clear_selections(self):
        """清除所有复选框的选中状态。"""
        any_changed = False
        for cb in self.agg_checkboxes.values():
            if cb.isChecked():
                cb.blockSignals(True)
                cb.setChecked(False)
                cb.blockSignals(False)
                any_changed = True
        if any_changed:
            self.aggregation_changed.emit()

# --- END OF MODIFIED ui_components/value_aggregation_widget.py ---