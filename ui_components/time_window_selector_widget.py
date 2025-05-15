# --- START OF FILE ui_components/time_window_selector_widget.py ---
from PySide6.QtWidgets import QWidget, QHBoxLayout, QLabel, QComboBox
from PySide6.QtCore import Signal, Slot

class TimeWindowSelectorWidget(QWidget):
    time_window_changed = Signal(str) # 发出选中的时间窗口文本

    def __init__(self, label_text="时间窗口:", parent=None):
        super().__init__(parent)
        self._options_with_data = [] # 存储 (display_text, data_value)
        self.init_ui(label_text)

    def init_ui(self, label_text):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0,0,0,0)

        self.label = QLabel(label_text)
        layout.addWidget(self.label)

        self.combo_box = QComboBox()
        self.combo_box.currentTextChanged.connect(self.time_window_changed.emit)
        # 或者用 currentIndexChanged，然后 emit self.combo_box.currentData()
        # currentTextChanged 更直接获取文本
        layout.addWidget(self.combo_box)
        layout.addStretch()
        
        self.setLayout(layout)

    def set_options(self, options: list[str] | list[tuple[str, any]]):
        """
        设置下拉框的选项。
        options: 可以是字符串列表，或者 (显示文本, 关联数据) 的元组列表。
        """
        self.combo_box.blockSignals(True)
        self.combo_box.clear()
        self._options_with_data = []

        if not options:
            self.combo_box.addItem("无可用时间窗口", None)
            self.combo_box.setEnabled(False)
            self.combo_box.blockSignals(False)
            return

        self.combo_box.setEnabled(True)
        for option in options:
            if isinstance(option, tuple) and len(option) == 2:
                display_text, data_value = option
                self.combo_box.addItem(display_text, data_value)
                self._options_with_data.append((display_text, data_value))
            else: # 假定是字符串列表
                self.combo_box.addItem(str(option), str(option)) # 数据和显示文本相同
                self._options_with_data.append((str(option), str(option)))
        
        if self.combo_box.count() > 0:
            self.combo_box.setCurrentIndex(0) # 默认选中第一个
            
        self.combo_box.blockSignals(False)
        if self.combo_box.count() > 0 : # 手动触发一次初始信号
             self.time_window_changed.emit(self.combo_box.currentText())


    def get_current_time_window_text(self) -> str:
        return self.combo_box.currentText()

    def get_current_time_window_data(self) -> any:
        return self.combo_box.currentData()

    def set_current_time_window_by_text(self, text: str):
        for i in range(self.combo_box.count()):
            if self.combo_box.itemText(i) == text:
                self.combo_box.setCurrentIndex(i)
                return
        print(f"警告: 未在时间窗口选项中找到文本 '{text}'")

    def set_current_time_window_by_data(self, data_value: any):
        for i in range(self.combo_box.count()):
            if self.combo_box.itemData(i) == data_value:
                self.combo_box.setCurrentIndex(i)
                return
        print(f"警告: 未在时间窗口选项中找到数据值 '{data_value}'")
        
    def clear_selection(self):
        if self.combo_box.count() > 0:
            self.combo_box.setCurrentIndex(0) # 或者 -1 如果允许不选
        # 或者 self.combo_box.clear() 如果是想清空选项


# --- END OF FILE ui_components/time_window_selector_widget.py ---