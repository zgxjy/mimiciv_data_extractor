# --- START OF PROPOSED MODIFICATION FOR conditiongroup.py ---

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QComboBox, QLabel, QFrame, QGroupBox)
from PySide6.QtCore import Qt, Signal

class ConditionGroupWidget(QWidget):
    condition_changed = Signal()

    def __init__(self, is_root=False, parent=None): # 移除 search_field 参数
        super().__init__(parent)
        self.is_root = is_root
        # self.search_field 不再是顶层属性，而是每行关键词自己的属性
        self._block_signals = False
        self._available_search_fields = [] # 存储 (db_col_name, display_name)
        self.init_ui()
        if self.is_root: # 确保根节点有一个默认的关键词行
             self.add_keyword()


    def init_ui(self):
        # ... (大部分 init_ui 保持不变，除了不再需要单一的 search_field 设置) ...
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

        self.add_keyword_btn = QPushButton("添加关键词条件") # 文本稍作修改
        self.add_group_btn = QPushButton("添加子组条件")   # 文本稍作修改
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

        self.items_layout = QVBoxLayout() # 用于放置关键词行和子组
        self.layout.addLayout(self.items_layout)

        self.add_keyword_btn.clicked.connect(self.add_keyword)
        self.add_group_btn.clicked.connect(self.add_group)

        self.keywords = [] # 存储关键词行UI和数据的字典列表
        self.child_groups = [] # 存储子 ConditionGroupWidget 实例


    def set_available_search_fields(self, fields: list[tuple[str, str]]):
        """
        设置此条件组及其子组中所有关键词行可选的搜索字段。
        fields: 一个元组列表，每个元组是 (database_column_name, display_name_for_combobox)
        """
        self._available_search_fields = fields
        # 更新现有关键词行的字段选择器
        for kw_data in self.keywords:
            field_combo = kw_data.get("field_combo")
            if field_combo:
                current_field_db_name = field_combo.currentData() # 保存当前选中的db列名
                field_combo.blockSignals(True)
                field_combo.clear()
                for db_col, display_name in self._available_search_fields:
                    field_combo.addItem(display_name, db_col)
                
                # 尝试恢复之前的选择
                idx_to_select = -1
                if current_field_db_name:
                    for i in range(field_combo.count()):
                        if field_combo.itemData(i) == current_field_db_name:
                            idx_to_select = i
                            break
                if idx_to_select != -1:
                    field_combo.setCurrentIndex(idx_to_select)
                elif field_combo.count() > 0 : # 默认选第一个
                    field_combo.setCurrentIndex(0)
                field_combo.blockSignals(False)
        
        # 递归更新子组
        for child_group in self.child_groups:
            child_group.set_available_search_fields(fields)
        self._emit_condition_changed() # 字段列表变化也算条件变化


    def add_keyword(self, field_db_name=None, keyword_type="包含", keyword_text=""): # 新增 field_db_name 参数
        kw_widget = QWidget()
        kw_layout = QHBoxLayout(kw_widget)
        kw_layout.setContentsMargins(0, 0, 0, 0)

        # 新增：字段选择器 QComboBox
        field_combo = QComboBox()
        current_selection_restored = False
        if self._available_search_fields:
            for db_col, display_name in self._available_search_fields:
                field_combo.addItem(display_name, db_col)
            if field_db_name: # 如果是加载状态，尝试选中指定的字段
                for i in range(field_combo.count()):
                    if field_combo.itemData(i) == field_db_name:
                        field_combo.setCurrentIndex(i)
                        current_selection_restored = True
                        break
            if not current_selection_restored and field_combo.count() > 0:
                field_combo.setCurrentIndex(0) # 默认选中第一个可用字段
        else:
            field_combo.addItem("无可用字段", None) # 占位符
            field_combo.setEnabled(False)
        field_combo.currentTextChanged.connect(self._emit_condition_changed)


        kw_type_combo = QComboBox()
        kw_type_combo.addItems(["包含", "排除", "等于", "不等于", "大于", "小于", "大于等于", "小于等于"]) # 扩展操作符
        kw_type_combo.setCurrentText(keyword_type)
        kw_type_combo.currentTextChanged.connect(self._emit_condition_changed)

        kw_input = QLineEdit()
        kw_input.setPlaceholderText("输入关键词/值...")
        kw_input.setText(keyword_text)
        kw_input.textChanged.connect(self._emit_condition_changed)
        
        del_btn = QPushButton("删除")

        kw_layout.addWidget(QLabel("字段:"))
        kw_layout.addWidget(field_combo)
        kw_layout.addWidget(QLabel("类型:"))
        kw_layout.addWidget(kw_type_combo)
        kw_layout.addWidget(QLabel("值:")) # "关键词" 改为 "值" 更通用
        kw_layout.addWidget(kw_input)
        kw_layout.addWidget(del_btn)
        
        self.items_layout.addWidget(kw_widget)
        keyword_data = {
            "widget": kw_widget, 
            "field_combo": field_combo, # 新增
            "type_combo": kw_type_combo, 
            "input": kw_input, 
            "layout": kw_layout
        }
        self.keywords.append(keyword_data)
        del_btn.clicked.connect(lambda: self.remove_keyword(keyword_data))
        self._emit_condition_changed()
        return keyword_data

    # remove_keyword, delete_self, _emit_condition_changed 保持不变
    def remove_keyword(self, keyword_data: dict):
        """
        Removes a keyword row from the UI and internal list.
        keyword_data: The dictionaryแสงข้อมูลของคำสำคัญที่จะถูกลบ
        """
        if keyword_data in self.keywords:
            # 从布局中移除 widget
            if keyword_data["widget"] is not None:
                # 如果 items_layout 仍然有效且 widget 在其中
                if self.items_layout and keyword_data["widget"].parentWidget() is not None:
                     # self.items_layout.removeWidget(keyword_data["widget"]) # 这只是从布局中移除，不删除
                    pass # deleteLater 会处理好从父布局移除
                keyword_data["widget"].deleteLater() # 安全地删除 widget
            
            self.keywords.remove(keyword_data)
            self._emit_condition_changed()
    
    def set_search_field(self, field_name):
        self._block_signals = True
        self.search_field = field_name
        for group in self.child_groups:
            group.set_search_field(field_name)
        self._block_signals = False
        self._emit_condition_changed()

    def add_group(self, group_data=None): # group_data 是用于加载状态的
        # 子组将继承父组的 _available_search_fields (通过 set_available_search_fields 传递)
        group = ConditionGroupWidget(is_root=False, parent=self)
        group.set_available_search_fields(self._available_search_fields) # 传递可用字段
        group.condition_changed.connect(self._emit_condition_changed)
        self.child_groups.append(group)
        self.items_layout.addWidget(group)
        if group_data:
            group.set_state(group_data) # 假设有 set_state 方法
        self._emit_condition_changed()
        return group

    def remove_child_group(self, group: 'ConditionGroupWidget'):
         if group in self.child_groups:
            self.child_groups.remove(group)
            self._emit_condition_changed()

    def delete_self(self):
        parent_group = self.parent()
        if isinstance(parent_group, ConditionGroupWidget):
             parent_group.remove_child_group(self)
        self.deleteLater()

    def _emit_condition_changed(self):
        if not self._block_signals:
             self.condition_changed.emit()

    def get_condition(self):
        cond_parts = []
        params = []

        for kw_data in self.keywords:
            search_field_for_kw = kw_data["field_combo"].currentData() # 获取当前行选择的字段
            kw_text = kw_data["input"].text().strip()
            kw_operator_text = kw_data["type_combo"].currentText() # "包含", "等于" 等

            if search_field_for_kw and kw_text: # 必须选择了字段且输入了值
                # 根据操作符构建SQL和参数
                # 注意：search_field_for_kw 仍然直接嵌入，需要确保它是安全的列名
                # 如果 search_field_for_kw 需要是标识符，外部传入时就应处理好
                # 或者在这里使用 psycopg2.sql.Identifier(search_field_for_kw) 但会改变模板格式
                
                # 简单的类型判断（非常粗略，实际应用可能需要更精确的元数据）
                is_numeric_field = "id" in search_field_for_kw.lower() or "count" in search_field_for_kw.lower() or "age" in search_field_for_kw.lower()
                
                # CAST to TEXT for ILIKE if field is not text type, to avoid errors
                # This is a simplification. Ideally, know the column type.
                field_expression = pgsql.Identifier(search_field_for_kw).as_string(None) # Get as "field_name"
                
                if kw_operator_text == "包含":
                    cond_parts.append(f"CAST({field_expression} AS TEXT) ILIKE %s")
                    params.append(f"%{kw_text}%")
                elif kw_operator_text == "排除":
                    cond_parts.append(f"CAST({field_expression} AS TEXT) NOT ILIKE %s")
                    params.append(f"%{kw_text}%")
                elif kw_operator_text == "等于":
                    cond_parts.append(f"{field_expression} = %s")
                    params.append(float(kw_text) if is_numeric_field and kw_text.replace('.','',1).isdigit() else kw_text)
                elif kw_operator_text == "不等于":
                    cond_parts.append(f"{field_expression} != %s")
                    params.append(float(kw_text) if is_numeric_field and kw_text.replace('.','',1).isdigit() else kw_text)
                elif kw_operator_text == "大于":
                    cond_parts.append(f"{field_expression} > %s")
                    params.append(float(kw_text) if is_numeric_field and kw_text.replace('.','',1).isdigit() else kw_text)
                elif kw_operator_text == "小于":
                    cond_parts.append(f"{field_expression} < %s")
                    params.append(float(kw_text) if is_numeric_field and kw_text.replace('.','',1).isdigit() else kw_text)
                elif kw_operator_text == "大于等于":
                    cond_parts.append(f"{field_expression} >= %s")
                    params.append(float(kw_text) if is_numeric_field and kw_text.replace('.','',1).isdigit() else kw_text)
                elif kw_operator_text == "小于等于":
                    cond_parts.append(f"{field_expression} <= %s")
                    params.append(float(kw_text) if is_numeric_field and kw_text.replace('.','',1).isdigit() else kw_text)
                # 可以添加对日期、布尔值等的处理

        for group in self.child_groups:
            group_sql_template, group_params = group.get_condition()
            if group_sql_template:
                cond_parts.append(f"({group_sql_template})")
                params.extend(group_params)

        if not cond_parts:
            return "", []

        logic_operator = f" {self.logic_combo.currentText()} " # " AND " or " OR "
        full_sql_template = logic_operator.join(cond_parts)

        return full_sql_template, params


    def has_valid_input(self): # 需要更新，检查每个关键词行是否选择了字段
        for kw_data in self.keywords:
            if kw_data["field_combo"].currentData() and kw_data["input"].text().strip():
                return True
        for group in self.child_groups:
            if group.has_valid_input():
                return True
        return False

    def get_state(self) -> dict:
        state = {
            "logic": self.logic_combo.currentText(),
            # "search_field" is no longer a group-level property for keywords
            "keywords": [],
            "child_groups": []
        }
        for kw_data in self.keywords:
            state["keywords"].append({
                "field_db_name": kw_data["field_combo"].currentData(), # 保存选中字段的数据库名
                "type": kw_data["type_combo"].currentText(),
                "text": kw_data["input"].text()
            })
        for child_group in self.child_groups:
            state["child_groups"].append(child_group.get_state())
        return state

    def set_state(self, state: dict, available_fields_for_state: list[tuple[str,str]] = None):
        self._block_signals = True
        # 如果外部传入了可用字段列表（例如，在加载整个配置时），则优先使用它
        if available_fields_for_state is not None:
            self.set_available_search_fields(available_fields_for_state)
        
        try:
            self.logic_combo.setCurrentText(state.get("logic", "AND"))

            # 清理现有UI元素
            for kw_data in reversed(self.keywords): self.remove_keyword(kw_data)
            for child_group in reversed(self.child_groups): child_group.delete_self()
            self.keywords.clear(); self.child_groups.clear()

            for kw_state in state.get("keywords", []):
                self.add_keyword(
                    field_db_name=kw_state.get("field_db_name"),
                    keyword_type=kw_state.get("type", "包含"),
                    keyword_text=kw_state.get("text", "")
                )
            
            for child_group_state in state.get("child_groups", []):
                new_child = self.add_group() # add_group 会自动传递 _available_search_fields
                new_child.set_state(child_group_state) # 递归设置子组状态 (这里子组也会用父组刚设置的fields)

        except Exception as e:
            print(f"Error setting ConditionGroupWidget state: {e}")
        finally:
            self._block_signals = False
        self._emit_condition_changed()

    def clear_all(self):
        self._block_signals = True
        try:
            for kw_data in reversed(self.keywords): self.remove_keyword(kw_data)
            for child_group in reversed(self.child_groups): child_group.delete_self()
            self.keywords.clear(); self.child_groups.clear()
            if self.is_root: self.add_keyword()
            self.logic_combo.setCurrentIndex(0)
        finally:
            self._block_signals = False
        self._emit_condition_changed()

# --- END OF PROPOSED MODIFICATION FOR conditiongroup.py ---