# --- START OF FILE source_panels/base_panel.py ---
from PySide6.QtWidgets import QWidget, QMessageBox
from PySide6.QtCore import Signal
import psycopg2

class BaseSourceConfigPanel(QWidget):
    config_changed_signal = Signal() # 当面板内部配置变化，可能影响主Tab按钮状态时发出

    def __init__(self, db_params_getter, parent=None):
        super().__init__(parent)
        self.get_db_params = db_params_getter
        self._db_conn = None
        self._db_cursor = None
        self.init_panel_ui()

    def init_panel_ui(self):
        """子类将在这里构建其特定的UI。"""
        # 默认实现为空，子类通常会重写
        pass

    def _connect_panel_db(self):
        if self._db_conn and self._db_conn.closed == 0:
            try:
                if not self._db_cursor or self._db_cursor.closed:
                    self._db_cursor = self._db_conn.cursor()
                # 尝试一个轻量级操作来检查连接是否仍然有效
                self._db_conn.isolation_level # 或者 self._db_cursor.execute("SELECT 1")
                return True
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                self._db_conn = None; self._db_cursor = None
        
        db_params = self.get_db_params()
        if not db_params: 
            # QMessageBox.warning(self, "数据库未连接", "无法获取数据库连接参数。") # 面板内部不宜直接弹窗
            return False
        try:
            self._db_conn = psycopg2.connect(**db_params)
            self._db_cursor = self._db_conn.cursor()
            return True
        except Exception as e:
            print(f"Error connecting panel DB: {e}")
            self._db_conn = None; self._db_cursor = None
            return False

    def _close_panel_db(self):
        if self._db_cursor: 
            try: self._db_cursor.close() 
            except Exception as e: print(f"Error closing panel cursor: {e}")
        if self._db_conn: 
            try: self._db_conn.close()
            except Exception as e: print(f"Error closing panel connection: {e}")
        self._db_cursor = None; self._db_conn = None

    def populate_panel_if_needed(self):
        """
        当面板被显示时调用。子类可以重写此方法
        以执行任何必要的初始化或数据加载（例如，如果筛选项目列表需要预加载）。
        """
        pass

    def get_panel_config(self) -> dict:
        """
        子类必须实现此方法，返回一个包含该面板特定配置的字典。
        这个字典将包含构建SQL查询所需的所有特定于源的信息。
        """
        raise NotImplementedError("Subclasses must implement get_panel_config")

    def get_item_filtering_details(self) -> tuple:
        """
        返回用于 ConditionGroupWidget 和项目筛选的配置。
        格式: (dict_table_name_str or None, 
               name_col_in_dict_or_event_str, 
               id_col_in_dict_or_event_str, 
               friendly_hint_for_search_field_str,
               event_table_name_str_if_no_dict) 
        如果 dict_table_name_str 为 None，则表示直接从事件表筛选。
        """
        raise NotImplementedError("Subclasses must implement get_item_filtering_details")

    def clear_panel_state(self):
        """子类应重写此方法以清除其UI元素的状态并重置配置。"""
        raise NotImplementedError("Subclasses must implement clear_panel_state")

    def update_panel_action_buttons_state(self, general_config_ok: bool):
        """
        由主Tab调用，用于更新面板内按钮（如“筛选项目”）的状态。
        `general_config_ok` 表示主Tab的通用配置（如队列表）是否有效。
        """
        # 默认实现：如果通用配置OK，则启用筛选按钮（如果存在）
        if hasattr(self, 'filter_items_btn'):
            self.filter_items_btn.setEnabled(general_config_ok)


    def get_friendly_source_name(self) -> str:
        """返回一个用户友好的数据源名称，用于日志等。"""
        raise NotImplementedError("Subclasses must implement get_friendly_source_name")
    
    def get_value_column_for_aggregation(self) -> str | None:
        """返回此数据源用于数值聚合的列名 (例如 "valuenum")，如果不支持则返回 None。"""
        return None # 默认为不支持

    def get_time_column_for_windowing(self) -> str | None:
        """返回此数据源用于时间窗口的列名 (例如 "charttime")，如果不支持则返回 None。"""
        return None # 默认为不支持
        
    def get_aggregation_config_widget(self) -> QWidget | None:
        """返回包含此数据源聚合选项的QWidget，如果该源类型使用通用聚合逻辑，则返回None。"""
        return None 

    def get_time_window_options(self) -> list | None:
        """返回此数据源可用的时间窗口选项，如果该源类型使用通用时间窗口，则返回None。"""
        return None
        
    def get_specific_aggregation_methods(self) -> dict | None:
        """
        如果此面板有自己独特的聚合方法选择UI (覆盖了主Tab的通用选项)，
        则返回一个字典，键是方法标识符(如 "first", "min")，值是布尔值(是否选中)。
        否则返回None。
        """
        return None

    def get_specific_time_window_text(self) -> str | None:
        """
        如果此面板有自己独特的时间窗口选择UI，返回当前选中的时间窗口文本。
        否则返回None。
        """
        return None

    def get_selected_item_ids(self) -> list:
        """返回当前在 item_list 中选中的项目ID列表。"""
        if hasattr(self, 'item_list'):
            ids = []
            for i in range(self.item_list.count()): # 迭代所有项检查是否选中
                list_view_item = self.item_list.item(i)
                if list_view_item.isSelected():
                    data = list_view_item.data(Qt.ItemDataRole.UserRole)
                    if data and data[0] is not None: # data[0] is the ID
                        ids.append(data[0])
            return ids
        return []

    def __del__(self):
        self._close_panel_db() # 确保在面板销毁时关闭连接

# --- END OF FILE source_panels/base_panel.py ---