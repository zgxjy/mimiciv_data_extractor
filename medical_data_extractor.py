# --- START OF FILE medical_data_extractor.py ---

import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QTabWidget, QMessageBox
from PySide6.QtCore import Slot
from PySide6.QtGui import QIcon

from tabs.tab_connection import ConnectionTab
from tabs.tab_structure import StructureTab
from tabs.tab_query_cohort import QueryCohortTab
from tabs.tab_combine_base_info import BaseInfoDataExtractionTab
from tabs.tab_special_data_master import SpecialDataMasterTab # 导入新的主专项数据Tab
from tabs.tab_data_dictionary import DataDictionaryTab     # <-- 新增导入
from tabs.tab_data_export import DataExportTab


class MedicalDataExtractor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("医学数据提取与处理工具 - MIMIC-IV")
        self.setGeometry(100, 100, 900, 850) # 原始大小，可能需要根据内容调整

        # icon.ico 与 medical_data_extractor.py 在同一目录
        icon_path = "assets/icons/icon.ico" # 确保你有这个图标文件，或者注释掉这行
        self.setWindowIcon(QIcon(icon_path))
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # Instantiate tabs
        self.connection_tab = ConnectionTab()
        self.structure_tab = StructureTab(self.get_db_params)
        self.data_dictionary_tab = DataDictionaryTab(self.get_db_params) # <-- 实例化
        self.query_cohort_tab = QueryCohortTab(self.get_db_params)
        self.data_extraction_tab = BaseInfoDataExtractionTab(self.get_db_params)
        self.special_data_master_tab = SpecialDataMasterTab(self.get_db_params) # 实例化新的
        self.data_export_tab = DataExportTab(self.get_db_params)

        # Add tabs (调整顺序，将字典查看器放在结构查看后)
        self.tabs.addTab(self.connection_tab, "1. 数据库连接")         # Index 0
        self.tabs.addTab(self.structure_tab, "数据库结构查看")        # Index 1
        self.tabs.addTab(self.data_dictionary_tab, "数据字典查看")  # <-- 添加到Tab控件，Index 2
        self.tabs.addTab(self.query_cohort_tab, "2. 查找与创建队列") # Index 3
        self.tabs.addTab(self.data_extraction_tab, "3. 添加基础数据") # Index 4
        self.tabs.addTab(self.special_data_master_tab, "4. 添加专项数据")     # Index 5
        self.tabs.addTab(self.data_export_tab, "5. 数据预览与导出")     # Index 6

        # --- Signal Connections ---
        self.connection_tab.connected_signal.connect(self.on_db_connected)
        self.special_data_master_tab.request_preview_signal.connect(self.data_export_tab.preview_specific_table)
        self.structure_tab.request_table_preview_signal.connect(self.handle_structure_table_preview)
        # (未来)可以添加从其他面板到数据字典的联动信号连接
        # 例如，如果 CharteventsConfigPanel 发出一个信号，可以在这里连接到 data_dictionary_tab.lookup_and_display_item


    @Slot()
    def on_db_connected(self):
        print("Database connected signal received by main window.")
        # 启用/调用每个tab的on_db_connected（如果存在）
        # 这样更通用，并且可以按tab的顺序初始化
        for i in range(self.tabs.count()):
            tab_widget = self.tabs.widget(i)
            if hasattr(tab_widget, 'on_db_connected'):
                # 对于StructureTab，它有自己的set_btn_enabled和view_db_structure
                if tab_widget == self.structure_tab:
                    self.structure_tab.set_btn_enabled(True)
                    self.structure_tab.view_db_structure() # 自动刷新结构
                else:
                    tab_widget.on_db_connected()


    @Slot(str, str)
    def handle_structure_table_preview(self, schema_name, table_name):
        """Handles the preview request from StructureTab."""
        print(f"Main window: Received preview request for {schema_name}.{table_name}")
        
        export_tab_index = -1
        for i in range(self.tabs.count()):
            if self.tabs.widget(i) == self.data_export_tab:
                export_tab_index = i
                break
        
        if export_tab_index != -1:
            self.tabs.setCurrentIndex(export_tab_index)
            # 确保 DataExportTab 在数据库连接后被正确初始化（如果它之前不是活动的）
            if self.connection_tab.connected and not self.data_export_tab.refresh_btn.isEnabled():
                self.data_export_tab.on_db_connected() # 这会刷新列表等
                QApplication.processEvents() # 允许 on_db_connected 完成其UI更新

            self.data_export_tab.preview_specific_table(schema_name, table_name)
        else:
            print("Error: DataExportTab not found.")
            QMessageBox.warning(self, "错误", "无法找到数据导出标签页。")


    def get_db_params(self):
        return self.connection_tab.db_params if hasattr(self.connection_tab, 'connected') and self.connection_tab.connected else None

    def closeEvent(self, event):
        # 尝试优雅地关闭/取消任何正在运行的工作线程
        tabs_with_workers = [
            self.query_cohort_tab,      # Has cohort_worker_thread
            self.data_extraction_tab,    # Has worker_thread (for base info)
            self.special_data_master_tab # Has worker_thread (for special info merge)
        ]

        for tab_instance in tabs_with_workers:
            worker_thread_attr_name = None
            worker_obj_attr_name = None

            if hasattr(tab_instance, 'cohort_worker_thread'): # For QueryCohortTab
                worker_thread_attr_name = 'cohort_worker_thread'
                worker_obj_attr_name = 'cohort_worker'
            elif hasattr(tab_instance, 'worker_thread'): # For BaseInfoDataExtractionTab and SpecialDataMasterTab
                worker_thread_attr_name = 'worker_thread'
                if hasattr(tab_instance, 'worker'): # BaseInfo
                     worker_obj_attr_name = 'worker'
                elif hasattr(tab_instance, 'merge_worker'): # SpecialDataMasterTab
                     worker_obj_attr_name = 'merge_worker'
            
            if worker_thread_attr_name and worker_obj_attr_name:
                thread = getattr(tab_instance, worker_thread_attr_name, None)
                worker = getattr(tab_instance, worker_obj_attr_name, None)
                
                if thread and thread.isRunning():
                    tab_name = tab_instance.__class__.__name__
                    print(f"Attempting to stop worker in {tab_name} on close...")
                    if worker and hasattr(worker, 'cancel'):
                        worker.cancel()
                    thread.quit()
                    if not thread.wait(1500): # 稍增加等待时间
                        print(f"Warning: Worker thread in {tab_name} did not quit in time.")
                        # thread.terminate() # 强制终止作为最后手段，可能不安全
                elif thread: # 如果线程对象存在但未运行，确保清理
                    if thread.isFinished():
                         thread.deleteLater() # 确保已完成的线程被删除
                    # else: # 线程对象存在但从未启动或以其他方式结束
                    #     pass


        # 关闭特定Tab持有的数据库连接 (如果它们实现了 _close_db 或 _close_main_db)
        # 例如, SpecialDataMasterTab 可能会有一个 _close_main_db
        # 而其子面板有 _close_panel_db (这些应该通过 SpecialDataMasterTab 的 closeEvent 或 __del__ 调用)
        if hasattr(self.special_data_master_tab, '_close_main_db'): # 示例
             self.special_data_master_tab._close_main_db()
        
        # 调用每个子 widget 的 closeEvent (如果它们重写了)
        # 通常不需要，除非子 widget 有特殊的清理需求未被 QObject 的析构处理
        # for i in range(self.tabs.count()):
        #     widget = self.tabs.widget(i)
        #     if hasattr(widget, 'closeEvent'): # 仅在它们真的重写了 closeEvent 时
        #         # 这个调用方式是错误的，closeEvent是事件处理器，不应直接调用
        #         # 正确的做法是让子控件自己处理它们的资源释放，例如在其 __del__ 或特定的清理方法中
        #         pass

        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MedicalDataExtractor()
    window.show()
    sys.exit(app.exec())

# --- END OF FILE medical_data_extractor.py ---