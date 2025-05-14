# --- START OF FILE medical_data_extractor.py ---

import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QTabWidget, QMessageBox # Added QMessageBox
from PySide6.QtCore import Slot
from PySide6.QtGui import QIcon

from tab_connection import ConnectionTab
from tab_structure import StructureTab
from tab_query_disease import QueryDiseaseTab
from tab_combine_base_info import BaseInfoDataExtractionTab
from tab_combine_special_info import SpecialInfoDataExtractionTab
from tab_data_export import DataExportTab


class MedicalDataExtractor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("医学数据提取与处理工具 - MIMIC-IV")
        self.setGeometry(100, 100, 900, 850)

        # icon.ico 与 medical_data_extractor.py 在同一目录
        icon_path = "icon.ico"
        self.setWindowIcon(QIcon(icon_path))
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # Instantiate tabs
        self.connection_tab = ConnectionTab()
        self.structure_tab = StructureTab(self.get_db_params)
        self.query_disease_tab = QueryDiseaseTab(self.get_db_params)
        self.data_extraction_tab = BaseInfoDataExtractionTab(self.get_db_params)
        self.special_data_tab = SpecialInfoDataExtractionTab(self.get_db_params)
        self.data_export_tab = DataExportTab(self.get_db_params)

        # Add tabs (ensure correct order for index-based lookup if any)
        self.tabs.addTab(self.connection_tab, "1. 数据库连接")         # Index 0
        self.tabs.addTab(self.structure_tab, "数据库结构查看")        # Index 1
        self.tabs.addTab(self.query_disease_tab, "2. 查找与创建队列") # Index 2
        self.tabs.addTab(self.data_extraction_tab, "3. 添加基础数据") # Index 3
        self.tabs.addTab(self.special_data_tab, "4. 添加专项数据")     # Index 4
        self.tabs.addTab(self.data_export_tab, "5. 数据预览与导出")     # Index 5

        # --- Signal Connections ---
        self.connection_tab.connected_signal.connect(self.on_db_connected)
        self.special_data_tab.request_preview_signal.connect(self.data_export_tab.preview_specific_table)
        # Connect signal from StructureTab to main window's handler
        self.structure_tab.request_table_preview_signal.connect(self.handle_structure_table_preview)


    @Slot()
    def on_db_connected(self):
        print("Database connected signal received by main window.")
        # Enable buttons/features in other tabs
        self.structure_tab.set_btn_enabled(True)
        self.structure_tab.view_db_structure() # Automatically refresh structure on connect

        if hasattr(self.query_disease_tab, 'on_db_connected'):
             self.query_disease_tab.on_db_connected()
        # ... (rest of on_db_connected remains the same) ...
        if hasattr(self.data_extraction_tab, 'on_db_connected'):
            self.data_extraction_tab.on_db_connected()
        if hasattr(self.special_data_tab, 'on_db_connected'):
            self.special_data_tab.on_db_connected()
        if hasattr(self.data_export_tab, 'on_db_connected'):
            self.data_export_tab.on_db_connected()


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
            # Ensure DataExportTab is initialized if DB is connected but tab wasn't active
            if self.connection_tab.connected and not self.data_export_tab.refresh_btn.isEnabled():
                self.data_export_tab.on_db_connected()
                QApplication.processEvents() # Allow on_db_connected to populate combos

            self.data_export_tab.preview_specific_table(schema_name, table_name)
        else:
            print("Error: DataExportTab not found.")
            QMessageBox.warning(self, "错误", "无法找到数据导出标签页。")


    def get_db_params(self):
        return self.connection_tab.db_params if hasattr(self.connection_tab, 'connected') and self.connection_tab.connected else None

    def closeEvent(self, event):
        # Attempt to gracefully close/cancel any running worker threads
        tabs_with_workers = [
            self.query_disease_tab,      # Has cohort_worker_thread
            self.data_extraction_tab,    # Has worker_thread (for base info)
            self.special_data_tab        # Has worker_thread (for special info merge)
        ]

        for tab_instance in tabs_with_workers:
            worker_thread_attr = None
            worker_attr = None
            if hasattr(tab_instance, 'cohort_worker_thread'): # For QueryDiseaseTab
                worker_thread_attr = 'cohort_worker_thread'
                worker_attr = 'cohort_worker'
            elif hasattr(tab_instance, 'worker_thread'): # For BaseInfoDataExtractionTab and SpecialInfoDataExtractionTab
                worker_thread_attr = 'worker_thread'
                if hasattr(tab_instance, 'worker'): # BaseInfo
                     worker_attr = 'worker'
                elif hasattr(tab_instance, 'merge_worker'): # SpecialInfo
                     worker_attr = 'merge_worker'
            
            if worker_thread_attr and worker_attr:
                thread = getattr(tab_instance, worker_thread_attr, None)
                worker = getattr(tab_instance, worker_attr, None)
                if thread and thread.isRunning():
                    tab_name = tab_instance.__class__.__name__
                    print(f"Attempting to stop worker in {tab_name} on close...")
                    if worker and hasattr(worker, 'cancel'):
                        worker.cancel()
                    thread.quit()
                    if not thread.wait(1000): # Wait 1 sec
                        print(f"Warning: Worker thread in {tab_name} did not quit in time.")

        # Close any persistent DB connections held by tabs (if any)
        if hasattr(self.special_data_tab, '_close_db'):
             self.special_data_tab._close_db()
        
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MedicalDataExtractor()
    window.show()
    sys.exit(app.exec())

# --- END OF FILE medical_data_extractor.py ---