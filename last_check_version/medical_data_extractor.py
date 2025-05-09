# --- START OF FILE medical_data_extractor.py ---

import sys
# from PyQt5.QtWidgets import QApplication, QMainWindow, QTabWidget # Old
from PySide6.QtWidgets import QApplication, QMainWindow, QTabWidget # New
from PySide6.QtCore import Slot # New

from tab_connection import ConnectionTab
from tab_structure import StructureTab
from tab_query_disease import QueryDiseaseTab
from tab_combine_base_info import BaseInfoDataExtractionTab
from tab_combine_special_info import SpecialInfoDataExtractionTab # Make sure this import is correct
from tab_data_export import DataExportTab


class MedicalDataExtractor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("医学数据提取与处理工具 - MIMIC-IV") # Improved title
        self.setGeometry(100, 100, 900, 850) # Slightly larger window
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # Instantiate tabs
        self.connection_tab = ConnectionTab()
        self.structure_tab = StructureTab(self.get_db_params)
        self.query_disease_tab = QueryDiseaseTab(self.get_db_params)
        self.data_extraction_tab = BaseInfoDataExtractionTab(self.get_db_params)
        # Pass get_db_params to SpecialInfoDataExtractionTab
        self.special_data_tab = SpecialInfoDataExtractionTab(self.get_db_params)
        self.data_export_tab = DataExportTab(self.get_db_params) # Ensure this takes get_db_params

        # Add tabs
        self.tabs.addTab(self.connection_tab, "1. 数据库连接")
        self.tabs.addTab(self.structure_tab, "数据库结构查看")
        self.tabs.addTab(self.query_disease_tab, "2. 查找与创建队列") # Renamed for clarity
        self.tabs.addTab(self.data_extraction_tab, "3. 添加基础数据") # Renamed
        self.tabs.addTab(self.special_data_tab, "4. 添加专项数据") # Renamed
        self.tabs.addTab(self.data_export_tab, "5. 数据预览与导出") # Renamed

        # --- Signal Connections ---
        self.connection_tab.connected_signal.connect(self.on_db_connected)
        # Connect the signal from special_data_tab to the preview function in data_export_tab
        self.special_data_tab.request_preview_signal.connect(self.data_export_tab.preview_specific_table)


    @Slot() # Decorator for PySide signals
    def on_db_connected(self):
        print("Database connected signal received by main window.")
        # Enable buttons/features in other tabs
        self.structure_tab.set_btn_enabled(True)

        if hasattr(self.query_disease_tab, 'on_db_connected'):
             self.query_disease_tab.on_db_connected()
        else: # Fallback for older structure
             if hasattr(self.query_disease_tab, 'query_btn'): self.query_disease_tab.query_btn.setEnabled(True)
             if hasattr(self.query_disease_tab, 'preview_btn'): self.query_disease_tab.preview_btn.setEnabled(True)
             # create_table_btn should only be enabled after a query runs

        if hasattr(self.data_extraction_tab, 'on_db_connected'):
            self.data_extraction_tab.on_db_connected()

        if hasattr(self.special_data_tab, 'on_db_connected'):
            self.special_data_tab.on_db_connected()

        if hasattr(self.data_export_tab, 'on_db_connected'):
            self.data_export_tab.on_db_connected()


    def get_db_params(self):
        # Provide DB params, ensuring connection status is checked
        return self.connection_tab.db_params if hasattr(self.connection_tab, 'connected') and self.connection_tab.connected else None

    def closeEvent(self, event):
        # Ensure special tab closes its connection if open
        if hasattr(self.special_data_tab, '_close_db'):
             self.special_data_tab._close_db()
        # Add similar calls for other tabs if they hold persistent connections
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    # Optional: Apply a style for better look
    # app.setStyle("Fusion")
    window = MedicalDataExtractor()
    window.show()
    sys.exit(app.exec())

# --- END OF FILE medical_data_extractor.py ---