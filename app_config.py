# --- START OF FILE app_config.py ---

# 应用版本信息
APP_VERSION = "0.2.0" 
APP_NAME = "医学数据提取与处理工具 - MIMIC-IV"

# 默认数据库连接参数 (用户可以在UI中覆盖)
# 将这些设为 None 或空字符串，以便首次启动时UI为空白，或提供通用占位符
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = "5432"
DEFAULT_DB_NAME = "mimiciv3"
DEFAULT_DB_USER = "postgres"
# DEFAULT_DB_PASSWORD = "" # 密码通常不硬编码，让用户输入

# 默认导出路径 (可以是一个相对路径或特殊标记如 "USER_DOCUMENTS")
# 在实际使用时，代码需要解析这个路径
DEFAULT_EXPORT_PATH = "USER_DESKTOP" # 程序可以将其解析为用户桌面

# 基础数据提取中“患者既往病史”的默认关键词类别
# 未来可以考虑从UI加载或允许用户编辑这个列表
DEFAULT_PAST_DIAGNOSIS_CATEGORIES = [
    "sleep apnea", "insomnia", "depressive", "anxiety", "anxiolytic",
    "diabetes", "hypertension", "myocardial infarction", "stroke", "asthma", "copd"
]

# SQL构建相关配置
SQL_PREVIEW_LIMIT = 100 # 数据字典和数据导出中预览的行数限制
SQL_BUILDER_DUMMY_DB_FOR_AS_STRING = "dbname=dummy user=dummy" # 用于 psycopg2.sql.SQL().as_string()

# UI相关的配置
DEFAULT_MAIN_WINDOW_WIDTH = 950
DEFAULT_MAIN_WINDOW_HEIGHT = 880
MIN_CONDITION_GROUP_SCROLL_HEIGHT = 150
MIN_PANEL_CONDITION_GROUP_SCROLL_HEIGHT = 200 # 面板内条件组的最小高度

# 日志配置 (如果使用文件日志)
LOG_FILE_ENABLED = False # 是否启用文件日志
LOG_FILE_PATH = "app_log.log" # 相对于应用运行目录
LOG_LEVEL = "INFO" # "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"


# --- END OF FILE app_config.py ---