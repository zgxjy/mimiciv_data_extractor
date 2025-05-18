# --- START OF FILE app_config.py ---

# 应用版本信息
APP_VERSION = "0.2.1" # 版本更新
APP_NAME = "医学数据提取与处理工具 - MIMIC-IV"

# 默认数据库连接参数 (用户可以在UI中覆盖)
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = "5432"
DEFAULT_DB_NAME = "mimiciv3" # 常用名
DEFAULT_DB_USER = "postgres"
# DEFAULT_DB_PASSWORD = ""

# 默认导出路径
DEFAULT_EXPORT_PATH = "USER_DESKTOP"

# 基础数据提取中“患者既往病史”的默认关键词类别
DEFAULT_PAST_DIAGNOSIS_CATEGORIES = [
    "sleep apnea", "insomnia", "depressive", "anxiety", "anxiolytic",
    "diabetes", "hypertension", "myocardial infarction", "stroke", "asthma", "copd"
]

# SQL构建相关配置
SQL_PREVIEW_LIMIT = 100
SQL_BUILDER_DUMMY_DB_FOR_AS_STRING = "dbname=dummy user=dummy"

# UI相关的配置
DEFAULT_MAIN_WINDOW_WIDTH = 950
DEFAULT_MAIN_WINDOW_HEIGHT = 880
MIN_CONDITION_GROUP_SCROLL_HEIGHT = 150
MIN_PANEL_CONDITION_GROUP_SCROLL_HEIGHT = 200

# 日志配置
LOG_FILE_ENABLED = False
LOG_FILE_PATH = "app_log.log"
LOG_LEVEL = "INFO"

# --- 专项数据聚合方法定义 ---

# 用户在UI上看到的聚合方法及其内部使用的唯一键
AGGREGATION_METHODS_DISPLAY = [
    ("平均值 (Mean)", "MEAN"),
    ("中位数 (Median)", "MEDIAN"),
    ("最小值 (Min)", "MIN"),
    ("最大值 (Max)", "MAX"),
    ("第一次测量值 (First)", "FIRST_VALUE"),
    ("最后一次测量值 (Last)", "LAST_VALUE"),
    ("计数 (Count)", "COUNT"), # 计数值的个数
    ("总和 (Sum)", "SUM"),
    ("标准差 (StdDev)", "STDDEV_SAMP"),    # 样本标准差
    ("方差 (Variance)", "VAR_SAMP"),        # 样本方差
    ("变异系数 (CV)", "CV"),
    ("第25百分位数 (P25)", "P25"),
    ("第75百分位数 (P75)", "P75"),
    ("四分位距 (IQR)", "IQR"),
    ("值域 (Range)", "RANGE"),
    ("原始时间序列 (JSON)", "TIMESERIES_JSON"), #
]

# 内部键对应的SQL聚合函数模板
# {val_col} 会被替换为实际的值列名 (可能包含CAST)
# {time_col} 会被替换为实际的时间列名 (用于需要排序的聚合)
SQL_AGGREGATES = {
    "MEAN": "AVG({val_col})",
    "MEDIAN": "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {val_col})",
    "MIN": "MIN({val_col})",
    "MAX": "MAX({val_col})",
    "COUNT": "COUNT({val_col})", # 只计数非NULL的值
    "SUM": "SUM({val_col})",
    "STDDEV_SAMP": "STDDEV_SAMP({val_col})",
    "VAR_SAMP": "VAR_SAMP({val_col})",
    "CV": "CASE WHEN AVG({val_col}) IS DISTINCT FROM 0 THEN STDDEV_SAMP({val_col}) / AVG({val_col}) ELSE NULL END", # 处理平均值为0或NULL的情况
    "FIRST_VALUE": "(ARRAY_AGG({val_col} ORDER BY {time_col} ASC NULLS LAST))[1]",
    "LAST_VALUE": "(ARRAY_AGG({val_col} ORDER BY {time_col} DESC NULLS LAST))[1]",
    "P25": "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {val_col})",
    "P75": "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {val_col})",
    "IQR": "(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {val_col})) - (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {val_col}))",
    "RANGE": "MAX({val_col}) - MIN({val_col})",
    "TIMESERIES_JSON": "JSONB_AGG(JSONB_BUILD_OBJECT('time', {time_col}, 'value', {val_col}) ORDER BY {time_col} ASC NULLS LAST)", # <-- 新增
}

# 内部键对应的SQL结果列类型 (用于 ALTER TABLE ADD COLUMN)
AGGREGATE_RESULT_TYPES = {
    "MEAN": "DOUBLE PRECISION", # PostgreSQL中 AVG 返回 double precision
    "MEDIAN": "DOUBLE PRECISION", # PERCENTILE_CONT 返回 double precision
    "MIN": "NUMERIC",          # 假设原始valuenum是NUMERIC或可以转为NUMERIC；如果是TEXT，则为TEXT
    "MAX": "NUMERIC",          # 同上
    "COUNT": "INTEGER",
    "SUM": "NUMERIC",          # SUM的结果类型依赖于输入类型，NUMERIC比较通用
    "STDDEV_SAMP": "DOUBLE PRECISION",
    "VAR_SAMP": "DOUBLE PRECISION",
    "CV": "DOUBLE PRECISION",
    "FIRST_VALUE": "NUMERIC",  # 结果类型依赖于 val_col 的类型, NUMERIC for valuenum, TEXT for value
    "LAST_VALUE": "NUMERIC",   # 同上
    "P25": "DOUBLE PRECISION",
    "P75": "DOUBLE PRECISION",
    "IQR": "DOUBLE PRECISION",
    "RANGE": "NUMERIC",        # 结果类型依赖于 MIN/MAX
    "TIMESERIES_JSON": "JSONB", # <-- 新增
}
# 注意: 对于 MIN, MAX, FIRST_VALUE, LAST_VALUE, RANGE，如果原始列是文本 (value)，
# 则结果类型应为 TEXT。这需要在 sql_builder_special.py 中根据 is_text_extraction 动态调整。
# 为了简化，这里先假设主要用于 valuenum。sql_builder_special.py 中会处理文本情况。


# 默认的值列和时间列名
DEFAULT_VALUE_COLUMN = "valuenum" # 通常用于 chartevents, labevents
DEFAULT_TEXT_VALUE_COLUMN = "value" # 通常用于 chartevents 的文本值
DEFAULT_TIME_COLUMN = "charttime" # 通常用于 chartevents, labevents, outputevents

# --- END OF FILE app_config.py ---