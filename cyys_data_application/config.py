"""
配置文件 - 数据库连接配置和表名
"""

# MySQL数据库配置 - 原始数据数据库
SOURCE_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '513921',
    'database': 'cyy_stg_data',  # 原始数据所在的数据库
    'charset': 'utf8mb4'
}

# MySQL数据库配置 - 应用数据数据库
APP_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '513921',
    'database': 'cyy_app_data',  # 清洗后数据存放的数据库
    'charset': 'utf8mb4'
}

# SQLAlchemy 连接字符串
SOURCE_DB_URL = f"mysql+pymysql://{SOURCE_DB_CONFIG['user']}:{SOURCE_DB_CONFIG['password']}@{SOURCE_DB_CONFIG['host']}:{SOURCE_DB_CONFIG['port']}/{SOURCE_DB_CONFIG['database']}?charset={SOURCE_DB_CONFIG['charset']}"
APP_DB_URL = f"mysql+pymysql://{APP_DB_CONFIG['user']}:{APP_DB_CONFIG['password']}@{APP_DB_CONFIG['host']}:{APP_DB_CONFIG['port']}/{APP_DB_CONFIG['database']}?charset={APP_DB_CONFIG['charset']}"

# 表名配置
# 原始表名（在原始数据库中）
SOURCE_TABLES = {
    'sales': 'sales_data',      # 原始销售表
    'inventory': 'inventory_data' # 原始库存表
}

# 应用表名（在cyy_app_data数据库中）
APP_TABLES = {
    'sales': 'app_sales_data',      # 清洗后销售表
    'inventory': 'app_inventory_data' # 清洗后库存表
}

# 日志配置
LOG_FILE = 'data_clean.log'