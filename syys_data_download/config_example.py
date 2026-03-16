"""
配置示例文件
复制此文件并根据实际情况修改配置
"""

# 企业微信机器人Webhook地址
# 获取方式：在企业微信群中添加机器人，复制webhook地址
WECOM_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY_HERE"

# 日志目录
LOG_DIR = r"E:\powerbi_data\data\私有云日志\check_logs"

# 检核任务配置示例
EXAMPLE_TASKS = [
    {
        "directory": r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级",
        "name": "贴膜升级",
        "file_filters": ["自店", ".xlsx"],  # 文件名包含"自店"且为xlsx格式
        "sheet_name": "膜升级登记表",
        "required_fields": "Config.STANDARD_HEADERS_自店贴膜",
        "check_rules": "Config.CHECK_FIELDS_自店贴膜"
    },
    {
        "directory": r"E:\powerbi_data\看板数据\私有云文件本地\投放市场费用",
        "name": "投放市场费用",
        "file_filters": [".xlsx", ".csv"],  # 支持xlsx和csv格式
        "sheet_name": None,  # 使用第一个sheet
        "required_fields": "Config.STANDARD_HEADERS_投放市场费用",
        "check_rules": "Config.CHECK_FIELDS_投放市场费用"
    }
]

# 使用说明：
# 1. 将此文件复制为 config_local.py
# 2. 修改 WECOM_WEBHOOK 为你的企业微信机器人地址
# 3. 修改 LOG_DIR 为你的日志目录
# 4. 在 main.py 中导入并使用这些配置
