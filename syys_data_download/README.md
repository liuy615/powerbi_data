# 数据检核程序

## 功能说明

这是一个用于检核表格数据的程序，主要功能包括：

1. 检查表格字段名是否符合标准
2. 检核每个字段的数据是否符合规范
3. 生成错误报告并保存为Excel文件
4. 发送检核结果通知到企业微信

## 文件结构

```
.
├── main.py              # 主程序入口
├── config.py            # 配置文件（标准表头、检核规则等）
├── field_checker.py     # 字段名检核类
├── data_checker.py      # 数据检核类
├── file_processor.py    # 文件处理器
├── logger.py            # 日志记录器
└── README.md            # 说明文档
```

## 使用方法

### 1. 配置检核任务

在 `main.py` 中，每个检核任务都有独立的函数。例如：

```python
def check_新车三方延保(processor: FileProcessor):
    """检核新车三方延保数据"""
    processor.process_task(
        directory=r"E:\path\to\data",
        file_filters=[".xlsx"],
        sheet_name=None,
        required_fields=Config.STANDARD_HEADERS_新车三方延保,
        check_rules=Config.CHECK_FIELDS_新车三方延保,
        task_name="新车三方延保",
        row_filter={  # 可选：行筛选条件
            "field": "延保销售日期",
            "condition": ">=",
            "value": "2025-01-01"
        }
    )
```

### 2. 文件筛选条件

`file_filters` 参数用于筛选需要检核的文件：

- 文件名必须包含列表中的所有关键词
- 示例：`["自店", ".xlsx"]` 表示文件名包含"自店"且为xlsx格式

### 3. 行筛选条件（可选）

`row_filter` 参数用于筛选数据行：

- `field`: 要筛选的字段名
- `condition`: 筛选条件（支持：>=, >, <=, <, ==）
- `value`: 筛选值（日期格式：YYYY-MM-DD）

示例：只检核2025年以后的数据
```python
row_filter={
    "field": "延保销售日期",
    "condition": ">=",
    "value": "2025-01-01"
}
```

### 4. 运行程序

```bash
python main.py
```

### 5. 查看结果

程序运行后会生成：

- 日志文件：`check_logs/data_check_YYYYMMDD_HHMMSS.log`
- 错误报告：`check_logs/data_errors_YYYYMMDD_HHMMSS.xlsx`

## 配置说明

### 标准表头配置

在 `config.py` 中定义标准表头：

```python
STANDARD_HEADERS_自店贴膜 = [
    "序号",
    "月份",
    "推送日期",
    # ... 更多字段
]
```

### 检核规则配置

在 `config.py` 中定义检核规则：

```python
CHECK_FIELDS_自店贴膜 = {
    "推送日期": "date_skip_check",  # 字段名: 检核方法名
    "到店日期": "arrival_date_check",
    "精品顾问": "consultant_check",
    # ... 更多规则
}
```

### 有效门店名称配置

在 `config.py` 中定义有效的门店名称：

```python
VALID_STORE_NAMES = {
    "新港建武", "上元盛世", "新港澜阔",
    # ... 更多门店
}
```

## 检核方法说明

### 字段名检核（FieldChecker）

- `check_fields()`: 检查字段是否完全匹配（不允许缺失和多余）
- `check_fields_subset()`: 检查字段是否包含必需字段（允许多余）

### 数据检核（DataChecker）

内置检核方法：

- `check_not_empty()`: 检查字段是否为空
- `check_month()`: 检查月份（1-12）
- `check_date()`: 检查日期格式
- `check_store_name()`: 检查门店名称
- `check_vin_6()`: 检查车架号后6位
- `check_vin_full()`: 检查完整车架号
- `check_phone()`: 检查电话号码
- `check_amount()`: 检查金额
- `check_date_logic()`: 检查日期逻辑关系

### 扩展检核方法

在 `data_checker.py` 中添加新的检核方法：

```python
def check_custom_field(self, value: Any) -> Tuple[bool, str]:
    """自定义检核方法"""
    if pd.isna(value):
        return False, "字段为空"
    
    # 添加你的检核逻辑
    
    return True, ""
```

然后在 `file_processor.py` 的 `call_check_method()` 方法中注册：

```python
method_map = {
    # ... 现有方法
    "custom_check": lambda v: self.data_checker.check_custom_field(v),
}
```

## 企业微信通知配置

在 `logger.py` 初始化时传入webhook地址：

```python
logger = DataCheckerLogger(
    log_dir=log_dir,
    wecom_webhook="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
)
```

或在调用时传入：

```python
logger.send_wecom_notification(webhook_url="YOUR_WEBHOOK_URL")
```

## 注意事项

1. 确保安装了必要的依赖：`pandas`, `openpyxl`, `requests`
2. 文件路径使用原始字符串（r"path"）避免转义问题
3. Excel文件需要指定正确的sheet名称
4. CSV文件会自动使用UTF-8编码读取
5. 所有数据都会以字符串类型读取，避免格式问题

## 依赖安装

```bash
pip install pandas openpyxl requests
```
