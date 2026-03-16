# 快速使用指南

## 一、程序架构

```
主程序 (main.py)
    ↓
    ├─→ check_投放市场费用()
    ├─→ check_新车三方延保()
    ├─→ check_自店贴膜()
    └─→ check_三方贴膜()
    ↓
文件处理器 (file_processor.py)
    ↓
    ├─→ 字段名检核 (field_checker.py)
    └─→ 数据检核 (data_checker.py)
    ↓
日志记录器 (logger.py)
    ↓
    ├─→ 生成日志文件
    ├─→ 生成错误报告Excel
    └─→ 发送企微通知
```

## 二、核心类说明

### 1. FieldChecker（字段名检核类）

负责检查表格的字段名是否符合标准。

```python
from field_checker import FieldChecker

checker = FieldChecker()

# 检查字段（不允许多余字段）
is_valid, missing, extra = checker.check_fields(
    actual_fields=["字段1", "字段2"],
    required_fields=["字段1", "字段2", "字段3"]
)
# 返回: (False, ["字段3"], [])

# 检查字段（允许多余字段）
is_valid, missing = checker.check_fields_subset(
    actual_fields=["字段1", "字段2", "字段4"],
    required_fields=["字段1", "字段2"]
)
# 返回: (True, [])
```

### 2. DataChecker（数据检核类）

负责检核每个字段的数据是否符合规范。

```python
from data_checker import DataChecker
from config import Config

checker = DataChecker(valid_store_names=Config.VALID_STORE_NAMES)

# 检查月份
is_valid, error_msg = checker.check_month(5)
# 返回: (True, "")

# 检查日期
is_valid, error_msg = checker.check_date("2024-01-01", allow_empty=False)
# 返回: (True, "")

# 检查门店名称
is_valid, error_msg = checker.check_store_name("新港建武")
# 返回: (True, "")

# 检查车架号后6位
is_valid, error_msg = checker.check_vin_6("ABC123")
# 返回: (True, "")
```

## 三、添加新的检核任务

### 步骤1：在config.py中添加配置

```python
# 标准表头列表 - 新模板
STANDARD_HEADERS_新模板 = [
    "字段1",
    "字段2",
    "字段3",
]

# 检核规则 - 新模板
CHECK_FIELDS_新模板 = {
    "字段1": "date_check",
    "字段2": "store_check",
}
```

### 步骤2：在main.py中添加检核函数

```python
def check_新模板(processor: FileProcessor):
    """
    检核新模板数据
    
    参数:
        processor: 文件处理器实例
    """
    processor.logger.logger.info("=" * 60)
    processor.logger.logger.info("开始检核: 新模板")
    processor.logger.logger.info("=" * 60)
    
    processor.process_task(
        directory=r"E:\path\to\data",
        file_filters=[".xlsx"],
        sheet_name=None,
        required_fields=Config.STANDARD_HEADERS_新模板,
        check_rules=Config.CHECK_FIELDS_新模板,
        task_name="新模板"
    )
    
    processor.logger.logger.info("完成检核: 新模板\n")
```

### 步骤3：在main()函数中调用

```python
def main():
    # ... 初始化代码 ...
    
    # 依次执行各个检核任务
    check_投放市场费用(processor)
    check_新车三方延保(processor)
    check_自店贴膜(processor)
    check_三方贴膜(processor)
    check_新模板(processor)  # 添加新任务
    
    # ... 总结和通知代码 ...
```

## 四、添加新的检核规则

### 步骤1：在data_checker.py中添加检核方法

```python
def check_custom_rule(self, value: Any) -> Tuple[bool, str]:
    """自定义检核规则"""
    if pd.isna(value):
        return False, "字段为空"
    
    # 你的检核逻辑
    str_value = str(value).strip()
    if len(str_value) < 5:
        return False, f"长度不足5位: {value}"
    
    return True, ""
```

### 步骤2：在config.py中配置规则

```python
CHECK_FIELDS_新模板 = {
    "自定义字段": "custom_rule_check",
}
```

### 步骤3：在file_processor.py中注册方法

```python
method_map = {
    # ... 现有方法
    "custom_rule_check": lambda v: self.data_checker.check_custom_rule(v),
}
```

## 五、配置企业微信通知

### 步骤1：获取Webhook地址

1. 在企业微信群中添加机器人
2. 复制webhook地址

### 步骤2：配置到程序中

在 `main.py` 中修改：

```python
wecom_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
logger = DataCheckerLogger(log_dir=log_dir, wecom_webhook=wecom_webhook)
```

### 步骤3：通知内容

程序会自动发送包含以下信息的通知：

- 检核时间
- 总文件数
- 检核文件数
- 表头错误数
- 数据错误数
- 总行数
- 错误行数
- 错误率
- 日志文件路径

## 六、常见使用场景

### 场景1：只运行特定检核任务

在 `main()` 函数中注释掉不需要的任务：

```python
def main():
    # ... 初始化代码 ...
    
    # 只检核投放市场费用
    check_投放市场费用(processor)
    # check_新车三方延保(processor)  # 注释掉
    # check_自店贴膜(processor)  # 注释掉
    # check_三方贴膜(processor)  # 注释掉
    
    # ... 总结和通知代码 ...
```

### 场景2：修改检核目录

直接修改对应函数中的directory参数：

```python
def check_投放市场费用(processor: FileProcessor):
    processor.process_task(
        directory=r"E:\new\path\to\data",  # 修改这里
        # ... 其他参数 ...
    )
```

### 场景3：调整文件筛选条件

修改file_filters参数：

```python
def check_自店贴膜(processor: FileProcessor):
    processor.process_task(
        directory=r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级",
        file_filters=["自店", "2024", ".xlsx"],  # 添加年份筛选
        # ... 其他参数 ...
    )
```

## 七、错误报告说明

程序会生成Excel格式的错误报告，包含两个sheet：

### Sheet1: 错误详情

| 列名 | 说明 |
|------|------|
| type | 错误类型（header_error/data_error） |
| file | 文件完整路径 |
| file_name | 文件名 |
| row | 错误所在行号（仅数据错误） |
| field | 错误字段名 |
| value | 错误值 |
| message | 错误信息 |
| template | 使用的模板名称 |
| timestamp | 错误发现时间 |

### Sheet2: 统计信息

包含本次检核的统计数据：

- total_files: 总文件数
- checked_files: 已检核文件数
- skipped_files: 跳过文件数
- header_errors: 表头错误数
- data_errors: 数据错误数
- total_rows: 总行数
- error_rows: 错误行数

## 八、程序执行流程

```
1. 程序启动
   ↓
2. 初始化日志记录器
   ↓
3. 初始化文件处理器
   ↓
4. 执行 check_投放市场费用()
   - 查找符合条件的文件
   - 检查字段名
   - 检核数据
   - 记录错误
   ↓
5. 执行 check_新车三方延保()
   - 查找符合条件的文件
   - 检查字段名
   - 检核数据
   - 记录错误
   ↓
6. 执行 check_自店贴膜()
   - 查找符合条件的文件
   - 检查字段名
   - 检核数据
   - 记录错误
   ↓
7. 执行 check_三方贴膜()
   - 查找符合条件的文件
   - 检查字段名
   - 检核数据
   - 记录错误
   ↓
8. 输出总结报告
   ↓
9. 保存错误报告Excel
   ↓
10. 发送企业微信通知
   ↓
11. 程序结束
```

## 九、注意事项

1. 所有文件路径使用原始字符串（r"path"）
2. Excel文件需要正确指定sheet名称
3. CSV文件自动使用UTF-8编码
4. 数据以字符串类型读取，避免格式问题
5. 日期逻辑检查仅在特定任务中启用（如自店贴膜）
6. 企微通知失败不影响程序运行
7. 每个检核任务独立执行，互不影响
8. 所有错误会累积到最终报告中
