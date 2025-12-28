from task_scheduler import ScheduledTaskRunner, generate_time_range_schedule
import threading
import time


def syy_auto_runner():
    """私有云任务执行器"""
    scripts = [
        r"E:\powerbi_data\代码执行\syys_data_processor\down_syy_all.py",
        r"E:\powerbi_data\代码执行\syys_data_processor\data_clean_syy.py",
        r"E:\powerbi_data\代码执行\syys_data_processor\tmsj.py"
    ]

    config = generate_time_range_schedule("08:00", "22:00", 3, "hours")
    runner = ScheduledTaskRunner("私有云数据清洗")
    runner.start_schedule(scripts, config)


def cyy_auto_runner():
    """车易云任务执行器"""
    scripts = [
        r"E:\powerbi_data\代码执行\车易云数据\cyy_to_mysql_99.py",
        r"E:\powerbi_data\代码执行\车易云数据\cyy_delete_data.py",
        r"E:\powerbi_data\代码执行\cyys_data_processor\main.py",
        r"E:\powerbi_data\代码执行\私有云数据\concat_dashboad.py",
    ]

    config = generate_time_range_schedule("08:48", "22:48", 30, "minutes")
    runner = ScheduledTaskRunner("车易云数据清洗")
    runner.start_schedule(scripts, config)


# 创建并启动两个独立线程
syy_thread = threading.Thread(target=syy_auto_runner, daemon=True, name="SYY_Task")
cyy_thread = threading.Thread(target=cyy_auto_runner, daemon=True, name="CYY_Task")

syy_thread.start()
cyy_thread.start()

# 主线程保持运行
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n程序已停止")