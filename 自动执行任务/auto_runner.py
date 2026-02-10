from task_scheduler import ScheduledTaskRunner, generate_time_range_schedule, generate_daily_schedule
import threading
import time


def syy_auto_runner():
    """私有云任务执行器"""
    scripts = [
        r"E:\powerbi_data\powerbi_data\syys_data_processor\down_syy_all.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\data_clean_syy.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\tmsj.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\jingpin.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\qbwy.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\sanfang.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\syy_5separately.py",
    ]

    config = generate_time_range_schedule("08:45", "22:45", 1, "hours")
    runner = ScheduledTaskRunner("私有云数据清洗")
    runner.start_schedule(scripts, config)


def cyy_auto_runner():
    """车易云任务执行器"""
    scripts = [
        r"E:\powerbi_data\powerbi_data\cyys_data_download\cyy_to_mysql_99.py",
        r"E:\powerbi_data\powerbi_data\cyys_data_download\cyy_delete_data.py",
        r"E:\powerbi_data\powerbi_data\cyys_data_processor\main.py",
        r"E:\powerbi_data\powerbi_data\cyys_data_application\concat_dashboad.py",
        r"E:\powerbi_data\看板更新\syy_files_upload.py",
        r"E:\powerbi_data\看板更新\data_download.py",
        r"E:\powerbi_data\powerbi_data\cyys_data_processor\数据备份.py",  # 数据备份
    ]

    config = generate_time_range_schedule("08:52", "22:52", 30, "minutes")
    runner = ScheduledTaskRunner("车易云数据清洗")
    runner.start_schedule(scripts, config)


def daypaper_auto_runner():
    """日报任务执行器"""
    scripts = [
        r"E:\pycharm_project\day_paper\daypaper_pbwy.py",
        r"E:\powerbi_data\powerbi_data\cyys_data_download\cyy_to_mysql_month_all.py",
    ]

    config = generate_daily_schedule("22:00")
    runner = ScheduledTaskRunner("日报定时发送")
    runner.start_schedule(scripts, config)


def cyy_all_auto_runner():
    """日报任务执行器"""
    scripts = [
        r"E:\powerbi_data\powerbi_data\cyys_data_download\cyy_to_mysql_month_all.py",
    ]

    config = generate_daily_schedule("01:00")
    runner = ScheduledTaskRunner("日报定时发送")
    runner.start_schedule(scripts, config)

# 创建并启动独立线程
syy_thread = threading.Thread(target=syy_auto_runner, daemon=True, name="SYY_Task")
cyy_thread = threading.Thread(target=cyy_auto_runner, daemon=True, name="CYY_Task")
Daypaper = threading.Thread(target=daypaper_auto_runner, daemon=True, name="Daypaper")
cyy_all_clean = threading.Thread(target=cyy_all_auto_runner, daemon=True, name="Cyy_All")


syy_thread.start()
cyy_thread.start()
Daypaper.start()
cyy_all_clean.start()




# =================================================测试使用======================================================================
def daypaper_runner():
    """直接调用立即执行方法"""
    scripts = [
        r"E:\pycharm_project\day_paper\daypaper_pbwy.py",
    ]
    runner = ScheduledTaskRunner("立即执行测试")
    runner.run_immediately(scripts)


# daypaper_runner()


# 主线程保持运行
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n程序已停止")