"""
灵活定时任务调度器
支持多种时间规则配置
"""
import subprocess
import time
import sys
import os
from datetime import datetime, timedelta
import threading
import json
from typing import List, Dict, Optional, Union


class ScheduledTaskRunner:
    """定时任务运行器"""

    def __init__(self, task_name="默认任务"):
        """
        初始化任务运行器

        Args:
            task_name: 任务名称，用于日志标识
        """
        self.task_name = task_name
        self.running = False
        self.last_run_time = None
        self.log_file = f"E:/powerbi_data/powerbi_data/data/自动执行任务/{task_name}_log.txt"

    def run_scripts(self, script_paths: List[str],
                   capture_output: bool = True) -> Dict:
        """
        运行指定的脚本列表

        Args:
            script_paths: 脚本路径列表
            capture_output: 是否捕获输出

        Returns:
            执行结果字典
        """
        results = {
            "total": len(script_paths),
            "success": 0,
            "failed": 0,
            "details": []
        }

        print(f"\n{'='*60}")
        print(f"开始执行任务 [{self.task_name}]")
        print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        for i, script_path in enumerate(script_paths, 1):
            script_name = os.path.basename(script_path)
            result_info = {
                "script": script_name,
                "path": script_path,
                "success": False,
                "returncode": None,
                "start_time": None,
                "end_time": None,
                "duration": None,
                "error": None
            }

            try:
                if not os.path.exists(script_path):
                    result_info["error"] = f"脚本文件不存在: {script_path}"
                    print(f"[{i}/{len(script_paths)}] ✗ {script_name}: 文件不存在")
                    results["failed"] += 1
                    results["details"].append(result_info)
                    continue

                print(f"[{i}/{len(script_paths)}] 执行: {script_name}")
                start_time = datetime.now()
                result_info["start_time"] = start_time.strftime('%H:%M:%S')

                # 执行脚本
                if capture_output:
                    proc = subprocess.run(
                        [sys.executable, script_path],
                        capture_output=True,
                        text=True,
                        encoding='gbk',  # 改为gbk编码（中文Windows常用编码）
                        errors='replace',  # 替换无法解码的字符
                        shell=True,
                        timeout=3600  # 1小时超时
                    )
                    result_info["returncode"] = proc.returncode
                else:
                    # 不捕获输出，直接运行
                    return_code = subprocess.call(
                        [sys.executable, script_path],
                        shell=True
                    )
                    result_info["returncode"] = return_code
                    proc = None

                end_time = datetime.now()
                result_info["end_time"] = end_time.strftime('%H:%M:%S')
                result_info["duration"] = str(end_time - start_time)

                if result_info["returncode"] == 0:
                    result_info["success"] = True
                    print(f"  ✓ 成功 (耗时: {result_info['duration']})")
                    results["success"] += 1

                    # 显示部分输出
                    if capture_output and proc and proc.stdout:
                        output = proc.stdout.strip()
                        if output and len(output) > 50:
                            print(f"  输出: 成功")
                        elif output:
                            print(f"  输出: {output}")
                else:
                    print(f"  ✗ 失败 (返回码: {result_info['returncode']})")
                    if capture_output and proc and proc.stderr:
                        error_msg = proc.stderr.strip()
                        result_info["error"] = error_msg[:200] if len(error_msg) > 200 else error_msg
                        print(f"  错误: {result_info['error']}")
                    results["failed"] += 1

            except subprocess.TimeoutExpired:
                result_info["error"] = "执行超时 (超过1小时)"
                print(f"  ✗ 超时 (超过1小时)")
                results["failed"] += 1
            except Exception as e:
                result_info["error"] = str(e)
                print(f"  ✗ 异常: {e}")
                results["failed"] += 1

            results["details"].append(result_info)

        print(f"\n执行完成: {results['success']}成功/{results['failed']}失败")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        # 记录日志
        self._log_results(results)

        return results

    def _log_results(self, results: Dict):
        """记录执行结果到日志文件"""
        log_entry = {
            "task_name": self.task_name,
            "run_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "results": results
        }

        try:
            # 读取现有日志
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    try:
                        log_data = json.load(f)
                        if not isinstance(log_data, list):
                            log_data = []
                    except:
                        log_data = []
            else:
                log_data = []

            # 添加新日志
            log_data.append(log_entry)

            # 只保留最近100条日志
            if len(log_data) > 100:
                log_data = log_data[-100:]

            # 写入日志
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            print(f"日志记录失败: {e}")

    def start_schedule(self, script_paths: List[str],
                       schedule_config: Dict):
        """
        启动定时任务调度

        Args:
            script_paths: 脚本路径列表
            schedule_config: 调度配置字典，支持以下格式:

                格式1: 指定具体时间点
                {
                    "type": "time_points",
                    "times": ["08:00", "12:00", "18:00"]  # 每天在这些时间执行
                }

                格式2: 时间范围 + 间隔
                {
                    "type": "time_range",
                    "start": "08:00",    # 开始时间
                    "end": "22:00",      # 结束时间
                    "interval": 30,      # 间隔分钟数 (如30表示每30分钟)
                    "interval_unit": "minutes"  # 可选: minutes或hours
                }

                格式3: 固定间隔
                {
                    "type": "fixed_interval",
                    "interval": 2,       # 间隔值
                    "interval_unit": "hours"  # hours, minutes, 或 seconds
                }

                格式4: 单次定时
                {
                    "type": "once",
                    "time": "23:00"      # 在指定时间执行一次
                }
        """
        print(f"\n启动定时任务调度 [{self.task_name}]")
        print(f"调度配置: {json.dumps(schedule_config, ensure_ascii=False, indent=2)}")
        print(f"脚本数量: {len(script_paths)}")
        print("-" * 60)

        # 验证脚本路径
        valid_scripts = []
        for script in script_paths:
            if os.path.exists(script):
                valid_scripts.append(script)
            else:
                print(f"警告: 脚本不存在，已忽略 - {script}")

        if not valid_scripts:
            print("错误: 没有有效的脚本可执行")
            return

        self.running = True

        # 根据配置类型启动不同的调度器
        schedule_type = schedule_config.get("type", "time_points")

        if schedule_type == "time_points":
            self._schedule_by_time_points(valid_scripts, schedule_config)
        elif schedule_type == "time_range":
            self._schedule_by_time_range(valid_scripts, schedule_config)
        elif schedule_type == "fixed_interval":
            self._schedule_fixed_interval(valid_scripts, schedule_config)
        elif schedule_type == "once":
            self._schedule_once(valid_scripts, schedule_config)
        else:
            print(f"错误: 不支持的调度类型 - {schedule_type}")
            return

        # 启动监控线程
        monitor_thread = threading.Thread(target=self._monitor_schedule)
        monitor_thread.daemon = True
        monitor_thread.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n停止定时任务 [{self.task_name}]")
            self.running = False

    def _schedule_by_time_points(self, script_paths: List[str], config: Dict):
        """按指定时间点调度"""
        time_points = config.get("times", [])

        if not time_points:
            print("警告: 未指定时间点，使用默认时间 [08:00, 12:00, 18:00]")
            time_points = ["08:00", "12:00", "18:00"]

        print(f"执行时间点: {', '.join(time_points)}")

        def scheduler():
            while self.running:
                now = datetime.now()
                current_time_str = now.strftime("%H:%M")

                for time_point in time_points:
                    if current_time_str == time_point:
                        # 避免同一分钟内重复执行
                        if (self.last_run_time is None or
                            (now - self.last_run_time).total_seconds() > 60):
                            print(f"\n[{current_time_str}] 到达执行时间点: {time_point}")
                            self.run_scripts(script_paths)
                            self.last_run_time = now
                            # 执行后等待61秒，避免重复
                            time.sleep(61)
                            break

                time.sleep(30)  # 每30秒检查一次

        threading.Thread(target=scheduler, daemon=True).start()

    def _schedule_by_time_range(self, script_paths: List[str], config: Dict):
        """按时间范围和间隔调度"""
        start_time = config.get("start", "08:00")
        end_time = config.get("end", "22:00")
        interval = config.get("interval", 30)
        interval_unit = config.get("interval_unit", "minutes")

        print(f"执行时间范围: {start_time} - {end_time}")
        print(f"执行间隔: 每{interval}{interval_unit}")

        # 解析时间
        start_hour, start_minute = map(int, start_time.split(":"))
        end_hour, end_minute = map(int, end_time.split(":"))

        def scheduler():
            while self.running:
                now = datetime.now()
                current_hour = now.hour
                current_minute = now.minute

                # 检查是否在时间范围内
                current_total_minutes = current_hour * 60 + current_minute
                start_total_minutes = start_hour * 60 + start_minute
                end_total_minutes = end_hour * 60 + end_minute

                if start_total_minutes <= current_total_minutes <= end_total_minutes:
                    # 计算从开始时间到当前时间经过了多少个间隔
                    elapsed_from_start = current_total_minutes - start_total_minutes

                    if interval_unit == "hours":
                        interval_minutes = interval * 60
                    else:  # minutes
                        interval_minutes = interval

                    # 检查是否是间隔的整数倍
                    if interval_minutes > 0 and elapsed_from_start % interval_minutes == 0:
                        # 避免同一分钟内重复执行
                        if (self.last_run_time is None or
                            (now - self.last_run_time).total_seconds() > 60):
                            print(f"\n[{now.strftime('%H:%M')}] 到达执行时间")
                            self.run_scripts(script_paths)
                            self.last_run_time = now
                            # 执行后等待61秒，避免重复
                            time.sleep(61)

                time.sleep(30)  # 每30秒检查一次

        threading.Thread(target=scheduler, daemon=True).start()

    def _schedule_fixed_interval(self, script_paths: List[str], config: Dict):
        """按固定间隔调度"""
        interval = config.get("interval", 2)
        interval_unit = config.get("interval_unit", "hours")

        print(f"执行间隔: 每{interval}{interval_unit}")

        # 转换间隔为秒
        if interval_unit == "hours":
            interval_seconds = interval * 3600
        elif interval_unit == "minutes":
            interval_seconds = interval * 60
        else:  # seconds
            interval_seconds = interval

        def scheduler():
            while self.running:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 到达执行时间")
                self.run_scripts(script_paths)
                self.last_run_time = datetime.now()

                # 等待指定间隔
                for _ in range(interval_seconds):
                    if not self.running:
                        break
                    time.sleep(1)

        threading.Thread(target=scheduler, daemon=True).start()

    def _schedule_once(self, script_paths: List[str], config: Dict):
        """单次定时执行"""
        target_time = config.get("time", "23:00")

        print(f"执行时间: 今天 {target_time} (单次执行)")

        def scheduler():
            executed = False
            while self.running and not executed:
                now = datetime.now()
                current_time_str = now.strftime("%H:%M")

                if current_time_str == target_time:
                    print(f"\n[{current_time_str}] 到达执行时间")
                    self.run_scripts(script_paths)
                    executed = True
                    self.running = False
                    print(f"单次任务执行完成，程序将退出")

                time.sleep(30)  # 每30秒检查一次

        threading.Thread(target=scheduler, daemon=True).start()

    def _monitor_schedule(self):
        """监控任务状态"""
        while self.running:
            time.sleep(3600)  # 每分钟检查一次

            now = datetime.now()
            if self.last_run_time:
                elapsed = (now - self.last_run_time).total_seconds()
                if elapsed > 3600:  # 超过1小时没有执行
                    print(f"[{now.strftime('%H:%M:%S')}] 监控: 上次执行于 {self.last_run_time.strftime('%H:%M:%S')}")

    def stop(self):
        """停止任务调度"""
        self.running = False
        print(f"\n任务 [{self.task_name}] 已停止")


def generate_time_range_schedule(start_time: str, end_time: str,
                                interval: int, interval_unit: str = "minutes") -> Dict:
    """
    生成时间范围调度配置

    Args:
        start_time: 开始时间，格式 "HH:MM"
        end_time: 结束时间，格式 "HH:MM"
        interval: 间隔值
        interval_unit: 间隔单位，可选 "minutes" 或 "hours"

    Returns:
        调度配置字典
    """
    return {
        "type": "time_range",
        "start": start_time,
        "end": end_time,
        "interval": interval,
        "interval_unit": interval_unit
    }


def generate_time_points_schedule(time_points: List[str]) -> Dict:
    """
    生成时间点调度配置

    Args:
        time_points: 时间点列表，格式 ["HH:MM", "HH:MM", ...]

    Returns:
        调度配置字典
    """
    return {
        "type": "time_points",
        "times": time_points
    }


def generate_fixed_interval_schedule(interval: int,
                                    interval_unit: str = "hours") -> Dict:
    """
    生成固定间隔调度配置

    Args:
        interval: 间隔值
        interval_unit: 间隔单位，可选 "hours", "minutes", "seconds"

    Returns:
        调度配置字典
    """
    return {
        "type": "fixed_interval",
        "interval": interval,
        "interval_unit": interval_unit
    }


def generate_once_schedule(time: str) -> Dict:
    """
    生成单次执行调度配置

    Args:
        time: 执行时间，格式 "HH:MM"

    Returns:
        调度配置字典
    """
    return {
        "type": "once",
        "time": time
    }


# 使用示例
if __name__ == "__main__":
    # 示例1: 原始需求 - 每天8点到22点，每半小时执行一次
    print("示例1: 原始需求 - 每天8点到22点，每半小时执行一次")
    print("-" * 60)

    scripts = [
        r"E:\powerbi_data\powerbi_data\syys_data_processor\down_syy_all.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\data_clean_syy.py",
        r"E:\powerbi_data\powerbi_data\syys_data_processor\tmsj.py"
    ]

    # 配置1: 时间范围 + 间隔
    schedule_config1 = generate_time_range_schedule(
        start_time="08:00",
        end_time="22:00",
        interval=30,  # 每30分钟
        interval_unit="minutes"
    )

    runner1 = ScheduledTaskRunner("PowerBI数据处理")
    # runner1.start_schedule(scripts, schedule_config1)

    # 示例2: 每天8点、12点、18点执行
    print("\n\n示例2: 每天8点、12点、18点执行")
    print("-" * 60)

    schedule_config2 = generate_time_points_schedule(
        time_points=["08:00", "12:00", "18:00"]
    )

    runner2 = ScheduledTaskRunner("每日三次任务")
    # runner2.start_schedule(scripts, schedule_config2)

    # 示例3: 每2小时执行一次
    print("\n\n示例3: 每2小时执行一次")
    print("-" * 60)

    schedule_config3 = generate_fixed_interval_schedule(
        interval=2,  # 每2小时
        interval_unit="hours"
    )

    runner3 = ScheduledTaskRunner("每2小时任务")
    # runner3.start_schedule(scripts, schedule_config3)

    # 示例4: 每天23点执行一次
    print("\n\n示例4: 每天23点执行一次")
    print("-" * 60)

    schedule_config4 = generate_once_schedule("23:00")

    runner4 = ScheduledTaskRunner("夜间任务")
    # runner4.start_schedule(scripts, schedule_config4)

    # 交互式配置
    print("\n\n请选择调度模式:")
    print("1. 时间范围 + 间隔 (如8-22点，每30分钟)")
    print("2. 指定时间点 (如8点、12点、18点)")
    print("3. 固定间隔 (如每2小时)")
    print("4. 单次执行 (如每天23点)")

    choice = input("\n请输入选择 (1-4): ").strip()

    # 默认使用原始需求的配置
    if choice == "1":
        start_time = input("开始时间 (HH:MM, 默认08:00): ").strip() or "08:00"
        end_time = input("结束时间 (HH:MM, 默认22:00): ").strip() or "22:00"
        interval = int(input("间隔分钟数 (默认30): ").strip() or "30")
        schedule_config = generate_time_range_schedule(start_time, end_time, interval, "minutes")
    elif choice == "2":
        times_input = input("时间点列表 (用逗号分隔, 如08:00,12:00,18:00): ").strip()
        if times_input:
            time_points = [t.strip() for t in times_input.split(",")]
        else:
            time_points = ["08:00", "12:00", "18:00"]
        schedule_config = generate_time_points_schedule(time_points)
    elif choice == "3":
        interval = int(input("间隔值 (默认2): ").strip() or "2")
        unit = input("间隔单位 (hours/minutes/seconds, 默认hours): ").strip() or "hours"
        schedule_config = generate_fixed_interval_schedule(interval, unit)
    elif choice == "4":
        time_point = input("执行时间 (HH:MM, 默认23:00): ").strip() or "23:00"
        schedule_config = generate_once_schedule(time_point)
    else:
        print("使用默认配置: 每天8-22点，每30分钟")
        schedule_config = schedule_config1

    task_name = input("任务名称 (默认: 定时任务): ").strip() or "定时任务"

    runner = ScheduledTaskRunner(task_name)
    runner.start_schedule(scripts, schedule_config)