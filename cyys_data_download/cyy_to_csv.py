from common_code import RequestFunction, WriteLog
from concurrent.futures import ThreadPoolExecutor, as_completed
from webdav3.client import Client
from time import sleep
import random
import copy
import requests
from typing import Dict, Any, Optional, Tuple, Set, List
import json
from datetime import datetime
import os
import csv

import sys
project_root = r"E:\powerbi_data"
sys.path.insert(0, project_root)
from config.cyys_data_download.config import Config, DataCenter, get_time_range


def should_fetch_next_page(current_items: list, page_size: int) -> bool:
    """
    判断是否满足翻页条件

    Args:
        current_items: 当前页获取的数据列表
        page_size: 每页预期数据量大小

    Returns:
        满足翻页条件返回True，否则返回False
    """
    if not current_items:
        return False
    return len(current_items) >= page_size


def flatten_data(data_list: List[Dict], parent_key: str = '', sep: str = '_') -> Tuple[List[Dict], List[str]]:
    """
    将嵌套的字典列表展平，用于处理API返回的嵌套结构数据

    Args:
        data_list: 嵌套字典的列表
        parent_key: 父键名，用于递归构建键名
        sep: 键名分隔符

    Returns:
        展平后的数据列表和所有字段名集合
    """
    flattened = []
    all_fields: Set[str] = set()

    for item in data_list:
        if not isinstance(item, dict):
            continue  # 跳过非字典类型的项

        flat_item = {}

        def _flatten(d: Dict, current_key: str = '') -> None:
            nonlocal flat_item, all_fields
            full_key = f"{parent_key}{sep}{current_key}" if parent_key and current_key else current_key or parent_key

            for k, v in d.items():
                new_key = f"{full_key}{sep}{k}" if full_key else k
                if isinstance(v, dict) and v:
                    _flatten(v, new_key)
                else:
                    flat_item[new_key] = v
                    all_fields.add(new_key)

        _flatten(item)
        flattened.append(flat_item)

    return flattened, list(all_fields)


def send_wechat_notification(content: str) -> bool:
    """
    发送企业微信机器人通知

    Args:
        content: 通知内容

    Returns:
        发送成功返回True，否则返回False
    """
    if not Config.WECHAT_WEBHOOK:
        print("未配置企业微信webhook，无法发送通知")
        return False

    try:
        data = {
            "msgtype": "text",
            "text": {
                "content": f"数据同步提醒：{content}\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
        }
        response = requests.post(Config.WECHAT_WEBHOOK, json=data, timeout=10)
        response.raise_for_status()
        result = response.json()

        if result.get("errcode") == 0:
            print("企微通知发送成功")
            return True
        else:
            print(f"企微通知发送失败: {result.get('errmsg')}")
            return False
    except Exception as e:
        print(f"发送企微通知出错: {str(e)}")
        return False


class CSVWriter:
    """CSV文件写入类"""

    def __init__(self, output_dir: str = "./output"):
        """
        初始化CSV写入器

        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def write_csv(self, data_list: List[Dict], api_name: str) -> Tuple[str, int]:
        """
        将数据写入CSV文件

        Args:
            data_list: 原始数据列表（可能包含嵌套）
            api_name: API名称，用于生成文件名

        Returns:
            (文件路径, 写入行数)
        """
        if not data_list:
            print(f"{api_name} 无数据，跳过写入")
            return "", 0

        # 展平数据
        flattened_data, fieldnames = flatten_data(data_list)

        if not flattened_data:
            print(f"{api_name} 展平后无数据，跳过写入")
            return "", 0

        # 生成文件名：任务名_日期.csv
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"{api_name}_{date_str}.csv"
        filepath = os.path.join(self.output_dir, filename)

        # 写入CSV
        try:
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(flattened_data)

            rows = len(flattened_data)
            print(f"{api_name} 数据已写入 {filepath}，共 {rows} 行")
            return filepath, rows
        except Exception as e:
            error_msg = f"{api_name} CSV写入失败: {str(e)}"
            print(error_msg)
            raise Exception(error_msg) from e


class DataSyncManager:
    """数据同步管理器"""

    def __init__(self, time_option: int = 2):
        """
        初始化数据同步管理器

        Args:
            time_option: 时间选项 1-当日 2-当月 3-全部数据
        """
        self.logger = WriteLog()
        self.requester = RequestFunction()
        self.token = self.get_token()
        self.time_option = time_option

        # 初始化CSV写入器
        self.csv_writer = CSVWriter(output_dir=Config.CSV_OUTPUT_DIR if hasattr(Config, 'CSV_OUTPUT_DIR') else "./output")

        # 初始化WebDAV客户端（可选，如需上传可后续扩展）
        self.client = Client(Config.WEBDAV_CONFIG) if hasattr(Config, 'WEBDAV_CONFIG') else None

    def get_token(self) -> str:
        """获取API访问令牌"""
        try:
            response = self.requester.request(
                url=f'{Config.API_BASE_URL}/api/Values/GetToken',
                method='GET',
                params={"Account": DataCenter.USER, "PassWord": DataCenter.PWD},
            )
            return response['data']['Msg']
        except Exception as e:
            error_msg = f"获取Token失败: {str(e)}"
            self.logger.log_error(error_msg)
            raise Exception(error_msg)

    def fetch_single_api(self, task_name: str, api_config: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        处理单个API的所有分页请求

        Args:
            task_name: 任务名称
            api_config: API配置

        Returns:
            包含所有数据的字典
        """
        url = api_config['url']
        method = api_config['method']
        payload = copy.deepcopy(api_config.get('payload', {}))

        all_data = []
        page_number = payload.get('PageNumber', 1)
        page_size = payload.get('PageSize', Config.PAGE_SIZE)

        try:
            while True:
                if 'PageNumber' in payload:
                    payload['PageNumber'] = page_number

                kwargs = {}
                if method.upper() == 'GET':
                    kwargs['params'] = payload
                else:
                    kwargs['json_data'] = payload

                payload['Token'] = self.token

                response = self.requester.request(
                    url=url,
                    method=method,
                    **kwargs
                )

                if not response or not isinstance(response, dict):
                    raise Exception(f"{task_name} 第{page_number}页请求返回无效数据")

                # 提取数据
                page_data = response['data']
                if 'Msg' in page_data:
                    page_data = page_data['Msg']['Model']
                else:
                    page_data = page_data['Data']

                # 处理不同类型的数据结构
                if isinstance(page_data, dict):
                    page_data = page_data.get('list', page_data.get('items', []))

                if not isinstance(page_data, list):
                    raise Exception(f"{task_name} 第{page_number}页返回数据格式不正确，预期列表类型")

                all_data.extend(page_data)

                if should_fetch_next_page(page_data, page_size):
                    page_number += 1
                else:
                    break

                # 添加随机延迟，避免请求过于频繁
                sleep(random.uniform(*Config.REQUEST_DELAY))
        except Exception as e:
            raise Exception(f"API请求过程出错: {str(e)}") from e

        return {task_name: all_data}

    def run_apis(self) -> Dict[str, List[Dict]]:
        """使用线程池并发处理API请求"""
        results = {}
        apis = DataCenter.get_apis(self.time_option)
        total_rows = 0  # 统计总写入行数
        success_tasks = 0  # 统计成功的任务数
        failed_tasks = []  # 记录失败的任务及详细错误
        file_notifications = []  # 记录每个任务生成的文件信息

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_task = {
                executor.submit(
                    self.fetch_single_api,
                    task_name,
                    api_config
                ): task_name
                for task_name, api_config in apis.items()
            }

            for future in as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results.update(result)
                    data = result[task_name]
                    data_count = len(data)
                    self.logger.log_info(f"任务 {task_name} 完成, 获取{data_count}条数据")

                    # 写入CSV
                    filepath, rows = self.csv_writer.write_csv(data, task_name)
                    if filepath:
                        total_rows += rows
                        success_tasks += 1
                        file_notifications.append(f"{task_name}: {rows}行 -> {os.path.basename(filepath)}")
                    else:
                        # 无数据写入也算成功，但不计入总行数
                        success_tasks += 1
                        file_notifications.append(f"{task_name}: 无数据")

                except Exception as e:
                    # 捕获API请求和处理过程中的所有异常
                    error_msg = f"任务 {task_name} 处理失败: {str(e)}"
                    self.logger.log_error(error_msg)
                    failed_tasks.append(error_msg)

        # 所有任务完成后汇总并发送企微通知
        completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_range_desc = {
            1: "当日",
            2: "当月",
            3: "25年04月以后"
        }.get(self.time_option, f"{self.time_option}")

        notification_content = (
            f"数据同步任务已完成\n"
            f"完成时间: {completion_time}\n"
            f"时间范围: {time_range_desc}\n"
            f"总任务数: {len(apis)}\n"
            f"成功任务数: {success_tasks}\n"
            f"失败任务数: {len(failed_tasks)}\n"
            f"总写入行数: {total_rows}\n"
        )

        # 添加各任务写入文件信息
        if file_notifications:
            notification_content += "\n各任务写入详情:\n" + "\n".join(file_notifications)

        # 添加失败任务详细信息
        if failed_tasks:
            notification_content += "\n失败任务详情:\n" + "\n".join(failed_tasks)

        # 发送汇总通知
        send_wechat_notification(notification_content)

        return results

    def close(self) -> None:
        """关闭资源（本实现中无需要关闭的资源，保留以兼容原调用）"""
        pass


# 程序入口
if __name__ == "__main__":
    # 1: 当日数据, 2: 当月数据, 3: 输入'01'-'12',获取当月的数据
    sync_manager = None
    try:
        sync_manager = DataSyncManager(time_option=1)  # 默认下载当月数据
        sync_manager.run_apis()
    except Exception as e:
        error_msg = f"数据同步主程序异常: {str(e)}"
        print(error_msg)
        send_wechat_notification(error_msg)
    finally:
        if sync_manager:
            sync_manager.close()