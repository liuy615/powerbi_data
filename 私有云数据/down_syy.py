from webdav3.client import Client
import os
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from openpyxl import Workbook
import threading
from typing import List, Tuple, Optional


class WebDAVDownloader:
    """
    WebDAV文件下载器：通过WebDAV协议从远程服务器下载文件，支持多线程并行下载，
    记录下载日志并生成成功/失败结果报告
    """

    def __init__(self,
                 base_dir: str = r"E:/powerbi_data/看板数据/辅助文件",
                 log_dir: str = r"E:/powerbi_data/powerbi_data/data/私有云日志/log",
                 webdav_hostname: str = 'http://222.212.88.126:5005',
                 webdav_login: str = 'wangdie',
                 webdav_password: str = 'wd#123456',
                 max_workers: int = 10):
        """
        初始化下载器配置，集中管理输入输出路径和WebDAV连接信息

        :param base_dir: 基础目录，输入文件（Excel路径配置）和输出文件（日志、结果）均在此目录下
        :param webdav_hostname: WebDAV服务器地址
        :param webdav_login: WebDAV登录用户名
        :param webdav_password: WebDAV登录密码
        :param max_workers: 线程池最大线程数
        """
        # 1. 路径集中管理（输入输出文件放在一起）
        self.base_dir = base_dir
        self.log_dir = log_dir
        self.excel_path = os.path.join(base_dir, "down_syy_mapping.xlsx")  # 路径配置Excel
        self.log_path = os.path.join(self.log_dir, "download_log.txt")  # 日志文件
        self.result_path = os.path.join(base_dir, "download_result.xlsx")  # 结果文件

        # 2. WebDAV配置
        self.webdav_options = {
            'webdav_hostname': webdav_hostname,
            'webdav_login': webdav_login,
            'webdav_password': webdav_password
        }
        self.client: Optional[Client] = None  # WebDAV客户端（延迟初始化）

        # 3. 线程配置
        self.max_workers = max_workers
        self.lock = threading.Lock()  # 线程安全锁
        self.success_list: List[Tuple[str, str]] = []  # 成功下载列表
        self.failure_list: List[Tuple[str, str, str]] = []  # 失败下载列表

        # 4. 初始化环境
        self._init_environment()

    def _init_environment(self) -> None:
        """初始化运行环境：创建必要目录"""
        # 确保日志目录存在
        os.makedirs(self.log_dir, exist_ok=True)
        # 确保基础目录存在
        os.makedirs(self.base_dir, exist_ok=True)
        print(f"初始化完成 - 基础目录: {self.base_dir}")

    def _setup_logging(self) -> None:
        """配置日志系统，记录下载过程"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # 避免重复添加处理器
        if logger.handlers:
            return

        # 创建CSV日志处理器
        csv_handler = logging.FileHandler(self.log_path, mode='w', encoding='utf-8')
        csv_handler.setLevel(logging.INFO)

        # 定义CSV日志格式（时间,级别,消息）
        csv_formatter = logging.Formatter('%(asctime)s,%(levelname)s,%(message)s')
        csv_handler.setFormatter(csv_formatter)

        logger.addHandler(csv_handler)

    def _read_paths_from_excel(self) -> Tuple[List[str], List[str]]:
        """
        从Excel文件读取远程路径和本地路径

        :return: 远程路径列表和本地路径列表
        """
        if not os.path.exists(self.excel_path):
            print(f"警告: 配置文件不存在: {self.excel_path}")
            return [], []

        try:
            df = pd.read_excel(self.excel_path)
            # 读取第1列（远程路径）和第3列（本地路径）（原逻辑保持）
            remote_paths = df.iloc[:, 0].dropna().astype(str).tolist()
            local_paths = df.iloc[:, 2].dropna().astype(str).tolist()

            # 确保路径数量一致
            if len(remote_paths) != len(local_paths):
                print(f"警告: 远程路径与本地路径数量不匹配（{len(remote_paths)} vs {len(local_paths)}）")
                # 取最小长度继续执行
                min_len = min(len(remote_paths), len(local_paths))
                remote_paths = remote_paths[:min_len]
                local_paths = local_paths[:min_len]

            print(f"加载 {len(remote_paths)} 条下载路径")
            return remote_paths, local_paths
        except Exception as e:
            print(f"读取Excel路径失败: {str(e)}")
            return [], []

    def _download_single_file(self, remote_path: str, local_path: str) -> None:
        """
        下载单个文件（线程任务），线程安全地更新成功/失败列表

        :param remote_path: 远程文件路径
        :param local_path: 本地保存路径
        """
        if not self.client:
            print(f"错误: WebDAV客户端未初始化，跳过 {remote_path}")
            return

        try:
            # 确保本地目录存在
            local_dir = os.path.dirname(local_path)
            os.makedirs(local_dir, exist_ok=True)

            # 执行下载
            self.client.download_sync(remote_path=remote_path, local_path=local_path)

            # 验证文件是否下载成功
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                # 线程安全地记录成功
                with self.lock:
                    self.success_list.append((remote_path, local_path))
                print(f"✓ {os.path.basename(remote_path)}")
            else:
                raise Exception("文件下载后为空或不存在")

        except Exception as e:
            # 线程安全地记录失败
            error_msg = str(e)
            with self.lock:
                self.failure_list.append((remote_path, local_path, error_msg))
            print(f"✗ {os.path.basename(remote_path)}")

    def _save_download_result(self) -> None:
        """保存下载结果到Excel（成功/失败分表）"""
        if not self.success_list and not self.failure_list:
            print("没有下载结果需要保存")
            return

        try:
            wb = Workbook()

            # 成功工作表
            success_sheet = wb.active
            success_sheet.title = "成功"
            success_sheet.append(["远程路径", "本地路径"])
            for remote, local in self.success_list:
                success_sheet.append([remote, local])

            # 失败工作表
            if self.failure_list:
                failure_sheet = wb.create_sheet("失败")
                failure_sheet.append(["远程路径", "本地路径", "错误信息"])
                for remote, local, error in self.failure_list:
                    failure_sheet.append([remote, local, error])

            # 保存结果
            wb.save(self.result_path)
            print(f"结果保存: 成功 {len(self.success_list)} 个, 失败 {len(self.failure_list)} 个")
        except Exception as e:
            print(f"保存结果失败: {str(e)}")

    def run(self) -> None:
        """执行完整下载流程：初始化客户端 -> 配置日志 -> 读取路径 -> 多线程下载 -> 保存结果"""
        try:
            print("=" * 40)
            print("WebDAV文件下载器开始运行")
            print("=" * 40)

            # 1. 初始化WebDAV客户端
            print("初始化WebDAV客户端...")
            self.client = Client(self.webdav_options)
            self.client.verify = False  # 禁用SSL验证（根据服务器情况调整）

            # 2. 配置日志
            self._setup_logging()

            # 3. 读取下载路径
            remote_paths, local_paths = self._read_paths_from_excel()

            if not remote_paths or not local_paths:
                print("没有有效的下载路径，程序结束")
                return

            # 4. 多线程下载
            print(f"\n开始下载: {len(remote_paths)} 个文件")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有下载任务
                futures = [
                    executor.submit(self._download_single_file, remote, local)
                    for remote, local in zip(remote_paths, local_paths)
                ]

                # 等待所有任务完成，不抛出异常
                for future in futures:
                    try:
                        future.result()  # 触发可能的异常，但会被内部捕获
                    except Exception:
                        pass  # 异常已在_download_single_file中处理

            # 5. 保存结果
            print("\n下载完成，保存结果...")
            self._save_download_result()

            print("\n" + "=" * 40)
            print("下载流程完成")
            print("=" * 40)

        except Exception as e:
            print(f"\n错误: {str(e)}")


if __name__ == "__main__":
    # 实例化下载器（默认使用基础目录，可自定义）
    downloader = WebDAVDownloader()
    # 执行下载流程
    downloader.run()