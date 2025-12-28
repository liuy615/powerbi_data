import os
import threading
import requests
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote
import xml.etree.ElementTree as ET
import logging
from typing import List, Dict
import shutil


class Logger:
    """日志类，用于记录下载操作"""

    def __init__(self, log_file: str = "download.log"):
        """初始化日志器"""
        self.log_file = log_file
        self.setup_logging()

    def setup_logging(self):
        """配置日志系统"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def log_download(self, filename: str, success: bool, details: str = ""):
        """记录下载日志"""
        status = "成功" if success else "失败"
        message = f"下载 {filename} - {status}"
        if details:
            message += f" - {details}"

        if success:
            # self.logger.info(message)
            pass
        else:
            self.logger.error(message)

    def log_info(self, message: str):
        """记录一般信息日志"""
        self.logger.info(message)

    def log_error(self, message: str):
        """记录错误日志"""
        self.logger.error(message)

    def log_warning(self, message: str):
        """记录警告日志"""
        self.logger.warning(message)


class WebDAVDownloader:
    """WebDAV下载器类"""

    def __init__(self, base_url: str, username: str, password: str, max_workers: int = 10):
        """初始化下载器"""
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.max_workers = max_workers
        self.logger = Logger()
        self.session = None
        self.lock = threading.Lock()
        self._init_session()
        self._visited_urls = set()  # 用于记录已访问的URL，避免重复

    def _init_session(self):
        """初始化会话和认证"""
        try:
            self.session = requests.Session()
            # 使用基本认证
            self.session.auth = HTTPBasicAuth(self.username, self.password)

            # 设置通用请求头
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': '*/*',
                'Connection': 'keep-alive'
            })

            # 测试连接
            test_response = self.session.request('PROPFIND', self.base_url, headers={'Depth': '0'}, timeout=10)
            if test_response.status_code in [200, 207, 401]:
                self.logger.log_info("会话初始化成功")
            else:
                self.logger.log_warning(f"测试连接返回状态码: {test_response.status_code}")

        except Exception as e:
            self.logger.log_error(f"会话初始化失败: {e}")
            raise

    def clear_directory(self, directory: str) -> bool:
        """清空指定目录中的所有文件和子目录"""
        try:
            if not os.path.exists(directory):
                self.logger.log_info(f"目录不存在，无需清空: {directory}")
                return True

            self.logger.log_info(f"开始清空目录: {directory}")

            # 遍历目录中的所有项目
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # 删除文件或符号链接
                        # self.logger.log_info(f"删除文件: {filename}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # 递归删除子目录
                        self.logger.log_info(f"删除目录: {filename}")
                except Exception as e:
                    self.logger.log_error(f"删除 {file_path} 失败: {e}")
                    return False

            self.logger.log_info(f"目录清空完成: {directory}")
            return True

        except Exception as e:
            self.logger.log_error(f"清空目录 {directory} 时发生错误: {e}")
            return False

    def get_directory_listing(self, url: str) -> List[str]:
        """使用 PROPFIND 方法获取 WebDAV 目录列表"""
        # 检查是否已经访问过这个URL，避免重复访问
        normalized_url = self._normalize_url(url)
        if normalized_url in self._visited_urls:
            # self.logger.log_info(f"跳过已访问的目录: {url}")
            return []

        self._visited_urls.add(normalized_url)

        try:
            headers = {
                'Depth': '1',
                'Content-Type': 'application/xml'
            }

            response = self.session.request('PROPFIND', url, headers=headers, timeout=30)
            # self.logger.log_info(f"PROPFIND 响应状态码: {response.status_code}")

            if response.status_code == 207:
                return self.parse_webdav_response(response.text, url)
            elif response.status_code == 401:
                self.logger.log_error("认证失败: 401 Unauthorized")
                self.logger.log_error("请检查用户名和密码是否正确")
                return []
            elif response.status_code == 404:
                self.logger.log_error("目录不存在: 404 Not Found")
                return []
            else:
                self.logger.log_error(f"HTTP错误: {response.status_code}")
                return []

        except requests.exceptions.RequestException as e:
            self.logger.log_error(f"网络请求失败: {e}")
            return []
        except Exception as e:
            self.logger.log_error(f"获取目录列表失败: {e}")
            return []

    def _normalize_url(self, url: str) -> str:
        """标准化URL，去除尾部斜杠和URL编码差异"""
        # 去除尾部斜杠
        normalized = url.rstrip('/')
        # 解码URL编码字符
        normalized = unquote(normalized)
        return normalized

    def parse_webdav_response(self, xml_content: str, base_url: str) -> List[str]:
        """解析 WebDAV 的 XML 响应"""
        files = []

        try:
            # 解析 XML
            root = ET.fromstring(xml_content)

            # WebDAV 命名空间
            ns = {'d': 'DAV:'}

            # 查找所有 response 元素
            for response_elem in root.findall('.//d:response', ns):
                href_elem = response_elem.find('d:href', ns)
                if href_elem is None:
                    continue

                href = href_elem.text

                # 将相对路径转换为完整 URL
                if href.startswith('/'):
                    full_url = self.base_url + href
                else:
                    full_url = href

                # 标准化URL
                normalized_full_url = self._normalize_url(full_url)

                # 跳过当前目录
                if normalized_full_url == self._normalize_url(base_url):
                    continue

                # 检查是否是集合（目录）
                propstat = response_elem.find('d:propstat', ns)
                if propstat is not None:
                    prop = propstat.find('d:prop', ns)
                    if prop is not None:
                        resourcetype = prop.find('d:resourcetype', ns)
                        is_collection = resourcetype is not None and resourcetype.find('d:collection', ns) is not None

                        if is_collection:
                            # 如果是目录，递归处理
                            # self.logger.log_info(f"发现子目录: {full_url}")
                            sub_files = self.get_directory_listing(full_url)
                            files.extend(sub_files)
                        else:
                            # 如果是文件，添加到列表
                            # 检查是否已经添加过这个文件（通过标准化URL）
                            if normalized_full_url not in [self._normalize_url(f) for f in files]:
                                files.append(full_url)
                            # else:
                            #     self.logger.log_info(f"跳过重复文件: {full_url}")

        except ET.ParseError as e:
            self.logger.log_error(f"XML解析错误: {e}")
        except Exception as e:
            self.logger.log_error(f"解析 WebDAV 响应失败: {e}")

        return files

    def download_file(self, file_url: str, local_dir: str) -> bool:
        """下载单个文件"""
        try:
            # 从URL提取文件名
            filename = os.path.basename(unquote(file_url))
            local_path = os.path.join(local_dir, filename)

            # 创建本地目录（如果不存在）
            os.makedirs(local_dir, exist_ok=True)

            # self.logger.log_info(f"开始下载: {filename}")

            # 下载文件
            response = self.session.get(file_url, stream=True, timeout=60)

            if response.status_code == 401:
                self.logger.log_download(filename, False, "下载认证失败")
                return False
            elif response.status_code != 200:
                self.logger.log_download(filename, False, f"HTTP状态码: {response.status_code}")
                return False

            response.raise_for_status()

            # 写入文件
            with open(local_path, 'wb') as f:
                downloaded_size = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)

            file_size = os.path.getsize(local_path)
            # self.logger.log_info(f"下载完成: {filename} ({self.format_size(file_size)})")
            self.logger.log_download(filename, True, f"文件大小: {self.format_size(file_size)}")
            return True

        except Exception as e:
            filename = os.path.basename(unquote(file_url))
            self.logger.log_download(filename, False, str(e))
            return False

    def filter_files_by_pattern(self, files: List[str], search_pattern: str, extensions: List[str]) -> List[str]:
        """根据搜索模式和扩展名过滤文件"""
        filtered_files = []
        seen_filenames = set()  # 用于记录已处理的文件名，避免重复

        for file_url in files:
            filename = os.path.basename(unquote(file_url))

            # 检查是否已经处理过这个文件名
            if filename in seen_filenames:
                self.logger.log_info(f"跳过重复文件名: {filename}")
                continue

            seen_filenames.add(filename)

            # 检查文件扩展名
            ext_matches = any(filename.lower().endswith(ext.lower()) for ext in extensions) if extensions else True

            # 检查搜索模式
            pattern_matches = search_pattern.lower() in filename.lower() if search_pattern else True

            if ext_matches and pattern_matches:
                filtered_files.append(file_url)
                # self.logger.log_info(f"找到匹配文件: {filename}")

        return filtered_files

    def format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes == 0:
            return "0B"

        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1

        return f"{size_bytes:.2f} {size_names[i]}"

    def download_from_url(self, target_url: str, local_dir: str,
                          search_pattern: str = "", file_extensions: List[str] = None) -> Dict[str, int]:
        """从指定URL下载文件"""
        if file_extensions is None:
            file_extensions = [".xlsx", ".csv"]

        # 重置已访问URL集合，避免多个任务间的干扰
        self._visited_urls.clear()

        self.logger.log_info(f"开始处理目录: {target_url}")
        self.logger.log_info(f"本地保存路径: {local_dir}")
        self.logger.log_info(f"搜索模式: {search_pattern}")
        self.logger.log_info(f"文件扩展名: {file_extensions}")

        # 获取目录列表
        files = self.get_directory_listing(target_url)

        if not files:
            self.logger.log_warning("没有找到文件")
            # 即使没有找到文件，也清空目录以确保一致性
            self.clear_directory(local_dir)
            return {"total": 0, "success": 0, "failed": 0}

        # 过滤文件
        filtered_files = self.filter_files_by_pattern(files, search_pattern, file_extensions)

        if not filtered_files:
            self.logger.log_warning("没有找到匹配的文件")
            # 如果没有匹配的文件，也清空目录
            self.clear_directory(local_dir)
            return {"total": 0, "success": 0, "failed": 0}

        # 在开始下载前清空目录
        if not self.clear_directory(local_dir):
            self.logger.log_error("清空目录失败，停止下载")
            return {"total": len(filtered_files), "success": 0, "failed": len(filtered_files)}

        self.logger.log_info(f"找到 {len(filtered_files)} 个匹配文件，开始下载...")

        # 使用线程池下载
        success_count = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self.download_file, file_url, local_dir): file_url
                for file_url in filtered_files
            }

            for future in as_completed(future_to_file):
                file_url = future_to_file[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    self.logger.log_error(f"文件 {file_url} 下载异常: {e}")

        failed_count = len(filtered_files) - success_count
        result = {
            "total": len(filtered_files),
            "success": success_count,
            "failed": failed_count
        }

        self.logger.log_info(f"下载完成! 成功: {success_count}/{len(filtered_files)}, 失败: {failed_count}")
        return result


class DownloadManager:
    """下载管理器，支持多个目录下载"""

    def __init__(self, base_url: str, username: str, password: str, max_workers: int = 10):
        """初始化下载管理器"""
        self.downloader = WebDAVDownloader(base_url, username, password, max_workers)

    def download_multiple_directories(self, download_tasks: List[Dict]) -> Dict[str, Dict]:
        """下载多个目录"""
        results = {}

        for i, task in enumerate(download_tasks, 1):
            self.downloader.logger.log_info(f"开始处理第 {i}/{len(download_tasks)} 个下载任务")

            target_url = task.get('target_url')
            local_dir = task.get('local_dir')
            search_pattern = task.get('search_pattern', '')
            file_extensions = task.get('file_extensions', ['.xlsx', '.csv'])

            try:
                result = self.downloader.download_from_url(
                    target_url, local_dir, search_pattern, file_extensions
                )
                results[target_url] = result
            except Exception as e:
                self.downloader.logger.log_error(f"处理任务失败 {target_url}: {e}")
                results[target_url] = {"total": 0, "success": 0, "failed": 0, "error": str(e)}

        return results


def test_connection(base_url: str, username: str, password: str) -> bool:
    """测试连接和认证"""
    try:
        session = requests.Session()
        session.auth = HTTPBasicAuth(username, password)

        # 发送一个简单的 PROPFIND 请求测试连接
        response = session.request('PROPFIND', base_url, headers={'Depth': '0'}, timeout=10)

        if response.status_code in [200, 207]:
            print("✓ 连接测试成功")
            return True
        elif response.status_code == 401:
            print("✗ 认证失败: 401 Unauthorized")
            print("请检查用户名和密码是否正确")
            return False
        else:
            print(f"✗ 连接测试失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        print(f"✗ 连接测试异常: {e}")
        return False


def main():
    """主函数"""
    # 配置信息
    CONFIG = {
        'WEBDAV_BASE_URL': 'http://222.212.88.126:5005',
        'WEBDAV_USERNAME': 'wangdie',
        'WEBDAV_PASSWORD': 'wd#123456',
        'MAX_WORKERS': 5  # 减少工作线程数以避免过多并发请求
    }

    print("正在测试 WebDAV 连接...")
    if not test_connection(CONFIG['WEBDAV_BASE_URL'], CONFIG['WEBDAV_USERNAME'], CONFIG['WEBDAV_PASSWORD']):
        print("连接测试失败，程序退出")
        return

    # 创建下载管理器
    try:
        manager = DownloadManager(
            CONFIG['WEBDAV_BASE_URL'],
            CONFIG['WEBDAV_USERNAME'],
            CONFIG['WEBDAV_PASSWORD'],
            CONFIG['MAX_WORKERS']
        )
        print("✓ 下载管理器初始化成功")
    except Exception as e:
        print(f"✗ 下载管理器初始化失败: {e}")
        return

    # 定义多个下载任务
    base_url = "http://222.212.88.126:5005/s001/data/udata/real"
    download_tasks = [
        {
            'target_url': f"{base_url}/信息部/售前数据/市场费用登记表/当期数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/投放市场费用",
            'search_pattern': "市场费用登记表",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/售前数据/衍生产品台账/当期数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/衍生产品",
            'search_pattern': "衍生产品登记表",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/售前数据/特殊事项登记台账/当期数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/特殊事项收入",
            'search_pattern': "特殊事项登记台账",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/售前数据/订车登记台账/当期数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/订车台账",
            'search_pattern': "订车登记台账",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/售前数据/新车延保/当期数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/新车三方延保",
            'search_pattern': "新车延保登记表",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/售前数据/贴膜升级/当期数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/贴膜升级",
            'search_pattern': "贴膜升级登记表",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/售前数据/精品销售/当期数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/精品销售",
            'search_pattern': "精品销售台账",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/数据部基盘/展厅数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/基盘表",
            'search_pattern': "A",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': fr"{base_url}/信息部/厂家系统_车易云/厂家系统数据/【原始数据】采购总量/2025",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/厂家采购",
            'search_pattern': "明细",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/信息部内部文件/收集数据/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/收集文件",
            'search_pattern': "表",
            'file_extensions': [".xlsx", ".csv"]
        },
        {
            'target_url': f"{base_url}/信息部/信息部内部文件/车易云-新车保险台账/",
            'local_dir': "E:/powerbi_data/看板数据/私有云文件本地/新车保险台账",  # 修改为独立子目录
            'search_pattern': "明细",
            'file_extensions': [".xlsx", ".csv"]
        },
    ]

    # 执行批量下载
    print("\n开始批量下载任务...")
    manager.download_multiple_directories(download_tasks)


if __name__ == "__main__":
    main()