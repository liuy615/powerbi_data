import os
import threading
from webdav3.client import Client
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from urllib.parse import unquote

# 配置极空间 WebDAV 信息
options = {
    'webdav_hostname': 'http://222.212.88.126:5005',
    'webdav_login': 'wangdie',
    'webdav_password': '13111855638'
}

# 线程池最大线程数
MAX_WORKERS = 10
# 创建锁
lock = threading.Lock()


class WebDAVDownloader:
    def __init__(self, options, max_workers=10):
        self.client = Client(options)
        self.max_workers = max_workers
        self.downloaded_files = 0
        self.total_files = 0

    def list_directory(self, remote_path):
        """列出远程目录下的所有文件"""
        try:
            files = self.client.list(remote_path)
            file_list = []

            for file in files:
                if file == '.' or file == '..':
                    continue

                full_path = f"{remote_path.rstrip('/')}/{file}"

                # 如果是目录，递归列出
                if self.client.is_dir(full_path):
                    sub_files = self.list_directory(full_path)
                    file_list.extend(sub_files)
                else:
                    file_list.append(full_path)

            return file_list
        except Exception as e:
            print(f"列出目录 {remote_path} 时出错: {e}")
            return []

    def get_file_size(self, remote_path):
        """获取远程文件大小"""
        try:
            return self.client.info(remote_path).get('size', 0)
        except:
            return 0

    def download_file(self, remote_path, local_dir='./downloads'):
        """下载单个文件"""
        try:
            # 创建本地目录结构
            relative_path = remote_path.lstrip('/')
            local_path = os.path.join(local_dir, relative_path)
            local_dir_path = os.path.dirname(local_path)

            os.makedirs(local_dir_path, exist_ok=True)

            # 检查文件是否已存在
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                remote_size = self.get_file_size(remote_path)

                if local_size == remote_size:
                    with lock:
                        self.downloaded_files += 1
                        print(f"[{self.downloaded_files}/{self.total_files}] 已存在: {relative_path}")
                    return True

            # 下载文件
            self.client.download_sync(remote_path=remote_path, local_path=local_path)

            with lock:
                self.downloaded_files += 1
                file_size = self.get_file_size(remote_path)
                size_str = self._format_size(file_size)
                print(f"[{self.downloaded_files}/{self.total_files}] 下载成功: {relative_path} ({size_str})")

            return True

        except Exception as e:
            with lock:
                print(f"下载失败 {remote_path}: {e}")
            return False

    def _format_size(self, size_bytes):
        """格式化文件大小显示"""
        if size_bytes == 0:
            return "0B"

        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1

        return f"{size_bytes:.2f} {size_names[i]}"

    def download_directory(self, remote_path, local_dir='./downloads', file_extensions=None):
        """下载整个目录"""
        print(f"正在扫描目录: {remote_path}")
        all_files = self.list_directory(remote_path)

        if file_extensions:
            all_files = [f for f in all_files if any(f.lower().endswith(ext.lower()) for ext in file_extensions)]

        self.total_files = len(all_files)
        self.downloaded_files = 0

        if self.total_files == 0:
            print("没有找到可下载的文件")
            return

        print(f"找到 {self.total_files} 个文件，开始下载...")

        # 使用线程池下载
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有下载任务
            future_to_file = {
                executor.submit(self.download_file, file_path, local_dir): file_path
                for file_path in all_files
            }

            # 等待所有任务完成
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"文件 {file_path} 下载异常: {e}")

        print(f"\n下载完成! 成功下载 {self.downloaded_files}/{self.total_files} 个文件")


def main():
    # 创建下载器实例
    downloader = WebDAVDownloader(options, MAX_WORKERS)

    # 配置下载参数
    remote_directory = input("请输入要下载的远程目录路径 (例如: /): ").strip() or "/"
    local_directory = input("请输入本地保存目录 (默认: ./downloads): ").strip() or "./downloads"

    # 可选：按文件类型过滤
    filter_ext = input("请输入要下载的文件扩展名 (用逗号分隔, 例如: .jpg,.png,.mp4, 留空下载所有文件): ").strip()
    file_extensions = None
    if filter_ext:
        file_extensions = [ext.strip() for ext in filter_ext.split(',') if ext.strip()]
        print(f"只下载以下类型的文件: {', '.join(file_extensions)}")

    print("\n开始下载...")
    print("=" * 50)

    try:
        downloader.download_directory(
            remote_path=remote_directory,
            local_dir=local_directory,
            file_extensions=file_extensions
        )
    except KeyboardInterrupt:
        print("\n用户中断下载")
    except Exception as e:
        print(f"下载过程中发生错误: {e}")


if __name__ == "__main__":
    main()