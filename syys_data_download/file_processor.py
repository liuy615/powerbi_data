import os
import glob
from typing import List
from checker import DataChecker
from logger import DataCheckerLogger


class FileProcessor:
    """文件处理器，用于遍历和处理文件"""

    def __init__(self, logger: DataCheckerLogger):
        self.logger = logger
        self.checker = DataChecker(logger)

    def find_excel_files(self, directory_path: str) -> List[str]:
        """查找目录下所有的Excel文件"""
        excel_patterns = ["*.xlsx", "*.xls"]
        excel_files = []

        for pattern in excel_patterns:
            excel_files.extend(glob.glob(os.path.join(directory_path, pattern)))

        # 去重并排序
        excel_files = sorted(list(set(excel_files)))

        self.logger.logger.info(f"在目录 {directory_path} 中找到 {len(excel_files)} 个Excel文件")
        return excel_files

    def process_directory(self, directory_path: str, sheet_name: str = None):
        """处理指定目录下的所有Excel文件"""
        if not os.path.exists(directory_path):
            self.logger.logger.error(f"目录不存在: {directory_path}")
            return

        excel_files = self.find_excel_files(directory_path)
        self.logger.summary["total_files"] += len(excel_files)

        for file_path in excel_files:
            self.checker.process_excel_file(file_path, sheet_name)

    def process_single_file(self, file_path: str, sheet_name: str = None):
        """处理单个文件"""
        if not os.path.exists(file_path):
            self.logger.logger.error(f"文件不存在: {file_path}")
            return

        self.logger.summary["total_files"] += 1
        self.checker.process_excel_file(file_path, sheet_name)