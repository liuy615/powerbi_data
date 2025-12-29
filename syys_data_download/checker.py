import pandas as pd
import os
from typing import List, Dict, Tuple
from config import Config, RuleChecker
from logger import DataCheckerLogger


class DataChecker:
    """数据检核主类"""

    def __init__(self, logger: DataCheckerLogger):
        self.logger = logger

    def check_headers(self, df_headers: List[str], file_path: str) -> bool:
        """检查表头是否匹配标准表头"""
        # 转换为集合进行比对
        df_headers_set = set(df_headers)
        standard_headers_set = set(Config.STANDARD_HEADERS)

        # 找出差异
        missing_headers = list(standard_headers_set - df_headers_set)
        extra_headers = list(df_headers_set - standard_headers_set)

        if missing_headers or extra_headers:
            self.logger.log_header_error(file_path, missing_headers, extra_headers)
            return False

        return True

    def check_data_row(self, row: pd.Series, row_index: int, file_path: str) -> bool:
        """检查单行数据"""
        has_error = False

        for field, rule_type in Config.CHECK_FIELDS.items():
            if field in row.index:
                value = row[field]
                is_valid, error_msg = RuleChecker.check_field(field, value)

                if not is_valid:
                    self.logger.log_data_error(file_path, row_index, field, value, error_msg)
                    has_error = True

        return not has_error

    def check_dataframe(self, df: pd.DataFrame, file_path: str) -> Tuple[int, int]:
        """检查整个DataFrame"""
        rows_processed = len(df)
        error_rows = 0

        self.logger.summary["total_rows"] += rows_processed

        for idx, row in df.iterrows():
            if not self.check_data_row(row, idx, file_path):
                error_rows += 1

        self.logger.summary["error_rows"] += error_rows

        return rows_processed, error_rows

    def process_excel_file(self, file_path: str, sheet_name: str = "膜升级登记表") -> bool:
        """处理单个Excel文件"""
        try:
            # 检查是否为跳过文件
            file_name = os.path.basename(file_path)
            if file_name in Config.SKIP_FILES:
                self.logger.logger.info(f"跳过文件: {file_name} (在跳过列表中)")
                self.logger.increment_counter("skipped_files")
                return True

            self.logger.logger.info(f"开始处理文件: {file_path}")

            # 读取Excel文件
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
            except Exception as e:
                self.logger.logger.error(f"读取文件失败 {file_path}: {str(e)}")
                return False

            # 检查表头
            if not self.check_headers(df.columns.tolist(), file_path):
                # 表头错误，跳过数据检核
                self.logger.increment_counter("checked_files")
                return False

            # 检核数据
            rows_processed, error_rows = self.check_dataframe(df, file_path)

            # 记录处理结果
            self.logger.log_file_processed(file_path, rows_processed, error_rows)
            self.logger.increment_counter("checked_files")

            return True

        except Exception as e:
            self.logger.logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
            return False