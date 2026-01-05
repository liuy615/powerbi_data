import pandas as pd
import os
from typing import List, Dict, Tuple
from config import Config, RuleChecker
from logger import DataCheckerLogger


class DataChecker:
    """数据检核主类"""

    def __init__(self, logger: DataCheckerLogger):
        self.logger = logger

    def get_data_source_type(self, file_path: str) -> str:
        """根据文件路径判断数据源类型"""
        file_name = os.path.basename(file_path)
        dir_path = os.path.dirname(file_path)

        # 根据目录路径判断数据源类型
        if "投放市场费用" in dir_path:
            return "投放市场费用"
        elif "新车三方延保" in dir_path:
            return "新车三方延保"
        elif "贴膜升级" in dir_path:
            # 贴膜升级目录下的文件需要进一步判断
            if file_name in Config.自店贴膜_FILES:
                return "自店贴膜"
            else:
                return "三方贴膜"
        else:
            # 默认返回三方贴膜
            return "三方贴膜"

    def get_standard_headers(self, data_source_type: str) -> List[str]:
        """根据数据源类型获取标准表头"""
        if data_source_type == "三方贴膜":
            return Config.STANDARD_HEADERS_三方贴膜
        elif data_source_type == "自店贴膜":
            return Config.STANDARD_HEADERS_自店贴膜
        elif data_source_type == "投放市场费用":
            return Config.STANDARD_HEADERS_投放市场费用
        elif data_source_type == "新车三方延保":
            return Config.STANDARD_HEADERS_新车三方延保
        else:
            return []

    def get_template_name(self, data_source_type: str) -> str:
        """根据数据源类型获取模板名称"""
        if data_source_type == "三方贴膜":
            return "三方贴膜模板"
        elif data_source_type == "自店贴膜":
            return "自店贴膜模板"
        elif data_source_type == "投放市场费用":
            return "投放市场费用模板"
        elif data_source_type == "新车三方延保":
            return "新车三方延保模板"
        else:
            return "未知模板"

    def check_headers(self, df_headers: List[str], file_path: str, data_source_type: str) -> bool:
        """检查表头是否匹配标准表头"""
        # 转换为集合进行比对
        df_headers_set = set(df_headers)

        # 根据数据源类型选择标准表头
        standard_headers = self.get_standard_headers(data_source_type)
        template_name = self.get_template_name(data_source_type)

        if not standard_headers:
            self.logger.logger.error(f"未知数据源类型: {data_source_type}")
            return False

        standard_headers_set = set(standard_headers)

        # 找出差异
        missing_headers = list(standard_headers_set - df_headers_set)
        extra_headers = list(df_headers_set - standard_headers_set)

        if missing_headers or extra_headers:
            self.logger.log_header_error(file_path, missing_headers, extra_headers, template_name)
            return False

        return True

    def check_data_row(self, row: pd.Series, row_index: int, file_path: str, data_source_type: str) -> bool:
        """检查单行数据"""
        has_error = False

        # 根据数据源类型选择检查字段
        check_fields = {}
        if data_source_type == "三方贴膜":
            check_fields = Config.CHECK_FIELDS_三方贴膜
        elif data_source_type == "自店贴膜":
            check_fields = Config.CHECK_FIELDS_自店贴膜
        elif data_source_type == "投放市场费用":
            check_fields = Config.CHECK_FIELDS_投放市场费用
        elif data_source_type == "新车三方延保":
            check_fields = Config.CHECK_FIELDS_新车三方延保

        for field, rule_type in check_fields.items():
            if field in row.index:
                value = row[field]
                is_valid, error_msg = RuleChecker.check_field(field, value, data_source_type)

                if not is_valid:
                    self.logger.log_data_error(file_path, row_index, field, value, error_msg)
                    has_error = True

        # 如果是自店贴膜文件，还需要检查日期逻辑：推送日期 < 到店日期
        if data_source_type == "自店贴膜" and "推送日期" in row.index and "到店日期" in row.index:
            is_valid, error_msg = RuleChecker.check_date_logic(row["推送日期"], row["到店日期"])
            if not is_valid:
                self.logger.log_data_error(file_path, row_index, "日期逻辑",
                                           f"推送日期:{row['推送日期']}, 到店日期:{row['到店日期']}",
                                           error_msg)
                has_error = True

        return not has_error

    def check_dataframe(self, df: pd.DataFrame, file_path: str, data_source_type: str) -> Tuple[int, int]:
        """检查整个DataFrame"""
        rows_processed = len(df)
        error_rows = 0

        self.logger.summary["total_rows"] += rows_processed

        for idx, row in df.iterrows():
            if not self.check_data_row(row, idx, file_path, data_source_type):
                error_rows += 1

        self.logger.summary["error_rows"] += error_rows

        return rows_processed, error_rows

    def process_excel_file(self, file_path: str, sheet_name: str = None) -> bool:
        """处理单个Excel文件"""
        try:
            # 判断数据源类型
            data_source_type = self.get_data_source_type(file_path)
            file_name = os.path.basename(file_path)
            template_name = self.get_template_name(data_source_type)

            self.logger.logger.info(f"处理文件: {file_name} (使用{template_name})")

            # 读取Excel文件
            try:
                # 如果没有指定sheet_name，默认使用第一个sheet
                if sheet_name is None:
                    # 使用第一个sheet
                    df = pd.read_excel(file_path, sheet_name=0, dtype=str)
                else:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
            except Exception as e:
                self.logger.logger.error(f"读取文件失败 {file_path}: {str(e)}")
                return False

            # 检查表头
            if not self.check_headers(df.columns.tolist(), file_path, data_source_type):
                # 表头错误，跳过数据检核
                self.logger.increment_counter("checked_files")
                self.logger.summary["header_errors"] += 1
                return False

            # 检核数据
            rows_processed, error_rows = self.check_dataframe(df, file_path, data_source_type)

            # 记录处理结果
            self.logger.log_file_processed(file_path, rows_processed, error_rows)
            self.logger.increment_counter("checked_files")

            return True

        except Exception as e:
            self.logger.logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
            return False