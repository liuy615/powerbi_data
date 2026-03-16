import os
import glob
import pandas as pd
from typing import List, Dict
from field_checker import FieldChecker
from data_checker import DataChecker
from logger import DataCheckerLogger
from config import Config


class FileProcessor:
    """文件处理器，用于遍历和处理文件"""

    def __init__(self, logger: DataCheckerLogger):
        self.logger = logger
        self.field_checker = FieldChecker()
        self.data_checker = DataChecker(valid_store_names=Config.VALID_STORE_NAMES)

    def match_filters(self, file_path: str, filters: List[str]) -> bool:
        """
        检查文件是否符合筛选条件
        
        参数:
            file_path: 文件路径
            filters: 筛选条件列表（文件名需包含的关键词）
            
        返回:
            是否符合所有筛选条件
        """
        file_name = os.path.basename(file_path)
        
        for filter_str in filters:
            if filter_str not in file_name:
                return False
        
        return True

    def find_files(self, directory_path: str, file_filters: List[str]) -> List[str]:
        """
        查找目录下符合筛选条件的文件
        
        参数:
            directory_path: 目录路径
            file_filters: 筛选条件列表
            
        返回:
            符合条件的文件路径列表
        """
        # 确定文件扩展名
        extensions = []
        for filter_str in file_filters:
            if filter_str.startswith("."):
                extensions.append(filter_str)
        
        # 如果没有指定扩展名，默认查找xlsx和csv
        if not extensions:
            extensions = [".xlsx", ".xls", ".csv"]
        
        # 查找文件
        all_files = []
        for ext in extensions:
            pattern = f"*{ext}"
            all_files.extend(glob.glob(os.path.join(directory_path, pattern)))
        
        # 去重
        all_files = list(set(all_files))
        
        # 应用筛选条件
        filtered_files = []
        for file_path in all_files:
            if self.match_filters(file_path, file_filters):
                filtered_files.append(file_path)
        
        # 排序
        filtered_files = sorted(filtered_files)
        
        self.logger.logger.info(f"在目录中找到 {len(filtered_files)} 个符合条件的文件")
        return filtered_files

    def process_task(self, directory: str, file_filters: List[str], sheet_name: str,
                     required_fields: List[str], check_rules: Dict, task_name: str,
                     row_filter: Dict = None):
        """
        处理一个检核任务
        
        参数:
            directory: 目录路径
            file_filters: 文件筛选条件
            sheet_name: Excel的sheet名称（None表示使用第一个sheet）
            required_fields: 必需的字段列表
            check_rules: 数据检核规则字典
            task_name: 任务名称
            row_filter: 行筛选条件（可选），格式: {"field": "字段名", "condition": ">=", "value": "2025-01-01"}
        """
        if not os.path.exists(directory):
            self.logger.logger.error(f"目录不存在: {directory}")
            return

        # 查找符合条件的文件
        files = self.find_files(directory, file_filters)
        self.logger.summary["total_files"] += len(files)

        # 处理每个文件
        for file_path in files:
            self.process_file(file_path, sheet_name, required_fields, check_rules, task_name, row_filter)

    def process_file(self, file_path: str, sheet_name: str, required_fields: List[str],
                     check_rules: Dict, task_name: str, row_filter: Dict = None):
        """
        处理单个文件
        
        参数:
            file_path: 文件路径
            sheet_name: Excel的sheet名称
            required_fields: 必需的字段列表
            check_rules: 数据检核规则字典
            task_name: 任务名称
            row_filter: 行筛选条件（可选）
        """
        file_name = os.path.basename(file_path)
        self.logger.logger.info(f"处理文件: {file_name}")

        try:
            # 读取文件
            df = self.read_file(file_path, sheet_name)
            if df is None:
                return

            # 检查字段名
            actual_fields = df.columns.tolist()
            is_valid, missing_fields, extra_fields = self.field_checker.check_fields(
                actual_fields, required_fields
            )

            if not is_valid:
                self.logger.log_header_error(
                    file_path, missing_fields, extra_fields, task_name
                )
                self.logger.increment_counter("checked_files")
                self.logger.summary["header_errors"] += 1
                return

            # 应用行筛选（如果有）
            original_row_count = len(df)
            if row_filter:
                df = self.apply_row_filter(df, row_filter, file_path)
                filtered_row_count = len(df)
                if filtered_row_count < original_row_count:
                    self.logger.logger.info(
                        f"  应用筛选条件: {row_filter['field']} {row_filter['condition']} {row_filter['value']}"
                    )
                    self.logger.logger.info(
                        f"  筛选前: {original_row_count}行, 筛选后: {filtered_row_count}行"
                    )

            # 检核数据
            rows_processed, error_rows = self.check_dataframe(
                df, file_path, check_rules, task_name
            )

            # 记录处理结果
            self.logger.log_file_processed(file_path, rows_processed, error_rows)
            self.logger.increment_counter("checked_files")

        except Exception as e:
            self.logger.logger.error(f"处理文件 {file_name} 时发生错误: {str(e)}")

    def read_file(self, file_path: str, sheet_name: str = None):
        """读取Excel或CSV文件"""
        file_name = os.path.basename(file_path)
        
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig')
            else:
                # Excel文件
                if sheet_name is None:
                    # 使用第一个sheet
                    df = pd.read_excel(file_path, sheet_name=0, dtype=str)
                else:
                    # 尝试读取指定的sheet
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
                    except ValueError as e:
                        # 如果找不到指定的sheet，列出所有可用的sheet并跳过该文件
                        try:
                            import openpyxl
                            wb = openpyxl.load_workbook(file_path, read_only=True)
                            available_sheets = wb.sheetnames
                            wb.close()
                            
                            self.logger.logger.warning(
                                f"跳过文件 {file_name}: 找不到sheet '{sheet_name}'"
                            )
                            self.logger.logger.warning(f"  可用的sheet: {', '.join(available_sheets)}")
                            self.logger.increment_counter("skipped_files")
                        except Exception as inner_e:
                            self.logger.logger.warning(f"跳过文件 {file_name}: 无法读取sheet信息")
                            self.logger.increment_counter("skipped_files")
                        
                        return None
            
            return df
        except Exception as e:
            self.logger.logger.error(f"读取文件失败 {file_name}: {str(e)}")
            return None

    def apply_row_filter(self, df: pd.DataFrame, row_filter: Dict, file_path: str):
        """
        应用行筛选条件
        
        参数:
            df: DataFrame对象
            row_filter: 筛选条件字典，格式: {"field": "字段名", "condition": ">=", "value": "2025-01-01"}
            file_path: 文件路径（用于日志）
            
        返回:
            筛选后的DataFrame
        """
        field = row_filter.get("field")
        condition = row_filter.get("condition", ">=")
        value = row_filter.get("value")
        
        if field not in df.columns:
            self.logger.logger.warning(f"筛选字段 '{field}' 不存在于文件中，跳过筛选")
            return df
        
        try:
            # 将字段转换为日期类型进行比较
            df_filtered = df.copy()
            df_filtered[field] = pd.to_datetime(df_filtered[field], errors='coerce')
            filter_value = pd.to_datetime(value)
            
            # 根据条件筛选
            if condition == ">=":
                mask = df_filtered[field] >= filter_value
            elif condition == ">":
                mask = df_filtered[field] > filter_value
            elif condition == "<=":
                mask = df_filtered[field] <= filter_value
            elif condition == "<":
                mask = df_filtered[field] < filter_value
            elif condition == "==":
                mask = df_filtered[field] == filter_value
            else:
                self.logger.logger.warning(f"不支持的筛选条件: {condition}，跳过筛选")
                return df
            
            # 只保留满足条件的行（排除日期转换失败的行）
            mask = mask & df_filtered[field].notna()
            
            # 返回原始df的筛选结果（保持原始数据类型）
            return df[mask].reset_index(drop=True)
            
        except Exception as e:
            self.logger.logger.error(f"应用筛选条件时出错: {str(e)}，跳过筛选")
            return df

    def check_dataframe(self, df: pd.DataFrame, file_path: str, check_rules: Dict, task_name: str):
        """
        检核DataFrame中的数据
        
        参数:
            df: DataFrame对象
            file_path: 文件路径
            check_rules: 检核规则字典 {字段名: 检核方法}
            task_name: 任务名称
            
        返回:
            (处理行数, 错误行数)
        """
        rows_processed = len(df)
        error_rows = 0

        self.logger.summary["total_rows"] += rows_processed

        for idx, row in df.iterrows():
            has_error = False

            # 检查每个字段
            for field_name, check_method in check_rules.items():
                if field_name not in row.index:
                    continue

                value = row[field_name]

                # 直接调用检核方法
                is_valid, error_msg = check_method(value)

                if not is_valid:
                    self.logger.log_data_error(
                        file_path, idx, field_name, value, error_msg
                    )
                    has_error = True

            # 特殊逻辑检查（如日期逻辑）
            if task_name == "自店贴膜" and "推送日期" in row.index and "到店日期" in row.index:
                is_valid, error_msg = self.data_checker.check_date_logic(
                    row["推送日期"], row["到店日期"], "推送日期", "到店日期"
                )
                if not is_valid:
                    self.logger.log_data_error(
                        file_path, idx, "日期逻辑",
                        f"推送:{row['推送日期']}, 到店:{row['到店日期']}",
                        error_msg
                    )
                    has_error = True

            if has_error:
                error_rows += 1

        self.logger.summary["error_rows"] += error_rows

        return rows_processed, error_rows