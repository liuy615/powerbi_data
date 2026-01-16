import logging
import os
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd


class DataCheckerLogger:
    """数据检核日志记录器"""

    def __init__(self, log_dir: str = r"E:\powerbi_data\data\私有云日志\check_logs"):
        """初始化日志记录器"""
        self.log_dir = log_dir
        self.errors = []
        self.summary = {
            "total_files": 0,
            "checked_files": 0,
            "skipped_files": 0,
            "header_errors": 0,
            "data_errors": 0,
            "total_rows": 0,
            "error_rows": 0
        }

        # 创建日志目录
        os.makedirs(log_dir, exist_ok=True)

        # 配置logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(log_dir, f"data_check_{timestamp}.log")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def log_header_error(self, file_path: str, missing_headers: List[str], extra_headers: List[str], template_name: str = "标准模板"):
        """记录表头错误"""
        file_name = os.path.basename(file_path)  # 获取文件名
        # error_msg = f"文件 {file_name} 表头不匹配{template_name}:\n"  # 只输出文件名
        if missing_headers:
            error_msg = f"  缺少字段: {', '.join(missing_headers)}\n"
        if extra_headers:
            error_msg += f"  多余字段: {', '.join(extra_headers)}"

        self.logger.error(error_msg)
        self.errors.append({
            "type": "header_error",
            "file": file_path,  # 错误报告中仍然保存完整路径
            "file_name": file_name,  # 新增文件名字段
            "message": error_msg,
            "template": template_name,
            "timestamp": datetime.now()
        })
        self.summary["header_errors"] += 1

    def log_data_error(self, file_path: str, row_index: int, field: str,
                       value: Any, error_msg: str):
        """记录数据错误"""
        file_name = os.path.basename(file_path)  # 获取文件名
        error_info = {
            "type": "data_error",
            "file": file_path,
            "file_name": file_name,  # 新增文件名字段
            "row": row_index + 2,  # Excel行号从1开始，加上表头行
            "field": field,
            "value": str(value),
            "message": error_msg,
            "timestamp": datetime.now()
        }

        self.logger.error(
            f"数据错误 - 文件: {file_name}, 行: {row_index + 2}, "  # 只输出文件名
            f"字段: {field}, 值: {value}, 错误: {error_msg}"
        )

        self.errors.append(error_info)
        self.summary["data_errors"] += 1

    def log_file_processed(self, file_path: str, rows_processed: int, errors_found: int):
        """记录文件处理完成"""
        file_name = os.path.basename(file_path)  # 获取文件名
        self.logger.info(
            f"文件处理完成: {file_name}, "  # 只输出文件名
            f"处理行数: {rows_processed}, 发现错误: {errors_found}"
        )

    def log_summary(self):
        """记录检核总结"""
        self.logger.info("=" * 50)
        self.logger.info("数据检核总结:")
        self.logger.info(f"总文件数: {self.summary['total_files']}")
        self.logger.info(f"检核文件数: {self.summary['checked_files']}")
        self.logger.info(f"跳过文件数: {self.summary['skipped_files']}")
        self.logger.info(f"表头错误数: {self.summary['header_errors']}")
        self.logger.info(f"数据错误数: {self.summary['data_errors']}")
        self.logger.info(f"总行数: {self.summary['total_rows']}")
        self.logger.info(f"错误行数: {self.summary['error_rows']}")

        # 计算错误率
        if self.summary['total_rows'] > 0:
            error_rate = (self.summary['error_rows'] / self.summary['total_rows']) * 100
            self.logger.info(f"错误率: {error_rate:.2f}%")

        self.logger.info("=" * 50)

    def save_errors_to_excel(self, output_file: str = None):
        """将错误记录保存到Excel文件"""
        if not self.errors:
            self.logger.info("没有发现错误，无需保存错误报告")
            return

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.log_dir, f"data_errors_{timestamp}.xlsx")

        try:
            errors_df = pd.DataFrame(self.errors)

            # 创建Excel写入器
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                errors_df.to_excel(writer, sheet_name='错误详情', index=False)

                # 添加统计信息
                summary_df = pd.DataFrame([self.summary])
                summary_df.to_excel(writer, sheet_name='统计信息', index=False)

            self.logger.info(f"错误报告已保存到: {output_file}")

        except Exception as e:
            self.logger.error(f"保存错误报告失败: {str(e)}")

    def increment_counter(self, counter_name: str, value: int = 1):
        """增加计数器"""
        if counter_name in self.summary:
            self.summary[counter_name] += value