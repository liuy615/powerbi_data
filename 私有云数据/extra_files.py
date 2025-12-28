import pandas as pd
import os
from typing import List, Optional, Dict, Any


class DataMerger:
    """
    数据合并处理器，用于读取多个Excel文件的指定工作表，进行数据处理并合并输出
    """

    def __init__(self, base_output_dir: str = r'E:\powerbi_data\看板数据\dashboard'):
        """
        初始化处理器

        :param base_output_dir: 输出文件的基础目录，所有输出文件将集中在此目录
        """
        self.base_output_dir = base_output_dir
        # 确保输出目录存在
        os.makedirs(self.base_output_dir, exist_ok=True)

        # 输入文件配置（集中管理所有输入文件路径）
        self.file_configs = self._init_file_configs()

        # 任务配置：包含每个处理任务的详细信息
        self.tasks: List[Dict[str, Any]] = [
            {
                "file_list_key": "sales_files",
                "sheet_name": "销量目标",
                "process_type": "default",
                "output_filename": "merged_sales_data.xlsx",
                "replace_col": "公司名称"
            },
            {
                "file_list_key": "quality_files",
                "sheet_name": "服务品质",
                "process_type": "filter_unnamed",
                "output_filename": "merged_quality_data.xlsx",
                "replace_col": "月份"
            },
            {
                "file_list_key": "quality_files1",
                "sheet_name": "NPS",
                "process_type": "default",
                "output_filename": "merged_nps_data.xlsx",
                "replace_col": "公司名称"
            },
            {
                "file_list_key": "quality_files1",
                "sheet_name": "提车",
                "process_type": "default",
                "output_filename": "merged_tiche_data.xlsx",
                "replace_col": "公司名称"
            },
            {
                "file_list_key": "wes_files",
                "sheet_name": "两网",
                "process_type": "filter_score",
                "output_filename": "merged_wes_data.xlsx",
                "replace_col": "公司名称"
            }
        ]

    def _init_file_configs(self) -> Dict[str, List[str]]:
        """初始化所有输入文件路径配置"""
        return {
            "sales_files": [
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年销量汇总表.xlsx',
                r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年销量汇总表.xlsx",
                r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\25年数据统计表.xlsx',
                r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\24年数据统计表.xlsx'
            ],
            "quality_files": [
                r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\25年数据统计表.xlsx',
                r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\24年数据统计表.xlsx',
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年数据汇总表.xlsx',
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年数据汇总表.xlsx'
            ],
            "quality_files1": [
                r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\25年数据统计表.xlsx',
                r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\24年数据统计表.xlsx',
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年数据汇总表.xlsx',
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年数据汇总表.xlsx'
            ],
            "wes_files": [
                r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年WES返利汇总表.xlsx"
            ]
        }

    def _process_single_file(self, file_path: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        处理单个Excel文件的指定工作表

        :param file_path: 文件路径
        :param sheet_name: 工作表名称
        :return: 处理后的DataFrame，失败则返回None
        """
        if not os.path.exists(file_path):
            print(f"警告：文件不存在 - {file_path}")
            return None

        try:
            # 读取Excel文件
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            # 数据透视：保留前2列作为id_vars
            unpivot_df = pd.melt(df, id_vars=df.columns[0:2], var_name='属性', value_name='值')
            return unpivot_df
        except Exception as e:
            print(f"处理文件 {file_path} 的 {sheet_name} 工作表时出错: {str(e)}")
            return None

    def _process_file_list(self, file_list: List[str], sheet_name: str, process_type: str) -> Optional[pd.DataFrame]:
        """
        处理文件列表，合并数据并根据类型进行过滤

        :param file_list: 文件路径列表
        :param sheet_name: 工作表名称
        :param process_type: 处理类型（default/filter_unnamed/filter_score）
        :return: 合并后的DataFrame，无有效数据则返回None
        """
        dfs = []
        for file_path in file_list:
            df = self._process_single_file(file_path, sheet_name)
            if df is not None:
                dfs.append(df)

        if not dfs:
            print(f"警告：未找到 {sheet_name} 工作表的有效数据")
            return None

        # 合并所有数据
        merged_df = pd.concat(dfs, ignore_index=True)

        # 根据处理类型应用不同过滤逻辑
        if process_type == "filter_unnamed":
            # 过滤掉属性包含"Unnamed"的行
            merged_df = merged_df[~merged_df['属性'].str.contains("Unnamed", na=False)]
        elif process_type == "filter_score":
            # 筛选属性包含"得分"且公司名称不为空的行
            merged_df = merged_df[
                (merged_df['属性'].str.contains("得分", na=False)) &
                (merged_df['公司名称'].notna())
                ]

        return merged_df

    def _replace_company_name(self, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        """
        替换特定列中的公司名称（文景初治 -> 上元盛世）

        :param df: 待处理的DataFrame
        :param col_name: 需要替换的列名
        :return: 处理后的DataFrame
        """
        if col_name in df.columns:
            df[col_name] = df[col_name].replace('文景初治', '上元盛世')
        else:
            print(f"警告：替换列 '{col_name}' 在数据中不存在，跳过替换")
        return df

    def _save_result(self, df: pd.DataFrame, output_filename: str) -> None:
        """
        保存处理结果到Excel文件

        :param df: 待保存的DataFrame
        :param output_filename: 输出文件名
        """
        output_path = os.path.join(self.base_output_dir, output_filename)
        try:
            df.to_excel(output_path, index=False)
            print(f"成功保存文件到: {output_path}")
        except Exception as e:
            print(f"保存文件 {output_filename} 失败: {str(e)}")

    def run(self) -> None:
        """执行所有数据处理任务"""
        for task in self.tasks:
            print(f"\n开始处理任务: {task['sheet_name']} -> {task['output_filename']}")

            # 获取文件列表
            file_list = self.file_configs.get(task['file_list_key'], [])
            if not file_list:
                print(f"警告：未找到文件列表 {task['file_list_key']}，跳过该任务")
                continue

            # 处理文件列表
            merged_df = self._process_file_list(
                file_list=file_list,
                sheet_name=task['sheet_name'],
                process_type=task['process_type']
            )

            if merged_df is None or merged_df.empty:
                print(f"警告：{task['sheet_name']} 处理后无有效数据，跳过保存")
                continue

            # 替换公司名称
            merged_df = self._replace_company_name(merged_df, task['replace_col'])

            # 保存结果
            self._save_result(merged_df, task['output_filename'])

        print("\n所有任务处理完成")


if __name__ == "__main__":
    # 初始化处理器并执行所有任务
    merger = DataMerger()
    merger.run()