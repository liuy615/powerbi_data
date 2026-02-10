from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pandas as pd
from typing import List, Optional, Tuple


class SanfangYBProcessor:
    """
    新车三方延保数据处理器
    功能：读取指定目录下的Excel延保数据，多线程处理文件合并，清洗数据后输出为CSV文件
    """

    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        """
        初始化处理器，集中配置输入输出路径及核心参数

        :param base_data_dir: 基础数据目录，输入/输出文件围绕此目录组织
        """
        # 1. 集中管理路径：输入输出文件统一在base_data_dir下，实现路径集中
        self.base_dir = base_data_dir
        self.input_dir = os.path.join(base_data_dir, "私有云文件本地", "新车三方延保")  # 输入目录
        self.output_dir = os.path.join(base_data_dir, "dashboard")  # 输出目录
        self.output_filename = "新车三方延保台账.csv"  # 输出文件名
        self.output_path = os.path.join(self.output_dir, self.output_filename)

        # 2. 核心配置参数（集中管理，便于后续修改）
        self.sheet_name = "登记表"  # 待读取的Excel工作表名
        self.max_workers = 10  # 线程池最大工作线程数
        self.required_columns = [  # 最终保留的列（原逻辑不变）
            '新车销售店名', '延保销售日期', '购车日期', '车系', '车架号', '客户姓名',
            '电话号码1', '电话号码2', '延保销售人员', '延保期限', '金额',
            '是否录入厂家系统', '录入厂家系统日期', '比亚迪系统录入金额',
            '超期录入比亚迪系统违约金', '备注', 'From'
        ]
        self.store_name_mapping = {  # 店名替换规则
            '文景初治': '上元盛世',
            '永乐盛世':'洪武盛世'
        }

        # 3. 初始化检查：确保输入目录存在，输出目录不存在则创建
        self._init_check()

    def _init_check(self) -> None:
        """初始化检查：验证输入目录有效性，创建输出目录"""
        # 检查输入目录是否存在
        if not os.path.exists(self.input_dir):
            raise FileNotFoundError(f"输入目录不存在，请检查路径：{self.input_dir}")

        # 确保输出目录存在（不存在则创建）
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"输入目录：{self.input_dir}")
        print(f"输出目录：{self.output_dir}")

    def _read_single_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        读取单个Excel文件，处理列名和添加From列（原逻辑拆分到独立方法）

        :param file_path: Excel文件路径
        :return: 处理后的DataFrame，读取失败返回None
        """
        try:
            # 原逻辑：读取Excel，指定header和dtype
            df = pd.read_excel(
                file_path,
                sheet_name=self.sheet_name,
                header=0,
                dtype=str  # 保持原逻辑的字符串类型读取
            )

            # 原逻辑：添加From列（文件名去掉后缀）
            file_base_name = os.path.basename(file_path).split('.')[0]
            df['From'] = file_base_name

            # 原逻辑：替换列名中的换行符
            df.columns = df.columns.str.replace('\n', '')

            print(f"成功读取文件：{os.path.basename(file_path)}（{len(df)}行）")
            return df

        except Exception as e:
            # 保留原逻辑的错误提示
            print(f"读取文件 {os.path.basename(file_path)} 时出错：{str(e)}")
            return None

    def _process_single_directory(self, directory: str) -> Optional[pd.DataFrame]:
        """
        处理单个目录：遍历目录下所有Excel文件，合并为单个DataFrame

        :param directory: 目录路径
        :return: 目录内所有Excel文件合并后的DataFrame，无有效文件返回None
        """
        # 原逻辑：筛选目录下的.xlsx文件
        excel_files = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.lower().endswith('.xlsx')
        ]

        if not excel_files:
            print(f"目录 {os.path.basename(directory)} 下无Excel文件，跳过")
            return None

        # 读取目录下所有Excel文件
        dir_dfs = []
        for file_path in excel_files:
            df = self._read_single_file(file_path)
            if df is not None:
                dir_dfs.append(df)

        # 合并目录下的所有DataFrame
        if dir_dfs:
            merged_dir_df = pd.concat(dir_dfs, axis=0, ignore_index=True)
            print(f"目录 {os.path.basename(directory)} 合并完成（{len(merged_dir_df)}行）")
            return merged_dir_df
        else:
            print(f"目录 {os.path.basename(directory)} 下无有效Excel数据，跳过")
            return None

    def _process_directories(self, directories: List[str]) -> pd.DataFrame:
        """
        多线程处理多个目录：调用线程池并行处理，合并所有目录数据

        :param directories: 待处理的目录列表
        :return: 所有目录数据合并后的DataFrame（无数据返回空DataFrame）
        """
        all_dfs = []

        # 原逻辑：使用ThreadPoolExecutor多线程处理目录
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有目录处理任务
            futures = {
                executor.submit(self._process_single_directory, dir_path): dir_path
                for dir_path in directories
            }

            # 等待任务完成并收集结果
            for future in as_completed(futures):
                dir_path = futures[future]
                try:
                    dir_df = future.result()
                    if dir_df is not None:
                        all_dfs.append(dir_df)
                except Exception as e:
                    print(f"处理目录 {os.path.basename(dir_path)} 时发生线程错误：{str(e)}")

        # 合并所有目录的DataFrame
        if all_dfs:
            final_merged_df = pd.concat(all_dfs, axis=0, ignore_index=True)
            print(f"\n所有目录数据合并完成（总计{len(final_merged_df)}行）")
            return final_merged_df
        else:
            print("\n没有找到任何有效数据")
            return pd.DataFrame()

    def _filter_and_select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据筛选：保留延保销售日期非空的行，选择指定列（原逻辑拆分）

        :param df: 原始合并后的DataFrame
        :return: 筛选和列选择后的DataFrame
        """
        if df.empty:
            return df

        # 原逻辑：筛选延保销售日期非空的行
        df_filtered = df[df['延保销售日期'].notna()].copy()
        print(f"筛选延保销售日期非空数据：{len(df_filtered)}行（原{len(df)}行）")

        # 原逻辑：选择指定列（确保列存在，避免KeyError）
        existing_columns = [col for col in self.required_columns if col in df_filtered.columns]
        missing_columns = set(self.required_columns) - set(existing_columns)
        if missing_columns:
            print(f"警告：部分指定列不存在，已跳过：{', '.join(missing_columns)}")

        df_selected = df_filtered[existing_columns].copy()
        return df_selected

    def _replace_store_name(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        店名替换：将"文景初治"替换为"上元盛世"（原逻辑拆分）

        :param df: 筛选后的DataFrame
        :return: 完成店名替换的DataFrame
        """
        if df.empty or '新车销售店名' not in df.columns:
            return df

        # 原逻辑：替换店名
        df['新车销售店名'] = df['新车销售店名'].replace(self.store_name_mapping)
        print("已完成店名替换：'文景初治' -> '上元盛世'")
        return df

    def save_result(self, df: pd.DataFrame) -> None:
        """
        保存结果：将最终数据保存为CSV文件（原逻辑拆分）

        :param df: 最终处理后的DataFrame
        """
        if df.empty:
            print("无有效数据，不保存文件")
            return

        try:
            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')  # 增加utf-8-sig避免中文乱码
            print(f"\n结果已保存至：{self.output_path}")
        except Exception as e:
            raise RuntimeError(f"保存文件失败：{str(e)}") from e

    def run(self, directories: Optional[List[str]] = None) -> pd.DataFrame:
        """
        核心执行方法：串联整个数据处理流程（外部调用入口）

        :param directories: 待处理的目录列表（默认使用初始化时的输入目录）
        :return: 最终处理后的DataFrame
        """
        # 若未指定目录，默认使用初始化的输入目录（保持原逻辑的默认目录）
        target_dirs = directories if directories is not None else [self.input_dir]
        print(f"\n开始处理三方延保数据，目标目录数量：{len(target_dirs)}")

        # 流程串联：多线程处理目录 -> 筛选列和数据 -> 替换店名 -> 保存结果
        df_merged = self._process_directories(target_dirs)
        df_filtered = self._filter_and_select_columns(df_merged)
        df_final = self._replace_store_name(df_filtered)
        self.save_result(df_final)

        return df_final


if __name__ == "__main__":
    # 实例化处理器并执行（保持原逻辑的默认目录）
    processor = SanfangYBProcessor()
    # 执行处理流程（可传入自定义目录列表，默认使用初始化的输入目录）
    final_df = processor.run()