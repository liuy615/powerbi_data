import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor


class SpecialIncomeProcessor:
    """
    特殊事项收入数据处理类：封装文件读取、数据清洗和保存功能
    """

    # 配置常量
    DIRECTORIES = [r'E:\powerbi_data\看板数据\私有云文件本地\特殊事项收入']
    OUTPUT_PATH = r'E:\powerbi_data\看板数据\dashboard\特殊费用收入.csv'
    TARGET_SHEET = '登记表'
    REQUIRED_COLUMNS = ['业务时间', '归属门店', '车架号', '客户名称', '事项名称', '收付类型', '金额', '备注', 'From']

    def __init__(self):
        """初始化处理器"""
        self.df_result = pd.DataFrame()

    def _get_all_file_paths(self):
        """
        获取所有Excel文件路径
        """
        file_paths = []
        for folder_path in self.DIRECTORIES:
            for file_name in os.listdir(folder_path):
                if file_name.endswith('.xlsx'):
                    file_path = os.path.join(folder_path, file_name)
                    file_paths.append(file_path)
        return file_paths

    def _process_single_file(self, file_path):
        """
        处理单个Excel文件
        """
        try:
            with pd.ExcelFile(file_path) as xls:
                if self.TARGET_SHEET not in xls.sheet_names:
                    return None

                data = pd.read_excel(xls, sheet_name=self.TARGET_SHEET)
                data['From'] = os.path.basename(file_path).split('.')[0]

                print(f"处理: {os.path.basename(file_path)}")
                return data

        except Exception as e:
            print(f"错误: {os.path.basename(file_path)} - {str(e)[:30]}")
            return None

    def _process_data_quality(self):
        """
        数据质量处理：列筛选、类型转换、数据清洗
        """
        # 筛选需要的列
        existing_columns = [col for col in self.REQUIRED_COLUMNS if col in self.df_result.columns]
        self.df_result = self.df_result[existing_columns]

        # 金额类型转换
        if '金额' in self.df_result.columns:
            self.df_result['金额'] = pd.to_numeric(self.df_result['金额'], errors='coerce').fillna(0)

        # 门店名称替换
        if '归属门店' in self.df_result.columns:
            self.df_result['归属门店'] = self.df_result['归属门店'].replace('文景初治', '上元盛世')

    def load_data(self):
        """
        加载并合并所有数据
        """
        file_paths = self._get_all_file_paths()
        if not file_paths:
            print("未找到Excel文件")
            return False

        combined_data = []

        # 使用线程池并行处理文件
        with ThreadPoolExecutor() as executor:
            results = executor.map(self._process_single_file, file_paths)
            for result in results:
                if result is not None and not result.empty:
                    combined_data.append(result)

        if combined_data:
            self.df_result = pd.concat(combined_data, ignore_index=True)
            print(f"数据加载完成: {len(self.df_result)} 行")
            return True
        else:
            print("未找到有效数据")
            return False

    def save_data(self):
        """
        保存处理后的数据
        """
        if self.df_result.empty:
            print("无数据可保存")
            return False

        try:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(self.OUTPUT_PATH), exist_ok=True)

            self.df_result.to_csv(self.OUTPUT_PATH, index=False)
            print(f"数据已保存: {self.OUTPUT_PATH}")
            return True
        except Exception as e:
            print(f"保存失败: {str(e)}")
            return False

    def run(self):
        """
        执行完整的数据处理流程
        """
        print("开始处理特殊事项收入数据...")

        if not self.load_data():
            return False

        self._process_data_quality()

        if not self.save_data():
            return False

        print("特殊事项收入数据处理完成")
        return True


def main():
    """主函数"""
    processor = SpecialIncomeProcessor()
    success = processor.run()

    if success:
        print("流程执行成功")
    else:
        print("流程执行失败")


if __name__ == "__main__":
    main()