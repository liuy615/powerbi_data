from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pandas as pd


class JingpinProcessor:
    """
    精品销售数据处理类：封装文件读取、数据清洗、计算和保存功能
    """

    # 配置常量
    DIRECTORIES = [r"E:\powerbi_data\看板数据\私有云文件本地\精品销售"]
    SHEET_NAME = '精品销售台账'
    OUTPUT_PATH = r"/看板数据/cyy_old_data/精品销售.csv"

    # 需要处理的列配置
    COLUMNS_TO_KEEP = [
        '月份', '精品销售日期', '精品销售人员', '新车销售门店', '车型', '车架号', '客户姓名', '电话号码',
        '护板组合套装', '电池下护板', '发动机/电机下护板', '行车记录仪', '360全景影像', 'FSD减震器', '侧踏板',
        '挡泥板', '隐形ETC', '360软包脚垫', '电尾门', '龙膜', '车贴贴花', '备件类别', '销售总金额',
        '总成本', '毛利润', '毛利率', '人员提成', '备注', 'From'
    ]

    ITEM_COLUMNS = [
        '护板组合套装', '电池下护板', '发动机/电机下护板', '行车记录仪', '360全景影像',
        'FSD减震器', '侧踏板', '挡泥板', '隐形ETC', '360软包脚垫', '电尾门', '龙膜', '车贴贴花'
    ]

    def __init__(self):
        """初始化处理器"""
        self.df_jingpin = pd.DataFrame()

    def _read_excel_files(self, directory, sheet_name):
        """
        读取指定目录下的 Excel 文件，返回 DataFrame 列表
        """
        dfs = []
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df['From'] = os.path.basename(file_path).split('.')[0]
                df.columns = df.columns.str.replace('\n', '')
                dfs.append(df)
                print(f"读取: {filename},共{len(df)}条数据")
            except Exception as e:
                print(f"错误: {filename} - {str(e)[:30]}")
        return dfs

    def _process_directory(self, directory, sheet_name):
        """
        处理单个目录，返回合并后的 DataFrame
        """
        dfs = self._read_excel_files(directory, sheet_name)
        return pd.concat(dfs, axis=0) if dfs else pd.DataFrame()

    def _clean_and_convert_to_float(self, series):
        """
        清洗数据并转换为float类型
        """
        cleaned_series = series.astype(str).str.replace(r'[^\d.]', '', regex=True)
        return cleaned_series.replace('', 0).astype('float')

    def _count_items(self, row):
        """
        计算每行的总次数
        """
        count = 0
        for item in self.ITEM_COLUMNS:
            if row[item] > 0:
                count += 2 if item == '护板组合套装' else 1
        return count

    def _process_data_quality(self):
        """
        数据质量处理：过滤空值、重命名门店等
        """
        # 过滤空值
        self.df_jingpin = self.df_jingpin[self.df_jingpin['精品销售日期'].notna()]

        # 重命名门店
        self.df_jingpin['新车销售门店'] = self.df_jingpin['新车销售门店'].replace('文景初治', '上元盛世')

    def load_data(self):
        """
        加载并合并所有数据
        """
        all_dfs = []

        # 使用线程池并行处理不同目录
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._process_directory, directory, self.SHEET_NAME)
                       for directory in self.DIRECTORIES]

            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    all_dfs.append(result)

        self.df_jingpin = pd.concat(all_dfs, axis=0) if all_dfs else pd.DataFrame()
        return not self.df_jingpin.empty

    def process_data(self):
        """
        处理数据：列筛选、类型转换、计算等
        """
        if self.df_jingpin.empty:
            print("无数据可处理")
            return False

        # 筛选列
        existing_columns = [col for col in self.COLUMNS_TO_KEEP if col in self.df_jingpin.columns]
        self.df_jingpin = self.df_jingpin[existing_columns]

        # 类型转换
        existing_item_columns = [col for col in self.ITEM_COLUMNS if col in self.df_jingpin.columns]
        self.df_jingpin[existing_item_columns] = self.df_jingpin[existing_item_columns].apply(
            self._clean_and_convert_to_float
        )

        # 计算总次数
        self.df_jingpin['总次数'] = self.df_jingpin.apply(self._count_items, axis=1)

        # 数据质量处理
        self._process_data_quality()

        print(f"数据处理完成: {len(self.df_jingpin)} 行")
        return True

    def save_data(self):
        """
        保存处理后的数据
        """
        if self.df_jingpin.empty:
            print("无数据可保存")
            return False

        try:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(self.OUTPUT_PATH), exist_ok=True)

            self.df_jingpin.to_csv(self.OUTPUT_PATH, index=False)
            print(f"数据已保存: {self.OUTPUT_PATH}")
            return True
        except Exception as e:
            print(f"保存失败: {str(e)}")
            return False

    def run(self):
        """
        执行完整的数据处理流程
        """
        print("开始处理精品销售数据...")

        if not self.load_data():
            print("数据加载失败")
            return False

        if not self.process_data():
            print("数据处理失败")
            return False

        if not self.save_data():
            print("数据保存失败")
            return False

        print("精品销售数据处理完成")
        return True


def main():
    """主函数"""
    processor = JingpinProcessor()
    success = processor.run()

    if success:
        print("流程执行成功")
    else:
        print("流程执行失败")


if __name__ == "__main__":
    main()