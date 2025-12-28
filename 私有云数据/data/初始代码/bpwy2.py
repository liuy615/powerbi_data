import pandas as pd
import numpy as np
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any


class InsuranceDataProcessor:
    """
    保险数据处理器：整合保赔无忧数据与新车保险台账数据的读取、清洗、合并与输出
    核心功能：处理衍生产品保赔数据、新车保险数据，合并后过滤无效数据，输出标准化CSV
    """

    def __init__(self, base_dir: str = r"E:\powerbi_data\看板数据"):
        """
        初始化处理器，集中管理输入/输出路径及核心配置

        :param base_dir: 基础数据目录，输入/输出文件围绕此目录组织
        """
        # 1. 路径集中配置（输入输出文件集中管理）
        self.base_dir = base_dir
        # 输入目录
        self.derivative_dir = os.path.join(base_dir, "私有云文件本地", "衍生产品")  # 保赔无忧数据目录
        self.insurance_dir = os.path.join(base_dir, "私有云文件本地", "新车保险台账")  # 新车保险数据目录
        # 维护文件路径
        self.supplement_car_path = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx"
        self.insurance_csv_path = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv"
        # 输出目录（所有结果集中存放）
        self.output_dir = os.path.join(base_dir, "dashboard")
        self.output_bpwy = os.path.join(self.output_dir, "保赔无忧.csv")
        self.output_insurance = os.path.join(self.output_dir, "新车保险台账.csv")

        # 2. 核心配置参数（集中管理，便于维护）
        self.bpwy_sheet = "登记表"  # 保赔数据工作表名
        self.insurance_sheet = "新车台账明细"  # 新车保险工作表名
        # 保赔数据必填列
        self.bpwy_required_cols = [
            '序号', '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码',
            '保赔无忧金额', '双保无忧金额', '终身保养金额', '销售顾问', '所属门店', '备注', '日期'
        ]
        # 运营车过滤条件
        self.exclude_fee_list = [1000, 1130, 1800]  # 排除的交强险保费
        self.exclude_company = "鼎和"  # 排除的保险公司
        # 公司名称映射
        self.company_mapping = {"文景初治": "上元盛世"}

        # 3. 初始化检查
        self._init_check()

    def _init_check(self) -> None:
        """初始化环境检查：确保输入目录和依赖文件存在，创建输出目录"""
        # 检查输入目录
        for dir_path in [self.derivative_dir, self.insurance_dir]:
            if not os.path.exists(dir_path):
                raise FileNotFoundError(f"输入目录不存在：{dir_path}")

        # 检查依赖文件
        for file_path in [self.supplement_car_path, self.insurance_csv_path]:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"依赖文件不存在：{file_path}")

        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"【初始化完成】基础目录：{self.base_dir}")
        print(f"【初始化完成】输出目录：{self.output_dir}")

    # -------------------------- 保赔无忧数据处理 --------------------------
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名：去除空格、转小写、空格替换为下划线"""
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\s+', '_', regex=True)
        return df

    def _read_bpwy_files(self, directory: str, sheet_name: str) -> List[pd.DataFrame]:
        """读取保赔无忧Excel文件（多线程）"""
        dfs: List[pd.DataFrame] = []

        def _read_single(file_path: str) -> Optional[pd.DataFrame]:
            filename = os.path.basename(file_path)
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df['from'] = filename.split('.')[0]
                df = self._standardize_columns(df)

                # 补全缺失列
                for col in self.bpwy_required_cols:
                    if col not in df.columns:
                        df[col] = None
                return df[self.bpwy_required_cols]
            except Exception as e:
                print(f"【保赔数据】读取{filename}失败：{str(e)}")
                return None

        # 获取目录下所有Excel文件
        excel_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]

        # 多线程读取
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(_read_single, f): f for f in excel_files}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)
                    print(f"【保赔数据】读取成功：{os.path.basename(futures[future])}（{len(res)}行）")

        return dfs

    def _process_bpwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理保赔无忧数据：日期处理、合并车系数据、门店名称处理"""
        # 日期处理
        df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed', errors='coerce')
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce')
        df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
        df['日期'] = df['开票日期'].fillna(df['销售日期'])
        df['日期'] = pd.to_datetime(df['日期'], format='mixed', errors='coerce').dt.date

        # 合并补充车系数据
        df_car = pd.read_excel(self.supplement_car_path, sheet_name='补充车系')
        df = pd.merge(df, df_car[['车系', '服务网络']], how='left', on='车系')

        # 处理直播基地门店名称
        df['所属门店'] = np.where(
            df['所属门店'] == '直播基地',
            df['服务网络'] + '-' + df['所属门店'],
            df['所属门店']
        )

        return df[self.bpwy_required_cols]

    def process_bpwy(self) -> pd.DataFrame:
        """保赔无忧数据完整处理流程"""
        print("\n【开始处理保赔无忧数据】")
        # 读取文件
        dfs = self._read_bpwy_files(self.derivative_dir, self.bpwy_sheet)
        if not dfs:
            raise ValueError("未读取到任何保赔无忧数据")

        # 合并与处理
        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_bpwy_data(df_combined)

        # 后续处理：添加标记、替换名称、城市分类、去重
        df_processed['是否保赔'] = '是'
        df_processed['所属门店'] = df_processed['所属门店'].replace(self.company_mapping)
        df_processed['城市'] = np.where(
            df_processed['所属门店'].str.contains('贵州'),
            '贵州',
            '成都'
        )
        df_processed.drop_duplicates(inplace=True)
        df_processed.dropna(subset='车架号', inplace=True)

        print(f"【保赔数据处理完成】共{len(df_processed)}行有效数据")
        return df_processed

    # -------------------------- 新车保险数据处理 --------------------------
    def _standardize_date(self, date_str: Any) -> Any:
        """标准化日期字符串：补充缺失的'日'"""
        if isinstance(date_str, str):
            date_str = date_str.strip()
            if "年" in date_str and "月" in date_str and "日" not in date_str:
                date_str += "1日"
        return date_str

    def _convert_date(self, date_str: Any) -> pd.Timestamp:
        """转换日期格式为Timestamp"""
        if isinstance(date_str, str):
            if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_str):
                return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
            elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
                return pd.to_datetime(date_str, format='%Y年%m月%d日')
        return pd.NaT

    def _process_insurance_files(self, filedir: str, sheet_name: str) -> pd.DataFrame:
        """读取新车保险Excel文件（多线程）"""
        dfs: List[pd.DataFrame] = []

        def _read_single(filename: str) -> Optional[pd.DataFrame]:
            if '新车' in filename and filename.endswith('.xlsx'):
                file_path = os.path.join(filedir, filename)
                try:
                    with pd.ExcelFile(file_path) as xls:
                        if sheet_name not in xls.sheet_names:
                            print(f"【保险数据】{filename}缺少'{sheet_name}'工作表，跳过")
                            return None
                        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                        df['From'] = filename.split('.')[0]
                        df.columns = df.columns.str.replace('\n', '')
                        return df
                except Exception as e:
                    print(f"【保险数据】读取{filename}失败：{str(e)}")
            return None

        # 多线程读取
        filenames = os.listdir(filedir)
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(_read_single, fn): fn for fn in filenames}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)
                    print(f"【保险数据】读取成功：{futures[future]}（{len(res)}行）")

        return pd.concat(dfs, axis=0, ignore_index=True) if dfs else pd.DataFrame()

    def _make_unique_columns(self, column_names: List[str]) -> List[str]:
        """列名去重：重复列名添加后缀"""
        unique_names = []
        count: Dict[str, int] = {}
        for name in column_names:
            if name in count:
                count[name] += 1
                unique_names.append(f"{name}_{count[name]}")
            else:
                count[name] = 0
                unique_names.append(name)
        return unique_names

    def process_insurance(self) -> pd.DataFrame:
        """新车保险数据完整处理流程"""
        print("\n【开始处理新车保险数据】")
        # 读取目录下的保险文件
        all_dfs = []
        for dir_path in [self.insurance_dir]:
            df = self._process_insurance_files(dir_path, self.insurance_sheet)
            if not df.empty:
                all_dfs.append(df)

        # 处理外部CSV数据
        df_cyy = pd.read_csv(self.insurance_csv_path)
        df_cyy = df_cyy[
            ['出单日期', '保险公司简称', '所属门店', '车系', '车架号', '交强险保费', '业务人员', '保费总额']]
        df_cyy.rename(columns={
            '出单日期': '签单日期',
            '保险公司简称': '保险公司',
            '车系': '车型',
            '所属门店': '归属公司',
            '业务人员': '销售顾问'
        }, inplace=True)

        # 合并补充车系数据
        df_car = pd.read_excel(self.supplement_car_path, sheet_name='补充车系')
        df_cyy = pd.merge(df_cyy, df_car[['车系', '服务网络']], how='left', left_on='车型', right_on='车系')
        df_cyy['归属公司'] = np.where(
            df_cyy['归属公司'] == '直播基地',
            df_cyy['服务网络'] + '-' + df_cyy['归属公司'],
            df_cyy['归属公司']
        )

        # 合并所有保险数据
        if all_dfs:
            df_combined = pd.concat(all_dfs, axis=0, ignore_index=True)
            # 列名去重
            df_combined.columns = self._make_unique_columns(df_combined.columns)
            df_cyy.columns = self._make_unique_columns(df_cyy.columns)
            # 合并Excel数据与CSV数据
            df_combined_all = pd.concat([df_combined, df_cyy], axis=0, join='outer', ignore_index=True)
        else:
            df_combined_all = df_cyy.copy()

        # 清洗数据
        df_combined_all['归属公司'] = df_combined_all['归属公司'].replace(self.company_mapping)
        df_combined_all.dropna(subset=['保险公司'], inplace=True)

        # 筛选所需列
        required_cols = [
            '月份', '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司',
            '车型', '车牌号', '车架号', '被保险人', '交强险保费', '销售顾问', '是否为保赔无忧客户'
        ]
        existing_cols = [col for col in required_cols if col in df_combined_all.columns]
        df_filtered = df_combined_all[existing_cols].copy()
        df_filtered['日期'] = df_filtered['签单日期']

        # 日期处理与去重
        df_filtered['日期'] = pd.to_datetime(df_filtered['日期'], errors='coerce').dt.date
        df_filtered = df_filtered.sort_values(by='日期', ascending=False)
        df_filtered.drop_duplicates(subset='车架号', inplace=True, keep='first')

        print(f"【保险数据处理完成】共{len(df_filtered)}行有效数据")
        return df_filtered

    # -------------------------- 数据合并与过滤 --------------------------
    def merge_and_filter(self, df_bpwy: pd.DataFrame, df_insurance: pd.DataFrame) -> pd.DataFrame:
        """合并保赔数据与保险数据，过滤运营车"""
        print("\n【开始合并与过滤数据】")
        # 合并保赔标记
        df_merged = pd.merge(
            df_insurance,
            df_bpwy[['车架号', '是否保赔']],
            how='left',
            on='车架号'
        )
        df_merged['是否保赔'] = df_merged['是否保赔'].fillna('否')

        # 筛选运营车
        df_except1 = df_merged[df_merged['保险公司'].str.contains(self.exclude_company, na=False)]
        df_except2 = df_merged[df_merged['交强险保费'].isin(self.exclude_fee_list)]
        df_excluded = pd.concat([df_except1, df_except2], axis=0).drop_duplicates()
        df_excluded = df_excluded[df_excluded['是否保赔'] == '否']

        # 筛选有效数据
        diff_df = df_merged[~df_merged['车架号'].isin(df_excluded['车架号'])].copy()
        diff_df['城市'] = np.where(
            diff_df['归属公司'].str.contains('贵州'),
            '贵州',
            '成都'
        )
        diff_df = diff_df.drop_duplicates()

        print(f"【合并过滤完成】有效数据{len(diff_df)}行，排除运营车{len(df_excluded)}行")
        return diff_df

    # -------------------------- 结果保存 --------------------------
    def save_results(self, df_bpwy: pd.DataFrame, df_insurance: pd.DataFrame) -> None:
        """保存处理结果到CSV"""
        # 保存保赔无忧数据
        df_bpwy.to_csv(self.output_bpwy, index=False, encoding='utf-8-sig')
        print(f"\n【结果保存】保赔无忧数据：{self.output_bpwy}（{len(df_bpwy)}行）")

        # 保存新车保险数据
        df_insurance.to_csv(self.output_insurance, index=False, encoding='utf-8-sig')
        print(f"【结果保存】新车保险数据：{self.output_insurance}（{len(df_insurance)}行）")

    # -------------------------- 主执行流程 --------------------------
    def run(self) -> None:
        """执行完整数据处理流程"""
        try:
            print("=" * 60)
            print("【保险数据处理系统】启动")
            print("=" * 60)

            # 1. 处理保赔无忧数据
            df_bpwy = self.process_bpwy()

            # 2. 处理新车保险数据
            df_insurance = self.process_insurance()

            # 3. 合并与过滤
            df_final_insurance = self.merge_and_filter(df_bpwy, df_insurance)

            # 4. 保存结果
            self.save_results(df_bpwy, df_final_insurance)

            print("\n" + "=" * 60)
            print("【保险数据处理系统】执行完成")
            print("=" * 60)
        except Exception as e:
            print(f"\n【错误终止】{str(e)}")
            raise


if __name__ == "__main__":
    # 实例化处理器并执行
    processor = InsuranceDataProcessor()
    processor.run()