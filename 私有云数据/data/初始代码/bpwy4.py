import pandas as pd
import numpy as np
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any


class InsuranceAndWarrantyProcessor:
    """
    保险与保修数据处理器：整合保赔无忧、全保无忧、新车保险台账数据的读取、清洗、合并与输出
    核心功能：多线程读取文件、数据标准化处理、运营车过滤、结果集中保存
    """

    def __init__(self,
                 base_input_dir: str = r"E:\powerbi_data\看板数据\私有云文件本地",
                 base_output_dir: str = r"E:\powerbi_data\看板数据\dashboard",
                 supplement_car_path: str = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx",
                 insurance_csv_path: str = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv"):
        """
        初始化处理器，集中管理输入/输出路径及核心配置参数

        :param base_input_dir: 基础输入目录（包含衍生产品、全保无忧、新车保险台账子目录）
        :param base_output_dir: 基础输出目录（所有结果文件集中存放）
        :param supplement_car_path: 补充车系数据文件路径（依赖文件）
        :param insurance_csv_path: 新车保险CSV数据路径（依赖文件）
        """
        # -------------------------- 1. 路径集中配置（输入输出统一管理）--------------------------
        # 输入子目录
        self.input_bpwy_dir = os.path.join(base_input_dir, "衍生产品")  # 保赔无忧数据
        self.input_qbwy_dir = os.path.join(base_input_dir, "全保无忧")  # 全保无忧数据
        self.input_insurance_dir = os.path.join(base_input_dir, "新车保险台账")  # 新车保险数据
        # 依赖文件路径
        self.supplement_car_path = supplement_car_path
        self.insurance_csv_path = insurance_csv_path
        # 输出目录与文件
        self.output_dir = base_output_dir
        self.output_bpwy = os.path.join(self.output_dir, "保赔无忧.csv")
        self.output_qbwy = os.path.join(self.output_dir, "全赔无忧.csv")
        self.output_insurance = os.path.join(self.output_dir, "新车保险台账.csv")

        # -------------------------- 2. 核心配置参数（集中管理，便于维护）--------------------------
        # 工作表名称配置
        self.bpwy_sheet = "登记表"
        self.qbwy_sheet = "全保无忧登记表"
        self.insurance_sheet = "新车台账明细"

        # 各模块必填列配置
        self.bpwy_required_cols = [
            '序号', '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码',
            '保赔无忧金额', '双保无忧金额', '终身保养金额', '销售顾问', '所属门店', '备注', '日期'
        ]
        self.qbwy_required_cols = [
            '客户姓名', '手机号码', '身份证号', '车架号', '发动机号', '车牌号', '车系',
            '新车开票价格', '车损险保额', '车辆类型', '车系网络', '销售日期', '全保无忧版本',
            '全保无忧金额', '起保日期', '终止日期', '销售顾问', '所属门店', '投保费用', 'from'
        ]
        self.insurance_required_cols = [
            '月份', '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司',
            '车型', '车牌号', '车架号', '被保险人', '交强险保费', '销售顾问', '是否为保赔无忧客户'
        ]

        # 业务规则配置
        self.company_mapping = {"文景初治": "上元盛世"}  # 门店名称替换
        self.exclude_operating_fee = [1000, 1130, 1800]  # 运营车排除保费
        self.exclude_operating_company = "鼎和"  # 运营车排除保险公司
        self.max_workers = 5  # 线程池最大线程数

        # -------------------------- 3. 初始化检查（确保环境就绪）--------------------------
        self._init_environment_check()

    def _init_environment_check(self) -> None:
        """初始化环境检查：验证输入目录、依赖文件存在，创建输出目录"""
        # 检查输入目录
        input_dirs = [self.input_bpwy_dir, self.input_qbwy_dir, self.input_insurance_dir]
        for dir_path in input_dirs:
            if not os.path.exists(dir_path):
                raise FileNotFoundError(f"输入目录不存在：{dir_path}")

        # 检查依赖文件
        dependency_files = [self.supplement_car_path, self.insurance_csv_path]
        for file_path in dependency_files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"依赖文件不存在：{file_path}")

        # 创建输出目录（不存在则创建）
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"【初始化完成】")
        print(f"  输入基础目录：{self.input_bpwy_dir.split(os.sep)[0]}{os.sep}{self.input_bpwy_dir.split(os.sep)[1]}")
        print(f"  输出目录：{self.output_dir}")
        print(f"  依赖文件：{os.path.basename(self.supplement_car_path)}、{os.path.basename(self.insurance_csv_path)}")

    # -------------------------- 4. 工具方法（私有，内部复用）--------------------------
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名：去除空格、转小写、空格替换为下划线（原standardize_columns函数）"""
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\s+', '_', regex=True)
        return df

    def _make_unique_columns(self, column_names: List[str]) -> List[str]:
        """列名去重：重复列名添加后缀（原make_unique函数）"""
        unique_names = []
        name_count: Dict[str, int] = {}
        for name in column_names:
            if name in name_count:
                name_count[name] += 1
                unique_names.append(f"{name}_{name_count[name]}")
            else:
                name_count[name] = 0
                unique_names.append(name)
        return unique_names

    def _read_supplement_car(self) -> pd.DataFrame:
        """读取补充车系数据（避免重复读取，多模块复用）"""
        try:
            df_car = pd.read_excel(self.supplement_car_path, sheet_name="补充车系")
            return df_car[["车系", "服务网络"]]
        except Exception as e:
            raise RuntimeError(f"读取补充车系数据失败：{str(e)}") from e

    def _standardize_date(self, date_str: Any) -> Any:
        """标准化日期字符串：补充缺失的'日'（原standardize_date函数）"""
        if isinstance(date_str, str):
            date_str = date_str.strip()
            if "年" in date_str and "月" in date_str and "日" not in date_str:
                date_str += "1日"
        return date_str

    def _convert_date(self, date_str: Any) -> pd.Timestamp:
        """转换日期格式为Timestamp（原convert_date函数）"""
        if isinstance(date_str, str):
            if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_str):
                return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
            elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
                return pd.to_datetime(date_str, format='%Y年%m月%d日')
        return pd.NaT

    def _read_excel_with_thread(self, directory: str, sheet_name: str, required_cols: List[str]) -> List[pd.DataFrame]:
        """
        通用Excel文件读取（多线程）：支持保赔、全保模块复用
        :param directory: 数据目录
        :param sheet_name: 工作表名
        :param required_cols: 必填列列表
        :return: 读取后的DataFrame列表
        """
        dfs: List[pd.DataFrame] = []

        def _read_single_file(file_path: str) -> Optional[pd.DataFrame]:
            """内部函数：读取单个Excel文件（线程任务）"""
            filename = os.path.basename(file_path)
            try:
                # 读取文件并处理
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df["from"] = filename.split('.')[0]  # 添加来源列
                df = self._standardize_columns(df)  # 列名标准化

                # 补全缺失列
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = None

                # 筛选必填列
                df_filtered = df[required_cols].copy()
                print(f"  读取{filename}：成功（{len(df_filtered)}行）")
                return df_filtered
            except Exception as e:
                print(f"  读取{filename}：失败（{str(e)[:50]}...）")
                return None

        # 获取目录下所有Excel文件
        excel_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]
        if not excel_files:
            print(f"  目录{os.path.basename(directory)}：无Excel文件")
            return []

        # 多线程读取
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(_read_single_file, f): f for f in excel_files}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        return dfs

    # -------------------------- 5. 保赔无忧数据处理流程 --------------------------
    def _process_bpwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理保赔无忧数据（原process_dataframe函数）"""
        # 日期处理：开票日期≥销售日期，填充空值并转date格式
        df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed', errors='coerce')
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce')
        df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
        df['日期'] = df['开票日期'].fillna(df['销售日期'])
        df['日期'] = pd.to_datetime(df['日期'], format='mixed', errors='coerce').dt.date

        # 合并补充车系数据，处理直播基地门店名称
        df_car = self._read_supplement_car()
        df = pd.merge(df, df_car, how='left', on='车系')
        df['所属门店'] = np.where(
            df['所属门店'] == '直播基地',
            df['服务网络'] + '-' + df['所属门店'],
            df['所属门店']
        )

        return df[self.bpwy_required_cols]

    def process_bpwy(self) -> pd.DataFrame:
        """保赔无忧数据完整处理流程（原bpwy_result函数）"""
        print("\n【开始处理保赔无忧数据】")
        # 1. 多线程读取文件
        dfs = self._read_excel_with_thread(
            directory=self.input_bpwy_dir,
            sheet_name=self.bpwy_sheet,
            required_cols=self.bpwy_required_cols
        )
        if not dfs:
            raise ValueError("未读取到任何保赔无忧数据")

        # 2. 合并与数据处理
        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_bpwy_data(df_combined)

        # 3. 业务规则处理：标记、替换、分类、去重
        df_processed['是否保赔'] = '是'
        df_processed['所属门店'] = df_processed['所属门店'].replace(self.company_mapping)
        df_processed['城市'] = np.where(
            df_processed['所属门店'].str.contains('贵州'),
            '贵州',
            '成都'
        )
        df_processed.drop_duplicates(inplace=True)
        df_processed.dropna(subset='车架号', inplace=True)

        print(f"【保赔无忧处理完成】共{len(df_processed)}行有效数据")
        return df_processed

    # -------------------------- 6. 全保无忧数据处理流程 --------------------------
    def _process_qbwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理全保无忧数据（原process_dataframe_qbwy函数）"""
        # 日期处理：销售日期转date格式
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce').dt.date

        # 合并补充车系数据，处理直播基地门店名称
        df_car = self._read_supplement_car()
        df = pd.merge(df, df_car, how='left', on='车系')
        df['所属门店'] = np.where(
            df['所属门店'] == '直播基地',
            df['服务网络'] + '-' + df['所属门店'],
            df['所属门店']
        )

        return df[self.qbwy_required_cols]

    def process_qbwy(self) -> pd.DataFrame:
        """全保无忧数据完整处理流程（原qpwy_result函数）"""
        print("\n【开始处理全保无忧数据】")
        # 1. 多线程读取文件
        dfs = self._read_excel_with_thread(
            directory=self.input_qbwy_dir,
            sheet_name=self.qbwy_sheet,
            required_cols=self.qbwy_required_cols
        )
        if not dfs:
            raise ValueError("未读取到任何全保无忧数据")

        # 2. 合并与数据处理
        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_qbwy_data(df_combined)

        # 3. 业务规则处理：去重、筛选非空门店
        df_processed = df_processed.drop_duplicates()
        df_processed = df_processed[df_processed['所属门店'].notnull()]

        print(f"【全保无忧处理完成】共{len(df_processed)}行有效数据")
        return df_processed

    # -------------------------- 7. 新车保险数据处理流程 --------------------------
    def _read_insurance_files(self, directory: str, sheet_name: str) -> pd.DataFrame:
        """读取新车保险Excel文件（原process_xinchebaoxian函数）"""
        dfs: List[pd.DataFrame] = []

        def _read_single_file(filename: str) -> Optional[pd.DataFrame]:
            """内部函数：读取单个保险Excel文件"""
            if '新车' in filename and filename.endswith('.xlsx'):
                file_path = os.path.join(directory, filename)
                try:
                    with pd.ExcelFile(file_path) as xls:
                        if sheet_name not in xls.sheet_names:
                            print(f"  保险文件{filename}：缺少'{sheet_name}'工作表，跳过")
                            return None
                        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                        df['From'] = filename.split('.')[0]
                        df.columns = df.columns.str.replace('\n', '')
                        print(f"  保险文件{filename}：成功（{len(df)}行）")
                        return df
                except Exception as e:
                    print(f"  保险文件{filename}：失败（{str(e)[:50]}...）")
            return None

        # 多线程读取
        filenames = os.listdir(directory)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(_read_single_file, fn): fn for fn in filenames}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        return pd.concat(dfs, axis=0, ignore_index=True) if dfs else pd.DataFrame()

    def _process_insurance_csv(self) -> pd.DataFrame:
        """处理新车保险CSV数据（原xinchebaoxianTZ中的CSV处理逻辑）"""
        df_cyy = pd.read_csv(self.insurance_csv_path)
        # 筛选列并更名
        df_cyy = df_cyy[
            ['出单日期', '保险公司简称', '所属门店', '车系', '车架号', '交强险保费', '业务人员', '保费总额']]
        df_cyy.rename(columns={
            '出单日期': '签单日期',
            '保险公司简称': '保险公司',
            '车系': '车型',
            '所属门店': '归属公司',
            '业务人员': '销售顾问'
        }, inplace=True)

        # 合并补充车系数据，处理直播基地归属公司
        df_car = self._read_supplement_car()
        df_cyy = pd.merge(df_cyy, df_car, how='left', left_on='车型', right_on='车系')
        df_cyy['归属公司'] = np.where(
            df_cyy['归属公司'] == '直播基地',
            df_cyy['服务网络'] + '-' + df_cyy['归属公司'],
            df_cyy['归属公司']
        )
        return df_cyy

    def process_insurance(self) -> pd.DataFrame:
        """新车保险数据完整处理流程（原xinchebaoxianTZ函数）"""
        print("\n【开始处理新车保险数据】")
        # 1. 读取Excel文件
        df_excel = self._read_insurance_files(self.input_insurance_dir, self.insurance_sheet)

        # 2. 处理CSV数据
        df_csv = self._process_insurance_csv()

        # 3. 合并Excel与CSV数据
        all_insurance_dfs = []
        if not df_excel.empty:
            all_insurance_dfs.append(df_excel)
        all_insurance_dfs.append(df_csv)

        # 列名去重
        df_combined = pd.concat(all_insurance_dfs, axis=0, ignore_index=True)
        df_combined.columns = self._make_unique_columns(df_combined.columns)
        df_csv.columns = self._make_unique_columns(df_csv.columns)
        df_combined_all = pd.concat([df_combined, df_csv], axis=0, join='outer', ignore_index=True)

        # 4. 数据清洗：替换名称、筛选列、日期处理
        df_combined_all['归属公司'] = df_combined_all['归属公司'].replace(self.company_mapping)
        df_combined_all.dropna(subset=['保险公司'], inplace=True)

        # 筛选必填列
        exist_cols = [col for col in self.insurance_required_cols if col in df_combined_all.columns]
        df_filtered = df_combined_all[exist_cols].copy()
        df_filtered['日期'] = df_filtered['签单日期']

        # 日期处理与去重
        df_filtered['日期'] = pd.to_datetime(df_filtered['日期'], errors='coerce').dt.date
        df_filtered = df_filtered.sort_values(by='日期', ascending=False)
        df_filtered.drop_duplicates(subset='车架号', inplace=True, keep='first')

        print(f"【新车保险处理完成】共{len(df_filtered)}行有效数据")
        return df_filtered

    # -------------------------- 8. 数据合并与过滤 --------------------------
    def merge_insurance_with_bpwy(self, df_insurance: pd.DataFrame, df_bpwy: pd.DataFrame) -> pd.DataFrame:
        """合并新车保险与保赔无忧数据，过滤运营车（原代码合并过滤逻辑）"""
        print("\n【开始合并保险与保赔数据】")
        # 1. 合并保赔标记（左连接）
        df_merged = pd.merge(
            df_insurance,
            df_bpwy[['车架号', '是否保赔']],
            how='left',
            on='车架号'
        )
        df_merged['是否保赔'] = df_merged['是否保赔'].fillna('否')

        # 2. 筛选运营车（鼎和保险 + 特定保费）
        df_exclude_company = df_merged[df_merged['保险公司'].str.contains(self.exclude_operating_company, na=False)]
        df_exclude_fee = df_merged[df_merged['交强险保费'].isin(self.exclude_operating_fee)]
        df_excluded = pd.concat([df_exclude_company, df_exclude_fee], axis=0).drop_duplicates()
        df_excluded = df_excluded[df_excluded['是否保赔'] == '否']

        # 3. 筛选有效数据（排除运营车）
        df_valid = df_merged[~df_merged['车架号'].isin(df_excluded['车架号'])].copy()
        df_valid['城市'] = np.where(
            df_valid['归属公司'].str.contains('贵州'),
            '贵州',
            '成都'
        )
        df_valid = df_valid.drop_duplicates()

        print(f"【合并过滤完成】有效数据{len(df_valid)}行，排除运营车{len(df_excluded)}行")
        return df_valid

    # -------------------------- 9. 结果保存 --------------------------
    def _save_results(self, df_bpwy: pd.DataFrame, df_qbwy: pd.DataFrame, df_insurance: pd.DataFrame) -> None:
        """保存所有结果文件"""
        print("\n【开始保存结果文件】")
        # 保存保赔无忧
        df_bpwy.to_csv(self.output_bpwy, index=False, encoding='utf-8-sig')
        print(f"  保赔无忧：{self.output_bpwy}（{len(df_bpwy)}行）")

        # 保存全保无忧
        df_qbwy.to_csv(self.output_qbwy, index=False, encoding='utf-8-sig')
        print(f"  全保无忧：{self.output_qbwy}（{len(df_qbwy)}行）")

        # 保存新车保险
        df_insurance.to_csv(self.output_insurance, index=False, encoding='utf-8-sig')
        print(f"  新车保险：{self.output_insurance}（{len(df_insurance)}行）")

    # -------------------------- 10. 主执行入口 --------------------------
    def run(self) -> None:
        """执行完整数据处理流程：串联所有模块"""
        try:
            print("=" * 60)
            print("【保险与保修数据处理器】启动")
            print("=" * 60)

            # 1. 处理三大数据模块
            df_bpwy = self.process_bpwy()  # 保赔无忧
            df_qbwy = self.process_qbwy()  # 全保无忧
            df_insurance = self.process_insurance()  # 新车保险

            # 2. 合并保险与保赔数据，过滤运营车
            df_valid_insurance = self.merge_insurance_with_bpwy(df_insurance, df_bpwy)

            # 3. 保存所有结果
            self._save_results(df_bpwy, df_qbwy, df_valid_insurance)

            print("\n" + "=" * 60)
            print("【保险与保修数据处理器】执行完成")
            print("=" * 60)
        except Exception as e:
            print(f"\n【错误终止】处理流程中断：{str(e)}")
            raise


if __name__ == "__main__":
    # 实例化处理器并执行完整流程
    processor = InsuranceAndWarrantyProcessor()
    processor.run()