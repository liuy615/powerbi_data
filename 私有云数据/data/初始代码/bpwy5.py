import pandas as pd
import numpy as np
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Tuple


class InsuranceWarrantyIntegrator:
    """
    保险与保修数据整合处理器：统一处理保赔无忧、全保无忧、新车保险台账数据
    核心功能：多线程读取文件、数据清洗标准化、数据合并、运营车过滤、结果集中输出
    """

    def __init__(self,
                 base_output_dir: str = r"E:\powerbi_data\看板数据\dashboard",
                 supplement_car_path: str = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx",
                 insurance_csv_path: str = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv"):
        """
        初始化处理器，集中管理路径、配置参数，实现输入输出集中化

        :param base_output_dir: 基础输出目录（所有结果文件集中存放）
        :param supplement_car_path: 补充车系数据文件路径（依赖文件）
        :param insurance_csv_path: 新车保险CSV台账路径（依赖文件）
        """
        # -------------------------- 1. 路径集中配置（输入输出统一管理）--------------------------
        # 输入路径配置（原代码分散路径集中管理）
        self.input_paths = {
            "bpwy": r"E:\powerbi_data\看板数据\私有云文件本地\衍生产品",  # 保赔无忧数据目录
            "qbwy": r"E:\powerbi_data\看板数据\私有云文件本地\全保无忧",  # 全保无忧数据目录
            "insurance": r"E:\powerbi_data\看板数据\私有云文件本地\新车保险台账"  # 新车保险数据目录
        }
        # 依赖文件路径
        self.supplement_car_path = supplement_car_path
        self.insurance_csv_path = insurance_csv_path
        # 输出路径配置（所有结果集中存放）
        self.output_dir = base_output_dir
        self.output_files = {
            "bpwy": os.path.join(self.output_dir, "保赔无忧.csv"),
            "qbwy": os.path.join(self.output_dir, "全赔无忧.csv"),
            "insurance": os.path.join(self.output_dir, "新车保险台账.csv")
        }

        # -------------------------- 2. 核心配置参数（集中管理，便于维护）--------------------------
        # 工作表名称配置
        self.sheet_names = {
            "bpwy": "登记表",
            "qbwy": "全保无忧登记表",
            "insurance": "新车台账明细"
        }
        # 各模块必填列配置
        self.required_cols = {
            "bpwy": [
                '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码',
                '保赔无忧金额', '双保无忧金额', '终身保养金额', '销售顾问', '所属门店', '备注', '日期'
            ],
            "qbwy": [
                '客户姓名', '手机号码', '身份证号', '车架号', '发动机号', '车牌号', '车系',
                '新车开票价格', '车损险保额', '车辆类型', '车系网络', '销售日期', '全保无忧版本',
                '全保无忧金额', '起保日期', '终止日期', '销售顾问', '所属门店', '投保费用', 'from'
            ],
            "insurance": [
                '月份', '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司',
                '车型', '车牌号', '车架号', '被保险人', '交强险保费', '销售顾问', '是否为保赔无忧客户'
            ]
        }
        # 全保无忧最终筛选列（原代码df_qbwy2的列）
        self.qbwy_final_cols = [
            '客户姓名', '手机号码', '车架号', '车系', '销售日期', '全保无忧版本',
            '全保无忧金额', '所属门店', '销售顾问'
        ]
        # 业务规则配置
        self.business_rules = {
            "company_mapping": {"文景初治": "上元盛世"},  # 门店名称替换
            "exclude_operating_fee": [1000, 1130, 1800],  # 运营车排除保费
            "exclude_operating_company": "鼎和",  # 运营车排除保险公司
            "max_workers": 5  # 线程池最大线程数
        }

        # -------------------------- 3. 初始化环境检查 --------------------------
        self._init_environment_check()

    def _init_environment_check(self) -> None:
        """初始化环境检查：验证输入目录、依赖文件存在，创建输出目录"""
        # 检查输入目录
        for key, path in self.input_paths.items():
            if not os.path.exists(path):
                raise FileNotFoundError(f"【初始化错误】{key}数据输入目录不存在：{path}")

        # 检查依赖文件
        for file_path in [self.supplement_car_path, self.insurance_csv_path]:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"【初始化错误】依赖文件不存在：{file_path}")

        # 创建输出目录（不存在则自动创建）
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"【初始化完成】")
        print(f"  输出目录：{self.output_dir}")
        print(f"  依赖文件：{os.path.basename(self.supplement_car_path)}、{os.path.basename(self.insurance_csv_path)}")
        print(f"  待处理数据模块：保赔无忧、全保无忧、新车保险台账")

    # -------------------------- 4. 通用工具方法（内部复用，减少冗余）--------------------------
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名：去除空格、转换为小写、替换空格为下划线（原standardize_columns函数）"""
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
        """读取补充车系数据（多模块复用，避免重复IO）"""
        try:
            df_car = pd.read_excel(self.supplement_car_path, sheet_name="补充车系")
            return df_car[["车系", "服务网络"]]
        except Exception as e:
            raise RuntimeError(f"【读取错误】补充车系数据读取失败：{str(e)}") from e

    def _read_excel_multi_thread(self, module_key: str) -> List[pd.DataFrame]:
        """
        通用Excel多线程读取方法（复用保赔、全保的读取逻辑）
        :param module_key: 数据模块标识（"bpwy"或"qbwy"）
        :return: 读取并预处理后的DataFrame列表
        """
        directory = self.input_paths[module_key]
        sheet_name = self.sheet_names[module_key]
        required_cols = self.required_cols[module_key]
        max_workers = self.business_rules["max_workers"]

        dfs: List[pd.DataFrame] = []

        def _read_single_file(file_path: str) -> Optional[pd.DataFrame]:
            """内部函数：读取单个Excel文件（线程任务）"""
            filename = os.path.basename(file_path)
            try:
                # 读取文件并添加来源列
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df["from"] = filename.split('.')[0]
                # 列名标准化
                df = self._standardize_columns(df)
                # 补全缺失列
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = None
                # 筛选必填列
                df_filtered = df[required_cols].copy()
                print(f"  【{module_key}】读取{filename}：成功（{len(df_filtered)}行）")
                return df_filtered
            except Exception as e:
                print(f"  【{module_key}】读取{filename}：失败（{str(e)[:50]}...）")
                return None

        # 获取目录下所有Excel文件
        excel_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]
        if not excel_files:
            print(f"  【{module_key}】目录无Excel文件")
            return []

        # 多线程读取文件
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_read_single_file, f): f for f in excel_files}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        return dfs

    # -------------------------- 5. 保赔无忧数据处理流程 --------------------------
    def _process_bpwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理保赔无忧数据（原process_dataframe函数逻辑）"""
        # 日期处理：开票日期≥销售日期，填充空值并转换为date格式
        df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed', errors='coerce')
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce')
        df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
        df['日期'] = df['开票日期'].fillna(df['销售日期'])
        df['日期'] = pd.to_datetime(df['日期'], format='mixed', errors='coerce').dt.date

        return df[self.required_cols["bpwy"]]

    def process_bpwy(self) -> pd.DataFrame:
        """保赔无忧数据完整处理流程（原bpwy_result函数逻辑）"""
        print("\n【开始处理保赔无忧数据】")
        # 1. 多线程读取文件
        dfs = self._read_excel_multi_thread(module_key="bpwy")
        if not dfs:
            raise ValueError("【处理错误】未读取到任何保赔无忧有效数据")

        # 2. 合并数据并处理
        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_bpwy_data(df_combined)

        # 3. 打印预览（原代码逻辑）
        print(f"【保赔无忧处理完成】共{len(df_processed)}行有效数据")
        return df_processed

    # -------------------------- 6. 全保无忧数据处理流程 --------------------------
    def _process_qbwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理全保无忧数据（原process_dataframe_qbwy函数逻辑）"""
        # 日期处理：销售日期转换为date格式
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce').dt.date

        # 合并补充车系数据，处理直播基地门店名称
        df_car = self._read_supplement_car()
        df = pd.merge(df, df_car, how='left', on='车系')
        df['所属门店'] = np.where(
            df['所属门店'] == '直播基地',
            df['服务网络'] + '-' + df['所属门店'],
            df['所属门店']
        )

        return df[self.required_cols["qbwy"]]

    def process_qbwy(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """全保无忧数据完整处理流程（原qpwy_result及后续筛选逻辑）"""
        print("\n【开始处理全保无忧数据】")
        # 1. 多线程读取文件
        dfs = self._read_excel_multi_thread(module_key="qbwy")
        if not dfs:
            raise ValueError("【处理错误】未读取到任何全保无忧有效数据")

        # 2. 合并数据并处理
        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_qbwy_data(df_combined)

        # 3. 后续筛选处理（原代码df_qbwy1、df_qbwy2逻辑）
        df_qbwy1 = df_processed.drop_duplicates()
        df_qbwy1 = df_qbwy1[df_qbwy1['所属门店'].notnull()]
        df_qbwy2 = df_qbwy1[self.qbwy_final_cols].copy()
        df_qbwy2['日期'] = df_qbwy2['销售日期']

        # 4. 打印预览（原代码逻辑）
        print(f"【全保无忧处理完成】原始{len(df_processed)}行 → 筛选后{len(df_qbwy2)}行有效数据")
        return df_qbwy1, df_qbwy2

    # -------------------------- 7. 保赔+全保数据合并处理 --------------------------
    def merge_warranty_data(self, df_bpwy: pd.DataFrame, df_qbwy2: pd.DataFrame) -> pd.DataFrame:
        """合并保赔无忧与全保无忧数据（原代码df_wuyou逻辑）"""
        print("\n【开始合并保赔+全保数据】")
        # 1. 合并两个数据集
        df_wuyou = pd.concat([df_qbwy2, df_bpwy], axis=0, join='outer', ignore_index=True)

        # 2. 合并补充车系数据，处理直播基地门店名称
        df_car = self._read_supplement_car()
        df_wuyou = pd.merge(df_wuyou, df_car, how='left', on='车系')
        df_wuyou['所属门店'] = np.where(
            df_wuyou['所属门店'] == '直播基地',
            df_wuyou['服务网络'] + '-' + df_wuyou['所属门店'],
            df_wuyou['所属门店']
        )

        # 3. 业务规则应用
        df_wuyou['是否保赔'] = '是'
        df_wuyou['所属门店'] = df_wuyou['所属门店'].replace(self.business_rules["company_mapping"])
        df_wuyou['城市'] = np.where(
            df_wuyou['所属门店'].str.contains('贵州'),
            '贵州',
            '成都'
        )
        df_wuyou.drop_duplicates(inplace=True)
        df_wuyou.dropna(subset='车架号', inplace=True)

        print(f"【合并完成】保赔{len(df_bpwy)}行 + 全保{len(df_qbwy2)}行 → 最终{len(df_wuyou)}行有效数据")
        return df_wuyou

    # -------------------------- 8. 新车保险数据处理流程 --------------------------
    def _read_insurance_excel(self) -> pd.DataFrame:
        """读取新车保险Excel文件（原process_xinchebaoxian函数逻辑）"""
        directory = self.input_paths["insurance"]
        sheet_name = self.sheet_names["insurance"]
        max_workers = self.business_rules["max_workers"]

        dfs: List[pd.DataFrame] = []

        def _read_single_file(filename: str) -> Optional[pd.DataFrame]:
            """内部函数：读取单个新车保险Excel文件"""
            if '新车' in filename and filename.endswith('.xlsx'):
                file_path = os.path.join(directory, filename)
                try:
                    with pd.ExcelFile(file_path) as xls:
                        if sheet_name not in xls.sheet_names:
                            print(f"  【保险】{filename}缺少'{sheet_name}'工作表，跳过")
                            return None
                        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                        df['From'] = filename.split('.')[0]
                        df.columns = df.columns.str.replace('\n', '')
                        print(f"  【保险】读取{filename}：成功（{len(df)}行）")
                        return df
                except Exception as e:
                    print(f"  【保险】读取{filename}：失败（{str(e)[:50]}...）")
            return None

        # 多线程读取文件
        filenames = os.listdir(directory)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_read_single_file, fn): fn for fn in filenames}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        return pd.concat(dfs, axis=0, ignore_index=True) if dfs else pd.DataFrame()

    def _process_insurance_csv(self) -> pd.DataFrame:
        """处理新车保险CSV数据（原xinchebaoxianTZ中CSV处理逻辑）"""
        try:
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
        except Exception as e:
            raise RuntimeError(f"【处理错误】新车保险CSV数据处理失败：{str(e)}") from e

    def process_insurance(self) -> pd.DataFrame:
        """新车保险数据完整处理流程（原xinchebaoxianTZ函数逻辑）"""
        print("\n【开始处理新车保险数据】")
        # 1. 读取Excel文件
        df_excel = self._read_insurance_excel()

        # 2. 处理CSV数据
        df_csv = self._process_insurance_csv()

        # 3. 合并Excel与CSV数据
        all_insurance_dfs = []
        if not df_excel.empty:
            all_insurance_dfs.append(df_excel)
        all_insurance_dfs.append(df_csv)

        # 4. 列名去重与数据合并
        df_combined = pd.concat(all_insurance_dfs, axis=0, ignore_index=True)
        df_combined.columns = self._make_unique_columns(df_combined.columns)
        df_csv.columns = self._make_unique_columns(df_csv.columns)
        df_combined_all = pd.concat([df_combined, df_csv], axis=0, join='outer', ignore_index=True)

        # 5. 数据清洗
        df_combined_all['归属公司'] = df_combined_all['归属公司'].replace(self.business_rules["company_mapping"])
        df_combined_all.dropna(subset=['保险公司'], inplace=True)

        # 6. 筛选必填列
        exist_cols = [col for col in self.required_cols["insurance"] if col in df_combined_all.columns]
        df_filtered = df_combined_all[exist_cols].copy()
        df_filtered['日期'] = df_filtered['签单日期']

        # 7. 日期处理与去重
        df_filtered['日期'] = pd.to_datetime(df_filtered['日期'], errors='coerce').dt.date
        df_filtered = df_filtered.sort_values(by='日期', ascending=False)
        df_filtered.drop_duplicates(subset='车架号', inplace=True, keep='first')

        print(f"【新车保险处理完成】共{len(df_filtered)}行有效数据")
        return df_filtered

    # -------------------------- 9. 保险与保修数据合并+运营车过滤 --------------------------
    def merge_insurance_with_warranty(self, df_insurance: pd.DataFrame, df_wuyou: pd.DataFrame) -> pd.DataFrame:
        """合并新车保险与保赔数据，过滤运营车（原代码df5、diff_df逻辑）"""
        print("\n【开始合并保险与保赔数据+过滤运营车】")
        # 1. 合并保赔标记（左连接）
        df_merged = pd.merge(
            df_insurance,
            df_wuyou[['车架号', '是否保赔']],
            how='left',
            on='车架号'
        )
        df_merged['是否保赔'] = df_merged['是否保赔'].fillna('否')

        # 2. 筛选运营车（鼎和保险 + 特定保费）
        df_exclude_company = df_merged[
            df_merged['保险公司'].str.contains(self.business_rules["exclude_operating_company"], na=False)]
        df_exclude_fee = df_merged[df_merged['交强险保费'].isin(self.business_rules["exclude_operating_fee"])]
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

    # -------------------------- 10. 结果保存 --------------------------
    def _save_results(self, df_wuyou: pd.DataFrame, df_qbwy1: pd.DataFrame, df_valid_insurance: pd.DataFrame) -> None:
        """保存三个输出文件（原代码最后保存逻辑）"""
        print("\n【开始保存结果文件】")
        # 保存保赔无忧（合并后的数据）
        df_wuyou.to_csv(self.output_files["bpwy"], index=False, encoding='utf-8-sig')
        print(f"  ✅ 保赔无忧：{os.path.basename(self.output_files['bpwy'])}（{len(df_wuyou)}行）")

        # 保存全赔无忧（原df_qbwy1）
        df_qbwy1.to_csv(self.output_files["qbwy"], index=False, encoding='utf-8-sig')
        print(f"  ✅ 全赔无忧：{os.path.basename(self.output_files['qbwy'])}（{len(df_qbwy1)}行）")

        # 保存新车保险台账（过滤后的数据）
        df_valid_insurance.to_csv(self.output_files["insurance"], index=False, encoding='utf-8-sig')
        print(f"  ✅ 新车保险台账：{os.path.basename(self.output_files['insurance'])}（{len(df_valid_insurance)}行）")

    # -------------------------- 11. 主执行入口 --------------------------
    def run(self) -> None:
        """完整流程执行入口：串联所有处理步骤"""
        try:
            print("=" * 60)
            print("【保险与保修数据整合处理器】启动")
            print("=" * 60)

            # 1. 处理全保无忧数据（返回df_qbwy1用于保存，df_qbwy2用于合并）
            df_qbwy1, df_qbwy2 = self.process_qbwy()

            # 2. 处理保赔无忧数据
            df_bpwy = self.process_bpwy()

            # 3. 合并保赔+全保数据
            df_wuyou = self.merge_warranty_data(df_bpwy, df_qbwy2)

            # 4. 处理新车保险数据
            df_insurance = self.process_insurance()

            # 5. 合并保险与保赔数据，过滤运营车
            df_valid_insurance = self.merge_insurance_with_warranty(df_insurance, df_wuyou)

            # 6. 保存所有结果
            self._save_results(df_wuyou, df_qbwy1, df_valid_insurance)

            print("\n" + "=" * 60)
            print("【保险与保修数据整合处理器】执行完成！")
            print("=" * 60)
        except Exception as e:
            print(f"\n【执行错误】处理流程中断：{str(e)}")
            raise


if __name__ == "__main__":
    # 实例化处理器并执行完整流程

    processor = InsuranceWarrantyIntegrator()
    processor.run()