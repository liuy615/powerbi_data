import pandas as pd
import numpy as np
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Tuple
from logging.handlers import RotatingFileHandler
import datetime
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, message='.*DataFrame concatenation with empty or all-NA entries.*')

class InsuranceWarrantyIntegrator:
    """
    保险与保修数据整合处理器：统一处理保赔无忧、全保无忧、新车保险台账数据
    核心功能：多线程读取文件、数据清洗标准化、数据合并、运营车过滤、结果集中输出
    """

    def __init__(self,
                 base_output_dir: str = r"E:\powerbi_data\看板数据\dashboard",
                 logger_output_dir: str = r"E:\powerbi_data\代码执行\data\私有云日志",
                 supplement_car_path: str = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx",
                 insurance_csv_path: str = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv"):
        """
        初始化处理器，集中管理路径、配置参数，实现输入输出集中化

        :param base_output_dir: 基础输出目录（所有结果文件集中存放）
        :param supplement_car_path: 补充车系数据文件路径（依赖文件）
        :param insurance_csv_path: 新车保险CSV台账路径（依赖文件）
        """
        # -------------------------- 1. 初始化日志配置 --------------------------
        self.logger = self._init_logger(logger_output_dir)
        self._supplement_car_read = False  # 标记补充车系数据是否已读取（避免重复日志）

        # -------------------------- 2. 路径集中配置（输入输出统一管理）--------------------------
        self.input_paths = {
            "bpwy": r"E:\powerbi_data\看板数据\私有云文件本地\衍生产品",  # 保赔无忧数据目录
            "qbwy": r"E:\powerbi_data\看板数据\私有云文件本地\全保无忧",  # 全保无忧数据目录
            "insurance": r"E:\powerbi_data\看板数据\私有云文件本地\新车保险台账"  # 新车保险数据目录
        }
        self.supplement_car_path = supplement_car_path
        self.insurance_csv_path = insurance_csv_path
        self.output_dir = base_output_dir
        self.output_files = {
            "bpwy": os.path.join(self.output_dir, "保赔无忧.csv"),
            "qbwy": os.path.join(self.output_dir, "全赔无忧.csv"),
            "insurance": os.path.join(self.output_dir, "新车保险台账.csv")
        }

        # -------------------------- 3. 核心配置参数 --------------------------
        self.sheet_names = {
            "bpwy": "登记表",
            "qbwy": "全保无忧登记表",
            "insurance": "新车台账明细"
        }
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
        self.qbwy_final_cols = [
            '客户姓名', '手机号码', '车架号', '车系', '销售日期', '全保无忧版本',
            '全保无忧金额', '所属门店', '销售顾问'
        ]
        self.business_rules = {
            "company_mapping": {"文景初治": "上元盛世", "王朝网-直播基地":"直播基地"},
            "exclude_operating_fee": [1000, 1130, 1800],
            "exclude_operating_company": "鼎和",
            "max_workers": 5
        }

        # -------------------------- 4. 初始化环境检查 --------------------------
        self._init_environment_check()

    def _init_logger(self, log_dir: str) -> logging.Logger:
        """初始化日志配置：精简输出，同时保留关键信息"""
        log_dir = os.path.join(log_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_filename = f"insurance_integration_{datetime.datetime.now().strftime('%Y%m%d')}.log"
        log_filepath = os.path.join(log_dir, log_filename)

        # 简化日志格式（去掉冗余字段）
        log_format = "%(asctime)s - %(levelname)s - %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

        logger = logging.getLogger("InsuranceWarrantyIntegrator")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # 控制台处理器（仅输出关键信息）
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))

        # 文件处理器（保留完整信息，按大小轮转）
        file_handler = RotatingFileHandler(
            log_filepath,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    def _init_environment_check(self) -> None:
        """初始化环境检查：仅输出关键结果日志"""
        self.logger.info("初始化环境检查...")

        # 检查输入目录和依赖文件（错误才输出，成功静默）
        try:
            for key, path in self.input_paths.items():
                if not os.path.exists(path):
                    raise FileNotFoundError(f"{key}数据输入目录：{path}")

            for file_path in [self.supplement_car_path, self.insurance_csv_path]:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"依赖文件：{file_path}")

            os.makedirs(self.output_dir, exist_ok=True)
            self.logger.info("初始化完成 ✅")
        except FileNotFoundError as e:
            self.logger.error(f"初始化失败 ❌：{str(e)}")
            raise

    # -------------------------- 通用工具方法 --------------------------
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名（无日志）"""
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\s+', '_', regex=True)
        return df

    def _make_unique_columns(self, column_names: List[str]) -> List[str]:
        """列名去重（无日志）"""
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
        """读取补充车系数据（仅首次读取输出日志）"""
        if self._supplement_car_read:
            df_car = pd.read_excel(self.supplement_car_path, sheet_name="补充车系")
            return df_car[["车系", "服务网络"]]

        try:
            df_car = pd.read_excel(self.supplement_car_path, sheet_name="补充车系")
            self.logger.info(f"补充车系数据加载完成：{len(df_car)}行")
            self._supplement_car_read = True
            return df_car[["车系", "服务网络"]]
        except Exception as e:
            self.logger.error(f"补充车系数据读取失败 ❌：{str(e)}", exc_info=True)
            raise RuntimeError(f"补充车系数据读取失败：{str(e)}") from e

    def _read_excel_multi_thread(self, module_key: str) -> List[pd.DataFrame]:
        """通用Excel多线程读取（仅输出汇总日志，去掉单个文件日志）"""
        module_name = {"bpwy": "保赔无忧", "qbwy": "全保无忧"}.get(module_key, module_key)
        directory = self.input_paths[module_key]
        sheet_name = self.sheet_names[module_key]
        required_cols = self.required_cols[module_key]
        max_workers = self.business_rules["max_workers"]

        dfs: List[pd.DataFrame] = []
        failed_files = []  # 仅记录失败文件，统一输出

        def _read_single_file(file_path: str) -> Optional[pd.DataFrame]:
            """读取单个文件（无日志，仅记录失败）"""
            filename = os.path.basename(file_path)
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df["from"] = filename.split('.')[0]
                df = self._standardize_columns(df)
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = None
                return df[required_cols].copy()
            except Exception:
                failed_files.append(filename)
                return None

        # 获取Excel文件
        excel_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]
        if not excel_files:
            self.logger.warning(f"{module_name}：无待处理Excel文件 ⚠️")
            return []

        # 多线程读取
        self.logger.info(f"{module_name}：开始读取{len(excel_files)}个文件...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_read_single_file, f): f for f in excel_files}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        # 输出汇总日志
        total_success = len(dfs)
        total_failed = len(failed_files)
        self.logger.info(f"{module_name}：读取完成 → 成功{total_success}个，失败{total_failed}个")
        if failed_files:
            self.logger.warning(
                f"{module_name}：失败文件：{','.join(failed_files[:5])}{'...' if len(failed_files) > 5 else ''}")

        return dfs

    # -------------------------- 保赔无忧数据处理 --------------------------
    def _process_bpwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理保赔无忧数据（无日志）"""
        df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed', errors='coerce')
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce')
        df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
        df['日期'] = df['开票日期'].fillna(df['销售日期'])
        df['日期'] = pd.to_datetime(df['日期'], format='mixed', errors='coerce').dt.date
        return df[self.required_cols["bpwy"]]

    def process_bpwy(self) -> pd.DataFrame:
        """保赔无忧完整流程（仅输出关键节点日志）"""
        self.logger.info("\n" + "-" * 50)
        self.logger.info("开始处理【保赔无忧】数据")

        dfs = self._read_excel_multi_thread(module_key="bpwy")
        if not dfs:
            self.logger.error("保赔无忧：无有效数据 ❌")
            raise ValueError("未读取到保赔无忧有效数据")

        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_bpwy_data(df_combined)

        self.logger.info(f"保赔无忧：处理完成 → 有效数据{len(df_processed)}行 ✅")
        return df_processed

    # -------------------------- 全保无忧数据处理 --------------------------
    def _process_qbwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理全保无忧数据（无日志）"""
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce').dt.date
        df_car = self._read_supplement_car()
        df = pd.merge(df, df_car, how='left', on='车系')
        df['所属门店'] = np.where(
            df['所属门店'] == '直播基地',
            df['服务网络'] + '-' + df['所属门店'],
            df['所属门店']
        )
        return df[self.required_cols["qbwy"]]

    def process_qbwy(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """全保无忧完整流程（仅输出关键节点日志）"""
        self.logger.info("\n" + "-" * 50)
        self.logger.info("开始处理【全保无忧】数据")

        dfs = self._read_excel_multi_thread(module_key="qbwy")
        if not dfs:
            self.logger.error("全保无忧：无有效数据 ❌")
            raise ValueError("未读取到全保无忧有效数据")

        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_qbwy_data(df_combined)

        # 筛选处理
        df_qbwy1 = df_processed.drop_duplicates().query("所属门店.notnull()")
        df_qbwy2 = df_qbwy1[self.qbwy_final_cols].copy()
        df_qbwy2['日期'] = df_qbwy2['销售日期']

        self.logger.info(f"全保无忧：处理完成 → 有效数据{len(df_qbwy2)}行 ✅")
        return df_qbwy1, df_qbwy2

    # -------------------------- 保赔+全保数据合并 --------------------------
    def merge_warranty_data(self, df_bpwy: pd.DataFrame, df_qbwy2: pd.DataFrame) -> pd.DataFrame:
        """合并保赔+全保数据（仅输出合并结果）"""
        self.logger.info("\n" + "-" * 50)
        self.logger.info("开始合并【保赔+全保】数据")

        df_wuyou = pd.concat([df_qbwy2, df_bpwy], axis=0, join='outer', ignore_index=True)
        df_car = self._read_supplement_car()
        df_wuyou = pd.merge(df_wuyou, df_car, how='left', on='车系')
        df_wuyou['所属门店'] = np.where(
            df_wuyou['所属门店'] == '直播基地',
            df_wuyou['服务网络'] + '-' + df_wuyou['所属门店'],
            df_wuyou['所属门店']
        )

        # 业务规则应用
        df_wuyou['是否保赔'] = '是'
        df_wuyou['所属门店'] = df_wuyou['所属门店'].replace(self.business_rules["company_mapping"])
        df_wuyou['城市'] = np.where(df_wuyou['所属门店'].str.contains('贵州'), '贵州', '成都')
        df_wuyou = df_wuyou.drop_duplicates().dropna(subset='车架号')

        self.logger.info(f"合并完成 → 有效数据{len(df_wuyou)}行 ✅")
        return df_wuyou

    # -------------------------- 新车保险数据处理 --------------------------
    def _read_insurance_excel(self) -> pd.DataFrame:
        """读取新车保险Excel（仅输出汇总日志）"""
        directory = self.input_paths["insurance"]
        sheet_name = self.sheet_names["insurance"]
        max_workers = self.business_rules["max_workers"]

        dfs: List[pd.DataFrame] = []
        failed_files = []

        def _read_single_file(filename: str) -> Optional[pd.DataFrame]:
            """读取单个保险Excel（无日志，仅记录失败）"""
            if '新车' in filename and filename.endswith('.xlsx'):
                file_path = os.path.join(directory, filename)
                try:
                    with pd.ExcelFile(file_path) as xls:
                        if sheet_name not in xls.sheet_names:
                            return None
                        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                        df['From'] = filename.split('.')[0]
                        df.columns = df.columns.str.replace('\n', '')
                        return df
                except Exception:
                    failed_files.append(filename)
            return None

        filenames = os.listdir(directory)
        self.logger.info(f"保险Excel：开始读取{len(filenames)}个文件（筛选含'新车'的xlsx）...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_read_single_file, fn): fn for fn in filenames}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        df_combined = pd.concat(dfs, axis=0, ignore_index=True) if dfs else pd.DataFrame()
        self.logger.info(f"保险Excel：读取完成 → 有效数据{len(df_combined)}行，失败{len(failed_files)}个文件")
        return df_combined

    def _process_insurance_csv(self) -> pd.DataFrame:
        """处理新车保险CSV（仅输出关键日志）"""
        try:
            self.logger.info(f"保险CSV：开始读取{os.path.basename(self.insurance_csv_path)}...")
            df_cyy = pd.read_csv(self.insurance_csv_path)

            # 数据处理（无中间日志）
            df_cyy = df_cyy[
                ['出单日期', '保险公司简称', '所属门店', '车系', '车架号', '交强险保费', '业务人员', '保费总额']]
            df_cyy.rename(columns={
                '出单日期': '签单日期', '保险公司简称': '保险公司', '车系': '车型',
                '所属门店': '归属公司', '业务人员': '销售顾问'
            }, inplace=True)

            df_car = self._read_supplement_car()
            df_cyy = pd.merge(df_cyy, df_car, how='left', left_on='车型', right_on='车系')
            df_cyy['归属公司'] = np.where(
                df_cyy['归属公司'] == '直播基地',
                df_cyy['服务网络'] + '-' + df_cyy['归属公司'],
                df_cyy['归属公司']
            )

            self.logger.info(f"保险CSV：处理完成 → 有效数据{len(df_cyy)}行 ✅")
            return df_cyy
        except Exception as e:
            self.logger.error(f"保险CSV：处理失败 ❌：{str(e)}", exc_info=True)
            raise RuntimeError(f"保险CSV处理失败：{str(e)}") from e

    def process_insurance(self) -> pd.DataFrame:
        """新车保险完整流程（仅输出关键节点日志）"""
        self.logger.info("\n" + "-" * 50)
        self.logger.info("开始处理【新车保险】数据")

        # 读取Excel和CSV
        df_excel = self._read_insurance_excel()
        df_csv = self._process_insurance_csv()

        # 合并处理
        all_insurance_dfs = [df_excel] if not df_excel.empty else []
        all_insurance_dfs.append(df_csv)

        df_combined = pd.concat(all_insurance_dfs, axis=0, ignore_index=True)
        df_combined.columns = self._make_unique_columns(df_combined.columns)
        df_csv.columns = self._make_unique_columns(df_csv.columns)
        df_combined_all = pd.concat([df_combined, df_csv], axis=0, join='outer', ignore_index=True)

        # 数据清洗
        df_combined_all['归属公司'] = df_combined_all['归属公司'].replace(self.business_rules["company_mapping"])
        df_combined_all = df_combined_all.dropna(subset=['保险公司'])

        # 筛选必填列
        exist_cols = [col for col in self.required_cols["insurance"] if col in df_combined_all.columns]
        df_filtered = df_combined_all[exist_cols].copy()
        df_filtered['日期'] = pd.to_datetime(df_filtered['签单日期'], errors='coerce').dt.date
        df_filtered = df_filtered.sort_values(by='日期', ascending=False).drop_duplicates(subset='车架号', keep='first')

        self.logger.info(f"新车保险：处理完成 → 有效数据{len(df_filtered)}行 ✅")
        return df_filtered

    # -------------------------- 保险与保修数据合并+运营车过滤 --------------------------
    def merge_insurance_with_warranty(self, df_insurance: pd.DataFrame, df_wuyou: pd.DataFrame) -> pd.DataFrame:
        """合并+过滤运营车（仅输出关键结果）"""
        self.logger.info("\n" + "-" * 50)
        self.logger.info("开始合并【保险+保赔】数据并过滤运营车")

        # 合并保赔标记
        df_merged = pd.merge(df_insurance, df_wuyou[['车架号', '是否保赔']], how='left', on='车架号')
        df_merged['是否保赔'] = df_merged['是否保赔'].fillna('否')

        # 筛选运营车
        df_exclude_company = df_merged[
            df_merged['保险公司'].str.contains(self.business_rules["exclude_operating_company"], na=False)]
        df_exclude_fee = df_merged[df_merged['交强险保费'].isin(self.business_rules["exclude_operating_fee"])]
        df_excluded = pd.concat([df_exclude_company, df_exclude_fee], axis=0).drop_duplicates().query(
            "是否保赔 == '否'")

        # 有效数据
        df_valid = df_merged[~df_merged['车架号'].isin(df_excluded['车架号'])].copy()
        df_valid['城市'] = np.where(df_valid['归属公司'].str.contains('贵州'), '贵州', '成都')
        df_valid = df_valid.drop_duplicates()

        self.logger.info(f"合并过滤完成 → 有效数据{len(df_valid)}行，排除运营车{len(df_excluded)}行 ✅")
        return df_valid

    # -------------------------- 结果保存 --------------------------
    def _save_results(self, df_wuyou: pd.DataFrame, df_qbwy1: pd.DataFrame, df_valid_insurance: pd.DataFrame) -> None:
        """保存结果（仅输出保存状态）"""
        self.logger.info("\n" + "-" * 50)
        self.logger.info("开始保存结果文件")

        # 保存三个文件（无单个文件日志，统一输出结果）
        try:
            df_wuyou.to_csv(self.output_files["bpwy"], index=False, encoding='utf-8-sig')
            df_qbwy1.to_csv(self.output_files["qbwy"], index=False, encoding='utf-8-sig')
            df_valid_insurance.to_csv(self.output_files["insurance"], index=False, encoding='utf-8-sig')
            self.logger.info("结果文件保存完成 ✅")
            self.logger.info(f"  - 保赔无忧：{os.path.basename(self.output_files['bpwy'])}（{len(df_wuyou)}行）")
            self.logger.info(f"  - 全赔无忧：{os.path.basename(self.output_files['qbwy'])}（{len(df_qbwy1)}行）")
            self.logger.info(f"  - 新车保险台账：{os.path.basename(self.output_files['insurance'])}（{len(df_valid_insurance)}行）")
        except Exception as e:
            self.logger.error(f"结果文件保存失败 ❌：{str(e)}", exc_info=True)
            raise

    # -------------------------- 主执行入口 --------------------------
    def run(self) -> None:
        """完整流程执行入口（精简流程日志）"""
        self.logger.info("=" * 60)
        self.logger.info("【保险与保修数据整合处理器】启动")
        self.logger.info("=" * 60)

        try:
            # 1. 处理全保无忧
            df_qbwy1, df_qbwy2 = self.process_qbwy()

            # 2. 处理保赔无忧
            df_bpwy = self.process_bpwy()

            # 3. 合并保赔+全保
            df_wuyou = self.merge_warranty_data(df_bpwy, df_qbwy2)

            # 4. 处理新车保险
            df_insurance = self.process_insurance()

            # 5. 合并+过滤运营车
            df_valid_insurance = self.merge_insurance_with_warranty(df_insurance, df_wuyou)

            # 6. 保存结果
            self._save_results(df_wuyou, df_qbwy1, df_valid_insurance)

            self.logger.info("\n" + "=" * 60)
            self.logger.info("【保险与保修数据整合处理器】执行完成！🎉")
            self.logger.info("=" * 60)
        except Exception as e:
            self.logger.error(f"\n【执行错误】处理流程中断 ❌：{str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    processor = InsuranceWarrantyIntegrator()
    processor.run()