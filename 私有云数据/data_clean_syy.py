import pandas as pd
import numpy as np
import os
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Tuple, Any
from logging.handlers import RotatingFileHandler
from pymongo import MongoClient
import datetime

# 抑制警告
warnings.filterwarnings("ignore", category=UserWarning, message="Data Validation extension is not supported and will be removed")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*DataFrame concatenation with empty or all-NA entries.*")
pd.set_option('display.max_columns', 100)

"""数据处理基类，提供统一的日志和异常处理"""
class DataProcessorBase:


    def __init__(self, processor_name: str, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        self.processor_name = processor_name
        self.base_data_dir = base_data_dir
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """设置日志配置"""
        log_dir = os.path.join(self.base_data_dir, "代码执行", "data", "私有云日志", "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_filename = f"{self.processor_name}_{datetime.datetime.now().strftime('%Y%m%d')}.log"
        log_filepath = os.path.join(log_dir, log_filename)

        logger = logging.getLogger(self.processor_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # 避免重复添加handler
        if not logger.handlers:
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # 文件处理器
            file_handler = RotatingFileHandler(
                log_filepath,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8"
            )
            file_handler.setLevel(logging.INFO)

            # 格式化器
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        return logger

    def safe_execute(self, func, *args, **kwargs):
        """安全执行方法，捕获异常并记录日志"""
        try:
            self.logger.info(f"开始执行: {func.__name__}")
            result = func(*args, **kwargs)
            self.logger.info(f"完成执行: {func.__name__}")
            return result
        except Exception as e:
            self.logger.error(f"执行失败 {func.__name__}: {str(e)}", exc_info=True)
            return None

"""MongoDB连接配置类"""
class MongoDBConfig:
    def __init__(self, host='192.168.1.7', port=27017,username='xg_wd', password='H91NgHzkvRiKygTe4X4ASw',auth_source='xg', database='xg_JiaTao'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth_source = auth_source
        self.database_name = database

    def get_connection_string(self):
        """构建连接字符串"""
        return f'mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}?authSource={self.auth_source}&authMechanism=SCRAM-SHA-256'

    def get_database_name(self):
        return self.database_name

"""MongoDB客户端操作类"""
class MongoDBClient:
    def __init__(self, config):
        self.config = config
        self.client = None
        self.db = None
        self.connected = False

    def connect(self):
        """建立数据库连接"""
        try:
            self.client = MongoClient(self.config.get_connection_string())
            self.db = self.client[self.config.get_database_name()]

            # 测试连接
            self.client.admin.command('ping')
            self.connected = True
            print("成功连接到MongoDB!")
            return True

        except Exception as e:
            print(f"连接失败: {e}")
            self.connected = False
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.client:
            self.client.close()
            self.connected = False
            print("数据库连接已关闭")

    def get_collection_count(self, collection_name):
        """获取指定集合的文档数量"""
        if not self.connected:
            print("未建立数据库连接")
            return 0

        try:
            collection = self.db[collection_name]
            return collection.count_documents({})
        except Exception as e:
            print(f"获取文档数量失败: {e}")
            return 0

    def query_data_with_projection(self, collection_name, desired_fields, limit=None, query_filter=None):
        """查询指定集合中指定字段的数据"""
        if not self.connected:
            print("未建立数据库连接")
            return None

        try:
            # 获取指定集合
            collection = self.db[collection_name]

            # 动态构建投影
            projection = {field: 1 for field in desired_fields}
            projection["_id"] = 0  # 不返回_id

            # 设置查询过滤器，默认为空
            if query_filter is None:
                query_filter = {}

            # 查询数据
            query = collection.find(query_filter, projection)
            if limit:
                query = query.limit(limit)

            # 转换为列表和DataFrame
            data_list = list(query)
            df = pd.DataFrame(data_list)

            return df

        except Exception as e:
            print(f"查询失败: {e}")
            return None

    def query_all_data(self, collection_name, limit=None, query_filter=None, exclude_id=True):
        """查询指定集合中的所有数据"""
        if not self.connected:
            print("未建立数据库连接")
            return None

        try:
            # 获取指定集合
            collection = self.db[collection_name]

            # 设置投影，默认不返回_id字段
            projection = {"_id": 0} if exclude_id else {}

            # 设置查询过滤器，默认为空
            if query_filter is None:
                query_filter = {}

            # 查询数据
            query = collection.find(query_filter, projection)
            if limit:
                query = query.limit(limit)

            # 转换为列表和DataFrame
            data_list = list(query)
            df = pd.DataFrame(data_list)

            return df

        except Exception as e:
            print(f"查询失败: {e}")
            return None

    def list_collections(self):
        """列出数据库中的所有集合"""
        if not self.connected:
            print("未建立数据库连接")
            return []

        try:
            collections = self.db.list_collection_names()
            print("数据库中的集合列表:")
            for i, collection in enumerate(collections, 1):
                print(f"{i}. {collection}")
            return collections
        except Exception as e:
            print(f"获取集合列表失败: {e}")
            return []

    def get_collection_fields(self, collection_name, sample_size=5):
        """获取指定集合的字段信息"""
        if not self.connected:
            print("未建立数据库连接")
            return []

        try:
            collection = self.db[collection_name]
            sample_doc = collection.find_one()
            if sample_doc:
                fields = list(sample_doc.keys())
                print(f"集合 '{collection_name}' 的字段:")
                for field in fields:
                    print(f"- {field}")
                return fields
            else:
                print(f"集合 '{collection_name}' 为空或不存在")
                return []
        except Exception as e:
            print(f"获取字段信息失败: {e}")
            return []

"""营销投放费用数据处理器"""
class YingxiaoMoneyProcessor(DataProcessorBase):


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        super().__init__("营销投放费用处理器", base_data_dir)

        # 路径配置
        self.input_dir = os.path.join(base_data_dir, "私有云文件本地", "投放市场费用")
        self.output_dir = os.path.join(base_data_dir, "dashboard")
        self.output_file = os.path.join(self.output_dir, "投放费用.csv")

        # 业务配置
        self.target_sheets = ["2024年", "2025年"]
        self.required_columns = [
            "年份", "月份", "归属门店", "项目大类", "项目分类", "具体项目",
            "费用金额", "核销发票金额", "核销发票税金", "费用合计", "备注", "From"
        ]
        self.store_map = {"文景初治": "上元盛世", "王朝网-直播基地":"直播基地"}

        self._init_check()

    def _init_check(self):
        """环境检查"""
        if not os.path.exists(self.input_dir):
            raise FileNotFoundError(f"输入目录不存在：{self.input_dir}")
        os.makedirs(self.output_dir, exist_ok=True)
        self.logger.info(f"初始化完成 - 输入目录: {self.input_dir}, 输出目录: {self.output_dir}")

    def _get_excel_files(self) -> List[str]:
        """获取所有Excel文件路径"""
        excel_files = [
            os.path.join(self.input_dir, f)
            for f in os.listdir(self.input_dir)
            if f.endswith(".xlsx")
        ]
        self.logger.info(f"找到 {len(excel_files)} 个Excel文件")
        return excel_files

    def _process_single_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """处理单个文件"""
        file_name = os.path.basename(file_path)
        try:
            dfs = []
            with pd.ExcelFile(file_path) as xls:
                for sheet in self.target_sheets:
                    if sheet in xls.sheet_names:
                        df = pd.read_excel(xls, sheet)
                        df["From"] = file_name.split('.')[0]
                        if not df.empty:
                            dfs.append(df)

            if dfs:
                df_comb = pd.concat(dfs, axis=0, ignore_index=True)
                self.logger.info(f"处理文件 {file_name} 成功 - {len(df_comb)}行")
                return df_comb
            else:
                self.logger.warning(f"文件 {file_name} 无目标工作表数据")
                return None
        except Exception as e:
            self.logger.error(f"处理文件 {file_name} 失败: {str(e)}")
            return None

    def _process_all_files(self, excel_files: List[str]) -> pd.DataFrame:
        """多线程处理所有文件"""
        self.logger.info(f"开始多线程处理 {len(excel_files)} 个文件")
        combined = []

        with ThreadPoolExecutor() as executor:
            results = executor.map(self._process_single_file, excel_files)
            for res in results:
                if res is not None:
                    combined.append(res)

        if not combined:
            raise ValueError("无有效数据可合并")

        df_all = pd.concat(combined, axis=0, ignore_index=True)
        self.logger.info(f"文件合并完成 - 总计 {len(df_all)} 行数据")
        return df_all

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据清洗"""
        exist_cols = [c for c in self.required_columns if c in df.columns]
        df_clean = df[exist_cols].copy()

        if "费用合计" in df_clean.columns and "费用金额" in df_clean.columns:
            df_clean["费用合计"] = df_clean["费用合计"].fillna(df_clean["费用金额"])
            df_clean["费用合计"] = pd.to_numeric(df_clean["费用合计"], errors="coerce").fillna(0)

        self.logger.info(f"数据清洗完成 - {len(df_clean)}行, {len(exist_cols)}列")
        return df_clean

    def _replace_store(self, df: pd.DataFrame) -> pd.DataFrame:
        """替换门店名称"""
        if "归属门店" in df.columns:
            df["归属门店"] = df["归属门店"].replace(self.store_map)
            self.logger.info("门店名称替换完成")
        return df

    def _save_result(self, df: pd.DataFrame) -> None:
        """保存结果"""
        df.to_csv(self.output_file, index=False, encoding="utf-8-sig")
        self.logger.info(f"结果保存成功: {self.output_file} - {len(df)}行")

    def run(self) -> bool:
        """执行完整流程"""
        self.logger.info("=" * 60)
        self.logger.info("开始营销投放费用处理流程")
        self.logger.info("=" * 60)

        try:
            excel_files = self.safe_execute(self._get_excel_files)
            if not excel_files:
                self.logger.error("未找到Excel文件")
                return False

            df_all = self.safe_execute(self._process_all_files, excel_files)
            if df_all is None:
                return False

            df_clean = self.safe_execute(self._clean_data, df_all)
            df_final = self.safe_execute(self._replace_store, df_clean)
            self.safe_execute(self._save_result, df_final)

            self.logger.info("营销投放费用处理完成!")
            return True

        except Exception as e:
            self.logger.error(f"营销投放费用处理失败: {str(e)}", exc_info=True)
            return False


"""特殊事项收入数据处理器"""
class SpecialIncomeProcessor(DataProcessorBase):


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        super().__init__("特殊事项收入处理器", base_data_dir)

        self.directories = [os.path.join(base_data_dir, "私有云文件本地", "特殊事项收入")]
        self.output_path = os.path.join(base_data_dir, "dashboard", "特殊费用收入.csv")
        self.target_sheet = '登记表'
        self.required_columns = [
            '业务时间', '归属门店', '车架号', '客户名称', '事项名称',
            '收付类型', '金额', '备注', 'From'
        ]

        self.df_result = pd.DataFrame()

    def _get_all_file_paths(self):
        """获取所有Excel文件路径"""
        file_paths = []
        for folder_path in self.directories:
            if os.path.exists(folder_path):
                for file_name in os.listdir(folder_path):
                    if file_name.endswith('.xlsx'):
                        file_path = os.path.join(folder_path, file_name)
                        file_paths.append(file_path)
        self.logger.info(f"找到 {len(file_paths)} 个Excel文件")
        return file_paths

    def _process_single_file(self, file_path):
        """处理单个Excel文件"""
        try:
            with pd.ExcelFile(file_path) as xls:
                if self.target_sheet not in xls.sheet_names:
                    self.logger.warning(f"文件 {os.path.basename(file_path)} 无目标工作表")
                    return None

                data = pd.read_excel(xls, sheet_name=self.target_sheet)
                data['From'] = os.path.basename(file_path).split('.')[0]

                self.logger.info(f"处理文件 {os.path.basename(file_path)} 成功 - {len(data)}行")
                return data

        except Exception as e:
            self.logger.error(f"处理文件 {os.path.basename(file_path)} 失败: {str(e)}")
            return None

    def _process_data_quality(self):
        """数据质量处理"""
        existing_columns = [col for col in self.required_columns if col in self.df_result.columns]
        self.df_result = self.df_result[existing_columns]

        if '金额' in self.df_result.columns:
            self.df_result['金额'] = pd.to_numeric(self.df_result['金额'], errors='coerce').fillna(0)

        if '归属门店' in self.df_result.columns:
            self.df_result['归属门店'] = self.df_result['归属门店'].replace('文景初治', '上元盛世')
            self.df_result['归属门店'] = self.df_result['归属门店'].replace("王朝网-直播基地", "直播基地")

        self.logger.info(f"数据质量处理完成 - 保留{len(existing_columns)}列")

    def load_data(self):
        """加载并合并所有数据"""
        file_paths = self._get_all_file_paths()
        if not file_paths:
            self.logger.error("未找到Excel文件")
            return False

        combined_data = []
        with ThreadPoolExecutor() as executor:
            results = executor.map(self._process_single_file, file_paths)
            for result in results:
                if result is not None and not result.empty:
                    combined_data.append(result)

        if combined_data:
            self.df_result = pd.concat(combined_data, ignore_index=True)
            self.logger.info(f"数据加载完成 - {len(self.df_result)} 行")
            return True
        else:
            self.logger.error("未找到有效数据")
            return False

    def save_data(self):
        """保存处理后的数据"""
        if self.df_result.empty:
            self.logger.error("无数据可保存")
            return False

        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            self.df_result.to_csv(self.output_path, index=False)
            self.logger.info(f"数据保存成功: {self.output_path}")
            return True
        except Exception as e:
            self.logger.error(f"数据保存失败: {str(e)}")
            return False

    def run(self) -> bool:
        """执行完整的数据处理流程"""
        self.logger.info("=" * 60)
        self.logger.info("开始特殊事项收入数据处理流程")
        self.logger.info("=" * 60)

        try:
            if not self.safe_execute(self.load_data):
                return False

            self.safe_execute(self._process_data_quality)

            if not self.safe_execute(self.save_data):
                return False

            self.logger.info("特殊事项收入数据处理完成!")
            return True

        except Exception as e:
            self.logger.error(f"特殊事项收入数据处理失败: {str(e)}", exc_info=True)
            return False


"""全保无忧数据整合处理器"""
class InsuranceWarrantyIntegrator(DataProcessorBase):


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        super().__init__("全保无忧整合处理器", base_data_dir)

        # 路径配置
        self.output_dir = os.path.join(base_data_dir, "dashboard")
        self.supplement_car_path = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx"
        self.insurance_csv_path = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv"
        self.chanpin_xls_path = r"E:\powerbi_data\看板数据\私有云文件本地\全保无忧产品明细\产品费用明细.xlsx"

        self.input_paths = {
            "bpwy": os.path.join(base_data_dir, "私有云文件本地", "衍生产品"),
            "insurance": os.path.join(base_data_dir, "私有云文件本地", "新车保险台账")
        }

        self.output_files = {
            "bpwy": os.path.join(self.output_dir, "保赔无忧.csv"),
            "qbwy": os.path.join(self.output_dir, "全赔无忧.csv"),
            "insurance": os.path.join(self.output_dir, "新车保险台账.csv")
        }

        # 核心配置
        self.sheet_names = {
            "bpwy": "登记表",
            "insurance": "新车台账明细"
        }

        self.required_cols = {
            "bpwy": [
                '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码',
                '保赔无忧金额', '双保无忧金额', '终身保养金额', '销售顾问', '所属门店', '备注', '日期',
            ],
            "qbwy": [
                '客户姓名', '手机号码', '身份证号', '车架号', '发动机号', '车牌号', '车系',
                '新车开票价格', '车损险保额', '车辆时间阶段', '车辆类型', '车系网络', '销售日期', '全保无忧版本', '车价/车损投保价',
                '全保无忧金额', '起保日期', '终止日期', '销售顾问', '所属门店', '投保费用', 'from'
            ],
            "insurance": [
                '月份', '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司',
                '车型', '车牌号', '车架号', '被保险人', '交强险保费', '销售顾问', '是否为保赔无忧客户'
            ]
        }

        self.qbwy_final_cols = [
            '客户姓名', '手机号码', '车架号', '车系', '销售日期', '全保无忧版本', '投保价', '车辆阶段', '车辆类型', '全保无忧金额', '所属门店', '销售顾问', '车系网络'
        ]

        self.business_rules = {
            "company_mapping": {"文景初治": "上元盛世", "王朝网-直播基地":"直播基地"},
            "exclude_operating_fee": [1000, 1130, 1800],
            "exclude_operating_company": "鼎和",
            "max_workers": 5
        }

        self._supplement_car_read = False
        self._init_environment_check()

    def _init_environment_check(self):
        """初始化环境检查"""
        self.logger.info("开始环境检查...")
        try:
            for key, path in self.input_paths.items():
                if not os.path.exists(path):
                    raise FileNotFoundError(f"{key}数据输入目录不存在: {path}")

            for file_path in [self.supplement_car_path, self.insurance_csv_path]:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"依赖文件不存在: {file_path}")

            os.makedirs(self.output_dir, exist_ok=True)
            self.logger.info("环境检查完成")
        except FileNotFoundError as e:
            self.logger.error(f"环境检查失败: {str(e)}")
            raise

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\s+', '_', regex=True)
        return df

    def _make_unique_columns(self, column_names: List[str]) -> List[str]:
        """列名去重"""
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
        """读取补充车系数据"""
        if self._supplement_car_read:
            df_car = pd.read_excel(self.supplement_car_path, sheet_name="补充车系")
            return df_car[["车系", "服务网络"]]

        try:
            df_car = pd.read_excel(self.supplement_car_path, sheet_name="补充车系")
            self.logger.info(f"补充车系数据加载完成: {len(df_car)}行")
            self._supplement_car_read = True
            return df_car[["车系", "服务网络"]]
        except Exception as e:
            self.logger.error(f"补充车系数据读取失败: {str(e)}")
            raise

    def _read_excel_multi_thread(self, module_key: str) -> List[pd.DataFrame]:
        """通用Excel多线程读取"""
        module_name = {"bpwy": "保赔无忧", "qbwy": "全保无忧"}.get(module_key, module_key)
        directory = self.input_paths[module_key]
        sheet_name = self.sheet_names[module_key]
        required_cols = self.required_cols[module_key]
        max_workers = self.business_rules["max_workers"]

        dfs: List[pd.DataFrame] = []
        failed_files = []

        def _read_single_file(file_path: str) -> Optional[pd.DataFrame]:
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

        excel_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]
        if not excel_files:
            self.logger.warning(f"{module_name}: 无待处理Excel文件")
            return []

        self.logger.info(f"{module_name}: 开始读取{len(excel_files)}个文件")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_read_single_file, f): f for f in excel_files}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        total_success = len(dfs)
        total_failed = len(failed_files)
        self.logger.info(f"{module_name}: 读取完成 → 成功{total_success}个, 失败{total_failed}个")
        if failed_files:
            self.logger.warning(
                f"{module_name}: 失败文件: {','.join(failed_files[:5])}{'...' if len(failed_files) > 5 else ''}")

        return dfs

    # 处理全保无忧数据
    def process_qbwy(self):
        # 创建配置和客户端实例
        config = MongoDBConfig(database='xg_JiaTao')
        mongo_client = MongoDBClient(config)
        collection_name_qbwy = '全保无忧销售数据'
        collection_name_qbwy_info = '全保无忧费用明细'

        try:
            # 连接数据库
            if mongo_client.connect():
                # 列出所有可用的集合
                # collections = mongo_client.list_collections()

                # 查询全保无忧的销售数据
                count_1 = mongo_client.get_collection_count(collection_name_qbwy)
                print(f"\n集合 '{collection_name_qbwy}' 中的文档总数: {count_1}")

                # 查询全部数据数据
                df_qbwy1 = mongo_client.query_all_data(collection_name_qbwy)
                df_qbwy1 = df_qbwy1[df_qbwy1["数据有效性"] == "有效"]
                df_qbwy1 = df_qbwy1[df_qbwy1["业务部门"] == "售前"]
                df_qbwy1 = df_qbwy1[df_qbwy1["产品状态"] == "生效"]
                df_qbwy1["全保无忧版本"] = df_qbwy1["全保无忧版本"].replace("畅行+终身版(送)", "畅行版")
                # df_qbwy1.to_csv("df_qbwy1.csv")

                # 查询指定字段
                # 定义需要的字段
                desired_fields = ['业务部门', '产品状态', '客户姓名', '手机号码', '车架号', '车系', '销售日期', '全保无忧版本', '新车开票价格', '车辆阶段', '车辆类型', '全保无忧金额', '终身保养金额', '所属门店','销售顾问', '车系网络', '数据有效性']
                df_qbwy2 = mongo_client.query_data_with_projection(collection_name_qbwy, desired_fields)

                # 设置城市
                df_qbwy2['城市'] = np.where(df_qbwy2['所属门店'].str.contains('贵州'), '贵州', '成都')
                df_qbwy2["全保无忧版本"] = df_qbwy2["全保无忧版本"].replace("畅行+终身版(送)", "畅行版")

                # 设置投保价
                bins = [0, 100000, 150000, 200000, 250000, 300000, 350000, np.inf]
                labels = ['0-10万', '10-15万', '15-20万', '20-25万', '25-30万', '30-35万', '35-40万']
                df_qbwy2['投保价'] = pd.cut(df_qbwy2['新车开票价格'], bins=bins, labels=labels, right=False).astype(str)
                # 筛选有效数据
                df_qbwy2 = df_qbwy2[df_qbwy2["数据有效性"] == "有效"]
                df_qbwy2 = df_qbwy2[df_qbwy2["业务部门"] == "售前"]
                df_qbwy2 = df_qbwy2[df_qbwy2["产品状态"] == "生效"]
                # df_qbwy2.to_csv("df_qbwy2.csv")
                # 查询全保无忧费用明细
                chanpin = mongo_client.query_all_data(collection_name_qbwy_info)
                chanpin["关联键"] = chanpin["区域"] + "_" + chanpin["网络"] + "_" + chanpin["车辆阶段"] + "_" + chanpin["车价/车损投保价"] + "_" + chanpin["产品规格"] + "_" + chanpin["车辆类型"]
                df_qbwy2["关联键"] = df_qbwy2["城市"] + "_" + df_qbwy2["车系网络"] + "_" + df_qbwy2["车辆阶段"] + "_" + df_qbwy2["投保价"] + "_" + df_qbwy2["全保无忧版本"] + "_" + df_qbwy2["车辆类型"]
                merged_df = pd.merge(
                    df_qbwy2,
                    chanpin[["关联键", "开始时间", "结束时间", "成本"]],  # 只选择需要的列
                    on="关联键",
                    how="left"
                )
                mask = (
                        (merged_df["销售日期"] >= merged_df["开始时间"]) &
                        (merged_df["销售日期"] <= merged_df["结束时间"])
                )
                # 只保留有效记录
                valid_matches = merged_df[mask].copy()
                valid_matches.sort_values(by=["车架号", "开始时间"], ascending=[True, False], inplace=True)
                valid_matches = valid_matches.drop_duplicates(subset=["车架号"], keep="first")
                valid_matches["利润"] = valid_matches["全保无忧金额"] - valid_matches["成本"]
                valid_matches["日期"] = pd.to_datetime(valid_matches["销售日期"], format='mixed', errors='coerce').dt.date

        except Exception as e:
            print(f"程序执行失败: {e}")
        finally:
            # 关闭连接
            mongo_client.disconnect()

        return df_qbwy1, valid_matches

    def _process_bpwy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理保赔无忧数据"""
        df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed', errors='coerce')
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce')
        df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
        df['日期'] = df['开票日期'].fillna(df['销售日期'])
        df['日期'] = pd.to_datetime(df['日期'], format='mixed', errors='coerce').dt.date
        return df[self.required_cols["bpwy"]]

    def process_bpwy(self) -> pd.DataFrame:
        """保赔无忧完整流程"""
        self.logger.info("开始处理【保赔无忧】数据")
        dfs = self._read_excel_multi_thread(module_key="bpwy")
        if not dfs:
            self.logger.error("保赔无忧: 无有效数据")
            raise ValueError("未读取到保赔无忧有效数据")

        df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
        df_processed = self._process_bpwy_data(df_combined)
        self.logger.info(f"保赔无忧处理完成: {len(df_processed)}行")
        return df_processed


    def merge_warranty_data(self, df_bpwy: pd.DataFrame, df_qbwy2: pd.DataFrame) -> pd.DataFrame:
        """合并保赔+全保数据"""
        self.logger.info("开始合并【保赔+全保】数据")
        df_wuyou = pd.concat([df_qbwy2, df_bpwy], axis=0, join='outer', ignore_index=True)
        df_car = self._read_supplement_car()
        df_wuyou = pd.merge(df_wuyou, df_car, how='left', on='车系')
        df_wuyou['所属门店'] = np.where(
            df_wuyou['所属门店'] == '直播基地',
            df_wuyou['服务网络'] + '-' + df_wuyou['所属门店'],
            df_wuyou['所属门店']
        )

        df_wuyou['是否保赔'] = '是'
        df_wuyou['所属门店'] = df_wuyou['所属门店'].replace(self.business_rules["company_mapping"])
        df_wuyou['城市'] = np.where(df_wuyou['所属门店'].str.contains('贵州'), '贵州', '成都')
        df_wuyou = df_wuyou.drop_duplicates().dropna(subset='车架号')

        self.logger.info(f"保赔数据合并完成: {len(df_wuyou)}行")
        return df_wuyou

    def _read_insurance_excel(self) -> pd.DataFrame:
        """读取新车保险Excel"""
        directory = self.input_paths["insurance"]
        sheet_name = self.sheet_names["insurance"]
        max_workers = self.business_rules["max_workers"]

        dfs: List[pd.DataFrame] = []
        failed_files = []

        def _read_single_file(filename: str) -> Optional[pd.DataFrame]:
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
        self.logger.info(f"保险Excel: 开始读取{len(filenames)}个文件")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_read_single_file, fn): fn for fn in filenames}
            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    dfs.append(res)

        df_combined = pd.concat(dfs, axis=0, ignore_index=True) if dfs else pd.DataFrame()
        self.logger.info(f"保险Excel读取完成: {len(df_combined)}行, 失败{len(failed_files)}个文件")
        return df_combined

    def _process_insurance_csv(self) -> pd.DataFrame:
        """处理新车保险CSV"""
        try:
            self.logger.info(f"保险CSV: 开始读取{os.path.basename(self.insurance_csv_path)}")
            df_cyy = pd.read_csv(self.insurance_csv_path)

            df_cyy = df_cyy[['出单日期', '保险公司简称', '所属门店', '车系', '车架号', '交强险保费', '业务人员', '保费总额']]
            df_cyy.rename(columns={'出单日期': '签单日期', '保险公司简称': '保险公司', '车系': '车型','所属门店': '归属公司', '业务人员': '销售顾问'}, inplace=True)

            df_car = self._read_supplement_car()
            df_cyy = pd.merge(df_cyy, df_car, how='left', left_on='车型', right_on='车系')
            df_cyy['归属公司'] = np.where(
                df_cyy['归属公司'] == '直播基地',
                df_cyy['服务网络'] + '-' + df_cyy['归属公司'],
                df_cyy['归属公司']
            )

            self.logger.info(f"保险CSV处理完成: {len(df_cyy)}行")
            return df_cyy
        except Exception as e:
            self.logger.error(f"保险CSV处理失败: {str(e)}")
            raise

    def process_insurance(self) -> pd.DataFrame:
        """新车保险完整流程"""
        self.logger.info("开始处理【新车保险】数据")
        df_excel = self._read_insurance_excel()
        df_csv = self._process_insurance_csv()

        all_insurance_dfs = [df_excel] if not df_excel.empty else []
        all_insurance_dfs.append(df_csv)

        df_combined = pd.concat(all_insurance_dfs, axis=0, ignore_index=True)
        df_combined.columns = self._make_unique_columns(df_combined.columns)
        df_csv.columns = self._make_unique_columns(df_csv.columns)
        df_combined_all = pd.concat([df_combined, df_csv], axis=0, join='outer', ignore_index=True)

        df_combined_all['归属公司'] = df_combined_all['归属公司'].replace(self.business_rules["company_mapping"])
        df_combined_all = df_combined_all.dropna(subset=['保险公司'])

        exist_cols = [col for col in self.required_cols["insurance"] if col in df_combined_all.columns]
        df_filtered = df_combined_all[exist_cols].copy()
        df_filtered['日期'] = pd.to_datetime(df_filtered['签单日期'], errors='coerce').dt.date
        df_filtered = df_filtered.sort_values(by='日期', ascending=False).drop_duplicates(subset='车架号', keep='first')

        self.logger.info(f"新车保险处理完成: {len(df_filtered)}行")
        return df_filtered

    def merge_insurance_with_warranty(self, df_insurance: pd.DataFrame, df_wuyou: pd.DataFrame) -> pd.DataFrame:
        """合并+过滤运营车"""
        self.logger.info("开始合并【保险+保赔】数据并过滤运营车")
        df_merged = pd.merge(df_insurance, df_wuyou[['车架号', '是否保赔']], how='left', on='车架号')
        df_merged['是否保赔'] = df_merged['是否保赔'].fillna('否')

        df_exclude_company = df_merged[df_merged['保险公司'].str.contains(self.business_rules["exclude_operating_company"], na=False)]
        df_exclude_fee = df_merged[df_merged['交强险保费'].isin(self.business_rules["exclude_operating_fee"])]
        df_excluded = pd.concat([df_exclude_company, df_exclude_fee], axis=0).drop_duplicates().query("是否保赔 == '否'")

        df_valid = df_merged[~df_merged['车架号'].isin(df_excluded['车架号'])].copy()
        df_valid['城市'] = np.where(df_valid['归属公司'].str.contains('贵州'), '贵州', '成都')
        df_valid['总费用_次数'] = 1
        df_valid = df_valid.drop_duplicates()

        self.logger.info(f"保险数据合并过滤完成: 有效{len(df_valid)}行, 排除{len(df_excluded)}行")
        return df_valid

    def _save_results(self, df_wuyou: pd.DataFrame, df_qbwy1: pd.DataFrame, df_valid_insurance: pd.DataFrame):
        """保存结果"""
        self.logger.info("开始保存结果文件")
        try:
            df_wuyou.to_csv(self.output_files["bpwy"], index=False, encoding='utf-8-sig')
            df_qbwy1.to_csv(self.output_files["qbwy"], index=False, encoding='utf-8-sig')
            df_valid_insurance.to_csv(self.output_files["insurance"], index=False, encoding='utf-8-sig')
            self.logger.info("结果文件保存完成")
            self.logger.info(f"  - 保赔无忧: {len(df_wuyou)}行")
            self.logger.info(f"  - 全赔无忧: {len(df_qbwy1)}行")
            self.logger.info(f"  - 新车保险台账: {len(df_valid_insurance)}行")
        except Exception as e:
            self.logger.error(f"结果文件保存失败: {str(e)}")
            raise

    def run(self) -> bool:
        """完整流程执行"""
        self.logger.info("=" * 60)
        self.logger.info("开始保险与保修数据整合处理流程")
        self.logger.info("=" * 60)

        try:
            df_qbwy1, df_qbwy2 = self.safe_execute(self.process_qbwy)
            df_bpwy = self.safe_execute(self.process_bpwy)
            df_wuyou = self.safe_execute(self.merge_warranty_data, df_bpwy, df_qbwy2)
            df_insurance = self.safe_execute(self.process_insurance)
            df_valid_insurance = self.safe_execute(self.merge_insurance_with_warranty, df_insurance, df_wuyou)
            self.safe_execute(self._save_results, df_wuyou, df_qbwy1, df_valid_insurance)

            self.logger.info("保险与保修数据整合处理完成!")
            return True

        except Exception as e:
            self.logger.error(f"保险与保修数据整合处理失败: {str(e)}", exc_info=True)
            return False


"""新车三方延保数据处理器"""
class SanfangYBProcessor(DataProcessorBase):


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        super().__init__("三方延保处理器", base_data_dir)

        self.input_dir = os.path.join(base_data_dir, "私有云文件本地", "新车三方延保")
        self.output_dir = os.path.join(base_data_dir, "dashboard")
        self.output_filename = "新车三方延保台账.csv"
        self.output_path = os.path.join(self.output_dir, self.output_filename)

        self.sheet_name = "登记表"
        self.max_workers = 10
        self.required_columns = [
            '新车销售店名', '延保销售日期', '购车日期', '车系', '车架号', '客户姓名',
            '电话号码1', '电话号码2', '延保销售人员', '延保期限', '金额',
            '是否录入厂家系统', '录入厂家系统日期', '比亚迪系统录入金额',
            '超期录入比亚迪系统违约金', '备注', 'From'
        ]
        self.store_name_mapping = {'文景初治': '上元盛世', "王朝网-直播基地":"直播基地"}

        self._init_check()

    def _init_check(self):
        """初始化检查"""
        if not os.path.exists(self.input_dir):
            raise FileNotFoundError(f"输入目录不存在: {self.input_dir}")
        os.makedirs(self.output_dir, exist_ok=True)
        self.logger.info(f"初始化完成 - 输入目录: {self.input_dir}, 输出目录: {self.output_dir}")

    def _read_single_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """读取单个Excel文件"""
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=self.sheet_name,
                header=0,
                dtype=str
            )

            file_base_name = os.path.basename(file_path).split('.')[0]
            df['From'] = file_base_name
            df.columns = df.columns.str.replace('\n', '')

            self.logger.info(f"读取文件 {os.path.basename(file_path)} 成功 - {len(df)}行")
            return df

        except Exception as e:
            self.logger.error(f"读取文件 {os.path.basename(file_path)} 失败: {str(e)}")
            return None

    def _process_single_directory(self, directory: str) -> Optional[pd.DataFrame]:
        """处理单个目录"""
        excel_files = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.lower().endswith('.xlsx')
        ]

        if not excel_files:
            self.logger.warning(f"目录 {os.path.basename(directory)} 下无Excel文件")
            return None

        dir_dfs = []
        for file_path in excel_files:
            df = self._read_single_file(file_path)
            if df is not None:
                dir_dfs.append(df)

        if dir_dfs:
            merged_dir_df = pd.concat(dir_dfs, axis=0, ignore_index=True)
            self.logger.info(f"目录 {os.path.basename(directory)} 合并完成 - {len(merged_dir_df)}行")
            return merged_dir_df
        else:
            self.logger.warning(f"目录 {os.path.basename(directory)} 下无有效Excel数据")
            return None

    def _process_directories(self, directories: List[str]) -> pd.DataFrame:
        """多线程处理多个目录"""
        all_dfs = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_directory, dir_path): dir_path
                for dir_path in directories
            }

            for future in as_completed(futures):
                dir_path = futures[future]
                try:
                    dir_df = future.result()
                    if dir_df is not None:
                        all_dfs.append(dir_df)
                except Exception as e:
                    self.logger.error(f"处理目录 {os.path.basename(dir_path)} 失败: {str(e)}")

        if all_dfs:
            final_merged_df = pd.concat(all_dfs, axis=0, ignore_index=True)
            self.logger.info(f"所有目录数据合并完成 - 总计{len(final_merged_df)}行")
            return final_merged_df
        else:
            self.logger.error("没有找到任何有效数据")
            return pd.DataFrame()

    def _filter_and_select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据筛选"""
        if df.empty:
            return df

        df_filtered = df[df['延保销售日期'].notna()].copy()
        self.logger.info(f"筛选延保销售日期非空数据: {len(df_filtered)}行 (原{len(df)}行)")

        existing_columns = [col for col in self.required_columns if col in df_filtered.columns]
        missing_columns = set(self.required_columns) - set(existing_columns)
        if missing_columns:
            self.logger.warning(f"部分指定列不存在: {', '.join(missing_columns)}")

        df_selected = df_filtered[existing_columns].copy()
        return df_selected

    def _replace_store_name(self, df: pd.DataFrame) -> pd.DataFrame:
        """店名替换"""
        if df.empty or '新车销售店名' not in df.columns:
            return df

        df['新车销售店名'] = df['新车销售店名'].replace(self.store_name_mapping)
        self.logger.info("店名替换完成")
        return df

    def save_result(self, df: pd.DataFrame):
        """保存结果"""
        if df.empty:
            self.logger.error("无有效数据，不保存文件")
            return

        try:
            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"结果保存成功: {self.output_path}")
        except Exception as e:
            self.logger.error(f"保存文件失败: {str(e)}")
            raise

    def run(self, directories: Optional[List[str]] = None) -> bool:
        """核心执行方法"""
        self.logger.info("=" * 60)
        self.logger.info("开始三方延保数据处理流程")
        self.logger.info("=" * 60)

        try:
            target_dirs = directories if directories is not None else [self.input_dir]
            self.logger.info(f"目标目录数量: {len(target_dirs)}")

            df_merged = self.safe_execute(self._process_directories, target_dirs)
            df_filtered = self.safe_execute(self._filter_and_select_columns, df_merged)
            df_final = self.safe_execute(self._replace_store_name, df_filtered)
            self.safe_execute(self.save_result, df_final)

            self.logger.info("三方延保数据处理完成!")
            return True

        except Exception as e:
            self.logger.error(f"三方延保数据处理失败: {str(e)}", exc_info=True)
            return False


"""数据合并处理器"""
class DataMerger(DataProcessorBase):


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        super().__init__("数据合并处理器", base_data_dir)

        self.base_output_dir = os.path.join(base_data_dir, "dashboard")
        os.makedirs(self.base_output_dir, exist_ok=True)

        self.file_configs = self._init_file_configs()
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
            ],
            "quality_files": [
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年数据汇总表.xlsx',
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年数据汇总表.xlsx'
            ],
            "quality_files1": [
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年数据汇总表.xlsx',
                r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年数据汇总表.xlsx'
            ],
            "wes_files": [
                r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年WES返利汇总表.xlsx"
            ]
        }

    def _process_single_file(self, file_path: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """处理单个Excel文件的指定工作表"""
        if not os.path.exists(file_path):
            self.logger.warning(f"文件不存在: {file_path}")
            return None

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            unpivot_df = pd.melt(df, id_vars=df.columns[0:2], var_name='属性', value_name='值')
            self.logger.info(f"处理文件 {os.path.basename(file_path)} 成功 - {len(unpivot_df)}行")
            return unpivot_df
        except Exception as e:
            self.logger.error(f"处理文件 {file_path} 失败: {str(e)}")
            return None

    def _process_file_list(self, file_list: List[str], sheet_name: str, process_type: str) -> Optional[pd.DataFrame]:
        """处理文件列表"""
        dfs = []
        for file_path in file_list:
            df = self._process_single_file(file_path, sheet_name)
            if df is not None:
                dfs.append(df)

        if not dfs:
            self.logger.warning(f"未找到 {sheet_name} 工作表的有效数据")
            return None

        merged_df = pd.concat(dfs, ignore_index=True)

        if process_type == "filter_unnamed":
            merged_df = merged_df[~merged_df['属性'].str.contains("Unnamed", na=False)]
        elif process_type == "filter_score":
            merged_df = merged_df[
                (merged_df['属性'].str.contains("得分", na=False)) &
                (merged_df['公司名称'].notna())
                ]

        self.logger.info(f"文件列表处理完成 - {sheet_name}: {len(merged_df)}行")
        return merged_df

    def _replace_company_name(self, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        """替换公司名称"""
        if col_name in df.columns:
            df[col_name] = df[col_name].replace('文景初治', '上元盛世')
            df[col_name] = df[col_name].replace("王朝网-直播基地", "直播基地")
            self.logger.info("公司名称替换完成")
        else:
            self.logger.warning(f"替换列 '{col_name}' 在数据中不存在")
        return df

    def _save_result(self, df: pd.DataFrame, output_filename: str):
        """保存处理结果"""
        output_path = os.path.join(self.base_output_dir, output_filename)
        try:
            df.to_excel(output_path, index=False)
            self.logger.info(f"结果保存成功: {output_path}")
        except Exception as e:
            self.logger.error(f"保存文件 {output_filename} 失败: {str(e)}")

    def run(self) -> bool:
        """执行所有数据处理任务"""
        self.logger.info("=" * 60)
        self.logger.info("开始数据合并处理流程")
        self.logger.info("=" * 60)

        try:
            success_count = 0
            total_count = len(self.tasks)

            for task in self.tasks:
                self.logger.info(f"开始处理任务: {task['sheet_name']} -> {task['output_filename']}")

                file_list = self.file_configs.get(task['file_list_key'], [])
                if not file_list:
                    self.logger.warning(f"未找到文件列表 {task['file_list_key']}，跳过该任务")
                    continue

                merged_df = self._process_file_list(
                    file_list=file_list,
                    sheet_name=task['sheet_name'],
                    process_type=task['process_type']
                )

                if merged_df is None or merged_df.empty:
                    self.logger.warning(f"{task['sheet_name']} 处理后无有效数据，跳过保存")
                    continue

                merged_df = self._replace_company_name(merged_df, task['replace_col'])
                self._save_result(merged_df, task['output_filename'])
                success_count += 1

            self.logger.info(f"数据合并处理完成! 成功{success_count}/{total_count}个任务")
            return success_count > 0

        except Exception as e:
            self.logger.error(f"数据合并处理失败: {str(e)}", exc_info=True)
            return False


"""贴膜升级数据处理器"""
class TMSJProcessor(DataProcessorBase):


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        super().__init__("贴膜升级处理器", base_data_dir)

        self.input_dir = os.path.join(base_data_dir, "私有云文件本地", "贴膜升级")
        self.output_dir = os.path.join(base_data_dir, "dashboard")
        os.makedirs(self.output_dir, exist_ok=True)

        self.tmsj_sheet = "膜升级登记表"
        self.tmsj1_sheet = "汇总表"
        self.target_stores = ['上元臻智', '上元臻享', '上元臻盛', '上元坤灵', '上元曦和', '上元弘川']
        self.tmsj1_files = [
            "方程豹-乐山上元曦和-贴膜升级登记表-最新年.xlsx",
            "腾势-乐山上元臻智-贴膜升级登记表-最新年.xlsx",
            "方程豹-泸州上元坤灵-贴膜升级登记表-最新年.xlsx",
            "两网-总部-贴膜升级登记表-最新年.xlsx",
        ]
        self.tmsj1_file_paths = [os.path.join(self.input_dir, f) for f in self.tmsj1_files]

    def _read_excel_files(self, directory: str, sheet_name: str) -> List[pd.DataFrame]:
        """读取指定目录下的所有Excel文件"""
        dfs = []
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            if not (filename.lower().endswith('.xlsx') or filename.lower().endswith('.xls')):
                self.logger.warning(f"跳过非Excel文件: {filename}")
                continue

            self.logger.info(f"正在读取文件: {filename}")
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df['From'] = os.path.basename(file_path).split('.')[0]
                df.columns = df.columns.str.replace('\n', '')
                dfs.append(df)
                self.logger.info(f"文件 {filename} 读取成功 - {len(df)}行")
            except Exception as e:
                self.logger.error(f"读取文件 {filename} 失败: {str(e)}")

        return dfs

    def _process_directory(self, directory: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """处理单个目录下的Excel文件"""
        dfs = self._read_excel_files(directory, sheet_name)
        if dfs:
            result = pd.concat(dfs, axis=0)
            self.logger.info(f"目录 {directory} 处理完成 - {len(result)}行")
            return result
        else:
            self.logger.warning(f"目录 {directory} 无有效数据")
            return None

    def _process_single_file(self, file_path: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """处理单个Excel文件"""
        if not (file_path.lower().endswith('.xlsx') or file_path.lower().endswith('.xls')):
            self.logger.warning(f"跳过非Excel文件: {file_path}")
            return None

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            self.logger.info(f"文件 {os.path.basename(file_path)} 读取成功 - {len(df)}行")
            return df
        except Exception as e:
            self.logger.error(f"读取文件 {file_path} 失败: {str(e)}")
            return None

    def _calculate_rebate(self, row: pd.Series) -> int:
        """计算贴膜返利金额"""
        store_name = str(row.get('新车销售店名', ''))
        vehicle_model = str(row.get('车型', ''))
        gift_detail = row.get('赠送装饰')
        gift_detail_str = '' if pd.isna(gift_detail) else str(gift_detail).strip()

        if store_name in self.target_stores:
            if '腾势' in vehicle_model:
                rebate = 1000
            elif vehicle_model == '钛3':
                rebate = 400
            elif '豹5' in vehicle_model or '豹8' in vehicle_model:
                rebate = 800
            else:
                rebate = 0

            if gift_detail_str != '' and rebate > 0:
                rebate -= 200
            return rebate
        return 0

    def process_tmsj(self) -> pd.DataFrame:
        """处理膜升级登记表数据"""
        self.logger.info("开始处理膜升级登记表数据")
        all_dfs = []

        with ThreadPoolExecutor() as executor:
            future_to_dir = {
                executor.submit(self._process_directory, self.input_dir, self.tmsj_sheet): self.input_dir
            }

            for future in as_completed(future_to_dir):
                directory = future_to_dir[future]
                try:
                    df_combined = future.result()
                    if df_combined is not None:
                        all_dfs.append(df_combined)
                except Exception as e:
                    self.logger.error(f"处理目录 {directory} 失败: {str(e)}")

        if all_dfs:
            df_final = pd.concat(all_dfs, axis=0)
            df_final = df_final[df_final['到店日期'].notna()]
            self.logger.info(f"膜升级登记表数据处理完成 - {len(df_final)}行")
        else:
            df_final = pd.DataFrame()
            self.logger.warning("膜升级登记表数据为空")

        if not df_final.empty:
            required_columns = [
                '月份', '到店日期', '新车销售店名', '车型', '车架号（后6位）', '客户姓名',
                '是否送龙膜/高等级膜', '是否有满意度风险', '是否有效客户', '是否收劵',
                '膜升级金额', '其它施工项目', '其它项目金额', '合计升级金额',
                '合作三方公司名称', '备注', '精品顾问', '是否算到店量', '是否代办',
                '是否不推膜', '膜升级具体内容', '膜升级成本', '膜升级毛利润', '其他项升级成本',
                '其他项升级毛利润', '合计升级毛利润', '三方返还佣金', '赠送装饰', 'From'
            ]
            existing_columns = [col for col in required_columns if col in df_final.columns]
            df_final = df_final[existing_columns]

            df_final['成都腾豹贴膜返利'] = df_final.apply(self._calculate_rebate, axis=1)
            df_final['新车销售店名'] = df_final['新车销售店名'].replace('文景初治', '上元盛世')
            df_final['新车销售店名'] = df_final['新车销售店名'].replace("王朝网-直播基地", "直播基地")

        return df_final

    def process_tmsj1(self) -> pd.DataFrame:
        """处理汇总表数据"""
        self.logger.info("开始处理汇总表数据")
        all_dfs = []

        for file_path in self.tmsj1_file_paths:
            df = self._process_single_file(file_path, self.tmsj1_sheet)
            if df is not None:
                all_dfs.append(df)

        if all_dfs:
            df_final = pd.concat(all_dfs, axis=0, join='outer')
            df_final = df_final.dropna(subset=['新车销售店名'], how='all')
            df_final['新车销售店名'] = df_final['新车销售店名'].replace('文景初治', '上元盛世')
            df_final['新车销售店名'] = df_final['新车销售店名'].replace("王朝网-直播基地", "直播基地")
            self.logger.info(f"汇总表数据处理完成 - {len(df_final)}行")
        else:
            df_final = pd.DataFrame()
            self.logger.warning("汇总表数据为空")

        return df_final

    def save_results(self, df_tmsj: pd.DataFrame, df_tmsj1: pd.DataFrame):
        """保存处理结果"""
        tmsj_path = os.path.join(self.output_dir, '贴膜升级.csv')
        tmsj1_path = os.path.join(self.output_dir, '贴膜升级1.csv')

        if not df_tmsj.empty:
            df_tmsj.to_csv(tmsj_path, index=False)
            self.logger.info(f"膜升级登记表结果保存成功: {tmsj_path}")
        else:
            self.logger.warning("膜升级登记表数据为空，未保存文件")

        if not df_tmsj1.empty:
            df_tmsj1.to_csv(tmsj1_path, index=False)
            self.logger.info(f"汇总表结果保存成功: {tmsj1_path}")
        else:
            self.logger.warning("汇总表数据为空，未保存文件")

    def run(self) -> bool:
        """执行完整的数据处理流程"""
        self.logger.info("=" * 60)
        self.logger.info("开始贴膜升级数据处理流程")
        self.logger.info("=" * 60)

        try:
            df_tmsj = self.safe_execute(self.process_tmsj)
            df_tmsj1 = self.safe_execute(self.process_tmsj1)
            self.safe_execute(self.save_results, df_tmsj, df_tmsj1)

            self.logger.info("贴膜升级数据处理完成!")
            return True

        except Exception as e:
            self.logger.error(f"贴膜升级数据处理失败: {str(e)}", exc_info=True)
            return False


"""主数据处理器 - 统一调度所有数据处理任务"""
class MainDataProcessor:


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        self.base_data_dir = base_data_dir
        self.logger = self._setup_main_logger()
        self.processors = []

    def _setup_main_logger(self) -> logging.Logger:
        """设置主程序日志配置"""
        log_dir = os.path.join(self.base_data_dir, "代码执行", "data", "私有云日志", "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_filename = f"main_processor_{datetime.datetime.now().strftime('%Y%m%d')}.log"
        log_filepath = os.path.join(log_dir, log_filename)

        logger = logging.getLogger("MainProcessor")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            file_handler = RotatingFileHandler(
                log_filepath,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8"
            )
            file_handler.setLevel(logging.INFO)

            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        return logger

    def run_all_processors(self):
        """运行所有数据处理器"""
        self.logger.info("=" * 80)
        self.logger.info("开始执行所有数据处理任务")
        self.logger.info("=" * 80)

        # 定义处理器执行顺序
        processor_classes = [
            ("营销投放费用处理器", YingxiaoMoneyProcessor),
            ("特殊事项收入处理器", SpecialIncomeProcessor),
            ("全保无忧整合处理器", InsuranceWarrantyIntegrator),
            ("三方延保处理器", SanfangYBProcessor),
            ("数据合并处理器", DataMerger),
            # ("贴膜升级处理器", TMSJProcessor)
        ]

        results = {}
        success_count = 0

        for processor_name, processor_class in processor_classes:
            self.logger.info(f"\n▶ 开始执行: {processor_name}")
            try:
                processor = processor_class(self.base_data_dir)
                success = processor.run()
                results[processor_name] = "成功" if success else "失败"
                if success:
                    success_count += 1
                self.logger.info(f"✔ {processor_name} 执行完成: {'成功' if success else '失败'}")
            except Exception as e:
                self.logger.error(f"✗ {processor_name} 执行异常: {str(e)}", exc_info=True)
                results[processor_name] = "异常"

        # 输出总结报告
        self.logger.info("\n" + "=" * 80)
        self.logger.info("数据处理任务执行总结")
        self.logger.info("=" * 80)

        for processor_name, result in results.items():
            status_icon = "✅" if result == "成功" else "❌"
            self.logger.info(f"{status_icon} {processor_name}: {result}")

        self.logger.info(f"\n总计: {success_count}/{len(processor_classes)} 个任务执行成功")

        if success_count == len(processor_classes):
            self.logger.info("🎉 所有数据处理任务均执行成功!")
        else:
            self.logger.warning(f"⚠️  {len(processor_classes) - success_count} 个任务执行失败或异常")

        self.logger.info("=" * 80)


def main():
    """主函数"""
    try:
        base_data_dir = r"E:\powerbi_data\看板数据"
        main_processor = MainDataProcessor(base_data_dir)
        main_processor.run_all_processors()
    except Exception as e:
        print(f"主程序执行失败: {str(e)}")


if __name__ == "__main__":
    main()