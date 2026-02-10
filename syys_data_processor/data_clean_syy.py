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
        log_dir = os.path.join(self.base_data_dir, "powerbi_data", "data", "私有云日志", "logs")
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
        self.target_sheets = ["2024年", "2025年", "2026年"]
        self.required_columns = ["年份", "月份", "归属门店", "项目大类", "项目分类", "具体项目","费用金额", "核销发票金额", "核销发票税金", "费用合计", "备注", "From"]
        self.store_map = {"文景初治": "上元盛世", "王朝网-直播基地":"直播基地", "永乐盛世":"洪武盛世"}

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
            self.df_result['归属门店'] = self.df_result['归属门店'].replace("永乐盛世", "洪武盛世")

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
            df[col_name] = df[col_name].replace("永乐盛世", "洪武盛世")
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


"""主数据处理器 - 统一调度所有数据处理任务"""
class MainDataProcessor:


    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        self.base_data_dir = base_data_dir
        self.logger = self._setup_main_logger()
        self.processors = []

    def _setup_main_logger(self) -> logging.Logger:
        """设置主程序日志配置"""
        log_dir = os.path.join(self.base_data_dir, "powerbi_data", "data", "私有云日志", "logs")
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
            ("数据合并处理器", DataMerger),
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