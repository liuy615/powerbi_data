import pandas as pd
import numpy as np
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from typing import List, Tuple, Optional, Dict, Any


class Config:
    """配置管理类"""

    # 文件路径配置
    FILE_PATHS = {
        'car_network_excel': r'E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源\各公司银行额度.xlsx',
        'new_insurance_csv': r"E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源\202401_202503新车保险台账.csv",
        'yongle_csv': r"E:\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv",
        'output_dir': r'E:\powerbi_data\看板数据\dashboard',
        'derivative_products_dir': r"E:\powerbi_data\看板数据\私有云文件本地\衍生产品"
    }

    # MongoDB配置
    MONGODB = {
        'host': '192.168.1.7',
        'port': 27017,
        'username': 'xg_wd',
        'password': 'H91NgHzkvRiKygTe4X4ASw',
        'auth_source': 'xg',
        'database': 'xg_JiaTao'
    }

    # 其他配置
    EXCLUDE_INSURANCE_VALUES = [1000, 1130, 1800]
    EXCEL_SHEET_NAMES = {
        'car_network': '补充车系',
        'derivative_registration': '登记表'
    }


class MongoDBConfig:
    """MongoDB连接配置类"""

    def __init__(self, **kwargs):
        config = {**Config.MONGODB, **kwargs}
        self.host = config['host']
        self.port = config['port']
        self.username = config['username']
        self.password = config['password']
        self.auth_source = config['auth_source']
        self.database_name = config['database']

    def get_connection_string(self):
        """构建连接字符串"""
        return (f'mongodb://{self.username}:{self.password}@{self.host}:{self.port}/'
                f'{self.database_name}?authSource={self.auth_source}&authMechanism=SCRAM-SHA-256')

    def get_database_name(self):
        return self.database_name


class MongoDBClient:
    """MongoDB客户端操作类"""

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
            collection = self.db[collection_name]
            projection = {field: 1 for field in desired_fields}
            projection["_id"] = 0

            query_filter = query_filter or {}
            query = collection.find(query_filter, projection)
            if limit:
                query = query.limit(limit)

            return pd.DataFrame(list(query))
        except Exception as e:
            print(f"查询失败: {e}")
            return None

    def query_all_data(self, collection_name, limit=None, query_filter=None, exclude_id=True):
        """查询指定集合中的所有数据"""
        if not self.connected:
            print("未建立数据库连接")
            return None

        try:
            collection = self.db[collection_name]
            projection = {"_id": 0} if exclude_id else {}
            query_filter = query_filter or {}
            query = collection.find(query_filter, projection)
            if limit:
                query = query.limit(limit)

            return pd.DataFrame(list(query))
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


class DataProcessor:
    """数据处理基类"""

    @staticmethod
    def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名：去除空格、转换为小写、替换空格为下划线"""
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\\s+', '_', regex=True)
        return df

    @staticmethod
    def load_car_network_data() -> pd.DataFrame:
        """加载车系网络数据"""
        return pd.read_excel(
            Config.FILE_PATHS['car_network_excel'],
            sheet_name=Config.EXCEL_SHEET_NAMES['car_network']
        )

    @staticmethod
    def adjust_live_broadcast_store(df: pd.DataFrame, store_column: str = '所属门店',
                                    merge_column: str = '车系') -> pd.DataFrame:
        """调整直播基地门店名称"""
        car_network_df = DataProcessor.load_car_network_data()
        df = pd.merge(df, car_network_df[['车系', '服务网络']], how='left', left_on=merge_column, right_on='车系')
        df[store_column] = np.where(
            df[store_column] == '直播基地',
            df['服务网络'] + '-' + df[store_column],
            df[store_column]
        )
        return df

    @staticmethod
    def standardize_date_format(date_str):
        """标准化日期格式"""
        if isinstance(date_str, str):
            date_str = date_str.strip()
            if "年" in date_str and "月" in date_str and "日" not in date_str:
                date_str += "1日"
        return date_str

    @staticmethod
    def convert_to_datetime(date_str):
        """将不同格式的日期字符串转换为datetime对象"""
        if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_str):
            return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
        elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
            return pd.to_datetime(date_str, format='%Y年%m月%d日')
        return pd.NaT


class ComprehensiveInsuranceProcessor(DataProcessor):
    """全保无忧数据处理类"""

    COLLECTION_NAMES = {
        'sales_data': 'YS_sales',
        'cost_details': '全保无忧费用明细'
    }

    @classmethod
    def process_qbwy(cls) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """处理全保无忧数据"""
        config = MongoDBConfig(database='xg_JiaTao')
        mongo_client = MongoDBClient(config)

        try:
            if not mongo_client.connect():
                return pd.DataFrame(), pd.DataFrame()

            # 获取销售数据
            sales_data = cls._get_sales_data(mongo_client)

            # 获取详细数据
            detailed_data = cls._get_detailed_data(mongo_client)

            # 合并成本数据
            merged_data = cls._merge_cost_data(mongo_client, detailed_data)

            return sales_data, merged_data

        except Exception as e:
            print(f"处理全保无忧数据失败: {e}")
            return pd.DataFrame(), pd.DataFrame()
        finally:
            mongo_client.disconnect()

    @classmethod
    def _get_sales_data(cls, mongo_client: MongoDBClient) -> pd.DataFrame:
        """获取销售数据"""
        df = mongo_client.query_all_data(cls.COLLECTION_NAMES['sales_data'])

        if df.empty:
            return df

        # 数据过滤
        filters = [
            ("数据有效性", "有效"),
            ("业务部门", "售前"),
            ("产品状态", "生效")
        ]

        for column, value in filters:
            df = df[df[column] == value]

        # df.to_csv("qwby.csv")

        # 版本标准化
        df["全保无忧版本"] = df["产品销售版本"].str.replace(r"畅行.*", "畅行版", regex=True)

        return df

    @classmethod
    def _get_detailed_data(cls, mongo_client: MongoDBClient) -> pd.DataFrame:
        """获取详细数据"""
        desired_fields = [
            '业务部门', '产品状态', '客户姓名', '手机号码', '车架号', '车系', '产品销售日期',
            '开票日期', '产品销售版本', '新车开票价格', '车辆阶段', '车辆类型',
            '产品销售金额', '终身保养金额', '所属门店', '销售顾问', '车系网络',
            '数据有效性', 'created_at'
        ]

        df = mongo_client.query_data_with_projection(
            cls.COLLECTION_NAMES['sales_data'],
            desired_fields
        )

        if df.empty:
            return df

        # 数据过滤
        filters = [
            ("数据有效性", "有效"),
            ("业务部门", "售前"),
            ("产品状态", "生效")
        ]

        for column, value in filters:
            df = df[df[column] == value]

        # 数据处理
        df['城市'] = np.where(df['所属门店'].str.contains('贵州'), '贵州', '成都')
        df["全保无忧版本"] = df["产品销售版本"].str.replace(r"畅行.*", "畅行版", regex=True)

        # 设置投保价
        bins = [0, 100000, 150000, 200000, 250000, 300000, 350000, np.inf]
        labels = ['0-10万', '10-15万', '15-20万', '20-25万', '25-30万', '30-35万', '35-40万']
        df['投保价'] = pd.cut(df['新车开票价格'], bins=bins, labels=labels, right=False).astype(str)

        return df

    @classmethod
    def _merge_cost_data(cls, mongo_client: MongoDBClient, detailed_data: pd.DataFrame) -> pd.DataFrame:
        """合并成本数据"""
        cost_data = mongo_client.query_all_data(cls.COLLECTION_NAMES['cost_details'])

        if cost_data.empty or detailed_data.empty:
            return pd.DataFrame()

        # 创建关联键
        cost_data["关联键"] = cost_data["区域"] + "_" + cost_data["网络"] + "_" + \
                              cost_data["车辆阶段"] + "_" + cost_data["车价/车损投保价"] + "_" + \
                              cost_data["产品规格"] + "_" + cost_data["车辆类型"]

        detailed_data["关联键"] = detailed_data["城市"] + "_" + detailed_data["车系网络"] + "_" + \
                                  detailed_data["车辆阶段"] + "_" + detailed_data["投保价"] + "_" + \
                                  detailed_data["全保无忧版本"] + "_" + detailed_data["车辆类型"]

        # 合并数据
        merged_df = pd.merge(
            detailed_data,
            cost_data[["关联键", "开始时间", "结束时间", "成本"]],
            on="关联键",
            how="left"
        )

        # 筛选有效匹配
        mask = (
                (merged_df["产品销售日期"] >= merged_df["开始时间"]) &
                (merged_df["产品销售日期"] <= merged_df["结束时间"])
        )

        valid_matches = merged_df[mask].copy()
        valid_matches.sort_values(by=["车架号", "开始时间"], ascending=[True, False], inplace=True)
        valid_matches = valid_matches.drop_duplicates(subset=["车架号"], keep="first")

        # 计算利润
        valid_matches["利润"] = valid_matches["产品销售金额"] - valid_matches["成本"]
        valid_matches.rename(columns={'产品销售金额': '全保无忧金额'}, inplace=True)

        # 处理日期
        valid_matches["产品销售日期"] = pd.to_datetime(valid_matches["产品销售日期"], format='mixed', errors='coerce').dt.date
        valid_matches["开票日期"] = pd.to_datetime(valid_matches["开票日期"], format='mixed', errors='coerce').dt.date
        valid_matches["日期"] = np.where(
            valid_matches['开票日期'] > valid_matches['产品销售日期'],
            valid_matches['开票日期'],
            valid_matches['产品销售日期']
        )

        return valid_matches


class DerivativeInsuranceProcessor(DataProcessor):
    """衍生产品保险数据处理类"""

    REQUIRED_COLUMNS = {
        'derivative': [
            '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码',
            '保赔无忧金额', '双保无忧金额', '终身保养金额',
            '销售顾问', '所属门店', '备注', '日期'
        ]
    }

    @classmethod
    def process_derivative_insurance(cls) -> pd.DataFrame:
        """处理衍生产品保险数据"""
        all_dfs = []

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(
                cls._read_excel_files,
                Config.FILE_PATHS['derivative_products_dir'],
                Config.EXCEL_SHEET_NAMES['derivative_registration']
            )]

            for future in as_completed(futures):
                dfs = future.result()
                if dfs:
                    df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
                    df_processed = cls._process_derivative_data(df_combined)
                    all_dfs.append(df_processed)

        if all_dfs:
            return pd.concat(all_dfs, axis=0, join='outer', ignore_index=True)
        return pd.DataFrame()

    @classmethod
    def _read_excel_files(cls, directory: str, sheet_name: str) -> List[pd.DataFrame]:
        """读取指定目录下的Excel文件"""
        dfs = []

        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df['from'] = os.path.basename(file_path).split('.')[0]

                # 检查并补全缺失列
                required_columns = cls.REQUIRED_COLUMNS['derivative']
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None

                # 只保留需要的列
                available_columns = [col for col in required_columns if col in df.columns]
                df = df[available_columns]

                # 补全缺失的列
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None

                dfs.append(df)
                print(f"文件 {filename} 读取成功，一共 {len(df)} 行。")

            except Exception as e:
                print(f"读取 {filename} 时发生错误: {e}")

        return dfs

    @staticmethod
    def _process_derivative_data(df: pd.DataFrame) -> pd.DataFrame:
        """处理衍生产品数据"""
        # 日期处理
        df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed', errors='coerce')
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed', errors='coerce')
        df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
        df['日期'] = df['开票日期'].fillna(df['销售日期'])
        df['日期'] = pd.to_datetime(df['日期'], format='mixed', errors='coerce').dt.date

        return df


class NewCarInsuranceProcessor(DataProcessor):
    """新车保险数据处理类"""

    @classmethod
    def process_new_car_insurance(cls) -> pd.DataFrame:
        """处理新车保险数据"""
        # 读取数据
        all_dfs = pd.read_csv(Config.FILE_PATHS['new_insurance_csv'], low_memory=False)
        df_yongle = pd.read_csv(Config.FILE_PATHS['yongle_csv'])

        # 处理保险数据
        df_yongle = cls._process_yongle_data(df_yongle)

        # 合并数据
        all_dfs['总费用_次数'] = 1
        df_combined = pd.concat([all_dfs, df_yongle], axis=0, join='outer', ignore_index=True)

        # 数据清洗
        df_combined['归属公司'] = df_combined['归属公司'].replace('文景初治', '上元盛世')
        df_combined.dropna(subset=['保险公司'], inplace=True)

        # 添加日期列
        df_combined['日期'] = df_combined['签单日期']

        return df_combined[[
            '月份', '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司',
            '车型', '车牌号', '车架号', '被保险人', '交强险保费', '销售顾问',
            '是否为保赔无忧客户', '总费用_次数', '日期'
        ]]

    @classmethod
    def _process_yongle_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """处理新车保险数据"""
        # 选择需要的列
        df = df[[
            '出单日期', '保险公司简称', '所属门店', '车系', '车架号',
            '交强险保费', '业务人员', '保费总额', '总费用_次数'
        ]].copy()

        # 重命名列
        column_mapping = {
            '出单日期': '签单日期',
            '保险公司简称': '保险公司',
            '车系': '车型',  # 注意：这里重命名为车型，但后面合并时需要车系列
            '所属门店': '归属公司',
            '业务人员': '销售顾问'
        }

        # 使用 rename 方法的正确方式，避免 SettingWithCopyWarning
        df = df.rename(columns=column_mapping)

        # 调整直播基地门店名称
        # 因为重命名后车型列对应原车系列，所以使用 '车型' 作为合并列
        df = cls.adjust_live_broadcast_store(df, '归属公司', '车型')

        return df


class InsuranceDataMerger:
    """保险数据合并类"""

    @staticmethod
    def merge_insurance_data(comprehensive_data: pd.DataFrame,derivative_data: pd.DataFrame) -> pd.DataFrame:
        """合并保险数据"""
        merged_data = pd.concat([comprehensive_data, derivative_data],axis=0, join='outer', ignore_index=True)

        # 数据增强
        merged_data = InsuranceDataMerger._enhance_data(merged_data)

        return merged_data

    @staticmethod
    def _enhance_data(df: pd.DataFrame) -> pd.DataFrame:
        """增强数据"""
        # 调整直播基地门店名称
        df = DataProcessor.adjust_live_broadcast_store(df)

        # 添加标记和城市信息
        df['是否保赔'] = '是'
        df['所属门店'] = df['所属门店'].replace('文景初治', '上元盛世')
        df['城市'] = np.where(df['所属门店'].str.contains('贵州'), '贵州', '成都')

        # 数据清理
        df.drop_duplicates(inplace=True)
        df.dropna(subset='车架号', inplace=True)

        return df

    @staticmethod
    def mark_comprehensive_insurance(comprehensive_df: pd.DataFrame,insurance_df: pd.DataFrame) -> pd.DataFrame:
        """标记全保无忧保险数据"""
        # 创建标记
        comprehensive_flag = (comprehensive_df.assign(tmp=1)
                              .groupby('车架号')
                              .agg(是否保赔=('tmp', lambda x: '是' if len(x) > 0 else '否'))
                              .reset_index())

        # 合并标记
        insurance_df['日期'] = pd.to_datetime(insurance_df['日期'], errors='coerce').dt.date
        insurance_df = insurance_df.merge(comprehensive_flag, on='车架号', how='left')
        insurance_df['是否保赔'] = insurance_df['是否保赔'].fillna('否')

        return insurance_df

    @staticmethod
    def filter_excluded_vehicles(insurance_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """筛选排除的车辆"""
        # 筛选鼎和保险或特定交强险保费的车辆
        exclude_condition1 = insurance_df['保险公司'].str.contains('鼎和', na=False)
        exclude_condition2 = insurance_df['交强险保费'].astype(str).str.isnumeric() & insurance_df['交强险保费'].astype(float).isin(Config.EXCLUDE_INSURANCE_VALUES)

        df_excluded = insurance_df[(exclude_condition1 | exclude_condition2) & (insurance_df['是否保赔'] == '否')].drop_duplicates()

        # 获取剩余车辆
        df_remaining = insurance_df[~insurance_df['车架号'].isin(df_excluded['车架号'])].copy()

        # 添加城市信息
        df_remaining['城市'] = np.where(df_remaining['归属公司'].str.contains('贵州', na=False), '贵州', '成都')
        df_remaining["日期"] = df_remaining["签单日期"]
        df_remaining = df_remaining.drop_duplicates()

        return df_excluded, df_remaining


class DataExporter:
    """数据导出类"""

    @staticmethod
    def export_data(data_dict: Dict[str, pd.DataFrame]):
        """导出数据到CSV文件"""
        output_dir = Config.FILE_PATHS['output_dir']

        for name, df in data_dict.items():
            if df is not None and not df.empty:
                file_path = os.path.join(output_dir, f"{name}.csv")
                df.to_csv(file_path, index=False)
                print(f"已导出 {name} 数据到: {file_path}")


def main():
    """主函数"""
    print("开始处理保险数据...")

    # 1. 处理全保无忧数据
    print("正在处理全保无忧数据...")
    comprehensive_sales, comprehensive_detailed = ComprehensiveInsuranceProcessor.process_qbwy()

    # 2. 处理衍生产品保险数据
    print("正在处理衍生产品保险数据...")
    derivative_data = DerivativeInsuranceProcessor.process_derivative_insurance()

    # 3. 合并无忧数据
    print("正在合并无忧数据...")
    merged_insurance_data = InsuranceDataMerger.merge_insurance_data(comprehensive_detailed,derivative_data)

    # 4. 处理新车保险数据
    print("正在处理新车保险数据...")
    new_car_insurance = NewCarInsuranceProcessor.process_new_car_insurance()

    # 5. 标记并筛选新车保险数据
    print("正在标记新车保险数据...")
    marked_insurance = InsuranceDataMerger.mark_comprehensive_insurance(merged_insurance_data,new_car_insurance)

    # 6. 筛选排除的车辆
    print("正在筛选排除的车辆...")
    excluded_vehicles, remaining_vehicles = InsuranceDataMerger.filter_excluded_vehicles(marked_insurance)

    # 7. 导出数据
    print("正在导出数据...")
    DataExporter.export_data({
        '保赔无忧': merged_insurance_data,
        '新车保险台账': remaining_vehicles,
        '全赔无忧': comprehensive_sales})

    print("数据处理完成！")


if __name__ == "__main__":
    main()