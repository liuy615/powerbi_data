import sys
from datetime import datetime
import pandas as pd
import numpy as np
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from typing import List, Tuple, Optional, Dict, Any
project_root = r"E:\powerbi_data"
sys.path.insert(0, project_root)
from sqlalchemy import create_engine
from config.cyys_data_application.config import APP_DB_CONFIG
pd.set_option('future.no_silent_downcasting', True)


"""配置管理类"""
class Config:
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
    EXCLUDE_BUSINESS_SOURCE = ['非二级', '专营店', '出口', '试驾车', '调拨']  # 新增业务来源排除列表
    EXCEL_SHEET_NAMES = {
        'car_network': '补充车系',
        'derivative_registration': '登记表'
    }


"""MongoDB连接配置类"""
class MongoDBConfig:
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


"""数据处理基类"""
class DataProcessor:
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
    def adjust_live_broadcast_store(df: pd.DataFrame, store_column: str = '所属门店', merge_column: str = '车系') -> pd.DataFrame:
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


"""全保无忧数据处理类"""
class ComprehensiveInsuranceProcessor(DataProcessor):
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
            sales_data = cls._get_sales_data(mongo_client).replace("永乐盛世", "洪武盛世")
            sales_data.to_csv("E:/powerbi_data/看板数据/dashboard/全保无忧原始数据.csv")
            sales_data["产品销售金额"] = pd.to_numeric(sales_data["产品销售金额"], errors='coerce')
            sales_data["成本金额"] = pd.to_numeric(sales_data["成本金额"], errors='coerce')
            sales_data["利润"] = sales_data["产品销售金额"] - sales_data["成本金额"]
            # 处理日期
            sales_data["产品销售日期"] = pd.to_datetime(sales_data["产品销售日期"], format='mixed',errors='coerce').dt.date
            sales_data["开票日期"] = pd.to_datetime(sales_data["开票日期"], format='mixed', errors='coerce').dt.date
            sales_data["日期"] = np.where(
                sales_data['开票日期'] > sales_data['产品销售日期'],
                sales_data['开票日期'],
                sales_data['产品销售日期']
            )

            return sales_data[[
                '业务部门', '产品状态', '客户姓名', '手机号码', '车架号', '车系', '产品销售日期', '开票日期', '日期',
                '产品销售版本', '新车开票价格', '车辆阶段', '车辆类型','产品销售金额', '终身保养金额', '成本金额', '利润', '所属门店', '销售顾问', '车系网络', '数据有效性',
                'created_at']].rename(columns={"产品销售日期": "销售日期", "产品销售版本": "全保无忧版本", "产品销售金额": "全保无忧金额", "成本金额": "成本"})

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
        ]

        for column, value in filters:
            df = df[df[column] == value]

        return df


"""衍生产品保险数据处理类"""
class DerivativeInsuranceProcessor(DataProcessor):
    REQUIRED_COLUMNS = {
        'derivative': [
            '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码', '保赔无忧金额', '双保无忧金额', '终身保养金额','销售顾问', '所属门店', '备注', '日期'
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
                    df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True).replace("永乐盛世", "洪武盛世")
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


"""新车保险数据处理类"""
class NewCarInsuranceProcessor(DataProcessor):
    @classmethod
    def process_new_car_insurance(cls) -> pd.DataFrame:
        """处理新车保险数据"""
        # 读取主数据
        df_main = pd.read_csv(Config.FILE_PATHS['new_insurance_csv'], low_memory=False).replace("永乐盛世", "洪武盛世")
        df_main['总费用_次数'] = 1

        # 读取保险数据
        df_cyy = pd.read_csv(Config.FILE_PATHS['yongle_csv'])
        df_cyy = df_cyy[
            ['出单日期', '保险公司简称', '所属门店', '车系', '车架号', '交强险保费', '业务人员', '保费总额', '业务来源', '总费用_次数']]

        # 重命名列
        df_cyy.rename(columns={
            '出单日期': '签单日期',
            '保险公司简称': '保险公司',
            '车系': '车型',
            '所属门店': '归属公司',
            '业务人员': '销售顾问'
        }, inplace=True)

        # 处理总数据
        df_cyy['签单日期'] = pd.to_datetime(df_cyy['签单日期'], errors='coerce').dt.date
        start_date = pd.to_datetime('2025-04-01').date()
        df_cyy = df_cyy[df_cyy['签单日期'] >= start_date]
        df_cyy['到期日期'] = pd.NaT
        df_cyy['数据归属门店'] = df_cyy['归属公司']
        df_cyy['车牌号'] = None
        df_cyy['被保险人'] = None
        df_cyy['是否为保赔无忧客户'] = '否'

        # 加载车系网络数据并调整直播基地门店名称
        df_car = cls.load_car_network_data()
        df_cyy = df_cyy.merge(df_car[['车系', '服务网络']], left_on='车型', right_on='车系', how='left')

        # 调整直播基地门店名称
        mask = df_cyy['归属公司'] == '直播基地'
        df_cyy.loc[mask, '归属公司'] = df_cyy.loc[mask, '服务网络'] + '-直播基地'

        # 合并数据
        df_combined = pd.concat([df_main, df_cyy], axis=0, ignore_index=True)

        # 替换门店名称
        df_combined['归属公司'] = df_combined['归属公司'].replace('文景初治', '上元盛世')

        # 删除保险公司为空的记录
        df_combined.dropna(subset=['保险公司'], inplace=True)

        # 选择需要的列
        required_cols = [
            '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司', '车型', '车牌号',
            '车架号', '被保险人', '交强险保费', '销售顾问', '是否为保赔无忧客户', '业务来源', '总费用_次数'
        ]

        # 只保留存在的列
        available_cols = [col for col in required_cols if col in df_combined.columns]
        df_filtered = df_combined[available_cols].copy()

        # 添加日期列
        df_filtered.loc[:, '日期'] = df_filtered['签单日期']

        return df_filtered


"""保险数据合并类"""
class InsuranceDataMerger:
    @staticmethod
    def merge_insurance_data(comprehensive_data: pd.DataFrame, derivative_data: pd.DataFrame) -> pd.DataFrame:
        """合并保险数据"""
        merged_data = pd.concat([comprehensive_data, derivative_data], axis=0, join='outer', ignore_index=True)

        # 数据增强
        merged_data = InsuranceDataMerger._enhance_data(merged_data)

        # 筛选出销售日期在2026年3月1日之后的
        merged_data["是否3月1日之后"] = np.where(
            merged_data["销售日期"].isna(),  # 条件1：日期为空
            "日期为空",  # 条件1满足时的赋值
            np.where(  # 条件1不满足时，进入第二层判断
                merged_data["销售日期"] > pd.to_datetime("2026/03/01"),  # 条件2：日期在3月1日之后
                "是",  # 条件2满足时的赋值
                "否"  # 条件2不满足时的赋值
            )
        )

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

        # 添加一列记录是否近3个月购买过无忧产品
        df['销售日期'] = pd.to_datetime(df['销售日期'], format='%Y/%m/%d', errors='coerce')
        df['开票日期'] = pd.to_datetime(df['开票日期'], format='%Y/%m/%d', errors='coerce')
        missing = df['销售日期'].isna() | df['开票日期'].isna()
        within_3m = (df['销售日期'] < df['开票日期'] + pd.DateOffset(months=3)) & ~missing
        df['是否3月内购买无忧'] = np.where(missing, '缺少日期', np.where(within_3m, '是', '否'))

        # 数据清理
        df.drop_duplicates(inplace=True)
        df.dropna(subset='车架号', inplace=True)

        return df

    @staticmethod
    def mark_comprehensive_insurance(comprehensive_df: pd.DataFrame, insurance_df: pd.DataFrame) -> pd.DataFrame:
        """标记全保无忧保险数据"""
        # 创建标记
        wy_flag = (comprehensive_df.groupby('车架号')
                   .size()
                   .reset_index(name='cnt')
                   .assign(是否保赔=lambda x: '是'))
        wy_flag = wy_flag[['车架号', '是否保赔']]

        # 处理日期
        insurance_df['日期'] = pd.to_datetime(insurance_df['日期'], format='mixed', errors='coerce').dt.date

        # 合并标记
        insurance_df = insurance_df.merge(wy_flag, on='车架号', how='left')
        insurance_df['是否保赔'] = insurance_df['是否保赔'].fillna('否')

        return insurance_df

    @staticmethod
    def filter_excluded_vehicles(insurance_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """筛选排除的车辆"""
        # 排除运营车的逻辑
        exclude_mask = (
            insurance_df['保险公司'].str.contains('鼎和', na=False) | insurance_df['交强险保费'].isin(Config.EXCLUDE_INSURANCE_VALUES) |
                               insurance_df['业务来源'].isin(Config.EXCLUDE_BUSINESS_SOURCE)) & (insurance_df['是否保赔'] == '否')

        # 获取被排除的车架号
        excluded_vins = insurance_df[exclude_mask]['车架号'].unique()

        # 获取审批流中的数据
        config = MongoDBConfig(database='xg_JiaTao')
        mongo_client = MongoDBClient(config)
        COLLECTION_NAMES = {
            'sales_data': 'YS_sales',
            'cost_details': '全保无忧费用明细'
        }

        try:
            if not mongo_client.connect():
                return pd.DataFrame(), pd.DataFrame()

            # 获取销售数据
            sales_data = mongo_client.query_all_data(COLLECTION_NAMES['sales_data'])
            sales_data_number = sales_data["车架号"].to_list()
        except:
            print("数据库连接失败")


        # 处理日期
        insurance_df["日期"] = pd.to_datetime(insurance_df["日期"], errors='coerce')
        # 新增列【是否符合3月1日之前规则】
        # 先判断日期是否在3月1日之前，如果不是则为空值
        # 如果是，再判断车架号是否在excluded_vins中：在则标记为"不符合"，不在则标记为"符合"
        insurance_df['是否符合3月1日之前删除规则'] = np.where(
            insurance_df["日期"] < pd.to_datetime('2026/03/01'),
            np.where(
                insurance_df['车架号'].isin(excluded_vins),
                '不符合',
                '符合'
            ),
            np.where(
                insurance_df['车架号'].isin(sales_data_number),
                '不符合',
                '符合'
            )
        )

        # 添加城市信息
        insurance_df['城市'] = np.where(insurance_df['归属公司'].str.contains('贵州'), '贵州', '成都')
        df_result = insurance_df.drop_duplicates()

        return df_result


"""数据导出类"""
class DataExporter:
    @staticmethod
    def export_data(data_dict: Dict[str, pd.DataFrame]):
        """导出数据到CSV文件"""
        output_dir = Config.FILE_PATHS['output_dir']

        for name, df in data_dict.items():
            if df is not None and not df.empty:
                file_path = os.path.join(output_dir, f"{name}.csv")
                df.to_csv(file_path, index=False)
                print(f"已导出 {name} 数据到: {file_path}")

    @staticmethod
    # 新增方法：将DataFrame写入数据库
    def write_df_to_db(df: pd.DataFrame, table_name: str) -> bool:
        # 创建写入数据库引擎
        try:
            output_engine = create_engine(
                f"mysql+pymysql://{APP_DB_CONFIG['user']}:{APP_DB_CONFIG['password']}@{APP_DB_CONFIG['host']}:{APP_DB_CONFIG['port']}/{APP_DB_CONFIG['database']}?charset=utf8mb4",echo=False
            )
            print("输出数据库引擎创建成功")
        except Exception as e:
            print(f"输出数据库引擎创建失败: {str(e)}", exc_info=True)
            raise

        """将DataFrame写入数据库"""
        try:
            print(f"开始写入数据库表：{table_name}")
            # 写入数据库，如果表存在则替换
            df.to_sql(
                name=table_name,
                con=output_engine,
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            print(f"成功写入表 {table_name}，数据行数：{len(df)}")
            return True
        except Exception as e:
            print(f"数据写入失败（表：{table_name}）：{str(e)}", exc_info=True)
            return False


"""主函数"""
def main():
    # 1. 处理全保无忧数据
    print("正在处理全保无忧数据...")
    comprehensive_sales = ComprehensiveInsuranceProcessor.process_qbwy()

    # 2. 处理衍生产品保险数据
    print("正在处理衍生产品保险数据...")
    derivative_data = DerivativeInsuranceProcessor.process_derivative_insurance()

    # 3. 合并无忧数据
    print("正在合并无忧数据...")
    merged_insurance_data = InsuranceDataMerger.merge_insurance_data(comprehensive_sales, derivative_data)

    # 4. 处理新车保险数据
    print("正在处理新车保险数据...")
    new_car_insurance = NewCarInsuranceProcessor.process_new_car_insurance()

    # 5. 标记并筛选新车保险数据
    print("正在标记新车保险数据...")
    marked_insurance = InsuranceDataMerger.mark_comprehensive_insurance(merged_insurance_data, new_car_insurance)

    # 6. 筛选排除的车辆
    print("正在筛选排除的车辆...")
    remaining_vehicles = InsuranceDataMerger.filter_excluded_vehicles(marked_insurance)

    # 7. 导出数据
    print("正在导出数据...")
    DataExporter.export_data({
        '保赔无忧': merged_insurance_data,
        '新车保险台账': remaining_vehicles,
    })

    # 8. 保存到数据库
    merged_insurance_mysql = merged_insurance_data[["所属门店", "服务网络", "日期", "车系", "车架号", "客户姓名", "手机号码", "全保无忧版本", "全保无忧金额", "利润", "保赔无忧金额", "双保无忧金额"]].rename(columns={"所属门店": "公司名称"}).copy()
    DataExporter.write_df_to_db(merged_insurance_mysql, 'qbwy_sales')
    remaining_vehicles_mysql = remaining_vehicles[["数据归属门店", "车架号", "日期", "是否保赔"]].rename(columns={"数据归属门店": "公司名称"}).copy()
    DataExporter.write_df_to_db(remaining_vehicles_mysql, 'insurance_sales')


    print("数据处理完成！")


if __name__ == "__main__":
    main()