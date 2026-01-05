import pandas as pd
import numpy as np
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient



class MongoDBConfig:
    """MongoDB连接配置类"""

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


def process_qbwy():
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
            df_qbwy1["全保无忧版本"] = df_qbwy1["全保无忧版本"].str.replace(r"畅行.*", "畅行版", regex=True)
            # df_qbwy1["全保无忧版本"] = df_qbwy1["全保无忧版本"].str.replace(r"畅行+终身版(送)", "畅行版", regex=True)
            # df_qbwy1.to_csv("df_qbwy1.csv")

            # 查询指定字段
            # 定义需要的字段
            desired_fields = ['业务部门', '产品状态', '客户姓名', '手机号码', '车架号', '车系', '销售日期','开票日期', '全保无忧版本', '新车开票价格', '车辆阶段', '车辆类型', '全保无忧金额', '终身保养金额', '所属门店','销售顾问', '车系网络', '数据有效性','created_at']
            df_qbwy2 = mongo_client.query_data_with_projection(collection_name_qbwy, desired_fields)
            df_qbwy2 = df_qbwy2[df_qbwy2["数据有效性"] == "有效"]
            df_qbwy2 = df_qbwy2[df_qbwy2["业务部门"] == "售前"]
            df_qbwy2 = df_qbwy2[df_qbwy2["产品状态"] == "生效"]
            # 设置城市
            df_qbwy2['城市'] = np.where(df_qbwy2['所属门店'].str.contains('贵州'), '贵州', '成都')
            df_qbwy2["全保无忧版本"] = df_qbwy2["全保无忧版本"].str.replace(r"畅行.*", "畅行版", regex=True)

            # 设置投保价
            bins = [0, 100000, 150000, 200000, 250000, 300000, 350000, np.inf]
            labels = ['0-10万', '10-15万', '15-20万', '20-25万', '25-30万', '30-35万', '35-40万']
            df_qbwy2['投保价'] = pd.cut(df_qbwy2['新车开票价格'], bins=bins, labels=labels, right=False).astype(str)
            # 筛选有效数据

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
            valid_matches["销售日期"] = pd.to_datetime(valid_matches["销售日期"], format='mixed', errors='coerce').dt.date
            valid_matches["开票日期"] = pd.to_datetime(valid_matches["开票日期"], format='mixed', errors='coerce').dt.date
            valid_matches["日期"] = np.where(valid_matches['开票日期']>valid_matches['销售日期'],valid_matches['开票日期'], valid_matches['销售日期'])

    except Exception as e:
        print(f"程序执行失败: {e}")
    finally:
        # 关闭连接
        mongo_client.disconnect()

    return df_qbwy1, valid_matches


# 新增列名标准化函数
def standardize_columns(df):
    """
    标准化列名：去除空格、转换为小写、替换空格为下划线
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(r'\\s+', '_', regex=True)
    return df
# 处理保赔无忧

def read_excel_files(directory, sheet_name):
    """
    读取指定目录下的 Excel 文件，返回 DataFrame 列表。
    """
    dfs = []
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        print(f"正在读取文件 {filename}...")
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
            df['from'] = os.path.basename(file_path).split('.')[0]
            # 增强列名清洗逻辑
            df = standardize_columns(df)
            required_columns = [
               '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码', 
                '保赔无忧金额', '双保无忧金额', '终身保养金额',
                '销售顾问', '所属门店', '备注', '日期'
            ]
            
            # 检查并补全缺失列
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[required_columns]
            dfs.append(df)
            print(f"文件 {filename} 读取成功，一共 {len(df)} 行。列名: {list(df.columns)}")
        except Exception as e:
            print(f"读取 {filename} 时发生错误: {e}")
    return dfs

def process_dataframe(df):
    """
    处理 DataFrame，进行数据清洗和过滤。
    """
    df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed',errors='coerce')
    df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed',errors='coerce')
    # df[['开票日期','保赔销售日期']] = pd.to_datetime(df[['开票日期','保赔销售日期']], errors='coerce')
    df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
    df['日期'] = df['开票日期'].fillna(df['销售日期'])
    df['日期'] = pd.to_datetime(df['日期'], format='mixed',errors='coerce')
    # df = df[df['日期'].dt.year == 2024]
    df['日期'] = df['日期'].dt.date
    return df[[ '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码', '保赔无忧金额','双保无忧金额','终身保养金额',
               '销售顾问', '所属门店', '备注',  '日期']]

def bpwy_result():
    directories = [
        r"E:\powerbi_data\看板数据\私有云文件本地\data\衍生产品"
    ]

    all_dfs = []

    with ThreadPoolExecutor() as executor:
        # 提交任务到线程池
        futures = [executor.submit(read_excel_files, directory, '登记表') for directory in directories]

        # 获取任务结果
        for future in as_completed(futures):
            dfs = future.result()
            if dfs:
                df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
                df_processed = process_dataframe(df_combined)
                all_dfs.append(df_processed)

    if all_dfs:
        df_final = pd.concat(all_dfs, axis=0, join='outer', ignore_index=True)
        # Save or use df_final as needed
        print(df_final.head())  # Example: print the first few rows
    return df_final
# 处理全保无忧

def read_excel_files_qbwy(directory, sheet_name):
    """
    读取指定目录下的 Excel 文件，返回 DataFrame 列表。
    """
    dfs = []
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        print(f"正在读取文件 {filename}...")
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
            df['from'] = os.path.basename(file_path).split('.')[0]
            # 增强列名清洗逻辑
            df = standardize_columns(df)
            required_columns = [
                '客户姓名','手机号码','身份证号','车架号','发动机号','车牌号','车系','新车开票价格','车损险保额','车辆类型',
                '车系网络','销售日期','全保无忧版本','全保无忧金额','起保日期','终止日期','销售顾问','所属门店','投保费用','from'
            ]
            
            # 检查并补全缺失列
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[required_columns]
            dfs.append(df)
            print(f"文件 {filename} 读取成功，一共 {len(df)} 行。列名: {list(df.columns)}")
        except Exception as e:
            print(f"读取 {filename} 时发生错误: {e}")
    return dfs

def process_dataframe_qbwy(df):
    """
    处理 DataFrame，进行数据清洗和过滤。
    """
    df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed',errors='coerce')
    df['销售日期'] = df['销售日期'].dt.date
    df_Car = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充车系')
    df = pd.merge(df, df_Car[['车系', '服务网络']], how='left', on='车系')
    df['所属门店'] = np.where(df['所属门店'] == '直播基地', df['服务网络'] + '-' + df['所属门店'], df['所属门店'])
    return df[[ '客户姓名','手机号码','身份证号','车架号','发动机号','车牌号','车系','新车开票价格','车损险保额','车辆类型',
                '车系网络','销售日期','全保无忧版本','全保无忧金额','起保日期','终止日期','销售顾问','所属门店','投保费用','from']]

df_qbwy1, df_qbwy2 = process_qbwy()

df4 = bpwy_result()
df_wuyou = pd.concat([df_qbwy2, df4], axis=0,join='outer', ignore_index=True)

df_Car = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充车系')
df_wuyou = pd.merge(df_wuyou, df_Car[['车系', '服务网络']], how='left', on='车系')
df_wuyou['所属门店'] = np.where(df_wuyou['所属门店'] == '直播基地', df_wuyou['服务网络'] + '-' + df_wuyou['所属门店'], df_wuyou['所属门店'])

df_wuyou['是否保赔'] = '是'
df_wuyou['所属门店'] = df_wuyou['所属门店'].replace('文景初治', '上元盛世')
df_wuyou['城市'] = np.where(df_wuyou['所属门店'].str.contains('贵州'), '贵州', '成都')
df_wuyou.drop_duplicates(inplace=True)
df_wuyou.dropna(subset='车架号', inplace=True)


def standardize_date(date_str):
    if isinstance(date_str, str):  # 确保输入是字符串
        date_str = date_str.strip()  # 去除空格
        if "年" in date_str and "月" in date_str and "日" not in date_str:
            # 如果日期缺少“日”，补充为“1日”
            date_str += "1日"
    return date_str 
def convert_date(date_str):
    if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_str):
        return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
    elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
        return pd.to_datetime(date_str, format='%Y年%m月%d日')
    return pd.NaT
def process_xinchebaoxian(filedir, sheet_name):
    filenames = os.listdir(filedir)

    def read_single_file(filename):
        if '新车' in filename and filename.endswith('.xlsx'):
            file_path = os.path.join(filedir, filename)
            try:
                # 检查工作表是否存在
                with pd.ExcelFile(file_path) as xls:
                    if sheet_name in xls.sheet_names:
                        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                        df['From'] = os.path.basename(file_path).split('.')[0]
                        df.columns = df.columns.str.replace('\n', '')
                        return df
                    else:
                        print(f"Worksheet named '{sheet_name}' not found in file {filename}. Skipping this file.")
            except Exception as e:
                print(f"读取 {filename} 时发生错误: {e}")
        return None

    dfs = []
    with ThreadPoolExecutor() as executor:
        # 提交任务到线程池
        futures = [executor.submit(read_single_file, filename) for filename in filenames]

        # 获取任务结果
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                dfs.append(df)

    # 如果 dfs 为空，返回一个空的 DataFrame
    if not dfs:
        return pd.DataFrame()

    df_combined = pd.concat(dfs, axis=0, ignore_index=True)
    return df_combined

def xinchebaoxianTZ():
    all_dfs = pd.read_csv(r"E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源\202401_202503新车保险台账.csv")
    df_cyy = pd.read_csv(r"C:\Users\13111\Documents\WXWork\1688855282576011\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv")
    df_cyy = df_cyy[['出单日期', '保险公司简称', '所属门店', '车系', '车架号', '交强险保费', '业务人员','保费总额','总费用_次数']]
    df_cyy.rename(columns={'出单日期': '签单日期', '保险公司简称': '保险公司', '车系': '车型', '所属门店': '归属公司', '业务人员': '销售顾问'}, inplace=True)
    df_Car = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充车系')
    df_cyy = pd.merge(df_cyy, df_Car[['车系', '服务网络']], how='left', left_on='车型',right_on='车系')
    df_cyy['归属公司'] = np.where(df_cyy['归属公司'] == '直播基地', df_cyy['服务网络'] + '-' + df_cyy['归属公司'], df_cyy['归属公司'])
    all_dfs['总费用_次数'] = 1
    df_combined = pd.concat([all_dfs, df_cyy], axis=0, join='outer', ignore_index=True)
    df_combined['归属公司'] = df_combined['归属公司'].replace('文景初治', '上元盛世')
    df_combined.dropna(subset=['保险公司'], inplace=True)
    df_filtered = df_combined[['月份', '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司', '车型', '车牌号', '车架号', '被保险人',
                                '交强险保费', '销售顾问', '是否为保赔无忧客户','总费用_次数']]
    df_filtered['日期'] = df_filtered['签单日期']
    return df_filtered



        # ---------- 方案 A：只要“是否保赔” ----------
wy_flag = (df_wuyou.assign(tmp=1)
                .groupby('车架号')
                .agg(是否保赔=('tmp', lambda x: '是' if len(x)>0 else '否'))
                .reset_index())
df_xcbx = xinchebaoxianTZ()
df_xcbx['日期'] = pd.to_datetime(df_xcbx['日期'], errors='coerce').dt.date
df_xcbx = df_xcbx.merge(wy_flag, on='车架号', how='left')
df_xcbx['是否保赔'] = df_xcbx['是否保赔'].fillna('否')
# 筛选运营车出去
exclude_list = [1000, 1130, 1800]
df_except = df_xcbx[df_xcbx['保险公司'].str.contains('鼎和')]
df_except1 = df_xcbx[df_xcbx['交强险保费'].isin(exclude_list)]
df_excluded = pd.concat([df_except, df_except1], axis=0).drop_duplicates()
df_excluded = df_excluded[df_excluded['是否保赔'] == '否']

# 筛选出不包含在 df_excluded 中的行
diff_df = df_xcbx[~df_xcbx['车架号'].isin(df_excluded['车架号'])]
diff_df['城市'] = np.where(diff_df['归属公司'].str.contains('贵州'), '贵州', '成都')
diff_df = diff_df.drop_duplicates()
df_wuyou.to_csv(r'C:\Users\13111\code\dashboard\保赔无忧.csv', index=False)
diff_df.to_csv(r'C:\Users\13111\code\dashboard\新车保险台账.csv', index=False)
df_qbwy1.to_csv(r'C:\Users\13111\code\dashboard\全赔无忧.csv', index=False)