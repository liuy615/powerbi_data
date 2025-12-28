import numpy as np
from pymongo import MongoClient
import pandas as pd


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


# 主程序
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
            df_qbwy1["全保无忧版本"] = df_qbwy1["全保无忧版本"].replace("畅行+终身版(送)", "畅行版")
            df_qbwy1.to_csv("df_qbwy1.csv")

            # 查询指定字段
            # 定义需要的字段
            desired_fields = ['业务部门', '产品状态', '客户姓名', '手机号码', '车架号', '车系', '销售日期','全保无忧版本', '新车开票价格', '车辆阶段', '车辆类型', '全保无忧金额', '终身保养金额', '所属门店','销售顾问', '车系网络', '数据有效性']
            df_qbwy2 = mongo_client.query_data_with_projection(collection_name_qbwy, desired_fields)
            # 设置城市
            df_qbwy2['城市'] = np.where(df_qbwy2['所属门店'].str.contains('贵州'), '贵州', '成都')
            # 设置投保价
            bins = [0, 100000, 150000, 200000, 250000, 300000, 350000, np.inf]
            labels = ['0-10万', '10-15万', '15-20万', '20-25万', '25-30万', '30-35万', '35-40万']
            df_qbwy2['投保价'] = pd.cut(df_qbwy2['新车开票价格'], bins=bins, labels=labels, right=False).astype(str)
            # 筛选有效数据
            df_qbwy2 = df_qbwy2[df_qbwy2["数据有效性"] == "有效"]
            df_qbwy2 = df_qbwy2[df_qbwy2["业务部门"] == "售前"]
            df_qbwy2 = df_qbwy2[df_qbwy2["产品状态"] == "生效"]
            df_qbwy2["全保无忧版本"] = df_qbwy2["全保无忧版本"].replace("畅行+终身版(送)", "畅行版")
            # 查询全保无忧费用明细
            chanpin = mongo_client.query_all_data(collection_name_qbwy_info)
            chanpin.to_csv("chanpin.csv")
            df_qbwy2.to_csv("df_qbwy2.csv")
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

    except Exception as e:
        print(f"程序执行失败: {e}")
    finally:
        # 关闭连接
        mongo_client.disconnect()

    return df_qbwy1, valid_matches


process_qbwy()