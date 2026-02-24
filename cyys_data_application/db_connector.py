"""
数据库连接和操作 - 支持源数据库和目标数据库
"""
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import pymysql

class DatabaseConnector:
    """数据库连接器"""

    def __init__(self, source_db_url, app_db_url):
        self.source_db_url = source_db_url  # 源数据库连接URL
        self.app_db_url = app_db_url        # 应用数据库连接URL
        self.source_engine = None           # 源数据库引擎
        self.app_engine = None              # 应用数据库引擎
        self.source_conn = None             # 源数据库连接
        self.app_conn = None                # 应用数据库连接

    def connect(self):
        """连接两个数据库"""
        try:
            # 创建SQLAlchemy引擎
            self.source_engine = create_engine(self.source_db_url)
            self.app_engine = create_engine(self.app_db_url)

            # 创建连接
            self.source_conn = self.source_engine.connect()
            self.app_conn = self.app_engine.connect()

            print(f"[{datetime.now()}] 源数据库连接成功: {self.source_db_url.split('/')[-1].split('?')[0]}")
            print(f"[{datetime.now()}] 应用数据库连接成功: {self.app_db_url.split('/')[-1].split('?')[0]}")

            return True
        except Exception as e:
            print(f"[{datetime.now()}] 数据库连接失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.source_conn:
            self.source_conn.close()
        if self.source_engine:
            self.source_engine.dispose()

        if self.app_conn:
            self.app_conn.close()
        if self.app_engine:
            self.app_engine.dispose()

        print(f"[{datetime.now()}] 数据库连接已关闭")

    def load_source_data(self, table_name, where_clause=None):
        """从源数据库加载数据到DataFrame"""
        try:
            sql = f"SELECT * FROM {table_name}"
            if where_clause:
                sql += f" WHERE {where_clause}"

            df = pd.read_sql(sql, self.source_conn)
            print(f"[{datetime.now()}] 从源数据库 {table_name} 加载 {len(df)} 条记录")
            return df
        except Exception as e:
            print(f"[{datetime.now()}] 加载源数据失败: {e}")
            return pd.DataFrame()

    def save_app_data(self, df, table_name):
        """将DataFrame保存到应用数据库"""
        try:
            if df.empty:
                print(f"[{datetime.now()}] 数据为空，跳过保存")
                return 0

            # 使用to_sql保存数据
            affected_rows = df.to_sql(
                table_name,
                self.app_conn,
                if_exists='replace',  # 替换模式，每次清空重写
                index=False,
                chunksize=1000
            )
            print(f"[{datetime.now()}] 保存 {affected_rows} 条记录到应用数据库 {table_name}")
            return affected_rows
        except Exception as e:
            print(f"[{datetime.now()}] 保存应用数据失败: {e}")
            return 0

    def save_app_data_append(self, df, table_name):
        """将DataFrame追加保存到应用数据库"""
        try:
            if df.empty:
                print(f"[{datetime.now()}] 数据为空，跳过保存")
                return 0

            affected_rows = df.to_sql(
                table_name,
                self.app_conn,
                if_exists='append',  # 追加模式
                index=False,
                chunksize=1000
            )
            print(f"[{datetime.now()}] 追加 {affected_rows} 条记录到应用数据库 {table_name}")
            return affected_rows
        except Exception as e:
            print(f"[{datetime.now()}] 追加应用数据失败: {e}")
            return 0

    def execute_app_sql(self, sql):
        """在应用数据库执行SQL语句"""
        try:
            with self.app_conn.begin():
                result = self.app_conn.execute(text(sql))
                print(f"[{datetime.now()}] 应用数据库SQL执行成功: {sql[:50]}...")
                return True
        except Exception as e:
            print(f"[{datetime.now()}] 应用数据库SQL执行失败: {e}")
            return False

    def execute_source_sql(self, sql):
        """在源数据库执行SQL语句"""
        try:
            with self.source_conn.begin():
                result = self.source_conn.execute(text(sql))
                print(f"[{datetime.now()}] 源数据库SQL执行成功: {sql[:50]}...")
                return True
        except Exception as e:
            print(f"[{datetime.now()}] 源数据库SQL执行失败: {e}")
            return False

    def check_app_table_exists(self, table_name):
        """检查应用数据库中表是否存在"""
        try:
            # MySQL的查询语句
            sql = f"""
            SELECT COUNT(*) as table_count 
            FROM information_schema.tables 
            WHERE table_schema = '{self.app_db_url.split('/')[-1].split('?')[0]}' 
            AND table_name = '{table_name}'
            """

            result = pd.read_sql(sql, self.app_conn)
            exists = result['table_count'].iloc[0] > 0
            print(f"[{datetime.now()}] 检查表 {table_name} 存在: {exists}")
            return exists
        except Exception as e:
            print(f"[{datetime.now()}] 检查表存在失败: {e}")
            return False