# -*- coding: utf-8 -*-
"""
数据库操作模块
"""

import logging
import sys
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, DECIMAL, DATETIME, INTEGER
from sqlalchemy.exc import SQLAlchemyError
project_root = r"E:\powerbi_data"
sys.path.insert(0, project_root)
from config.cyys_data_processor.config import SOURCE_MYSQL_CONFIG, OUTPUT_MYSQL_CONFIG


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, source_config=None, output_config=None):
        self.source_config = source_config or SOURCE_MYSQL_CONFIG
        self.output_config = output_config or OUTPUT_MYSQL_CONFIG
        self.source_engine = None
        self.output_engine = None

    def connect(self):
        """连接数据库"""
        # 连接源数据库
        self.source_engine = self._create_engine(self.source_config)

        # 连接输出数据库
        self.output_engine = self._create_engine(self.output_config)

        logging.info("数据库连接完成")

    def _create_engine(self, db_config):
        """创建数据库引擎"""
        try:
            conn_str = (
                f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
                f"charset={db_config['charset']}"
            )
            engine = create_engine(conn_str, pool_pre_ping=True)
            logging.info(f"数据库[{db_config['database']}]连接成功")
            return engine
        except SQLAlchemyError as e:
            logging.error(f"数据库[{db_config['database']}]连接失败：{str(e)}")
            raise

    def _create_output_database(self):
        """创建输出数据库（如果不存在）"""
        temp_config = self.output_config.copy()
        target_db = temp_config.pop('database')
        temp_config['database'] = 'information_schema'

        try:
            with pymysql.connect(**temp_config) as conn:
                with conn.cursor() as cursor:
                    create_sql = f"""
                        CREATE DATABASE IF NOT EXISTS `{target_db}` 
                        CHARACTER SET utf8mb4 
                        COLLATE utf8mb4_unicode_ci;
                    """
                    cursor.execute(create_sql)
                    conn.commit()
            logging.info(f"输出数据库[{target_db}]创建完成（若不存在）")
        except pymysql.MySQLError as e:
            logging.error(f"输出数据库创建失败：{str(e)}")
            raise

    def read_from_mysql(self, table_name, field_mapping):
        """从MySQL读取数据（指定字段）"""
        if table_name not in field_mapping:
            logging.error(f"表[{table_name}]无字段映射，无法读取")
            return pd.DataFrame()

        try:
            english_cols = field_mapping[table_name]
            query_cols = ', '.join([f"`{col}`" for col in english_cols])
            query = f"SELECT {query_cols} FROM `{table_name}`"

            df = pd.read_sql(query, self.source_engine)
            logging.info(f"表[{table_name}]读取完成：{len(df)}条数据，{len(english_cols)}个字段")
            return df
        except SQLAlchemyError as e:
            logging.error(f"表[{table_name}]读取失败：{str(e)}")
            return pd.DataFrame()

    def write_to_output_db(self, df, table_name):
        """将DataFrame写入输出数据库"""
        if df.empty:
            logging.warning(f"数据为空，跳过表[{table_name}]写入")
            return

        # 检查并处理重复列名
        if df is not None and not df.empty:
            # 检查重复列名
            if len(df.columns) != len(set(df.columns)):
                duplicate_cols = df.columns[df.columns.duplicated()].tolist()
                logging.warning(f"表[{table_name}]存在重复列名：{duplicate_cols}，正在清理...")
                # 删除重复列（保留第一个）
                df = df.loc[:, ~df.columns.duplicated()]
                logging.info(f"表[{table_name}]重复列名清理完成")

        try:
            df.to_sql(
                name=table_name,
                con=self.output_engine,
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            logging.info(f"表[{table_name}]写入完成：{len(df)}条数据")
        except SQLAlchemyError as e:
            logging.error(f"表[{table_name}]写入失败：{str(e)}")
            raise

    def get_sql_dtype(self, df):
        """获取SQL数据类型映射"""
        dtype_map = {}
        for col in df.columns:
            col_type = str(df[col].dtype)
            # 日期时间类型
            if col_type in ['datetime64[ns]', 'datetime64']:
                dtype_map[col] = DATETIME()
            # 金额类字段
            elif col_type in ['float64', 'float32']:
                if any(key in col for key in ['金额', '毛利', '费用', '价格', '成本', '返利', '补贴']):
                    dtype_map[col] = DECIMAL(18, 2)
                else:
                    dtype_map[col] = DECIMAL(10, 2)
            # 整数类型
            elif col_type in ['int64', 'int32', 'Int64']:
                dtype_map[col] = INTEGER()
            # 字符串类型
            else:
                max_len = df[col].dropna().astype(str).str.len().max() if not df[col].dropna().empty else 50
                dtype_map[col] = VARCHAR(min(max_len + 20, 255))
        return dtype_map

    def close(self):
        """关闭数据库连接"""
        if self.source_engine:
            self.source_engine.dispose()
        if self.output_engine:
            self.output_engine.dispose()
        logging.info("数据库连接已关闭")