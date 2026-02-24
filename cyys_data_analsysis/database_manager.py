# -*- coding: utf-8 -*-
"""
数据库管理模块
处理数据库连接、读取和写入操作
"""

import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Any
import warnings

# 忽略特定警告
warnings.filterwarnings('ignore', category=FutureWarning, message='.*Downcasting object dtype arrays.*')
warnings.filterwarnings('ignore', category=UserWarning)

class DatabaseManager:
    """数据库管理器类：处理所有数据库相关操作"""
    
    def __init__(self, source_config: Dict[str, Any], output_config: Dict[str, Any]):
        """
        初始化数据库管理器
        
        Args:
            source_config: 源数据库配置
            output_config: 输出数据库配置
        """
        self.source_config = source_config
        self.output_config = output_config
        self.source_engine = None
        self.output_engine = None
        
        # 初始化数据库连接
        self._initialize_connections()
        
    def _initialize_connections(self):
        """初始化数据库连接"""
        try:
            # 创建源数据库连接（读取数据）
            self.source_engine = create_engine(
                f"mysql+pymysql://{self.source_config['user']}:{self.source_config['password']}"
                f"@{self.source_config['host']}:{self.source_config['port']}"
                f"/{self.source_config['database']}?charset=utf8mb4",
                echo=False
            )
            logging.info("源数据库引擎创建成功")
            
            # 创建输出数据库连接（写入数据）
            self.output_engine = create_engine(
                f"mysql+pymysql://{self.output_config['user']}:{self.output_config['password']}"
                f"@{self.output_config['host']}:{self.output_config['port']}"
                f"/{self.output_config['database']}?charset=utf8mb4",
                echo=False
            )
            logging.info("输出数据库引擎创建成功")
            
        except Exception as e:
            logging.error(f"数据库引擎创建失败: {str(e)}", exc_info=True)
            raise
    
    def read_from_database(self, table_name: str, engine_type: str = "source") -> pd.DataFrame:
        """
        从数据库读取数据
        
        Args:
            table_name: 表名
            engine_type: 数据库类型，'source' 或 'output'
            
        Returns:
            pd.DataFrame: 读取的数据
        """
        try:
            logging.info(f"开始读取数据库表：{table_name}")
            
            # 选择引擎
            engine = self.source_engine if engine_type == "source" else self.output_engine
            
            # 使用反引号包裹表名，兼容MySQL语法
            sql = f"SELECT * FROM `{table_name}`"
            df = pd.read_sql(sql, engine)
            
            logging.info(f"成功读取表 {table_name}，数据行数：{len(df)}")
            return df
            
        except SQLAlchemyError as e:
            logging.error(f"数据库操作失败（表：{table_name}）：{str(e)}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"数据处理失败（表：{table_name}）：{str(e)}", exc_info=True)
            raise
    
    def write_to_database(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        将DataFrame写入数据库
        
        Args:
            df: 要写入的数据
            table_name: 目标表名
            
        Returns:
            bool: 是否成功
        """
        try:
            logging.info(f"开始写入数据库表：{table_name}")
            
            # 写入数据库，如果表存在则替换
            df.to_sql(
                name=table_name,
                con=self.output_engine,
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            
            logging.info(f"成功写入表 {table_name}，数据行数：{len(df)}")
            return True
            
        except Exception as e:
            logging.error(f"数据写入失败（表：{table_name}）：{str(e)}", exc_info=True)
            return False
    
    def read_table(self, table_name: str) -> pd.DataFrame:
        """读取源数据库表（简化方法）"""
        return self.read_from_database(table_name, "source")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """获取连接状态信息"""
        return {
            "source_connected": self.source_engine is not None,
            "output_connected": self.output_engine is not None,
            "source_config": {k: v for k, v in self.source_config.items() if k != 'password'},
            "output_config": {k: v for k, v in self.output_config.items() if k != 'password'}
        }