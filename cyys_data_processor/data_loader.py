# -*- coding: utf-8 -*-
"""
数据加载模块
"""

import logging
import pandas as pd
from config.cyys_data_processor.config import MAPPING_EXCEL_PATH, SERVICE_NET_PATH, API_TABLE_MAPPING


class DataLoader:
    """数据加载器"""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.dfname_to_col_rename = {}
        self.table_to_english_cols = {}
        self.df_service_net = pd.DataFrame()
        self.df_vat = pd.DataFrame()
        self.company_belongs = pd.DataFrame()
        self._load_field_mapping()
        self._load_external_data()

    def _load_field_mapping(self):
        """加载字段映射关系"""
        try:
            self.mapping_df = pd.read_excel(MAPPING_EXCEL_PATH, sheet_name="Sheet1")
            required_cols = ["df_name", "英文字段", "中文字段"]
            missing_cols = [col for col in required_cols if col not in self.mapping_df.columns]

            if missing_cols:
                raise ValueError(f"字段映射表缺少必要列：{missing_cols}")

            # 构建数据类型→字段重命名映射
            self.dfname_to_col_rename = {
                df_name: dict(zip(group["英文字段"].dropna(), group["中文字段"].dropna()))
                for df_name, group in self.mapping_df.groupby("df_name")
            }

            # 构建源表名→读取字段列表映射
            dfname_to_english = {
                df_name: group["英文字段"].dropna().tolist()
                for df_name, group in self.mapping_df.groupby("df_name")
            }

            for df_name, table_name in API_TABLE_MAPPING.items():
                if df_name in dfname_to_english:
                    self.table_to_english_cols[table_name] = dfname_to_english[df_name]
                else:
                    logging.warning(f"数据类型[{df_name}]无字段映射，表[{table_name}]无法读取")

            logging.info(f"字段映射初始化完成：覆盖{len(self.table_to_english_cols)}个源表")
        except Exception as e:
            logging.error(f"字段映射初始化失败：{str(e)}")
            raise

    def _load_external_data(self):
        """加载外部数据"""
        try:
            # 加载服务网络数据
            self.df_service_net = pd.read_excel(SERVICE_NET_PATH, sheet_name='补充车系')

            # 加载增值税处理数据
            self.df_vat = pd.read_excel(SERVICE_NET_PATH, sheet_name='汉唐_增值税处理')

            # 加载公司归属数据
            self.company_belongs = pd.read_excel(SERVICE_NET_PATH, sheet_name='补充团队')

            logging.info("外部数据加载完成")
        except Exception as e:
            logging.error(f"外部数据加载失败：{str(e)}")
            raise

    def load_all_data(self):
        """加载所有数据"""
        raw_data = {}

        for df_name, table_name in API_TABLE_MAPPING.items():
            df = self.db_manager.read_from_mysql(table_name, self.table_to_english_cols)

            if not df.empty and df_name in self.dfname_to_col_rename:
                # 重命名列
                rename_map = self.dfname_to_col_rename[df_name]
                valid_rename = {k: v for k, v in rename_map.items() if k in df.columns}
                if valid_rename:
                    df = df.rename(columns=valid_rename)

            raw_data[df_name] = df

        logging.info(f"数据加载完成：共{len(raw_data)}个数据表")
        return raw_data

    def get_field_mapping(self):
        """获取字段映射"""
        return self.dfname_to_col_rename

    def get_external_data(self):
        """获取外部数据"""
        return {
            'service_net': self.df_service_net,
            'vat': self.df_vat,
            'company_belongs': self.company_belongs
        }