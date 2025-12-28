# -*- coding: utf-8 -*-
"""
工具函数模块
"""

import logging
import pandas as pd
import numpy as np
import re
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


class DataUtils:
    """数据处理工具类"""

    @staticmethod
    def init_logger(log_dir):
        """初始化日志系统"""
        import os
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/log_api{datetime.now().strftime('%Y_%m_%d')}.log"

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] [%(message)s]',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_file, encoding='utf-8')
            ]
        )
        return logging.getLogger(__name__)

    @staticmethod
    def to_numeric_safe(df, cols, fill_value=0):
        """安全转换为数值类型"""
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(fill_value)
        return df

    @staticmethod
    def get_valid_columns(df, required_cols):
        """获取DataFrame中实际存在的列，避免KeyError"""
        valid_cols = [col for col in required_cols if col in df.columns]
        missing_cols = [col for col in required_cols if col not in valid_cols]
        if missing_cols:
            logging.warning(f"数据缺失字段：{missing_cols}，将跳过这些字段处理")
        return valid_cols

    @staticmethod
    def convert_numeric_cols(df, cols):
        """批量转换指定列为数值类型，异常值设为0"""
        valid_cols = DataUtils.get_valid_columns(df, cols)
        for col in valid_cols:
            try:
                df[col] = df[col].replace(',', 0, regex=True).fillna(0)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except Exception as e:
                logging.error(f"列[{col}]数值转换失败：{str(e)}")
                df[col] = 0
        return df

    @staticmethod
    def clean_deleted_records(df_dict):
        """清洗删除状态记录"""
        filters = {
            "车辆销售明细_开票日期": "删除状态",
            "装饰订单": ["删除状态", "删除出库状态"],
            "二手车成交": "删除状态",
            "保险业务": "删除状态",
            "开票维护": "删除状态",
            "套餐销售": "删除状态",
            "衍生订单": "删除状态",
            "库存车辆已售": "删除状态"
        }

        for name, col in filters.items():
            if name not in df_dict:
                continue
            df = df_dict[name]
            if isinstance(col, list):
                mask = pd.Series([True] * len(df))
                for c in col:
                    if c in df.columns:
                        mask &= (df[c] == False)
                df_dict[name] = df[mask].copy()
            else:
                if col in df.columns:
                    df_dict[name] = df[df[col] == False].copy()

        return df_dict

    @staticmethod
    def rename_inventory(df):
        """重命名库存字段"""
        # 先删除重复的列名
        if len(df.columns) != len(set(df.columns)):
            logging.warning(f"发现重复列名，正在清理：{df.columns[df.columns.duplicated()].tolist()}")
            # 删除重复列（保留第一个）
            df = df.loc[:, ~df.columns.duplicated()]

        # 创建重命名字典
        rename_dict = {
            '订单号': '采购订单号',
            '所属门店': '归属系统',
            '车系': '车系',
            '车型': '车型',
            '车架号': '车架号',
            '入库日期': '到库日期',
            '生产日期': '生产日期',
            '配车日期': '配车日期',
            '仓库地': '运输方式存放地点',
            '备注': '备注',
            '合格证': '合格证状态',
            '车辆状态': '车辆状态',
            '库存天数': '库存天数',
            '销售日期': '销售日期',
            '销售人员': '销售顾问',
            '订单客户': '客户姓名',
            '锁库日期': '锁库日期',
            '开票日期': '开票日期',
            '质损信息': '质损信息',
            '调拨日期': '调拨日期',
            '调拨记录': '调拨记录',
            '订单来源': '所属团队',
            '订单公司': '匹配定单归属门店',
            '合格证门店': '合格证门店',
            '赎证日期': '赎证日期',
            '出厂价格': '提货价',
            '厂家官价': '指导价'
        }

        # 只重命名存在的列
        existing_rename = {k: v for k, v in rename_dict.items() if k in df.columns}
        df.rename(columns=existing_rename, inplace=True)

        # 定义期望的列顺序
        expected_columns = [
            '车源门店', '供应商', '采购订单号', '归属系统', '匹配定单归属门店', '合格证门店', '所属团队',
            '车系', '车型', '配置', '颜色', '车架号', '发动机号', '指导价', '提货价', '生产日期',
            '赎证日期', '合格证状态', '发车日期', '到库日期', '库存天数', '运输方式存放地点', '车辆状态',
            '调拨日期', '调拨记录', '锁库日期', '销售日期', '开票日期', '配车日期', '销售顾问',
            '客户姓名', '质损信息', '备注', '操作日期'
        ]

        # 检查是否有重复列名
        if len(df.columns) != len(set(df.columns)):
            logging.error(f"重命名后仍有重复列名：{df.columns[df.columns.duplicated()].tolist()}")
            # 删除重复列
            df = df.loc[:, ~df.columns.duplicated()]

        # 只选择存在的列
        existing_columns = [col for col in expected_columns if col in df.columns]
        missing_columns = [col for col in expected_columns if col not in df.columns]

        if missing_columns:
            logging.warning(f"缺少预期的列：{missing_columns}")

        return df[existing_columns]

    @staticmethod
    def join_str(series):
        """拼接字符串"""
        return ','.join(series.dropna().astype(str).unique())

    @staticmethod
    def join_dates(series):
        """拼接日期"""
        s = series.dropna().sort_values()
        return ','.join(s.dt.strftime('%Y/%m/%d').unique())