# -*- coding: utf-8 -*-
"""
整合版车易云商数据处理器
整合逻辑：
1. 使用 data_clean_mysql_cyy.py 的数据库连接和表结构
2. 使用 cyy_api_db.py 的数据处理逻辑
3. 保留 MongoDB 写入功能
"""
from datetime import datetime
import pandas as pd
import logging
import numpy as np
import re
import os
import warnings

import requests
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, DECIMAL, DATETIME, INTEGER
from sqlalchemy.exc import SQLAlchemyError
import pymysql

# 忽略警告
warnings.filterwarnings('ignore')
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_columns', 100)


class CyysDataProcessor:
    """整合版车易云商数据处理器"""

    # -------------------------- 配置常量 --------------------------
    # 1. 日志与文件路径配置
    LOG_DIR = r"E:/pycharm_project/powerbi/data/log"
    SERVICE_NET_PATH = r'C:/Users/刘洋/Documents/WXWork/1688858189749305/WeDrive/成都永乐盛世/维护文件/看板部分数据源/各公司银行额度.xlsx'
    MAPPING_EXCEL_PATH = r"E:/powerbi_data/代码执行/data/字段对应 - csv.xlsx"
    USED_CAR_REBATE_PATH = r"E:/powerbi_data/看板数据/cyy_old_data/二手车返利存档.csv"

    # 2. 数据库配置（使用 data_clean_mysql_cyy.py 的配置）
    SOURCE_MYSQL_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '513921',
        'database': 'cyy_data',
        'charset': 'utf8mb4'
    }
    OUTPUT_MYSQL_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '513921',
        'database': 'cyy_stg_data',
        'charset': 'utf8mb4'
    }

    # 3. 表映射配置
    API_TABLE_MAPPING = {
        '按揭业务': 'mortgage_business',
        '保险业务': 'insurance_business',
        '车辆成本管理': 'car_cost_management',
        '车辆销售明细_开票日期': 'car_sales_invoice_date',
        '调车结算': 'vehicle_transfer_settlement',
        '汇票管理': 'bill_management',
        '计划车辆': 'planned_vehicles',
        '开票维护': 'invoice_maintenance',
        '库存车辆查询': 'inventory_vehicle_query',
        '库存车辆已售': 'inventory_vehicle_sold',
        '二手车成交': 'evaluation_deal',
        '二手车入库': 'evaluation_stored',
        '套餐销售': 'package_sales',
        '未售订单': 'unsold_orders',
        '成交订单': 'sales_deal_orders',
        '作废订单': 'sales_canceled_orders',
        '衍生订单': 'sales_derivative_orders',
        '装饰订单': 'decoration_orders',
        '销售回访': 'car_sales_data',
        '删除日志': 'delete_log',

    }

    # 4. 业务配置常量（使用 cyy_api_db.py 的配置）
    list_companys = [
        '成都新港建元汽车销售服务有限公司',
        '成都永乐盛世汽车销售服务有限公司',
        '成都新港永初汽车服务有限公司',
        '成都新港海川汽车销售服务有限公司',
        '成都新港先秦汽车服务有限公司',
        '成都新港治元汽车销售服务有限公司',
        '成都新港建隆汽车销售服务有限公司',
        '成都上元盛世汽车销售服务有限公司',
        '成都文景初治新能源汽车销售有限公司',
        '成都新港建武汽车销售服务有限公司',
        '成都新港文景海洋汽车销售服务有限公司',
        '成都文景盛世汽车销售服务有限公司',
        '成都新港澜舰汽车销售服务有限公司',
        '成都新港澜阔汽车销售服务有限公司',
        '成都鑫港鲲鹏汽车销售服务有限公司',
        '成都新茂元大汽车销售服务有限公司',
        '成都新港澜轩汽车销售服务有限公司',
        '成都新港浩蓝汽车销售服务有限公司',
        '贵州新港蔚蓝汽车销售服务有限责任公司',
        '贵州新港浩蓝汽车销售服务有限责任公司',
        '贵州新港澜源汽车服务有限责任公司',
        '贵州新港海之辇汽车销售服务有限责任公司',
        '成都新港上元坤灵汽车销售服务有限公司',
        '乐山新港上元曦和汽车销售服务有限公司',
        '宜宾新港上元曦和汽车销售服务有限公司',
        '泸州新港上元坤灵汽车销售服务有限公司',
        '贵州新港上元臻智汽车贸易有限公司',
        '成都新港上元臻智汽车销售服务有限公司',
        '乐山新港上元臻智汽车销售服务有限公司',
        '宜宾新港上元臻智汽车销售服务有限公司',
        '成都新港上元臻享汽车销售服务有限公司',
        '成都新港上元曦和汽车销售服务有限公司',
        '贵州新港澜轩汽车销售有限责任公司',
        '贵州新港上元曦和汽车销售服务有限公司',
        '成都新港上元臻盛汽车销售服务有限公司',
        '成都新港上元弘川汽车销售服务有限公司',
        '绵阳新港鑫泽汽车销售服务有限公司',
        '西藏新港上元曦和汽车销售服务有限公司',
        '贵州仁怀新港上元坤灵汽车销售服务有限公司',
        '成都新港上元星汉汽车销售服务有限公司',
        '直播基地'
    ]

    # -------------------------- 初始化方法 --------------------------
    def __init__(self):
        self._init_logger()
        self._init_field_mapping()
        self._init_excel_data()  # 加载Excel数据
        self.source_engine = self._init_db_connection(self.SOURCE_MYSQL_CONFIG)
        self._create_output_database()
        self.output_engine = self._init_db_connection(self.OUTPUT_MYSQL_CONFIG)

        # 初始化数据存储
        self.raw_data = {}
        self.processed_data = {}

    def _init_logger(self):
        """初始化日志系统"""
        os.makedirs(self.LOG_DIR, exist_ok=True)
        log_file = f"{self.LOG_DIR}/log_api{datetime.now().strftime('%Y_%m_%d')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] [%(message)s]',
            handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding='utf-8')]
        )

    def _init_field_mapping(self):
        """初始化字段映射关系（使用 data_clean_mysql_cyy.py 的逻辑）"""
        try:
            self.mapping_df = pd.read_excel(self.MAPPING_EXCEL_PATH, sheet_name="Sheet1")
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
            self.table_to_english_cols = {}
            for df_name, table_name in self.API_TABLE_MAPPING.items():
                if df_name in dfname_to_english:
                    self.table_to_english_cols[table_name] = dfname_to_english[df_name]
                else:
                    logging.warning(f"数据类型[{df_name}]无字段映射，表[{table_name}]无法读取")

            logging.info(f"字段映射初始化完成：覆盖{len(self.table_to_english_cols)}个源表")
        except Exception as e:
            logging.error(f"字段映射初始化失败：{str(e)}")
            raise

    def _init_excel_data(self):
        """加载Excel数据（使用 cyy_api_db.py 的逻辑）"""
        try:
            # 加载增值税处理数据
            self.df_vat = pd.read_excel(
                self.SERVICE_NET_PATH,
                sheet_name='汉唐_增值税处理'
            )
            logging.info("增值税处理数据加载完成")

            # 加载服务网络数据
            self.df_service_net = pd.read_excel(
                self.SERVICE_NET_PATH,
                sheet_name='补充车系'
            )
            logging.info("服务网络数据加载完成")

            # 加载公司归属数据
            self.company_belongs = pd.read_excel(
                self.SERVICE_NET_PATH,
                sheet_name='补充团队'
            )
            logging.info("公司归属数据加载完成")

        except Exception as e:
            logging.error(f"Excel数据加载失败：{str(e)}")
            raise

    def _create_output_database(self):
        """自动创建输出数据库（使用 data_clean_mysql_cyy.py 的逻辑）"""
        temp_config = self.OUTPUT_MYSQL_CONFIG.copy()
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

    def _init_db_connection(self, db_config):
        """通用数据库连接初始化（使用 data_clean_mysql_cyy.py 的逻辑）"""
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

    # -------------------------- 数据读取工具方法 --------------------------
    def _get_valid_columns(self, df, required_cols):
        """获取DataFrame中实际存在的列"""
        valid_cols = [col for col in required_cols if col in df.columns]
        missing_cols = [col for col in required_cols if col not in valid_cols]
        if missing_cols:
            logging.warning(f"数据缺失字段：{missing_cols}，将跳过这些字段处理")
        return valid_cols

    def _to_numeric_safe(self, df, cols, fill_value=0):
        """批量转换指定列为数值类型（使用 cyy_api_db.py 的逻辑）"""
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(fill_value)
        return df

    def read_from_mysql(self, table_name):
        """从源库读取指定列数据（使用 data_clean_mysql_cyy.py 的逻辑）"""
        if table_name not in self.table_to_english_cols:
            logging.error(f"表[{table_name}]无字段映射，无法读取")
            return pd.DataFrame()

        try:
            english_cols = self.table_to_english_cols[table_name]
            query_cols = ', '.join([f"`{col}`" for col in english_cols])
            query = f"SELECT {query_cols} FROM `{table_name}`"

            df = pd.read_sql(query, self.source_engine)
            initial_cnt = len(df)

            # 去重
            df = df.drop_duplicates()
            if len(df) < initial_cnt:
                logging.info(f"表[{table_name}]去重：移除{initial_cnt - len(df)}条重复记录")

            logging.info(f"表[{table_name}]读取完成：{len(df)}条数据，{len(english_cols)}个字段")
            return df
        except SQLAlchemyError as e:
            logging.error(f"表[{table_name}]读取失败：{str(e)}")
            return pd.DataFrame()

    def load_all_data_from_db(self):
        """加载所有数据并重命名字段（整合两个文件的逻辑）"""
        data_dict = {}

        for df_name, table_name in self.API_TABLE_MAPPING.items():
            # 从MySQL读取数据
            df = self.read_from_mysql(table_name)

            # 应用字段映射重命名（使用 cyy_api_db.py 的逻辑）
            if df_name in self.dfname_to_col_rename and not df.empty:
                rename_map = self.dfname_to_col_rename[df_name]
                valid_cols = [col for col in rename_map.keys() if col in df.columns]
                if valid_cols:
                    df = df[valid_cols].rename(columns=rename_map)
                    logging.info(f"表[{table_name}]重命名了{len(valid_cols)}个字段")

            data_dict[df_name] = df
        return data_dict

    # 第三部分：数据清洗方法
    # -------------------------- 数据清洗方法（使用 cyy_api_db.py 的逻辑） --------------------------
    def _clean_insurance(self, df_insurance):
        """清洗保险业务数据"""
        df_insurance['保费总额'] = pd.to_numeric(df_insurance['保费总额'], errors='coerce').fillna(0)
        df_insurance['总费用_次数'] = df_insurance['保费总额'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        return df_insurance

    def _clean_used_cars(self, df_ershou):
        """清洗二手车成交数据"""
        df_Ers = df_ershou[df_ershou['收款状态'] == '已收款'].copy()
        return df_Ers

    def _clean_decoration_orders(self, df_decoration):
        """清洗装饰订单数据"""
        # 筛选有效数据
        df_decoration = df_decoration[
            (df_decoration['收款日期'] != "") &
            (df_decoration['收款日期'].notnull())
            ].copy()


        # 转换数值类型
        df_decoration = self._to_numeric_safe(df_decoration,
                                              ['销售合计', '成本合计(含税)', '工时费', '出/退/销数量'])

        # 排除特定物资状态
        invalid_status = ['已退款', '已退货', '待退货', '已换货','全退款', '全退货', '部分退款']
        df_decoration = df_decoration[~df_decoration['物资状态'].isin(invalid_status)]

        # === 赠送装饰逻辑 ===
        condition_new = (df_decoration['单据类型'] == '新车销售')
        condition_other = (df_decoration['单据类型'].isin(['客户增购', '换货销售', '独立销售'])) & \
                          (df_decoration['销售合计'] == 0)
        gift_mask = condition_new | condition_other
        df_gift = df_decoration[gift_mask].copy()

        df_gift['装饰赠送成本'] = df_gift[['成本合计(含税)', '工时费']].sum(axis=1)

        result = df_gift.groupby('车架号')['物资名称'].agg(','.join).reset_index()
        df_decoration2 = (
            df_gift.groupby('车架号')[['装饰赠送成本', '销售合计']].sum()
            .reset_index()
            .merge(result, on='车架号', how='left')
            .rename(columns={
                '订单门店': '公司名称',
                '装饰赠送成本': '装饰成本',
                '销售合计': '装饰收入',
                '物资名称': '赠送装饰项目'
            })
        )

        # === 精品销售逻辑 ===
        df_jingpin = df_decoration[~gift_mask].copy()
        df_jingpin = df_jingpin[~df_jingpin['销售顾问'].isin(['郑仁彬', '刘红梅', '衡珊珊', '郝小龙'])].copy()
        df_jingpin['装饰赠送成本'] = df_jingpin[['成本合计(含税)', '工时费']].sum(axis=1)

        # 构造物资明细
        result_JP = df_jingpin.groupby('订单编号').apply(
            lambda x: ','.join(f"{name}*{qty}" for name, qty in zip(x['物资名称'], x['出/退/销数量']))
        ).reset_index(name='物资明细')

        df_jingpin = df_jingpin.merge(result_JP, on='订单编号', how='left')
        df_jingpin.rename(columns={'销售顾问': '精品销售人员'}, inplace=True)

        # 转换日期列
        df_jingpin['收款日期'] = pd.to_datetime(df_jingpin['收款日期'], format='mixed', errors='coerce')
        df_jingpin['开票日期'] = pd.to_datetime(df_jingpin['开票日期'], format='mixed', errors='coerce')

        # 聚合函数
        def join_str(series):
            return ','.join(series.dropna().astype(str).unique())

        def join_dates(series):
            s = series.dropna().sort_values()
            return ','.join(s.dt.strftime('%Y/%m/%d').unique())

        # 分组聚合
        grouped = df_jingpin.groupby(['车架号', '精品销售人员'], as_index=False)

        df_jingpin_result = grouped.agg({
            '单据类型': join_str,
            '订单门店': 'first',
            '开票日期': join_dates,
            '收款日期': join_dates,
            '客户名称': 'first',
            '联系电话': 'first',
            '物资明细': join_str,
            '装饰赠送成本': 'sum',
            '销售合计': 'sum',
            '出/退/销数量': 'sum'
        })

        # 新增最早收款日期
        earliest = grouped['收款日期'].min().reset_index()
        earliest.rename(columns={'收款日期': '最早收款日期'}, inplace=True)
        df_jingpin_result = df_jingpin_result.merge(earliest, on=['车架号', '精品销售人员'], how='left')
        df_jingpin_result['最早收款日期'] = pd.to_datetime(df_jingpin_result['最早收款日期']).dt.strftime('%Y/%m/%d')

        # 重命名与计算
        df_jingpin_result.rename(columns={
            '装饰赠送成本': '总成本',
            '销售合计': '销售总金额',
            '出/退/销数量': '总次数'
        }, inplace=True)
        df_jingpin_result['毛利润'] = df_jingpin_result['销售总金额'] - df_jingpin_result['总成本']

        # 指定输出列顺序
        output_cols = [
            '单据类型', '订单门店', '开票日期', '收款日期', '最早收款日期', '精品销售人员',
            '车架号', '客户名称', '联系电话', '物资明细', '销售总金额', '总成本', '毛利润', '总次数'
        ]

        return df_decoration2, df_jingpin_result[output_cols]

    def _clean_service_packages(self, df_service):
        """清洗套餐销售数据"""
        df_service.rename(columns={'领取车架号/车牌号': '车架号'}, inplace=True)

        df_service = df_service[
            (df_service['套餐名称'] != '保赔无忧') &
            (df_service['审批状态'] != '审批驳回') &
            (df_service['订单状态'].isin(['已登记', '已退卡'])) == False
            ].copy()

        df_service['实售金额'] = pd.to_numeric(df_service['实售金额'], errors='coerce').fillna(0)
        df_service = df_service[~((df_service['套餐名称'].str.contains('终身保养')) & (df_service['实售金额'] > 0))]
        df_service = df_service[~(df_service['实售金额'] > 0)]
        df_service['车架号'] = df_service['车架号'].astype(str)

        # 套餐明细
        details_service = df_service.groupby('车架号').apply(
            lambda x: ','.join(f"{name}*{qty}" for name, qty in zip(x['套餐名称'], x['总次数']))
        ).reset_index(name='套餐明细')

        df_service['结算成本'] = pd.to_numeric(df_service['结算成本'], errors='coerce').fillna(0)
        df_service.rename(columns={'结算成本': '保养升级成本'}, inplace=True)

        df_service_aggregated = (
            df_service.groupby('车架号')['保养升级成本'].sum()
            .reset_index()
            .merge(details_service, on='车架号', how='left')
        )

        return df_service_aggregated

    def _clean_vehicle_costs(self, df_carcost):
        """清洗车辆成本管理数据"""
        cols_to_convert = ['车辆成本_返介绍费', '其他成本_退代金券', '其他成本_退按揭押金']
        df_carcost[cols_to_convert] = df_carcost[cols_to_convert].apply(
            pd.to_numeric, errors='coerce').fillna(0)

        df_carcost.rename(columns={
            '车辆/订单门店': '公司名称',
            '采购成本_调整项': '调整项',
            '车辆成本_二手车返利': '二手车返利金额',
            '车辆成本_返介绍费': '返介绍费',
            '车辆成本_退成交车辆定金（未抵扣）': '退成交车辆定金（未抵扣）',
            '车辆成本_区补': '政府返回区补',
            '车辆成本_保险返利': '保险返利',
            '车辆成本_终端返利': '终端返利',
            '车辆成本_上牌服务费': '上牌成本',
            '车辆成本_票据事务费': '高开票税费',
            '车辆成本_票据事务费-公司': '票据事务费-公司',
            '车辆成本_综合结算服务费': '代开票支付费用',
            '车辆成本_合作返利': '回扣款',
            '车辆成本_其他成本': '其他成本',
            '其他成本_退代金券': '退代金券',
            '其他成本_退按揭押金': '退按揭押金',
            '其他成本_退置换补贴保证金': '退置换补贴保证金',
            '车辆采购成本_质损费': '质损赔付金额',
            '计划单号': '采购订单号'
        }, inplace=True)

        df_carcost['操作日期'] = pd.to_datetime(df_carcost['操作日期'], format='mixed', errors='coerce')
        df_carcost.sort_values(by='操作日期', ascending=False, inplace=True)
        df_carcost.drop_duplicates(subset=['车架号'], keep='first', inplace=True)

        return df_carcost[[
            '公司名称', '采购订单号', '车架号', '车辆状态', '调整项', '返介绍费', '退成交车辆定金（未抵扣）',
            '政府返回区补', '保险返利', '终端返利', '上牌成本', '票据事务费-公司', '代开票支付费用',
            '回扣款', '退代金券', '退按揭押金', '退置换补贴保证金', '质损赔付金额', '其他成本', '操作日期'
        ]]

    def _clean_loans(self, df_loan):
        """清洗按揭业务数据"""
        df_loan.rename(columns={
            '按揭渠道': '金融性质',
            '贷款总额': '贷款金额',
            '期限': '贷款期限',
            '按揭产品': '金融方案',
            '返利系数': '返利系数',
            '实收金融服务费': '金融服务费',
            '厂家贴息': '厂家贴息金额',
            '公司贴息': '经销商贴息金额',
            '返利金额': '金融返利'
        }, inplace=True)

        # 金融类型划分
        df_loan['金融类型'] = np.where(
            df_loan['金融性质'].str.contains('非贴息'), '厂家非贴息贷',
            np.where(df_loan['金融性质'].str.contains('贴息'), '厂家贴息贷',
                     np.where(df_loan['金融方案'].isin(['交行信用卡中心五年两免-9%', '建行5免2', '5免2']),
                              '无息贷', '非贴息贷'))
        )

        # 返利系数转换
        df_loan['返利系数'] = pd.to_numeric(
            df_loan['返利系数'].str.replace('%', ''), errors='coerce').fillna(0) / 100

        # 金额字段转换
        loan_cols = ['开票价', '贷款金额', '返利系数', '金融返利',
                     '厂家贴息金额', '经销商贴息金额', '金融服务费']
        df_loan = self._to_numeric_safe(df_loan, loan_cols)

        # 计算衍生字段
        df_loan['首付金额'] = df_loan['开票价'] - df_loan['贷款金额']
        df_loan['贷款期限'] = df_loan['贷款期限'].astype(str).apply(
            lambda x: re.sub(r'[\u4e00-\u9fa5]', '', x))
        df_loan['金融税费'] = df_loan['厂家贴息金额'] / 1.13 * 0.13 * 1.12 + df_loan['金融返利'] / 1.06 * 0.06 * 1.12
        df_loan['金融毛利'] = df_loan['金融返利'] - df_loan['经销商贴息金额'] - df_loan['金融税费']

        # 去重
        df_loan.sort_values(by=['车架号', '收费状态'], ascending=True, inplace=True)
        df_loan.drop_duplicates(subset=['车架号'], keep='first', inplace=True)

        return df_loan

    def _clean_inventory_and_plan(self, df_inventory, df_inventory1, df_plan, df_debit):
        """清洗库存和计划车辆数据"""

        # 重命名字段
        def _rename_inventory(df):
            df.rename(columns={
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
            }, inplace=True)

            required_cols = [
                '车源门店', '供应商', '采购订单号', '归属系统', '匹配定单归属门店',
                '合格证门店', '所属团队', '车系', '车型', '配置', '颜色', '车架号',
                '发动机号', '指导价', '提货价', '生产日期', '赎证日期', '合格证状态',
                '发车日期', '到库日期', '库存天数', '运输方式存放地点', '车辆状态',
                '调拨日期', '调拨记录', '锁库日期', '销售日期', '开票日期', '配车日期',
                '销售顾问', '客户姓名', '质损信息', '备注', '操作日期'
            ]

            valid_cols = self._get_valid_columns(df, required_cols)
            return df[valid_cols]

        # 重命名库存数据
        df_inventory = _rename_inventory(df_inventory)
        df_inventory1 = _rename_inventory(df_inventory1)

        # 清洗计划车辆数据
        df_plan.rename(columns={'车型': '车系', '整车型号': '车型', '订单号': '采购订单号'}, inplace=True)

        # 合并汇票数据
        if not df_debit.empty:
            merge_cols = ['采购订单号', '提货价', '开票银行', '合格证门店',
                          '赎证日期', '到期日期', '保证金比例', '赎证款']
            valid_merge_cols = self._get_valid_columns(df_debit, merge_cols)
            df_plan = pd.merge(df_plan, df_debit[valid_merge_cols],
                               on='采购订单号', how='left')

        df_plan['车辆状态'] = '未发'
        df_plan['开票银行'] = df_plan['开票银行'].fillna('公司')
        df_plan.rename(columns={'开票银行': '合格证状态', '门店': '归属系统'}, inplace=True)

        # 合并库存和计划数据
        df_inventory_all = pd.concat([df_inventory, df_plan], axis=0, ignore_index=True)

        # 标记调入类型
        list_company = self.company_belongs['公司名称'].tolist() if not self.company_belongs.empty else []
        df_inventory_all['调入类型'] = np.where(
            df_inventory_all['供应商'].isin(list_company),
            '内部调入',
            np.where(
                (~df_inventory_all['供应商'].isin(list_company)) &
                (df_inventory_all['供应商'] != '比亚迪') &
                (df_inventory_all['供应商'] != ""),
                '外部调入',
                None
            )
        )

        # 合并服务网络数据
        if not self.df_service_net.empty:
            df_inventory_all = pd.merge(
                df_inventory_all,
                self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

            # 处理直播基地归属
            if '归属系统' in df_inventory_all.columns and '服务网络' in df_inventory_all.columns:
                df_inventory_all['归属系统'] = np.where(
                    df_inventory_all['归属系统'] == '直播基地',
                    df_inventory_all['服务网络'] + '-' + df_inventory_all['归属系统'],
                    df_inventory_all['归属系统']
                )

        return df_inventory_all, df_inventory, df_inventory1

    def _clean_debit_and_merge(self, df_debit, df_carcost):
        """清洗汇票管理数据"""
        df_debit.rename(columns={
            '车辆金额': '提货价',
            '开票金额(含税)': '汇票金额',
            '汇票开票日期': '开票日期',
            'VIN码': '车架号',
            '计划单号': '采购订单号',
            '开票银行': '开票银行',
            '所属门店': '合格证门店',
            '汇票到期日期': '到期日期',
            '首付比例': '保证金比例',
            '赎证金额': '赎证款'
        }, inplace=True)

        df_debit['是否赎证'] = np.where(df_debit['是否结清'] == '已清', 1, 0)

        debit_cols = [
            '合格证门店', '车源门店', '开票日期', '保证金比例', '首付金额', '汇票金额',
            '到期日期', '开票银行', '汇票号', '合格证号', '采购订单号', '车架号',
            '提货价', '审核状态', '赎证日期', '赎证款', '首付单号', '赎证单号',
            '是否赎证', '是否结清'
        ]

        df_debit = df_debit[self._get_valid_columns(df_debit, debit_cols)]

        # 合并车辆状态
        if not df_carcost.empty and '采购订单号' in df_carcost.columns:
            df_debit = pd.merge(
                df_debit,
                df_carcost[['采购订单号', '车辆状态']],
                on='采购订单号', how='left'
            )
            df_debit['车辆状态'] = df_debit['车辆状态'].fillna('未发')

        return df_debit

    def _clean_book_orders(self, df_books, df_books2, df_unsold):
        """清洗订单数据"""
        df_books.rename(columns={
            '计划单/车架号': '车架号',
            '订金日期': '定单日期',
            '开票日期': '销售日期',
            '订单订金': '定金金额',
            '车辆车系': '车系',
            '车辆车型': '车型',
            '车辆配置': '配置',
            '订单门店': '定单归属门店',
            '业务来源': '所属团队',
            '客户名称': '客户姓名',
            '客户电话': '联系电话',
            '客户电话2': '联系电话2'
        }, inplace=True)

        # 过滤作废状态
        if '作废状态' in df_books.columns:
            df_books = df_books[df_books['作废状态'] == False]

        # 处理成交订单
        print(df_books2)
        df_books2['订单日期'] = pd.to_datetime(df_books2['订单日期'], format='mixed', errors='coerce')
        df_books2.sort_values(by='订单日期', ascending=False, inplace=True)
        df_books2.rename(columns={'联系方式': '联系电话', '联系方式2': '联系电话2'}, inplace=True)
        df_books2 = df_books2.drop_duplicates(subset=['车架号'], keep='first')
        df_sold = df_books2[
            ['ID', '联系电话', '联系电话2', '主播人员', '车系', '客户姓名', '订单公司']].drop_duplicates()

        # 处理未售订单
        df_unsold.rename(columns={'客户电话': '联系电话', '客户电话2': '联系电话2', '客户': '客户姓名'}, inplace=True)
        df_unsold1 = df_unsold[['ID', '联系电话', '联系电话2', '主播人员', '车系', '客户姓名', '订单公司']]

        # 合并主播列表
        df_zhubolist = pd.concat([df_sold, df_unsold1], ignore_index=True).drop_duplicates()
        cols = ['联系电话', '联系电话2']
        df_zhubolist[cols] = (
            df_zhubolist[cols]
            .replace('', pd.NA)
            .fillna(0)
            .astype('int64')
            .astype('str')
            .replace('0', '')
        )

        df_zhubolist['辅助列'] = (
                df_zhubolist['联系电话'] + df_zhubolist['联系电话2'] +
                df_zhubolist['客户姓名'] + df_zhubolist['车系'] + df_zhubolist['订单公司']
        )
        df_zhubolist = df_zhubolist.drop_duplicates(subset=['辅助列'], keep='first')

        # 筛选订单字段
        order_cols = [
            'ID', '车架号', '订单日期', '定单日期', '订金状态', '审批状态', '销售人员',
            '销售日期', '定金金额', '定单归属门店', '所属团队', '车系', '外饰颜色',
            '车型', '配置', '客户姓名', '联系电话', '联系电话2'
        ]
        valid_order_cols = self._get_valid_columns(df_books, order_cols)
        df_dings = df_books[valid_order_cols].copy()

        # 合并服务网络数据
        if not self.df_service_net.empty and '车系' in df_dings.columns:
            df_dings = pd.merge(
                df_dings,
                self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

            # 处理直播基地归属
            if '定单归属门店' in df_dings.columns and '服务网络' in df_dings.columns:
                df_dings['定单归属门店'] = np.where(
                    df_dings['定单归属门店'] == '直播基地',
                    df_dings['服务网络'] + '-' + df_dings['定单归属门店'],
                    df_dings['定单归属门店']
                )

        # 匹配主播人员
        df_dings['辅助列'] = (
                df_dings['联系电话'] + df_dings['联系电话2'] +
                df_dings['客户姓名'] + df_dings['车系'] + df_dings['定单归属门店']
        )
        df_dings = pd.merge(df_dings, df_zhubolist[['辅助列', '主播人员']],
                            how='left', on='辅助列')

        # 标记现定现交
        df_dings['现定现交'] = np.where(
            (df_dings['定单日期'] == "") & (df_dings['销售日期'] != ""),
            '现定现交',
            np.where((df_dings['订金状态'] == "待收款") &
                     (df_dings['定单日期'] != "") &
                     (df_dings['销售日期'] != ""),
                     '现定现交', None)
        )

        df_dings['定单状态'] = np.where((df_dings['销售日期'] != ""), df_dings['销售日期'], None)
        df_dings['定金金额'] = np.where(df_dings['现定现交'] == '现定现交', 3000, df_dings['定金金额'])
        df_dings = df_dings.drop_duplicates()

        # 提取主播信息
        df_zhubo = df_dings[['车架号', '主播人员']]

        return df_dings, df_zhubo

    def _clean_void_orders(self, tui_dings_df):
        """清洗作废订单数据"""
        # 排除特定退订类型
        if '退订类型' in tui_dings_df.columns:
            tui_dings_df = tui_dings_df[~tui_dings_df['退订类型'].isin(['重复录入', '错误录入'])]

        # 合并服务网络数据
        if not self.df_service_net.empty and '车系' in tui_dings_df.columns:
            tui_dings_df = pd.merge(
                tui_dings_df,
                self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

            # 处理直播基地归属
            if '订单门店' in tui_dings_df.columns and '服务网络' in tui_dings_df.columns:
                tui_dings_df['订单门店'] = np.where(
                    tui_dings_df['订单门店'] == '直播基地',
                    tui_dings_df['服务网络'] + '-' + tui_dings_df['订单门店'],
                    tui_dings_df['订单门店']
                )

        # 日期转换
        tui_dings_df['退定日期'] = pd.to_datetime(tui_dings_df['作废时间'], format='mixed', errors='coerce')
        tui_dings_df['定单日期'] = pd.to_datetime(tui_dings_df['订单日期'], format='mixed', errors='coerce')

        # 标记非退定核算
        if all(col in tui_dings_df.columns for col in ['定单日期', '退定日期']):
            tui_dings_df['非退定核算'] = np.where(
                (tui_dings_df['定单日期'].dt.year == tui_dings_df['退定日期'].dt.year) &
                (tui_dings_df['定单日期'].dt.month == tui_dings_df['退定日期'].dt.month),
                0, 1
            )

        # 筛选必要字段
        cancel_cols = [
            '订单门店', '业务渠道', '销售人员', '主播人员', '订单日期', '车系',
            '外饰颜色', '车型', '配置', '客户名称', '客户电话', '退订类型',
            '退订原因', '退定日期', '非退定核算'
        ]
        valid_cancel_cols = self._get_valid_columns(tui_dings_df, cancel_cols)

        return tui_dings_df[valid_cancel_cols]

    def _clean_sales_detail(self, df_salesAgg):
        """清洗销售明细数据"""
        df_salesAgg.rename(columns={
            '订单门店': '公司名称',
            '订单日期': '订车日期',
            '开票日期': '销售日期',
            '购车方式': '购买方式',
            '业务渠道': '所属团队',
            '销售人员': '销售人员',
            '分销/邀约人员': '邀约人员',
            '交付专员': '交付专员',
            '客户名称': '车主姓名',
            '车辆信息_车辆车系': '车系',
            '车辆信息_车辆车型': '车型',
            '车辆信息_车辆颜色': '外饰颜色',
            '车辆信息_车辆配置': '车辆配置',
            '车辆信息_车架号': '车架号',
            '订金信息_订金金额': '定金金额',
            '整车销售_厂家官价': '指导价',
            '整车销售_裸车成交价': '裸车成交价',
            '整车销售_开票价格': '车款（发票价）',
            '整车销售_票据事务金额': '票据事务金额',
            '整车销售_最终结算价': '提货价',
            '整车销售_调拨费': '调拨费',
            '其它业务_上牌费': '上牌费',
            '其它业务_置换补贴保证金': '置换款',
            '其它业务_精品款': '精品款',
            '其它业务_金融押金': '金融押金',
            '其它业务_保险押金': '保险押金',
            '其它业务_代金券': '代金券',
            '其它业务_其它押金': '其它押金',
            '其它业务_其它费用': '其它费用',
            '其它业务_特殊事项': '特殊事项',
            '其它业务_综合服务费': '金融服务费_顾问',
            '其它业务_票据事务费': '票据事务费',
            '其它业务_置换服务费': '置换服务费',
            '装饰业务_出库成本': '装饰成本',
            '其它业务_拖车费用': '拖车费用'
        }, inplace=True)

        # 过滤无效数据
        df_salesAgg = df_salesAgg[(df_salesAgg['车架号'] != "") & (df_salesAgg['销售日期'] != "")]

        # 合并服务网络数据
        if not self.df_service_net.empty and '车系' in df_salesAgg.columns:
            df_salesAgg = pd.merge(
                df_salesAgg,
                self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

            # 处理直播基地归属
            if '公司名称' in df_salesAgg.columns and '服务网络' in df_salesAgg.columns:
                df_salesAgg['公司名称'] = np.where(
                    df_salesAgg['公司名称'] == '直播基地',
                    df_salesAgg['服务网络'] + '-' + df_salesAgg['公司名称'],
                    df_salesAgg['公司名称']
                )

        # 过滤销售日期
        df_salesAgg['销售日期'] = pd.to_datetime(df_salesAgg['销售日期'], format='mixed', errors='coerce')
        df_salesAgg = df_salesAgg[df_salesAgg['销售日期'] > pd.to_datetime('2025-03-31')]

        # 筛选字段
        sales_cols = [
            '服务网络', '公司名称', '订车日期', '入库日期', '销售日期', '车架号',
            '车系', '车型', '车辆配置', '外饰颜色', '所属团队', '销售人员',
            '邀约人员', '交付专员', '车主姓名', '联系电话', '联系电话2',
            '身份证号', '定金金额', '指导价', '裸车成交价', '车款（发票价）',
            '提货价', '调拨费', '置换款', '精品款', '上牌费', '购买方式',
            '置换服务费', '金融服务费_顾问', '票据事务金额', '票据事务费',
            '代金券', '金融押金', '保险押金', '其它押金', '其它费用',
            '特殊事项', '拖车费用'
        ]

        valid_sales_cols = self._get_valid_columns(df_salesAgg, sales_cols)
        return df_salesAgg[valid_sales_cols]

    # 主表合并和毛利计算
    def _merge_main_sales_table(self, df_salesAgg, df_zhubo, df_service_aggregated,
                                df_carcost, df_loan, df_decoration2, df_kaipiao,
                                df_Ers2, df_Ers2_archive):
        """合并主销售表"""
        # 合并所有相关数据
        df_salesAgg1 = (
            df_salesAgg.merge(df_zhubo[['车架号', '主播人员']], on='车架号', how='left')
            .merge(df_service_aggregated[['车架号', '保养升级成本', '套餐明细']], on='车架号', how='left')
            .merge(df_carcost[['车架号', '调整项', '返介绍费', '退成交车辆定金（未抵扣）',
                               '政府返回区补', '保险返利', '终端返利', '上牌成本',
                               '票据事务费-公司', '代开票支付费用', '回扣款', '退代金券',
                               '退按揭押金', '退置换补贴保证金', '质损赔付金额', '其他成本']],
                   on='车架号', how='left')
            .merge(df_loan[['车架号', '金融类型', '金融性质', '首付金额', '贷款金额',
                            '贷款期限', '金融方案', '返利系数', '金融服务费',
                            '厂家贴息金额', '经销商贴息金额', '金融返利', '金融税费', '金融毛利']],
                   on='车架号', how='left')
            .merge(df_decoration2[['车架号', '装饰成本', '装饰收入', '赠送装饰项目']], on='车架号', how='left')
            .merge(df_kaipiao, on='车架号', how='left')
            .merge(df_Ers2[['车架号', '二手车返利金额1', '收款日期']], on='车架号', how='left')
            .merge(df_Ers2_archive[['车架号', '二手车返利金额']], on='车架号', how='left')
        )

        # 全款车辆金融字段清空
        financial_columns_to_clear = [
            '金融类型', '金融性质', '首付金额', '贷款金额', '贷款期限', '金融方案',
            '返利系数', '金融服务费', '厂家贴息金额', '经销商贴息金额',
            '金融返利', '金融税费', '金融毛利'
        ]

        if '购买方式' in df_salesAgg1.columns:
            df_salesAgg1.loc[df_salesAgg1['购买方式'] == '全款', financial_columns_to_clear] = None

        # 转换数值类型
        financial_columns = [
            '定金金额', '指导价', '裸车成交价', '车款（发票价）', '提货价', '调拨费',
            '置换款', '精品款', '代金券', '其它押金', '其它费用', '特殊事项',
            '金融押金', '保险押金', '置换服务费', '金融服务费_顾问', '票据事务金额',
            '票据事务费', '调整项', '金融返利', '金融服务费', '金融毛利', '上牌费',
            '保险返利', '终端返利', '返利合计', '二手车返利金额', '代开票支付费用',
            '回扣款', '票据事务费-公司', '返介绍费', '质损赔付金额', '其他成本',
            '政府返回区补', '装饰收入', '保养升级成本', '装饰成本', '拖车费用', '上牌成本'
        ]

        for col in financial_columns:
            if col in df_salesAgg1.columns:
                df_salesAgg1[col] = pd.to_numeric(df_salesAgg1[col], errors='coerce').fillna(0)

        return df_salesAgg1

    def _apply_promotion_logic(self, df_salesAgg1):
        """应用促销逻辑"""
        # 计算返利合计
        df_salesAgg1['返利合计'] = df_salesAgg1['终端返利'] + df_salesAgg1['保险返利']

        # 计算增值税利润差
        if '票据事务费' in df_salesAgg1.columns:
            df_salesAgg1['增值税利润差'] = np.where(
                df_salesAgg1['票据事务费'] > 0,
                df_salesAgg1[['车款（发票价）', '置换款', '返利合计']].sum(axis=1) -
                df_salesAgg1[['提货价', '票据事务金额']].sum(axis=1),
                df_salesAgg1[['车款（发票价）', '置换款', '返利合计']].sum(axis=1) -
                df_salesAgg1['提货价']
            )
        else:
            df_salesAgg1['增值税利润差'] = 0

        # 处理增值税逻辑
        df_salesAgg1['车系辅助'] = df_salesAgg1['车系'] + df_salesAgg1['车型']

        # 合并增值税数据
        self.df_vat['起始日期'] = pd.to_datetime(self.df_vat['起始日期'], format='mixed', errors='coerce')
        df_salesAgg1 = pd.merge(
            df_salesAgg1,
            self.df_vat[['辅助列', '最终结算价（已抵扣超级置换）', '抵扣金额', '起始日期']],
            left_on='车系辅助', right_on='辅助列', how='left'
        )

        df_salesAgg1['抵扣金额'] = df_salesAgg1['抵扣金额'].fillna(0)
        df_salesAgg1['最终结算价（已抵扣超级置换）'] = pd.to_numeric(
            df_salesAgg1['最终结算价（已抵扣超级置换）']).fillna(0)
        df_salesAgg1['起始日期'] = df_salesAgg1['起始日期'].fillna(pd.Timestamp('1900-01-01'))

        # 计算税费
        condition = (
                (df_salesAgg1['销售日期'] >= df_salesAgg1['起始日期']) &
                (df_salesAgg1['辅助列'] == df_salesAgg1['车系辅助']) &
                (df_salesAgg1['提货价'] <= df_salesAgg1['最终结算价（已抵扣超级置换）']) &
                (df_salesAgg1['置换款'] > 0)
        )

        df_salesAgg1['税费'] = np.where(
            condition,
            np.where(df_salesAgg1['增值税利润差'] - df_salesAgg1['抵扣金额'] > 0,
                     np.round((df_salesAgg1['增值税利润差'] - df_salesAgg1['抵扣金额']) / 1.13 * 0.13 * 1.12, 2), 0),
            np.where(df_salesAgg1['增值税利润差'] > 0,
                     np.round(df_salesAgg1['增值税利润差'] / 1.13 * 0.13 * 1.12, 2), 0)
        )

        # 计算后返客户款项
        df_salesAgg1['后返客户款项'] = df_salesAgg1[['代金券', '金融押金', '保险押金', '其它押金']].sum(axis=1)

        # 计算促销费用（贵州地区加200）
        if '公司名称' in df_salesAgg1.columns and '所属团队' in df_salesAgg1.columns:
            guizhou_mask = df_salesAgg1['公司名称'].str.contains('贵州', na=False)
            df_salesAgg1['促销费用'] = np.where(
                guizhou_mask & (df_salesAgg1['所属团队'] != "调拨"),
                df_salesAgg1['后返客户款项'] + 200,
                df_salesAgg1['后返客户款项']
            )
        else:
            df_salesAgg1['促销费用'] = df_salesAgg1['后返客户款项']

        # 处理二手车返利金额
        if '二手车返利金额' in df_salesAgg1.columns and '二手车返利金额1' in df_salesAgg1.columns:
            df_salesAgg1['二手车返利金额'] = np.where(
                (df_salesAgg1['二手车返利金额'] == "") | (df_salesAgg1['二手车返利金额'] == 0),
                df_salesAgg1['二手车返利金额1'],
                df_salesAgg1['二手车返利金额']
            )

        # 处理金融服务费
        if all(col in df_salesAgg1.columns for col in ['金融服务费', '金融服务费_顾问', '购买方式']):
            df_salesAgg1['金融服务费'] = np.where(
                (df_salesAgg1['金融服务费'].round(10) == 0) & (df_salesAgg1['购买方式'] != "全款"),
                df_salesAgg1['金融服务费_顾问'],
                df_salesAgg1['金融服务费']
            )

        # 更新金融毛利
        if all(col in df_salesAgg1.columns for col in ['金融毛利', '金融服务费']):
            df_salesAgg1['金融毛利'] = df_salesAgg1[['金融毛利', '金融服务费']].sum(axis=1)

        # 更新金融类型
        if '购买方式' in df_salesAgg1.columns:
            df_salesAgg1['金融类型'] = np.where(
                df_salesAgg1['购买方式'] == '全款',
                '全款',
                df_salesAgg1['金融类型']
            )

        # 处理上牌费
        if all(col in df_salesAgg1.columns for col in ['金融服务费_顾问', '购买方式', '上牌费']):
            df_salesAgg1['上牌费'] = np.where(
                (df_salesAgg1['金融服务费_顾问'] > 0) & (df_salesAgg1['购买方式'] == "全款"),
                df_salesAgg1['金融服务费_顾问'] + df_salesAgg1['上牌费'],
                df_salesAgg1['上牌费']
            )

        # 计算上牌毛利
        if all(col in df_salesAgg1.columns for col in ['上牌费', '上牌成本']):
            df_salesAgg1['上牌毛利'] = df_salesAgg1[['上牌费', '上牌成本']].sum(axis=1)

        # 更新精品款
        if '票据事务金额' in df_salesAgg1.columns:
            df_salesAgg1['精品款'] = df_salesAgg1['票据事务金额']

        # 计算装饰赠送合计
        if all(col in df_salesAgg1.columns for col in ['装饰成本', '保养升级成本']):
            df_salesAgg1['装饰赠送合计'] = df_salesAgg1[['装饰成本', '保养升级成本']].sum(axis=1)
        else:
            df_salesAgg1['装饰赠送合计'] = 0

        # 计算销售车价
        if all(col in df_salesAgg1.columns for col in ['车款（发票价）', '置换款', '后返客户款项', '精品款']):
            df_salesAgg1['销售车价'] = df_salesAgg1[['车款（发票价）', '置换款', '后返客户款项']].sum(axis=1) - \
                                       df_salesAgg1['精品款']
        else:
            df_salesAgg1['销售车价'] = 0

        # 计算固定支出
        if all(col in df_salesAgg1.columns for col in ['促销费用', '装饰赠送合计']):
            df_salesAgg1['固定支出'] = df_salesAgg1[['促销费用', '装饰赠送合计']].sum(axis=1)
        else:
            df_salesAgg1['固定支出'] = 0

        # 计算毛利
        if all(col in df_salesAgg1.columns for col in ['销售车价', '返利合计', '税费', '提货价']):
            df_salesAgg1['毛利'] = df_salesAgg1[['销售车价', '返利合计']].sum(axis=1) - df_salesAgg1[
                ['税费', '提货价']].sum(axis=1)
        else:
            df_salesAgg1['毛利'] = 0

        return df_salesAgg1

    def _handle_diaobo_merge(self, df_diao, df_salesAgg1):
        """处理调拨数据合并"""
        # 删除重复的调拨费列
        df_diao = df_diao.drop(columns=['调拨费'], errors='ignore')

        # 排序并去重
        df_diao['结算日期'] = pd.to_datetime(df_diao['结算日期'], errors='coerce')
        df_diao = df_diao.sort_values(by=['结算日期'], ascending=False)
        df_diao = df_diao.drop_duplicates(subset=['车架号'], keep='first')

        # 合并销售数据
        if not df_salesAgg1.empty and all(
                col in df_salesAgg1.columns for col in ['车架号', '销售日期', '车系', '车型', '车辆配置', '调拨费']):
            df_diao1 = pd.merge(
                df_diao,
                df_salesAgg1[['车架号', '销售日期', '车系', '车型', '车辆配置', '调拨费']],
                on='车架号', how='left'
            )
        else:
            df_diao1 = df_diao.copy()

        # 补充默认值和重命名
        df_diao1[['所属团队', '金融类型']] = '其他'
        df_diao1['金融类型'] = '调出车'
        df_diao1['调出车'] = '是'
        df_diao1.rename(columns={
            '车系': '车系1',
            '调出门店': '公司名称',
            '支付门店': '车主姓名'
        }, inplace=True)

        df_diao1['车系'] = '调拨车'

        # 处理车辆信息
        if '车辆信息' in df_diao1.columns:
            df_diao1['车辆信息'] = df_diao1['车辆信息'].apply(
                lambda x: x[x.find(" ") + 1:] if x.find(" ") != -1 else x
            )

        df_diao1['单车毛利'] = df_diao1['调拨费']

        # 筛选必要字段
        diao_cols = [
            '公司名称', '销售日期', '车架号', '车系', '车系1', '车型',
            '车辆信息', '车辆配置', '所属团队', '金融类型', '车主姓名',
            '调拨费', '调出车', '单车毛利'
        ]
        valid_diao_cols = self._get_valid_columns(df_diao1, diao_cols)

        return df_diao1[valid_diao_cols]

    def _finalize_and_export(self, df_salesAgg1, df_dings, df_inventory_all,
                             tui_dings_df, df_debit, df_salesAgg_,
                             df_jingpin_result, df_inventory1, df_Ers1,
                             df_diao2, df_inventory0_1):
        """最终数据处理和导出"""
        # 计算单车毛利
        profit_cols_positive = [
            '毛利', '金融毛利', '上牌毛利', '二手车返利金额', '代开票支付费用',
            '置换服务费', '回扣款', '票据事务费-公司', '返介绍费', '质损赔付金额',
            '其他成本', '政府返回区补', '装饰收入', '调整项', '其它费用',
            '特殊事项', '拖车费用'
        ]
        profit_cols_negative = ['促销费用', '装饰赠送合计']

        # 转换数值类型
        for col in profit_cols_positive + profit_cols_negative:
            if col in df_salesAgg1.columns:
                df_salesAgg1[col] = pd.to_numeric(df_salesAgg1[col], errors='coerce').fillna(0)

        # 计算单车毛利
        valid_profit_cols = self._get_valid_columns(df_salesAgg1, profit_cols_positive)
        valid_deduct_cols = self._get_valid_columns(df_salesAgg1, profit_cols_negative + ['调拨费'])

        if valid_profit_cols and valid_deduct_cols:
            df_salesAgg1['单车毛利'] = (
                    df_salesAgg1[valid_profit_cols].sum(axis=1) -
                    df_salesAgg1[valid_deduct_cols].sum(axis=1)
            )

        # 标记调出类型
        if all(col in df_salesAgg1.columns for col in ['车主姓名', '所属团队']):
            df_salesAgg1['调出类型'] = np.where(
                ((df_salesAgg1['车主姓名'].isin(self.list_companys)) |
                 (df_salesAgg1['车主姓名'].str.len() <= 5)) &
                (df_salesAgg1['所属团队'] == '调拨'),
                '内部调出',
                np.where(
                    (~df_salesAgg1['车主姓名'].isin(self.list_companys)) &
                    (df_salesAgg1['车主姓名'].str.len() > 5) &
                    (df_salesAgg1['所属团队'] == '调拨'),
                    '外部调出',
                    None
                )
            )

        # 筛选最终销售毛利表字段
        sales_final_cols = [
            '服务网络', '公司名称', '订车日期', '入库日期', '收款日期', '销售日期',
            '车架号', '车系', '车辆配置', '车型', '外饰颜色', '所属团队',
            '调出类型', '销售人员', '邀约人员', '交付专员', '主播人员', '车主姓名',
            '身份证号', '联系电话', '联系电话2', '定金金额', '指导价', '裸车成交价',
            '销售车价', '车款（发票价）', '提货价', '置换款', '精品款', '后返客户款项',
            '保险返利', '终端返利', '返利合计', '增值税利润差', '税费', '毛利',
            '购买方式', '金融类型', '金融性质', '金融方案', '首付金额', '贷款金额',
            '贷款期限', '返利系数', '金融返利', '厂家贴息金额', '经销商贴息金额',
            '金融税费', '金融服务费', '金融毛利', '上牌费', '上牌成本', '上牌毛利',
            '二手车返利金额', '置换服务费', '促销费用', '赠送装饰项目', '装饰收入',
            '装饰成本', '套餐明细', '保养升级成本', '装饰赠送合计', '其他成本',
            '返介绍费', '回扣款', '代开票支付费用', '调拨费', '票据事务费',
            '票据事务费-公司', '其它费用', '特殊事项', '政府返回区补', '质损赔付金额',
            '调整项', '单车毛利', '开票门店', '退代金券', '退成交车辆定金（未抵扣）',
            '退按揭押金', '退置换补贴保证金', '拖车费用'
        ]

        valid_sales_final_cols = self._get_valid_columns(df_salesAgg1, sales_final_cols)
        df_salesAgg2 = df_salesAgg1[valid_sales_final_cols].copy()

        # 合并库存信息
        if not df_inventory0_1.empty:
            merge_inventory_cols = ['车架号', '车源门店', '供应商', '发动机号']
            valid_inventory_cols = self._get_valid_columns(df_inventory0_1, merge_inventory_cols)
            if valid_inventory_cols:
                df_salesAgg2 = pd.merge(
                    df_salesAgg2,
                    df_inventory0_1[valid_inventory_cols],
                    on='车架号', how='left'
                )

        df_salesAgg2 = df_salesAgg2.drop_duplicates()

        # 过滤调拨数据
        if not df_diao2.empty:
            df_diao2 = df_diao2[(df_diao2['调拨费'] != 0) & (df_diao2['调拨费'].notnull())]

        # 处理二手车数据日期
        if not df_Ers1.empty and '收款日期' in df_Ers1.columns:
            df_Ers1['收款日期'] = pd.to_datetime(df_Ers1['收款日期'], format='mixed', errors='coerce')

        # 合并所有数据（销售毛利、二手车、调拨）
        dfs_to_combine = []
        if not df_salesAgg2.empty:
            dfs_to_combine.append(df_salesAgg2)
        if not df_Ers1.empty:
            dfs_to_combine.append(df_Ers1)
        if not df_diao2.empty:
            dfs_to_combine.append(df_diao2)

        if dfs_to_combine:
            df_salesAgg_combined = pd.concat(dfs_to_combine, axis=0, ignore_index=True)
        else:
            df_salesAgg_combined = pd.DataFrame()

        # 补充收款日期
        if all(col in df_salesAgg_combined.columns for col in ['二手车返利金额', '收款日期', '销售日期']):
            df_salesAgg_combined['收款日期'] = np.where(
                df_salesAgg_combined['二手车返利金额'] > 0,
                df_salesAgg_combined['收款日期'].fillna(df_salesAgg_combined['销售日期']),
                df_salesAgg_combined['收款日期']
            )
            df_salesAgg_combined['销售日期'] = df_salesAgg_combined['销售日期'].fillna(
                df_salesAgg_combined['收款日期']
            )

        # 合并精品销售数据的车系
        if not df_jingpin_result.empty and not df_salesAgg2.empty:
            df_jingpin_result = pd.merge(
                df_jingpin_result,
                df_salesAgg2[['车架号', '车系']],
                on='车架号', how='left'
            )

        # 准备写入MySQL的数据
        self.df_sales_final = df_salesAgg_combined.drop_duplicates() if not df_salesAgg_combined.empty else pd.DataFrame()
        self.df_order_final = df_dings.drop_duplicates() if not df_dings.empty else pd.DataFrame()

        # 库存数据（未开票）
        if not df_inventory_all.empty and '开票日期' in df_inventory_all.columns:
            self.df_inventory_final = df_inventory_all[
                (df_inventory_all['开票日期'].isna()) | (df_inventory_all['开票日期'] == "")
                ].copy()
        else:
            self.df_inventory_final = pd.DataFrame()

        self.df_cancel_final = tui_dings_df.drop_duplicates() if not tui_dings_df.empty else pd.DataFrame()
        self.df_debit_final = df_debit.drop_duplicates() if not df_debit.empty else pd.DataFrame()
        self.df_sales_detail_final = df_salesAgg_.drop_duplicates() if not df_salesAgg_.empty else pd.DataFrame()
        self.df_jingpin_final = df_jingpin_result.drop_duplicates() if not df_jingpin_result.empty else pd.DataFrame()
        self.df_sold_inventory_final = df_inventory1.drop_duplicates() if not df_inventory1.empty else pd.DataFrame()

        logging.info("最终数据处理完成，准备写入数据库")

        # 准备MongoDB数据
        self._prepare_mongodb_data(df_salesAgg_combined, df_jingpin_result)

    def _prepare_mongodb_data(self, df_salesAgg_combined, df_jingpin_result):
        """准备MongoDB数据"""
        if df_salesAgg_combined.empty:
            logging.warning("销售数据为空，跳过MongoDB数据准备")
            return

        # 复制数据
        df_salesAgg4 = df_salesAgg_combined.copy()

        # 重命名字段
        df_salesAgg4.rename(columns={
            '公司名称': '订单门店',
            '订车日期': '订车日期',
            '销售日期': '开票日期',
            '车架号': '车架号',
            '车系': '车辆车系',
            '车辆配置': '车辆车型',
            '外饰颜色': '车辆颜色',
            '所属团队': '业务渠道',
            '销售人员': '销售人员',
            '车主姓名': '客户名称',
            '定金金额': '订金金额',
            '指导价': '厂家官价',
            '裸车成交价': '裸车成交价',
            '销售车价': '销售车价',
            '车款（发票价）': '开票价格',
            '提货价': '最终结算价',
            '置换款': '置换补贴保证金',
            '精品款': '票据事务金额',
            '保险返利': '保险返利',
            '终端返利': '终端返利',
            '返利合计': '厂家返利合计',
            '后返客户款项': '后返客户款项',
            '增值税利润差': '增值税利润差',
            '税费': '税费',
            '毛利': '毛利',
            '返介绍费': '返介绍费',
            '政府返回区补': '区补',
            '退代金券': '退代金券',
            '退成交车辆定金（未抵扣）': '退成交车辆定金（未抵扣）',
            '退按揭押金': '退按揭押金',
            '退置换补贴保证金': '退置换补贴保证金',
            '质损赔付金额': '质损赔付金额',
            '购买方式': '购买方式',
            '金融类型': '金融类型',
            '金融性质': '按揭渠道',
            '首付金额': '首付金额',
            '贷款金额': '贷款总额',
            '贷款期限': '期限',
            '金融方案': '按揭产品',
            '返利系数': '返利系数',
            '金融服务费': '实收金融服务费',
            '厂家贴息金额': '厂家贴息',
            '经销商贴息金额': '公司贴息',
            '金融返利': '返利金额',
            '金融税费': '金融税费',
            '金融毛利': '金融毛利',
            '上牌费': '上牌费',
            '上牌成本': '上牌服务费',
            '上牌毛利': '上牌毛利',
            '二手车返利金额': '二手车返利',
            '置换服务费': '置换服务费',
            '赠送装饰项目': '赠送装饰项目',
            '促销费用': '促销费用',
            '保养升级成本': '保养升级成本',
            '装饰成本': '装饰成本',
            '装饰赠送合计': '装饰赠送合计',
            '回扣款': '合作返利',
            '代开票支付费用': '综合结算服务费',
            '调拨费': '调拨费',
            '票据事务费-公司': '票据事务费-公司',
            '单车毛利': '单车毛利'
        }, inplace=True)

        # 筛选MongoDB字段
        mongodb_cols = [
            '服务网络', '车源门店', '供应商', '订单门店', '订车日期', '开票日期',
            '收款日期', '车架号', '发动机号', '车辆车系', '车辆车型', '车辆颜色',
            '业务渠道', '销售人员', '邀约人员', '交付专员', '主播人员', '客户名称',
            '身份证号', '联系电话', '联系电话2', '订金金额', '厂家官价', '裸车成交价',
            '销售车价', '开票价格', '最终结算价', '置换补贴保证金', '票据事务金额',
            '后返客户款项', '保险返利', '终端返利', '厂家返利合计', '增值税利润差',
            '税费', '毛利', '购买方式', '金融类型', '按揭渠道', '按揭产品',
            '首付金额', '贷款总额', '期限', '返利系数', '返利金额', '厂家贴息',
            '公司贴息', '金融税费', '实收金融服务费', '金融毛利', '上牌费',
            '上牌服务费', '上牌毛利', '二手车成交价', '二手车返利', '置换服务费',
            '促销费用', '赠送装饰项目', '装饰收入', '装饰成本', '套餐明细',
            '保养升级成本', '装饰赠送合计', '其他成本', '返介绍费', '合作返利',
            '综合结算服务费', '调拨费', '票据事务费', '票据事务费-公司', '其它费用',
            '特殊事项', '拖车费用', '区补', '质损赔付金额', '调整项', '单车毛利',
            '开票门店', '调出类型', '退代金券', '退成交车辆定金（未抵扣）',
            '退按揭押金', '退置换补贴保证金'
        ]

        valid_mongodb_cols = self._get_valid_columns(df_salesAgg4, mongodb_cols)
        self.df_mongodb_sales = df_salesAgg4[valid_mongodb_cols].copy()

        # 类型转换
        float_columns = [
            '订金金额', '厂家官价', '裸车成交价', '销售车价', '开票价格', '最终结算价',
            '置换补贴保证金', '票据事务金额', '保险返利', '终端返利', '厂家返利合计',
            '后返客户款项', '增值税利润差', '税费', '毛利', '返介绍费', '区补',
            '退代金券', '退成交车辆定金（未抵扣）', '退按揭押金', '退置换补贴保证金',
            '质损赔付金额', '首付金额', '贷款总额', '实收金融服务费', '厂家贴息',
            '公司贴息', '返利金额', '金融税费', '金融毛利', '上牌费', '上牌服务费',
            '上牌毛利', '二手车返利', '置换服务费', '促销费用', '保养升级成本',
            '装饰成本', '装饰赠送合计', '其他成本', '合作返利', '综合结算服务费',
            '调拨费', '票据事务费', '票据事务费-公司', '单车毛利', '二手车成交价',
            '装饰收入', '调整项', '其它费用', '特殊事项', '拖车费用'
        ]

        string_columns = [
            '车源门店', '供应商', '订单门店', '订车日期', '开票日期', '收款日期',
            '车架号', '发动机号', '车辆车系', '车辆车型', '车辆颜色', '业务渠道',
            '销售人员', '客户名称', '身份证号', '联系电话', '联系电话2', '金融类型',
            '购买方式', '按揭渠道', '期限', '按揭产品', '赠送装饰项目', '返利系数',
            '套餐明细', '开票门店', '调出类型', '邀约人员', '交付专员', '主播人员'
        ]

        # 字符串列转换
        valid_string_cols = self._get_valid_columns(self.df_mongodb_sales, string_columns)
        if valid_string_cols:
            self.df_mongodb_sales[valid_string_cols] = (
                self.df_mongodb_sales[valid_string_cols]
                .replace('nan', '')
                .fillna('')
                .astype('str')
            )

        # 数值列转换
        valid_float_cols = self._get_valid_columns(self.df_mongodb_sales, float_columns)
        if valid_float_cols:
            self.df_mongodb_sales[valid_float_cols] = (
                self.df_mongodb_sales[valid_float_cols]
                .apply(pd.to_numeric, errors='coerce')
                .fillna(0)
                .astype('str')
            )

        # 处理电话号码
        def clean_phone_series(data, keep_mobile_only=False, default=''):
            if isinstance(data, pd.DataFrame):
                result = data.copy()
                for col in result.columns:
                    result[col] = clean_phone_series(result[col], keep_mobile_only, default)
                return result

            s = data.astype(str).replace({'nan': '', 'None': '', '<NA>': ''})
            s = s.str.replace(r'[()\-\s—–﹘ ext转#]+', '', regex=True)
            s = s.str.extract(r'(\d{3,12})', expand=False).fillna('')

            def valid(phone):
                if phone == '':
                    return default
                if len(phone) == 11 and phone.startswith('1'):
                    return phone
                if not keep_mobile_only and 10 <= len(phone) <= 12:
                    return phone
                return default

            return s.apply(valid)

        # 清理联系电话
        if all(col in self.df_mongodb_sales.columns for col in ['联系电话', '联系电话2']):
            self.df_mongodb_sales[['联系电话', '联系电话2']] = clean_phone_series(
                self.df_mongodb_sales[['联系电话', '联系电话2']],
                keep_mobile_only=False,
                default=''
            )

        # 过滤数据（2025年4月1日之后）
        start_date = datetime(2025, 4, 1)

        # 处理销售数据
        self.df_mongodb_sales['开票日期'] = pd.to_datetime(
            self.df_mongodb_sales['开票日期'],
            errors='coerce',
            format='mixed'
        )

        # 处理直播基地
        if '订单门店' in self.df_mongodb_sales.columns:
            self.df_mongodb_sales['订单门店'] = np.where(
                self.df_mongodb_sales['订单门店'].str.contains('直播基地'),
                '直播基地',
                self.df_mongodb_sales['订单门店']
            )

        # 过滤日期
        filtered_df = self.df_mongodb_sales[
            self.df_mongodb_sales['开票日期'] >= start_date
            ].copy()

        # 格式化日期
        if not filtered_df.empty:
            filtered_df['开票日期'] = filtered_df['开票日期'].dt.strftime('%Y/%m/%d')
            filtered_df['订车日期'] = pd.to_datetime(
                filtered_df['订车日期'],
                errors='coerce',
                format='mixed'
            ).dt.strftime('%Y/%m/%d')

        # 调拨数据
        self.df_mongodb_diaobo = filtered_df[
            filtered_df['业务渠道'].isin(['调拨', '其他'])
        ].copy()

        diaobo_cols = [
            '订单门店', '订车日期', '开票日期', '车架号', '车辆车系', '车辆车型',
            '车辆颜色', '业务渠道', '销售人员', '邀约人员', '交付专员', '客户名称',
            '身份证号', '联系电话', '联系电话2', '订金金额', '厂家官价', '裸车成交价',
            '销售车价', '开票价格', '最终结算价', '置换补贴保证金', '票据事务金额',
            '后返客户款项', '保险返利', '终端返利', '厂家返利合计', '增值税利润差',
            '税费', '毛利', '上牌费', '上牌服务费', '上牌毛利', '质损赔付金额',
            '单车毛利', '开票门店'
        ]

        valid_diaobo_cols = self._get_valid_columns(self.df_mongodb_diaobo, diaobo_cols)
        self.df_mongodb_diaobo = self.df_mongodb_diaobo[valid_diaobo_cols]

        # 处理精品数据
        if not df_jingpin_result.empty:
            df_jingpin_result['最早收款日期'] = pd.to_datetime(
                df_jingpin_result['最早收款日期'],
                errors='coerce',
                format='mixed'
            )

            # 过滤日期
            self.df_mongodb_jingpin = df_jingpin_result[
                df_jingpin_result['最早收款日期'] >= start_date
                ].copy()

            # 格式化日期
            if not self.df_mongodb_jingpin.empty:
                self.df_mongodb_jingpin['收款日期'] = pd.to_datetime(
                    self.df_mongodb_jingpin['收款日期'],
                    format='mixed',
                    errors='coerce'
                ).dt.strftime('%Y/%m/%d')

                # 处理直播基地
                if '订单门店' in self.df_mongodb_jingpin.columns:
                    self.df_mongodb_jingpin['订单门店'] = np.where(
                        self.df_mongodb_jingpin['订单门店'].str.contains('直播基地'),
                        '直播基地',
                        self.df_mongodb_jingpin['订单门店']
                    )

        logging.info("MongoDB数据准备完成")

    def send_md_to_person(self, number: str = "13111855638", msg: str = ""):
        """发送通知（使用 cyy_api_db.py 的逻辑）"""
        try:
            data = {"touser": number, "msg": msg}
            res = requests.post('http://192.168.1.7/send_md_to_person', json=data, timeout=10)
            if res.status_code == 200:
                print(f"📢 通知发送成功")
            else:
                print(f"⚠️ 通知发送失败，状态码: {res.status_code}, 响应: {res.text}")
        except Exception as e:
            print(f"⚠️ 发送通知异常: {e}")
            log_file = "./logs/notify_fail.log"
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now()}: {msg}\n")

    def export_to_mongodb(self):
        """导出数据到MongoDB（使用 cyy_api_db.py 的逻辑）"""
        try:
            client = MongoClient(
                'mongodb://xg_wd:H91NgHzkvRiKygTe4X4ASw@192.168.1.7:27017/xg?' +
                'authSource=xg&authMechanism=SCRAM-SHA-256'
            )
            db = client['xg']

            # 写入销售数据
            if hasattr(self, 'df_mongodb_sales') and not self.df_mongodb_sales.empty:
                db['sales_data3'].delete_many({})
                db['sales_data3'].insert_many(self.df_mongodb_sales.to_dict('records'))
                logging.info(f"销售数据写入MongoDB完成：{len(self.df_mongodb_sales)}条")

            # 写入精品数据
            if hasattr(self, 'df_mongodb_jingpin') and not self.df_mongodb_jingpin.empty:
                db['jingpin_data'].delete_many({})
                db['jingpin_data'].insert_many(
                    self.df_mongodb_jingpin.fillna('').to_dict('records')
                )
                logging.info(f"精品数据写入MongoDB完成：{len(self.df_mongodb_jingpin)}条")

            # 写入调拨数据
            if hasattr(self, 'df_mongodb_diaobo') and not self.df_mongodb_diaobo.empty:
                db['diao_data'].delete_many({})
                db['diao_data'].insert_many(
                    self.df_mongodb_diaobo.fillna('').to_dict('records')
                )
                logging.info(f"调拨数据写入MongoDB完成：{len(self.df_mongodb_diaobo)}条")

            # 发送通知
            self.send_md_to_person(
                msg=f"✅ **数据已成功写入 MongoDB 数据库**\n" +
                    f"- 日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

            logging.info("数据已成功写入 MongoDB 数据库")
            client.close()

        except Exception as e:
            error_msg = f"❌ **数据写入 MongoDB 数据库失败**\n" + \
                        f"- 日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n" + \
                        f"- 错误信息: {str(e)}"
            self.send_md_to_person(msg=error_msg)
            logging.error(f"导出到MongoDB失败: {str(e)}")

        # -------------------------- MySQL写入方法 --------------------------
        def _get_sql_dtype(self, df):
            """获取SQL数据类型映射（使用 data_clean_mysql_cyy.py 的逻辑）"""
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

        def write_to_output_db(self, df, table_name):
            """写入数据到输出数据库（使用 data_clean_mysql_cyy.py 的逻辑）"""
            if df.empty:
                logging.warning(f"数据为空，跳过表[{table_name}]写入")
                return

            try:
                # 写入数据
                df.to_sql(
                    name=table_name,
                    con=self.output_engine,
                    if_exists='replace',
                    index=False,
                    chunksize=1000,
                    dtype=self._get_sql_dtype(df)
                )
                logging.info(f"表[{table_name}]写入完成：{len(df)}条数据")

            except SQLAlchemyError as e:
                logging.error(f"表[{table_name}]写入失败：{str(e)}")
                raise

        def write_all_to_mysql(self):
            """写入所有数据到MySQL"""
            logging.info(f"开始写入MySQL数据库[{self.OUTPUT_MYSQL_CONFIG['database']}]")

            # 1. 销售毛利数据
            if hasattr(self, 'df_sales_final') and not self.df_sales_final.empty:
                self.write_to_output_db(self.df_sales_final, 'sales_data')

            # 2. 订单数据
            if hasattr(self, 'df_order_final') and not self.df_order_final.empty:
                # 处理现定现交逻辑
                if '现定现交' in self.df_order_final.columns:
                    self.df_order_final['定金金额'] = np.where(
                        self.df_order_final['现定现交'] == '现定现交',
                        self.df_order_final['定金金额'] + 3000,
                        self.df_order_final['定金金额']
                    )
                self.write_to_output_db(self.df_order_final, 'order_data')

            # 3. 库存数据（未开票）
            if hasattr(self, 'df_inventory_final') and not self.df_inventory_final.empty:
                self.write_to_output_db(self.df_inventory_final, 'inventory_data')

            # 4. 作废订单数据
            if hasattr(self, 'df_cancel_final') and not self.df_cancel_final.empty:
                self.write_to_output_db(self.df_cancel_final, 'tuiding_data')

            # 5. 汇票数据
            if hasattr(self, 'df_debit_final') and not self.df_debit_final.empty:
                self.write_to_output_db(self.df_debit_final, 'debit_data')

            # 6. 销售明细数据
            if hasattr(self, 'df_sales_detail_final') and not self.df_sales_detail_final.empty:
                self.write_to_output_db(self.df_sales_detail_final, 'sales_invoice_data')

            # 7. 精品销售数据
            if hasattr(self, 'df_jingpin_final') and not self.df_jingpin_final.empty:
                # 过滤调出车
                if hasattr(self, 'df_sales_final') and not self.df_sales_final.empty:
                    df_profit_combined_1 = self.df_sales_final[
                        self.df_sales_final.get('调出车', '') != '是'
                        ]
                    if '车架号' in df_profit_combined_1.columns and '车系' in df_profit_combined_1.columns:
                        self.df_jingpin_final = pd.merge(
                            self.df_jingpin_final,
                            df_profit_combined_1[['车架号', '车系']],
                            on='车架号',
                            how='left'
                        )
                self.write_to_output_db(self.df_jingpin_final, 'jingpin_data')

            # 8. 库存已售数据
            if hasattr(self, 'df_sold_inventory_final') and not self.df_sold_inventory_final.empty:
                self.write_to_output_db(self.df_sold_inventory_final, 'sold_inventory')

            logging.info("所有数据已写入MySQL数据库")

    # -------------------------- 主流程控制方法 --------------------------
    def run(self):
        """主运行流程"""
        logging.info("=" * 50)
        logging.info("车易云商数据整合处理流程启动")
        logging.info("=" * 50)

        try:
            # 1. 加载所有数据
            logging.info("开始从数据库加载数据...")
            self.raw_data = self.load_all_data_from_db()
            logging.info(f"数据加载完成，共{len(self.raw_data)}个表")

            # 3. 解包各表数据
            logging.info("开始数据清洗处理...")
            # 保险业务
            df_insurance = self._clean_insurance(self.raw_data.get("保险业务"))

            # 二手车成交
            df_Ers = self._clean_used_cars(self.raw_data.get("二手车成交"))

            # 装饰订单
            df_decoration2, df_jingpin_result = self._clean_decoration_orders(self.raw_data.get("装饰订单"))

            # 套餐销售
            df_service_aggregated = self._clean_service_packages(
                self.raw_data.get("套餐销售")
            )

            # 车辆成本管理
            df_carcost = self._clean_vehicle_costs(
                self.raw_data.get("车辆成本管理")
            )

            # 按揭业务
            df_loan = self._clean_loans(
                self.raw_data.get("按揭业务")
            )

            # 汇票管理
            df_debit = self._clean_debit_and_merge(
                self.raw_data.get("汇票管理"),
                df_carcost
            )

            # 库存和计划车辆
            df_inventory_all, df_inventory, df_inventory1 = self._clean_inventory_and_plan(
                self.raw_data.get("库存车辆查询"),
                self.raw_data.get("库存车辆已售"),
                self.raw_data.get("计划车辆"),
                df_debit
            )

            # 订单数据
            df_dings, df_zhubo = self._clean_book_orders(
                self.raw_data.get("衍生订单"),
                self.raw_data.get("成交订单"),
                self.raw_data.get("未售订单")
            )

            # 作废订单
            tui_dings_df = self._clean_void_orders(
                self.raw_data.get("作废订单")
            )

            # 销售明细
            df_salesAgg = self._clean_sales_detail(
                self.raw_data.get("车辆销售明细_开票日期")
            )

            # 开票维护
            df_kaipiao = self.raw_data.get("开票维护")
            if not df_kaipiao.empty and '单据类别' in df_kaipiao.columns:
                df_kaipiao = df_kaipiao[df_kaipiao['单据类别'] == "车辆销售单"]
                df_kaipiao['下载时间'] = pd.to_datetime(df_kaipiao['下载时间'], format='mixed')
                df_kaipiao = df_kaipiao.sort_values(
                    by=['车架号', '下载时间'],
                    ascending=[True, False]
                ).drop_duplicates(subset=['车架号'], keep='first')

            # 二手车特殊处理
            tichu = ['苏秀清', '杜甯', '周杨', '李钰', '易阳梅', '黄毓香',
                     '王勇', '钟鸣', '刘前虎']

            df_Ers0 = df_Ers[(df_Ers['置换车架号'].notna()) & (df_Ers['置换车架号'] != '')]

            # 合并开票数据
            df_Ers1_ = pd.merge(
                df_Ers, df_kaipiao,
                how='left',
                left_on='置换车架号',
                right_on='车架号'
            )

            df_Ers1_['置换车架号'] = np.where(
                df_Ers1_['开票门店'].isna(),
                np.nan,
                df_Ers1_['置换车架号']
            )

            df_Ers1 = df_Ers1_[
                ((df_Ers1_['置换车架号'].isnull()) | (df_Ers1_['置换车架号'] == "")) &
                (~df_Ers1_['客户'].isin(tichu))
                ].copy()

            df_Ers1 = df_Ers1[[
                '评估门店', '成交金额', '其他费用', '线索提供人', '客户',
                '车型', '收款日期'
            ]]

            df_Ers1[['车系', '车架号', '所属团队']] = '二手车返利'
            df_Ers1['金融类型'] = '其他'
            df_Ers1['金融性质'] = '全款'

            df_Ers1.rename(columns={
                '评估门店': '公司名称',
                '成交金额': '二手车成交价',
                '其他费用': '二手车返利金额',
                '线索提供人': '销售人员',
                '客户': '车主姓名',
                '收款日期': '收款日期'
            }, inplace=True)

            df_Ers1['单车毛利'] = df_Ers1['二手车返利金额']

            # 二手车返利数据2
            df_Ers2 = df_Ers[(df_Ers['置换车架号'].notna()) & (df_Ers['置换车架号'] != '')].copy()
            df_Ers2.rename(columns={
                '车架号': '置换车架号_车牌',
                '置换车架号': '车架号',
                '其他费用': '二手车返利金额1'
            }, inplace=True)

            # 读取二手车返利存档
            try:
                df_Ers2_archive = pd.read_csv(self.USED_CAR_REBATE_PATH)
            except Exception as e:
                logging.warning(f"二手车返利存档读取失败：{str(e)}")
                df_Ers2_archive = pd.DataFrame()

            # 4. 主表合并
            logging.info("开始主表合并...")
            df_salesAgg1 = self._merge_main_sales_table(
                df_salesAgg, df_zhubo, df_service_aggregated, df_carcost, df_loan,
                df_decoration2, df_kaipiao, df_Ers2, df_Ers2_archive
            )

            # 5. 应用促销逻辑
            logging.info("应用促销逻辑...")
            df_salesAgg1 = self._apply_promotion_logic(df_salesAgg1)

            # 6. 调拨数据处理
            logging.info("处理调拨数据...")
            df_diao2 = self._handle_diaobo_merge(
                self.raw_data.get("调车结算"),
                df_salesAgg1
            )

            # 7. 准备销售明细数据
            logging.info("准备销售明细数据...")
            df_salesAgg_ = df_salesAgg1.copy()
            df_salesAgg_.rename(columns={
                '入库日期': '到库日期',
                '公司名称': '匹配定单归属门店',
                '订车日期': '定单日期',
                '销售人员': '销售顾问',
                '车主姓名': '客户姓名'
            }, inplace=True)

            df_salesAgg_ = df_salesAgg_[
                (df_salesAgg_['车架号'] != "") & (df_salesAgg_['销售日期'] != "")
                ]

            sales_detail_cols = [
                '服务网络', '车架号', '车系', '车型', '车辆配置', '外饰颜色',
                '定金金额', '指导价', '提货价', '销售车价', '匹配定单归属门店',
                '到库日期', '定单日期', '销售日期', '所属团队', '销售顾问',
                '客户姓名', '身份证号', '联系电话', '联系电话2'
            ]

            valid_sales_detail_cols = self._get_valid_columns(df_salesAgg_, sales_detail_cols)
            df_salesAgg_ = df_salesAgg_[valid_sales_detail_cols]
            df_salesAgg_ = df_salesAgg_[
                (df_salesAgg_['所属团队'] != "调拨") &
                (df_salesAgg_['所属团队'].notna()) &
                (df_salesAgg_['所属团队'] != "")
                ].drop_duplicates()

            # 8. 合并库存数据
            df_inventory0_1 = pd.concat([df_inventory, df_inventory1], axis=0, ignore_index=True)

            # 9. 最终数据处理和导出准备
            logging.info("进行最终数据处理...")
            self._finalize_and_export(
                df_salesAgg1, df_dings, df_inventory_all, tui_dings_df, df_debit,
                df_salesAgg_, df_jingpin_result, df_inventory1, df_Ers1,
                df_diao2, df_inventory0_1
            )

            # 10. 写入MySQL数据库
            logging.info("写入MySQL数据库...")
            self.write_all_to_mysql()

            # # 11. 写入MongoDB数据库
            # logging.info("写入MongoDB数据库...")
            # self.export_to_mongodb()

            logging.info("=" * 50)
            logging.info("车易云商数据整合处理流程全部完成！")
            logging.info(f"输出MySQL数据库：{self.OUTPUT_MYSQL_CONFIG['database']}")
            logging.info("=" * 50)

        except Exception as e:
            logging.error(f"数据处理流程失败：{str(e)}")
            import traceback
            traceback.print_exc()
            raise

if __name__ == "__main__":
    # 导入requests（用于通知功能）
    import requests

    # 运行处理器
    processor = CyysDataProcessor()
    processor.run()
