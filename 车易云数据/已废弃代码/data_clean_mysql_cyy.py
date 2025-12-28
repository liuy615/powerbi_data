# -*- coding: utf-8 -*-
"""
cyys.py
车易云商数据爬取与清洗：读取MySQL指定列数据，完成处理后写入新数据库
重构说明：
1. 按功能拆分模块：工具方法、数据读取、字段预处理、数据清洗、毛利计算、数据写入
2. 每个方法仅负责单一职责，便于维护和调试
3. 统一变量命名规范，补充完整文档字符串
4. 修复主键约束添加逻辑，补充数据校验
"""

import os
from datetime import datetime
import pandas as pd
import logging
import numpy as np
import re
import pymysql
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import VARCHAR, DECIMAL, DATE, DATETIME, INTEGER, BOOLEAN
from sqlalchemy.exc import SQLAlchemyError
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, message='.*DataFrame concatenation with empty or all-NA entries.*')

# 全局显示配置：显示所有列
pd.set_option('display.max_columns', 100)


class CyysDataProcessor:
    """车易云商数据处理器：封装数据读取(指定列)、清洗、合并、写入输出库全流程"""
    # -------------------------- 配置常量（按功能分类） --------------------------
    # 1. 日志与文件路径配置
    LOG_DIR = r"E:/pycharm_project/powerbi/data/log"
    SERVICE_NET_PATH = r'C:/Users/刘洋/Documents/WXWork/1688858189749305/WeDrive/成都永乐盛世/维护文件/看板部分数据源/各公司银行额度.xlsx'
    MAPPING_EXCEL_PATH = r"E:/powerbi_data/代码执行/data/字段对应 - csv.xlsx"
    USED_CAR_REBATE_PATH = r"E:/powerbi_data/看板数据/cyy_old_data/二手车返利存档.csv"

    # 2. 数据库配置
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
        'database': 'cyy_stg_data',  # 输出库名
        'charset': 'utf8mb4'
    }

    # 3. 表映射与唯一键配置
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
        '销售回访': 'car_sales_data'
    }
    TABLE_UNIQUE_KEY = {
        'mortgage_business': 'ID',
        'insurance_business': 'ID',
        'car_cost_management': 'OrderCode',
        'car_sales_order_date': 'ID',
        'car_sales_invoice_date': 'ID',
        'vehicle_transfer_settlement': 'FrameNumber',
        'bill_management': 'OrderCode',
        'planned_vehicles': 'OrderCode',
        'invoice_maintenance': 'ID',
        'inventory_vehicle_query': 'ID',
        'inventory_vehicle_sold': 'ID',
        'evaluation_deal': 'ID',
        'evaluation_stored': 'ID',
        'package_sales': 'SaleId',
        'unsold_orders': 'ID',
        'sales_deal_orders': 'ID',
        'sales_canceled_orders': 'ID',
        'sales_derivative_orders': 'ID',
        'decoration_orders': 'ID',
        'car_sales_data': 'ID'
    }

    # 4. 业务配置常量
    COMPANIES = [  # 内部公司名单（判断内部/外部调出）
        '成都新港建元汽车销售服务有限公司', '成都永乐盛世汽车销售服务有限公司',
        '成都新港永初汽车服务有限公司', '成都新港海川汽车销售服务有限公司',
        '成都新港先秦汽车服务有限公司', '成都新港治元汽车销售服务有限公司',
        '成都新港建隆汽车销售服务有限公司', '成都文景初治新能源汽车销售有限公司',
        '成都新港建武汽车销售服务有限公司', '成都新港文景海洋汽车销售服务有限公司',
        '成都文景盛世汽车销售服务有限公司', '成都新港澜舰汽车销售服务有限公司',
        '成都新港澜阔汽车销售服务有限公司', '成都鑫港鲲鹏汽车销售服务有限公司',
        '成都新茂元大汽车销售服务有限公司', '成都新港澜轩汽车销售服务有限公司',
        '成都新港浩蓝汽车销售服务有限公司', '贵州新港蔚蓝汽车销售服务有限责任公司',
        '贵州新港浩蓝汽车销售服务有限责任公司', '贵州新港澜源汽车服务有限责任公司',
        '贵州新港海之辇汽车销售服务有限责任公司', '成都新港上元坤灵汽车销售服务有限公司',
        '乐山新港上元曦和汽车销售服务有限公司', '宜宾新港上元曦和汽车销售服务有限公司',
        '泸州新港上元坤灵汽车销售服务有限公司', '贵州新港上元臻智汽车贸易有限公司',
        '成都新港上元臻智汽车销售服务有限公司', '乐山新港上元臻智汽车销售服务有限公司',
        '宜宾新港上元臻智汽车销售服务有限公司', '成都新港上元臻享汽车销售服务有限公司',
        '成都新港上元曦和汽车销售服务有限公司', '贵州新港澜轩汽车销售有限责任公司',
        '贵州新港上元曦和汽车销售服务有限公司', '直播基地'
    ]
    GUIZHOU_COMPANIES = [  # 贵州区域公司（计算促销费用）
        '贵州新港澜源', '贵州新港海之辇', '贵州新港浩蓝', '贵州新港蔚蓝',
        '贵州上元曦和', '贵州新港澜轩', '贵州上元臻智'
    ]
    EXCLUDED_STAFF = [  # 排除的销售人员（二手车返利计算）
        '苏秀清', '杜甯', '周杨', '李钰', '易阳梅', '黄毓香', '王勇', '钟鸣', '刘前虎'
    ]
    INTERNAL_COMPANIES = [  # 内部调入公司名单
        '新港建元', '永乐盛世', '新港永初', '新港海川', '新港先秦', '新港治元', '新港建隆',
        '王朝网-直播基地', '文景初治', '新港建武', '文景海洋', '文景盛世', '新港澜阔', '鑫港鲲鹏',
        '新港澜舰', '新茂元大', '新港浩蓝', '新港澜轩', '贵州新港澜源', '贵州新港海之辇',
        '贵州新港浩蓝', '贵州新港蔚蓝', '上元臻智', '上元臻享', '贵州上元臻智', '腾势-直播基地',
        '宜宾上元臻智', '乐山上元臻智', '上元坤灵', '上元曦和', '贵州上元曦和', '贵州新港澜轩',
        '方程豹-直播基地', '宜宾上元曦和', '乐山上元曦和', '泸州上元坤灵', '新港澜洲'
    ]

    """初始化：日志→字段映射→数据库连接（按依赖顺序）"""

    def __init__(self):
        self._init_logger()
        self._init_field_mapping()  # 提前加载字段映射
        self.source_engine = self._init_db_connection(self.SOURCE_MYSQL_CONFIG)
        self._create_output_database()  # 自动创建输出库
        self.output_engine = self._init_db_connection(self.OUTPUT_MYSQL_CONFIG)

        # 初始化实例变量（存储各阶段数据）
        self.df_service_net = pd.DataFrame()
        self.raw_data = {}
        self.processed_data = {}
        self.df_profit_final = pd.DataFrame()

    # -------------------------- 基础工具方法 --------------------------
    """初始化日志系统：同时输出到控制台和按日期生成日志文件"""

    def _init_logger(self):
        os.makedirs(self.LOG_DIR, exist_ok=True)
        log_file = f"{self.LOG_DIR}/log_api{datetime.now().strftime('%Y_%m_%d')}.log"

        # 创建logger
        self.logger = logging.getLogger("CyysDataProcessor")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        # 清除已有的handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # 创建formatter
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

        # 控制台handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # 文件handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    """初始化字段映射关系：
    - dfname_to_col_rename: 数据类型→{英文字段:中文字段}（重命名用）
    - table_to_english_cols: 源表名→需读取的英文字段列表（读取用）
    """

    def _init_field_mapping(self):
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

            # 构建源表名→读取字段列表映射（关联API_TABLE_MAPPING）
            dfname_to_english = {
                df_name: group["英文字段"].dropna().tolist()
                for df_name, group in self.mapping_df.groupby("df_name")
            }
            self.table_to_english_cols = {}
            for df_name, table_name in self.API_TABLE_MAPPING.items():
                if df_name in dfname_to_english:
                    self.table_to_english_cols[table_name] = dfname_to_english[df_name]
                else:
                    self.logger.warning(f"数据类型[{df_name}]无字段映射，表[{table_name}]无法读取")

            self.logger.info(f"字段映射初始化完成：覆盖{len(self.table_to_english_cols)}个源表")
        except Exception as e:
            self.logger.error(f"字段映射初始化失败：{str(e)}")
            raise

    """自动创建输出数据库（若不存在），避免连接不存在的库报错"""

    def _create_output_database(self):
        temp_config = self.OUTPUT_MYSQL_CONFIG.copy()
        target_db = temp_config.pop('database')
        temp_config['database'] = 'information_schema'  # 临时连接到系统库

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
            self.logger.info(f"输出数据库[{target_db}]创建完成")
        except pymysql.MySQLError as e:
            self.logger.error(f"输出数据库创建失败：{str(e)}")
            raise

    """通用数据库连接初始化（支持源库/输出库），自动检查连接有效性"""

    def _init_db_connection(self, db_config):
        try:
            conn_str = (
                f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
                f"charset={db_config['charset']}"
            )
            engine = create_engine(conn_str, pool_pre_ping=True)
            self.logger.info(f"数据库[{db_config['database']}]连接成功")
            return engine
        except SQLAlchemyError as e:
            self.logger.error(f"数据库[{db_config['database']}]连接失败：{str(e)}")
            raise

    """工具方法：获取DataFrame中实际存在的列，避免KeyError"""

    def _get_valid_columns(self, df, required_cols):
        valid_cols = [col for col in required_cols if col in df.columns]
        return valid_cols

    """工具方法：批量转换指定列为数值类型，异常值设为0"""

    def _convert_numeric_cols(self, df, cols):
        valid_cols = self._get_valid_columns(df, cols)
        for col in valid_cols:
            try:
                df[col] = df[col].replace(',', 0, regex=True).fillna(0)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except Exception as e:
                self.logger.error(f"列[{col}]数值转换失败：{str(e)}")
                df[col] = 0
        return df

    """安全concat方法：避免FutureWarning"""

    def _safe_concat(self, dfs, **kwargs):
        """安全的DataFrame合并方法，避免空DataFrame的警告"""
        if not dfs:
            return pd.DataFrame()

        # 过滤空DataFrame
        non_empty_dfs = [df for df in dfs if df is not None and not df.empty]

        if not non_empty_dfs:
            return pd.DataFrame()

        # 确保所有DataFrame有相同的列结构
        all_columns = set()
        for df in non_empty_dfs:
            all_columns.update(df.columns)

        # 为每个DataFrame添加缺失的列
        aligned_dfs = []
        for df in non_empty_dfs:
            aligned_df = df.copy()
            for col in all_columns:
                if col not in aligned_df.columns:
                    aligned_df[col] = np.nan
            aligned_dfs.append(aligned_df)

        return pd.concat(aligned_dfs, **kwargs)

    # -------------------------- 数据读取方法 --------------------------
    """从源库读取指定列数据（按字段映射表，非SELECT *），并自动去重"""

    def read_from_mysql(self, table_name):
        if table_name not in self.table_to_english_cols:
            self.logger.error(f"表[{table_name}]无字段映射，无法读取")
            return pd.DataFrame()

        try:
            # 构建查询字段（反引号包裹避免关键字冲突）
            english_cols = self.table_to_english_cols[table_name]
            query_cols = ', '.join([f"`{col}`" for col in english_cols])
            query = f"SELECT {query_cols} FROM `{table_name}`"

            # 读取并去重
            df = pd.read_sql(query, self.source_engine)
            initial_cnt = len(df)
            if table_name in self.TABLE_UNIQUE_KEY:
                unique_key = self.TABLE_UNIQUE_KEY[table_name]
                if unique_key in df.columns:
                    df = df.drop_duplicates()
                    if len(df) < initial_cnt:
                        self.logger.debug(f"表[{table_name}]去重：移除{initial_cnt - len(df)}条重复记录")

            self.logger.info(f"表[{table_name}]读取：{len(df)}条")
            return df
        except SQLAlchemyError as e:
            self.logger.error(f"表[{table_name}]读取失败：{str(e)}")
            return pd.DataFrame()

    """读取基础数据：服务网络Excel + 所有源库MySQL数据"""

    def _read_basic_data(self):
        # 读取服务网络数据（补充车系）
        try:
            self.df_service_net = pd.read_excel(self.SERVICE_NET_PATH, sheet_name='补充车系')
            self.logger.info(f"服务网络数据：{len(self.df_service_net)}条")
        except Exception as e:
            self.logger.error(f"服务网络数据读取失败：{str(e)}")
            self.df_service_net = pd.DataFrame()

        # 读取所有源库表数据
        self.raw_data = {}
        for df_name, table_name in self.API_TABLE_MAPPING.items():
            self.raw_data[df_name] = self.read_from_mysql(table_name)

    # -------------------------- 数据预处理方法 --------------------------
    """字段映射预处理：将原始数据按映射表重命名，生成标准化处理数据"""

    def _preprocess_field_mapping(self):
        self.processed_data = {}
        for df_name, df_raw in self.raw_data.items():
            # 跳过无映射或空数据
            if df_name not in self.dfname_to_col_rename or df_raw.empty:
                self.logger.debug(f"跳过[{df_name}]：无映射或数据为空")
                continue

            # 执行字段映射与重命名
            rename_map = self.dfname_to_col_rename[df_name]
            valid_cols = [col for col in rename_map.keys() if col in df_raw.columns]

            if not valid_cols:
                self.logger.debug(f"数据类型[{df_name}]无有效字段，跳过处理")
                continue

            # 筛选并批量重命名（英文→中文）
            df_processed = df_raw[valid_cols].rename(columns=rename_map)
            self.processed_data[df_name] = df_processed
            self.logger.debug(f"数据类型[{df_name}]处理：{len(valid_cols)}字段，{len(df_processed)}数据")

    # -------------------------- 数据清洗方法 --------------------------
    """统一清洗库存车辆/库存已售车辆数据"""

    def _clean_inventory(self, df):
        if df.empty:
            return pd.DataFrame()

        # 字段重命名
        df = df.rename(columns={
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
            '入库公司': '入库公司',
            '订单来源': '所属团队',
            '订单公司': '匹配定单归属门店',
            '合格证门店': '合格证门店',
            '赎证日期': '赎证日期',
            '出厂价格': '提货价',
            '厂家官价': '指导价'
        })

        # 筛选必要字段
        required_cols = [
            '供应商', '采购订单号', '归属系统', '入库公司', '匹配定单归属门店', '合格证门店',
            '所属团队', '车系', '车型', '配置', '颜色', '车架号', '指导价', '提货价',
            '生产日期', '赎证日期', '合格证状态', '发车日期', '到库日期', '库存天数',
            '运输方式存放地点', '车辆状态', '调拨日期', '调拨记录', '锁库日期', '销售日期',
            '开票日期', '配车日期', '销售顾问', '客户姓名', '质损信息', '备注', '操作日期'
        ]
        return df[self._get_valid_columns(df, required_cols)]

    """清洗新车保险台账数据"""

    def _clean_insurance(self, df_insurance):
        if df_insurance.empty:
            self.logger.debug("新车保险数据为空")
            return pd.DataFrame()

        self.logger.debug(f"新车保险数据：{len(df_insurance)}条")
        return df_insurance

    """清洗二手车成交与入库数据"""

    def _clean_used_car(self, df_ershou, df_ershou1):
        df_Ers = self._safe_concat([df_ershou, df_ershou1], axis=0, ignore_index=True)
        df_Ers = df_Ers[df_Ers['收款状态'] == '已收款']  # 只保留已收款
        self.logger.debug(f"二手车数据：{len(df_Ers)}条已收款")
        return df_Ers

    """清洗装饰订单数据（区分赠送装饰与精品销售）"""

    def _clean_decoration(self, df_decoration):
        df_decoration_final = pd.DataFrame()
        df_jingpin_final = pd.DataFrame()

        if df_decoration.empty or '销售合计' not in df_decoration.columns:
            self.logger.debug("装饰订单数据不完整，跳过清洗")
            return df_decoration_final, df_jingpin_final

        # 创建副本避免SettingWithCopyWarning
        df_decoration = df_decoration.copy()

        # 数据类型转换
        df_decoration['销售合计'] = pd.to_numeric(df_decoration['销售合计'], errors='coerce')
        invalid_status = ['已退款', '已退货', '待退货', '已换货', '全退款', '全退货', '部分退款']
        df_decoration = df_decoration[~df_decoration['物资状态'].isin(invalid_status)]

        # 1. 计算每个ID的出现次数，标记出重复的ID（出现次数>1）
        df_decoration['id_count'] = df_decoration.groupby('ID')['ID'].transform('count')
        # 2. 筛选出需要删除的行：ID重复（id_count>1）且自身OutId=0
        to_drop = (df_decoration['id_count'] > 1) & (df_decoration['OutId'] == 0)
        # 3. 删除符合条件的行，同时删除临时计算的id_count列
        df_decoration = df_decoration[~to_drop].drop(columns='id_count')

        # 赠送装饰筛选
        cond_gift = (df_decoration['单据类型'] == '新车销售') | (
                    df_decoration['单据类型'].isin(['客户增购', '换货销售', '独立销售']) & (
                        df_decoration['销售合计'] == 0))
        df_decoration_gift = df_decoration[cond_gift].copy()

        # 赠送装饰聚合
        if '车架号' in df_decoration_gift.columns:
            gift_items = df_decoration_gift.groupby('车架号')['物资名称'].agg(','.join).reset_index()
            cost_cols = ['成本合计(含税)', '工时费']

            # 成本字段转换
            for col in cost_cols:
                if col in df_decoration_gift.columns:
                    df_decoration_gift[col] = pd.to_numeric(df_decoration_gift[col], errors='coerce')

            if all(col in df_decoration_gift.columns for col in cost_cols):
                df_decoration_gift['装饰赠送成本'] = df_decoration_gift[cost_cols].sum(axis=1)
                df_decoration_final = df_decoration_gift.groupby('车架号')[
                    ['装饰赠送成本', '销售合计']].sum().reset_index().merge(gift_items, on='车架号', how='left')
                df_decoration_final = df_decoration_final.rename(columns={
                    '装饰赠送成本': '装饰成本',
                    '销售合计': '装饰收入',
                    '物资名称': '赠送装饰项目'
                })

        # 精品销售筛选
        df_jingpin = df_decoration[~cond_gift].copy()
        if '销售顾问' in df_jingpin.columns:
            df_jingpin = df_jingpin[~df_jingpin['销售顾问'].isin(['郑仁彬', '刘红梅', '衡珊珊'])]

        # 精品销售聚合
        if all(col in df_jingpin.columns for col in ['订单编号', '物资名称', '出/退/销数量']):
            df_jingpin['出/退/销数量'] = pd.to_numeric(df_jingpin['出/退/销数量'], errors='coerce').astype('Int64')

            jingpin_items = df_jingpin.groupby('订单编号').apply(
                lambda x: ','.join(f"{name}*{qty}" for name, qty in zip(x['物资名称'], x['出/退/销数量'])
                                   if pd.notna(name) and pd.notna(qty)), include_groups=False).reset_index(
                name='物资明细')

            # 成本计算
            cost_cols = ['成本合计(含税)', '工时费']
            for col in cost_cols:
                if col in df_jingpin.columns:
                    df_jingpin[col] = pd.to_numeric(df_jingpin[col], errors='coerce')

            if all(col in df_jingpin.columns for col in cost_cols):
                df_jingpin['总成本'] = df_jingpin[cost_cols].sum(axis=1)
                group_cols = ['订单门店', '订单编号', '车架号', '开票日期', '收款日期', '销售顾问', '客户名称',
                              '联系电话']
                valid_group_cols = self._get_valid_columns(df_jingpin, group_cols)

                df_jingpin_final = df_jingpin.groupby(valid_group_cols)[
                    ['总成本', '销售合计', '出/退/销数量']].sum().reset_index().merge(jingpin_items, on='订单编号',
                                                                                      how='left')
                df_jingpin_final = df_jingpin_final.rename(columns={
                    '销售合计': '销售总金额',
                    '销售顾问': '精品销售人员',
                    '出/退/销数量': '总次数'
                })
                df_jingpin_final['毛利润'] = df_jingpin_final['销售总金额'] - df_jingpin_final['总成本']

        self.logger.debug(f"装饰数据：赠送{len(df_decoration_final)}条，精品{len(df_jingpin_final)}条")
        return df_decoration_final, df_jingpin_final

    """清洗套餐销售数据"""

    def _clean_service_package(self, df_service):
        if df_service.empty:
            self.logger.debug("套餐销售数据为空，跳过清洗")
            return pd.DataFrame()

        df_service = df_service.rename(columns={'领取车架号/车牌号': '车架号'})

        # 筛选有效套餐
        if not all(col in df_service.columns for col in ['套餐名称', '审批状态', '订单状态', '实售金额']):
            self.logger.debug("套餐数据缺少筛选字段，跳过清洗")
            return pd.DataFrame()

        df_service = df_service[
            (df_service['套餐名称'] != '保赔无忧') &
            (df_service['审批状态'] != '审批驳回') &
            (df_service['订单状态'] != '已退卡') &
            (df_service['订单状态'] != '已登记') &
            ~((df_service['套餐名称'].str.contains('终身保养', na=False)) & (df_service['实售金额'] > 0)) &
            (df_service['实售金额'] <= 0)
            ].copy()

        df_service['实售金额'] = pd.to_numeric(df_service['实售金额'], errors='coerce')
        df_service['车架号'] = df_service['车架号'].astype(str)

        # 聚合套餐数据
        if not all(col in df_service.columns for col in ['车架号', '套餐名称', '总次数', '结算成本']):
            return pd.DataFrame()

        service_items = df_service.groupby('车架号').apply(
            lambda x: ','.join(f"{name}*{qty}" for name, qty in zip(x['套餐名称'], x['总次数'])
                               if pd.notna(name) and pd.notna(qty)),
            include_groups=False
        ).reset_index(name='套餐明细')

        df_service['保养升级成本'] = pd.to_numeric(df_service['结算成本'], errors='coerce')
        df_service_final = df_service.groupby('车架号')['保养升级成本'].sum().reset_index()
        df_service_final = df_service_final.merge(service_items, on='车架号', how='left')
        self.logger.debug(f"套餐数据：{len(df_service_final)}条")
        return df_service_final

    """清洗车辆成本管理数据"""

    def _clean_car_cost(self, df_carcost):
        if df_carcost.empty:
            self.logger.debug("车辆成本数据为空，跳过清洗")
            return pd.DataFrame()

        # 金额字段转换
        cost_cols = ['车辆成本_返介绍费', '其他成本_退代金券', '其他成本_退按揭押金']
        self._convert_numeric_cols(df_carcost, cost_cols)

        # 字段重命名
        df_carcost = df_carcost.rename(columns={
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
        })

        # 按车架号去重（保留最新操作记录）
        if all(col in df_carcost.columns for col in ['操作日期', '车架号']):
            df_carcost['操作日期'] = pd.to_datetime(df_carcost['操作日期'], errors='coerce')
            df_carcost = df_carcost.sort_values(by='操作日期', ascending=False)
            df_carcost = df_carcost.drop_duplicates(subset=['车架号'], keep='first')

        # 筛选必要字段
        required_cols = [
            '公司名称', '采购订单号', '车架号', '车辆状态', '调整项', '返介绍费',
            '退成交车辆定金（未抵扣）', '政府返回区补', '保险返利', '终端返利',
            '上牌成本', '票据事务费-公司', '代开票支付费用', '回扣款', '退代金券',
            '退按揭押金', '退置换补贴保证金', '质损赔付金额', '其他成本', '操作日期'
        ]
        df_carcost = df_carcost[self._get_valid_columns(df_carcost, required_cols)]
        self.logger.debug(f"车辆成本数据：{len(df_carcost)}条")
        return df_carcost

    """清洗按揭业务数据"""

    def _clean_mortgage(self, df_loan):
        if df_loan.empty:
            self.logger.debug("按揭业务数据为空，跳过清洗")
            return pd.DataFrame()

        # 字段重命名
        df_loan = df_loan.rename(columns={
            '按揭渠道': '金融性质',
            '贷款总额': '贷款金额',
            '期限': '贷款期限',
            '按揭产品': '金融方案',
            '实收金融服务费': '金融服务费',
            '厂家贴息': '厂家贴息金额',
            '公司贴息': '经销商贴息金额',
            '返利金额': '金融返利'
        })

        # 划分金融类型
        if '金融性质' in df_loan.columns:
            df_loan['金融类型'] = np.where(
                df_loan['金融性质'].str.contains('非贴息', na=False), '厂家非贴息贷',
                np.where(df_loan['金融性质'].str.contains('贴息', na=False), '厂家贴息贷', '非贴息贷')
            )

        # 数据类型转换
        if '返利系数' in df_loan.columns:
            df_loan['返利系数'] = df_loan['返利系数'].astype(str).str.replace('%', '').astype(
                float, errors='ignore') / 100

        float_cols = ['开票价', '贷款金额', '返利系数', '金融返利', '厂家贴息金额',
                      '经销商贴息金额', '金融服务费']
        self._convert_numeric_cols(df_loan, float_cols)

        # 计算衍生字段
        if all(col in df_loan.columns for col in ['开票价', '贷款金额']):
            df_loan['首付金额'] = df_loan['开票价'] - df_loan['贷款金额']

        if '贷款期限' in df_loan.columns:
            df_loan['贷款期限'] = df_loan['贷款期限'].astype(str).apply(
                lambda x: re.sub(r'[\u4e00-\u9fa5]', '', x))  # 移除中文

        if all(col in df_loan.columns for col in ['厂家贴息金额', '金融返利']):
            df_loan['金融税费'] = (df_loan['厂家贴息金额'] / 1.13 * 0.13 * 1.12) + (
                    df_loan['金融返利'] / 1.06 * 0.06 * 1.12)

        if all(col in df_loan.columns for col in ['金融返利', '经销商贴息金额', '金融税费']):
            df_loan['金融毛利'] = df_loan['金融返利'] - df_loan['经销商贴息金额'] - df_loan['金融税费']

        # 去重（按车架号保留第一条）
        if '车架号' in df_loan.columns:
            df_loan = df_loan[df_loan['车架号'].notna()]
            sort_cols = ['车架号', '收费状态'] if '收费状态' in df_loan.columns else ['车架号']
            df_loan = df_loan.sort_values(by=sort_cols, ascending=True)
            df_loan = df_loan.drop_duplicates(subset=['车架号'], keep='first')

        self.logger.debug(f"按揭数据：{len(df_loan)}条")
        return df_loan

    """清洗汇票管理数据"""

    def _clean_bill(self, df_debit, df_carcost):
        if df_debit.empty:
            self.logger.debug("汇票管理数据为空，跳过清洗")
            return pd.DataFrame()

        # 字段重命名
        df_debit = df_debit.rename(columns={
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
        })

        # 新增赎证标识
        if '是否结清' in df_debit.columns:
            df_debit['是否赎证'] = np.where(df_debit['是否结清'] == '已清', 1, 0)

        # 合并车辆状态（从成本表获取）
        if not df_carcost.empty and '采购订单号' in df_carcost.columns and '车辆状态' in df_carcost.columns:
            df_debit = pd.merge(
                df_debit, df_carcost[['采购订单号', '车辆状态']],
                on='采购订单号', how='left'
            )
            df_debit['车辆状态'] = df_debit['车辆状态'].fillna('未发')

        # 筛选必要字段
        debit_cols = [
            '合格证门店', '车源门店', '开票日期', '保证金比例', '首付金额', '汇票金额', '到期日期',
            '开票银行', '汇票号', '合格证号', '采购订单号', '车架号', '提货价', '审核状态',
            '赎证日期', '赎证款', '首付单号', '赎证单号', '是否赎证', '是否结清', '车辆状态'
        ]
        df_debit = df_debit[self._get_valid_columns(df_debit, debit_cols)]
        self.logger.debug(f"汇票数据：{len(df_debit)}条")
        return df_debit

    """清洗计划车辆数据"""

    def _clean_planned_vehicles(self, df_plan, df_debit):
        if df_plan.empty:
            self.logger.debug("计划车辆数据为空，跳过清洗")
            return pd.DataFrame()

        df_plan = df_plan.rename(columns={'车型': '车系', '整车型号': '车型', '订单号': '采购订单号'})

        # 合并汇票数据（补充提货价、赎证信息等）
        if not df_debit.empty and '采购订单号' in df_debit.columns:
            merge_cols = ['采购订单号', '提货价', '开票银行', '合格证门店', '赎证日期', '到期日期',
                          '保证金比例', '赎证款']
            valid_merge_cols = self._get_valid_columns(df_debit, merge_cols)
            df_plan = pd.merge(
                df_plan, df_debit[valid_merge_cols],
                on='采购订单号', how='left'
            )

        # 补充默认值
        df_plan['车辆状态'] = '未发'
        if '开票银行' in df_plan.columns:
            df_plan['开票银行'] = df_plan['开票银行'].fillna('公司')
        df_plan = df_plan.rename(columns={'开票银行': '合格证状态', '门店': '归属系统'})
        self.logger.debug(f"计划车辆数据：{len(df_plan)}条")
        return df_plan

    """合并并清洗库存+计划车辆数据"""

    def _clean_inventory_combined(self, df_inventory, df_plan):
        df_inventory_all = self._safe_concat([df_inventory, df_plan], axis=0, ignore_index=True)

        # 标记调入类型（内部/外部）
        if '供应商' in df_inventory_all.columns:
            df_inventory_all['调入类型'] = np.where(
                df_inventory_all['供应商'].isin(self.INTERNAL_COMPANIES), '内部调入',
                np.where(
                    (~df_inventory_all['供应商'].isin(self.INTERNAL_COMPANIES)) &
                    (df_inventory_all['供应商'] != '比亚迪') &
                    (df_inventory_all['供应商'].notnull()),
                    '外部调入', None
                )
            )

        # 合并服务网络（补充直播基地归属）
        if '车系' in df_inventory_all.columns and not self.df_service_net.empty:
            df_inventory_all = pd.merge(
                df_inventory_all, self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )
            if all(col in df_inventory_all.columns for col in ['归属系统', '服务网络']):
                df_inventory_all['归属系统'] = np.where(
                    df_inventory_all['归属系统'] == '直播基地',
                    df_inventory_all['服务网络'] + '-' + df_inventory_all['归属系统'],
                    df_inventory_all['归属系统']
                )

        self.logger.debug(f"库存+计划数据：{len(df_inventory_all)}条")
        return df_inventory_all

    """清洗订单数据"""

    def _clean_orders(self, df_books, df_books2, tui_dings_df):
        if df_books.empty:
            self.logger.debug("衍生订单数据为空，跳过清洗")
            return pd.DataFrame()

        # 创建副本避免SettingWithCopyWarning
        df_books = df_books.copy()

        # 删除 df_books 中 ID 存在于 tui_dings_ids 中的行
        df_books['ID'] = df_books['ID'].astype(str)
        tui_dings_df['ID'] = tui_dings_df['ID'].astype(str)
        df_books = df_books[~df_books['ID'].isin(tui_dings_df['ID'])]

        # 字段重命名
        df_books = df_books.rename(columns={
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
            '客户电话': '联系电话'
        })

        # 合并主播人员信息（从成交订单表）
        if not df_books2.empty:
            df_books2 = df_books2.rename(columns={'联系方式': '联系电话'})
            if all(col in df_books2.columns for col in ['车架号', '联系电话', '主播人员']):
                df_books2 = df_books2[['车架号', '联系电话', '主播人员']].drop_duplicates()

        # 筛选订单必要字段
        order_cols = [
            '车架号', '订单日期', '定单日期', '订金状态', '审批状态', '销售人员',
            '销售日期', '定金金额', '定单归属门店', '所属团队', '车系', '外饰颜色',
            '车型', '配置', '客户姓名', '联系电话'
        ]
        valid_order_cols = self._get_valid_columns(df_books, order_cols)
        df_dings = df_books[valid_order_cols].copy()

        if '联系电话' in df_dings.columns:
            df_dings['联系电话'] = df_dings['联系电话'].astype(str)

        # 补充直播基地归属
        if '车系' in df_dings.columns and not self.df_service_net.empty:
            df_dings = pd.merge(
                df_dings, self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )
            if all(col in df_dings.columns for col in ['定单归属门店', '服务网络']):
                df_dings['定单归属门店'] = np.where(
                    df_dings['定单归属门店'] == '直播基地',
                    df_dings['服务网络'] + '-' + df_dings['定单归属门店'],
                    df_dings['定单归属门店']
                )

        self.logger.debug(f"订单数据：{len(df_dings)}条")
        return df_dings

    """清洗作废订单数据"""

    def _clean_canceled_orders(self, tui_dings_df):
        if tui_dings_df.empty:
            self.logger.debug("作废订单数据为空，跳过清洗")
            return pd.DataFrame()

        # 补充直播基地归属
        if '车系' in tui_dings_df.columns and not self.df_service_net.empty:
            tui_dings_df = pd.merge(
                tui_dings_df, self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

        if all(col in tui_dings_df.columns for col in ['订单门店', '服务网络']):
            tui_dings_df['订单门店'] = np.where(
                tui_dings_df['订单门店'] == '直播基地',
                tui_dings_df['服务网络'] + '-' + tui_dings_df['订单门店'],
                tui_dings_df['订单门店']
            )

        # 日期转换与核算标记
        date_cols = ['作废时间', '订单日期']
        for col in date_cols:
            if col in tui_dings_df.columns:
                tui_dings_df[col] = pd.to_datetime(tui_dings_df[col], errors='coerce')

        if all(col in tui_dings_df.columns for col in ['订单日期', '作废时间']):
            tui_dings_df['退定日期'] = tui_dings_df['作废时间']
            tui_dings_df['定单日期'] = tui_dings_df['订单日期']
            tui_dings_df['非退定核算'] = np.where(
                (tui_dings_df['定单日期'].dt.year == tui_dings_df['退定日期'].dt.year) & (
                            tui_dings_df['定单日期'].dt.month == tui_dings_df['退定日期'].dt.month), 0, 1)

        # 筛选必要字段
        cancel_cols = [
            '订单门店', '业务渠道', '销售人员', '订单日期', '车系', '外饰颜色',
            '车型', '配置', '客户名称', '客户电话', '作废类型', '退订原因', '退定日期', '非退定核算'
        ]
        valid_cancel_cols = self._get_valid_columns(tui_dings_df, cancel_cols)
        tui_dings_df = tui_dings_df[valid_cancel_cols]
        self.logger.debug(f"作废订单数据：{len(tui_dings_df)}条")
        return tui_dings_df

    """清洗销售明细（开票日期）数据"""

    def _clean_sales_invoice(self, df_salesAgg):
        if df_salesAgg.empty:
            self.logger.debug("销售明细（开票日期）数据为空，跳过清洗")
            return pd.DataFrame()

        df_salesAgg_clean = df_salesAgg.copy()
        df_salesAgg_clean = df_salesAgg_clean.rename(columns={
            '入库日期': '到库日期',
            '订单门店': '匹配定单归属门店',
            '订单日期': '定单日期',
            '开票日期': '销售日期',
            '业务渠道': '所属团队',
            '销售人员': '销售顾问',
            '客户名称': '客户姓名',
            '车辆信息_车辆车系': '车系',
            '车辆信息_车辆车型': '车型',
            '车辆信息_车辆颜色': '外饰颜色',
            '车辆信息_车辆配置': '车辆配置',
            '车辆信息_车架号': '车架号',
            '订金信息_订金金额': '定金金额',
            '整车销售_厂家官价': '指导价',
            '整车销售_最终结算价': '提货价'
        })

        df_salesAgg_clean = df_salesAgg_clean[
            ['车架号', '车系', '车型', '车辆配置', '外饰颜色', '定金金额', '指导价', '提货价', '匹配定单归属门店',
             '到库日期', '定单日期', '销售日期', '所属团队', '销售顾问', '客户姓名']]

        # 补充直播基地归属
        if '车系' in df_salesAgg_clean.columns and not self.df_service_net.empty:
            df_salesAgg_clean = pd.merge(
                df_salesAgg_clean, self.df_service_net[['车系', '服务网络']],
                how='left', on='车系'
            )
            if all(col in df_salesAgg_clean.columns for col in ['匹配定单归属门店', '服务网络']):
                df_salesAgg_clean['匹配定单归属门店'] = np.where(
                    df_salesAgg_clean['匹配定单归属门店'] == '直播基地',
                    df_salesAgg_clean['服务网络'] + '-' + df_salesAgg_clean['匹配定单归属门店'],
                    df_salesAgg_clean['匹配定单归属门店']
                )

        # 过滤调拨数据
        if '所属团队' in df_salesAgg_clean.columns:
            df_salesAgg_clean = df_salesAgg_clean[
                (df_salesAgg_clean['所属团队'] != "调拨") & (df_salesAgg_clean['所属团队'].notna())]

        self.logger.debug(f"销售明细（开票日期）：{len(df_salesAgg_clean)}条")
        return df_salesAgg_clean

    # -------------------------- 毛利计算方法 --------------------------
    """处理订单状态（标记现定现交）"""

    def _process_order_status(self, df_dings):
        if df_dings.empty:
            return pd.DataFrame()

        # 标记现定现交
        df_dings['现定现交'] = np.where(
            (df_dings['定单日期'].isnull()) & (df_dings['销售日期'].notnull()),
            '现定现交',
            np.where(
                (df_dings['订金状态'] == "待收款") &
                (df_dings['定单日期'].notnull()) &
                (df_dings['销售日期'].notnull()),
                '现定现交', None
            )
        )
        df_dings['定单状态'] = np.where(df_dings['销售日期'].notnull(), df_dings['销售日期'], None)

        # 订单去重（按车架号保留最新）
        df_dings_clean = df_dings[
            (df_dings['车架号'] != '') &
            (df_dings['定单日期'].notna())
            ].sort_values(by=['车架号', '销售日期'], ascending=[True, True])
        df_dings_clean = df_dings_clean.drop_duplicates(subset=['车架号'], keep='first')
        return df_dings_clean

    """筛选开票数据（只保留车辆销售单）"""

    def _process_invoice_data(self, df_kaipiao):
        if df_kaipiao.empty:
            self.logger.debug("开票维护数据为空，跳过筛选")
            return pd.DataFrame()
        df_kaipiao = df_kaipiao[df_kaipiao['单据类别'] == "车辆销售单"][['车架号', '开票门店']].drop_duplicates(
            subset='车架号', keep='last')
        self.logger.debug(f"车辆销售开票数据：{len(df_kaipiao)}条")
        return df_kaipiao

    """处理二手车返利数据（区分有无置换车架号）"""

    def _process_used_car_rebate(self, df_Ers, df_kaipiao):
        # 无置换车架号的二手车返利
        df_ers_rebate1 = pd.DataFrame()
        if not df_Ers.empty and not df_kaipiao.empty:
            df_ers_rebate1 = pd.merge(
                df_Ers, df_kaipiao,
                how='left', left_on='置换车架号', right_on='车架号'
            )
            df_ers_rebate1['置换车架号'] = np.where(
                df_ers_rebate1['开票门店'].isna(), np.nan, df_ers_rebate1['置换车架号']
            )
            df_ers_rebate1 = df_ers_rebate1[
                (df_ers_rebate1['置换车架号'].isnull()) &
                (~df_ers_rebate1['客户'].isin(self.EXCLUDED_STAFF))
                ][['评估门店', '成交金额', '其他费用', '线索提供人', '客户', '车型', '收款日期']]

            # 补充默认值与重命名
            df_ers_rebate1[['车系', '车架号', '所属团队']] = '二手车返利'
            df_ers_rebate1['金融类型'] = '其他'
            df_ers_rebate1['金融性质'] = '全款'
            df_ers_rebate1 = df_ers_rebate1.rename(columns={
                '评估门店': '公司名称',
                '成交金额': '二手车成交价',
                '其他费用': '二手车返利金额',
                '线索提供人': '销售人员',
                '客户': '车主姓名',
                '收款日期': '收款日期'
            })
            df_ers_rebate1['单车毛利'] = df_ers_rebate1['二手车返利金额']

        # 有置换车架号的二手车返利（从存档读取）
        df_ers_rebate2 = pd.DataFrame()
        if not df_Ers.empty:
            try:
                df_ers_rebate_archive = pd.read_csv(self.USED_CAR_REBATE_PATH, low_memory=False)
                df_ers_rebate2 = df_Ers[df_Ers['置换车架号'].notna()].copy()
                df_ers_rebate2 = df_ers_rebate2.rename(columns={
                    '车架号': '置换车架号_车牌',
                    '置换车架号': '车架号',
                    '其他费用': '二手车返利金额1'
                })
                self.logger.debug(f"二手车返利：无置换{len(df_ers_rebate1)}条，有置换{len(df_ers_rebate2)}条")
            except Exception as e:
                self.logger.error(f"二手车返利存档读取失败：{str(e)}")

        return df_ers_rebate1, df_ers_rebate2

    """创建销售主数据（筛选必要字段）"""

    def _create_sales_main_data(self, df_salesAgg):
        if df_salesAgg.empty:
            return pd.DataFrame()

        # 统一销售明细字段重命名
        df_salesAgg = df_salesAgg.rename(columns={
            '订单门店': '公司名称',
            '订单日期': '订车日期',
            '开票日期': '销售日期',
            '购车方式': '购买方式',
            '业务渠道': '所属团队',
            '分销/邀约人员': '邀约人员',
            '交付专员': '交付专员',
            '销售人员': '销售人员',
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
            '其它业务_综合服务费': '金融服务费_顾问',
            '其它业务_票据事务费': '票据事务费',
            '其它业务_置换服务费': '置换服务费',
            '装饰业务_出库成本': '装饰成本',
            '其它业务_特殊事项': '特殊事项'
        })

        # 筛选销售明细必要字段
        sales_cols = [
            '公司名称', '订车日期', '入库日期', '销售日期', '车架号', '车系', '车型', '车辆配置',
            '外饰颜色', '所属团队', '销售人员', '邀约人员', '交付专员', '车主姓名', '定金金额',
            '指导价', '裸车成交价', '车款（发票价）', '提货价', '调拨费', '置换款', '精品款',
            '上牌费', '购买方式', '置换服务费', '金融服务费_顾问', '票据事务金额', '票据事务费',
            '代金券', '金融押金', '保险押金', '其它押金', '其它费用', '特殊事项'
        ]
        valid_sales_cols = self._get_valid_columns(df_salesAgg, sales_cols)
        df_sales_main = df_salesAgg[valid_sales_cols].dropna(subset='销售日期')  # 过滤无销售日期的记录
        return df_sales_main

    """计算销售毛利数据（多表合并与衍生计算）"""

    def _calculate_profit(self, df_sales_main, df_books2, df_service_final, df_carcost, df_loan, df_decoration_final,
                          df_kaipiao, df_ers_rebate2):
        if df_sales_main.empty:
            self.logger.debug("销售主数据为空，无法进行毛利计算")
            return pd.DataFrame()

        # 多表合并（主播、套餐、成本、按揭、装饰、开票、二手车返利）
        df_sales_profit = df_sales_main.copy()

        # 合并主播信息
        if not df_books2.empty and all(
                col in df_books2.columns for col in ['车架号', '联系电话', '主播人员']):
            df_sales_profit = pd.merge(df_sales_profit, df_books2[['车架号', '联系电话', '主播人员']], on='车架号',how='left')

        # 合并套餐数据
        if not df_service_final.empty:
            df_sales_profit = df_sales_profit.merge(df_service_final[['车架号', '保养升级成本', '套餐明细']],
                                                    on='车架号', how='left')

        # 合并成本数据
        if not df_carcost.empty:
            cost_merge_cols = ['车架号', '调整项', '返介绍费', '退成交车辆定金（未抵扣）', '政府返回区补',
                               '保险返利', '终端返利', '上牌成本', '票据事务费-公司', '代开票支付费用',
                               '回扣款', '退代金券', '退按揭押金', '退置换补贴保证金', '质损赔付金额', '其他成本']
            valid_cost_cols = self._get_valid_columns(df_carcost, cost_merge_cols)
            df_sales_profit = df_sales_profit.merge(df_carcost[valid_cost_cols], on='车架号', how='left')

        # 合并按揭数据
        if not df_loan.empty:
            loan_merge_cols = ['车架号', '金融类型', '金融性质', '首付金额', '贷款金额', '贷款期限',
                               '金融方案', '返利系数', '金融返利', '厂家贴息金额', '经销商贴息金额',
                               '金融税费', '金融服务费', '金融毛利']
            valid_loan_cols = self._get_valid_columns(df_loan, loan_merge_cols)
            df_sales_profit = df_sales_profit.merge(df_loan[valid_loan_cols], on='车架号', how='left')

        # 合并装饰数据
        if not df_decoration_final.empty:
            df_sales_profit = df_sales_profit.merge(
                df_decoration_final[['车架号', '装饰成本', '装饰收入', '赠送装饰项目']], on='车架号', how='left')

        # 合并开票数据
        if not df_kaipiao.empty:
            df_sales_profit = df_sales_profit.merge(df_kaipiao, on='车架号', how='left')

        # 合并二手车返利数据
        if not df_ers_rebate2.empty:
            df_sales_profit = df_sales_profit.merge(df_ers_rebate2[['车架号', '二手车返利金额1', '收款日期']],
                                                    on='车架号', how='left')

        # 合并二手车返利存档
        try:
            df_ers_rebate_archive = pd.read_csv(self.USED_CAR_REBATE_PATH, low_memory=False)
            if not df_ers_rebate_archive.empty:
                df_sales_profit = df_sales_profit.merge(df_ers_rebate_archive[['车架号', '二手车返利金额']],
                                                        on='车架号', how='left')
        except Exception as e:
            self.logger.debug(f"未合并二手车返利存档：{str(e)}")

        self.logger.debug(f"销售毛利主数据：{len(df_sales_profit)}条")

        # 金额字段转换为float
        float_cols_profit = [
            '定金金额', '指导价', '裸车成交价', '车款（发票价）', '提货价', '调拨费', '上牌费',
            '置换款', '精品款', '代金券', '其它押金', '其它费用', '金融押金', '保险押金',
            '置换服务费', '金融服务费_顾问', '票据事务费', '调整项', '特殊事项']
        self._convert_numeric_cols(df_sales_profit, float_cols_profit)

        # 计算衍生字段
        if all(col in df_sales_profit.columns for col in ['代金券', '金融押金', '保险押金', '其它押金']):
            df_sales_profit['后返客户款项'] = df_sales_profit[['代金券', '金融押金', '保险押金', '其它押金']].sum(
                axis=1)
        else:
            df_sales_profit['后返客户款项'] = 0

        # 二手车返利金额合并
        if '二手车返利金额' in df_sales_profit.columns and '二手车返利金额1' in df_sales_profit.columns:
            df_sales_profit['二手车返利金额'] = df_sales_profit['二手车返利金额'].fillna(
                df_sales_profit['二手车返利金额1'])

        # 金融服务费补值（无金融服务费时用顾问服务费）
        for col in ['金融服务费', '金融服务费_顾问']:
            if col in df_sales_profit.columns:
                df_sales_profit[col] = df_sales_profit[col].replace('', 0).fillna(0).astype(float)

        if all(col in df_sales_profit.columns for col in ['金融服务费', '金融服务费_顾问', '购买方式']):
            df_sales_profit['金融服务费'] = np.where(
                (df_sales_profit['金融服务费'] == 0) & (df_sales_profit['购买方式'] != "全款"),
                df_sales_profit['金融服务费_顾问'],
                df_sales_profit['金融服务费']
            )

        # 金融毛利 = 原有金融毛利 + 金融服务费
        if all(col in df_sales_profit.columns for col in ['金融毛利', '金融服务费']):
            df_sales_profit['金融毛利'] = df_sales_profit[['金融毛利', '金融服务费']].sum(axis=1)
        df_sales_profit['金融类型'] = df_sales_profit['金融类型'].fillna('全款')

        # 全款车辆的上牌费包含顾问服务费
        if all(col in df_sales_profit.columns for col in ['金融服务费_顾问', '购买方式', '上牌费']):
            df_sales_profit['上牌费'] = np.where(
                (df_sales_profit['金融服务费_顾问'] > 0) & (df_sales_profit['购买方式'] == "全款"),
                df_sales_profit['金融服务费_顾问'] + df_sales_profit['上牌费'],
                df_sales_profit['上牌费']
            )

        # 上牌毛利、装饰成本合计、销售车价计算
        cols_to_process = ['上牌成本', '上牌费', '票据事务金额', '精品款', '票据事务费', '其它费用']
        self._convert_numeric_cols(df_sales_profit, cols_to_process)

        if all(col in df_sales_profit.columns for col in ['上牌费', '上牌成本']):
            df_sales_profit['上牌毛利'] = df_sales_profit['上牌费'] + df_sales_profit['上牌成本']

        if '精品款' in df_sales_profit.columns and '票据事务金额' in df_sales_profit.columns:
            df_sales_profit['精品款'] = df_sales_profit['票据事务金额']

        if all(col in df_sales_profit.columns for col in ['装饰成本', '保养升级成本']):
            df_sales_profit['装饰赠送合计'] = df_sales_profit[['装饰成本', '保养升级成本']].sum(axis=1)
        else:
            df_sales_profit['装饰赠送合计'] = 0

        if all(col in df_sales_profit.columns for col in ['车款（发票价）', '置换款', '后返客户款项', '精品款']):
            df_sales_profit['销售车价'] = df_sales_profit[['车款（发票价）', '置换款', '后返客户款项']].sum(axis=1) - \
                                          df_sales_profit['精品款']

        # 贵州区域促销费用（后返+200）
        if '公司名称' in df_sales_profit.columns and '所属团队' in df_sales_profit.columns:
            df_sales_profit['促销费用'] = np.where(
                (df_sales_profit['公司名称'].isin(self.GUIZHOU_COMPANIES)) &
                (df_sales_profit['所属团队'] != "调拨"),
                df_sales_profit['后返客户款项'] + 200,
                df_sales_profit['后返客户款项']
            )
        else:
            df_sales_profit['促销费用'] = df_sales_profit['后返客户款项']

        # 增值税利润差与税费计算
        if all(col in df_sales_profit.columns for col in ['终端返利', '保险返利']):
            df_sales_profit['返利合计'] = df_sales_profit['终端返利'] + df_sales_profit['保险返利']
        else:
            df_sales_profit['返利合计'] = 0

        if all(col in df_sales_profit.columns for col in
               ['车款（发票价）', '置换款', '返利合计', '提货价', '票据事务金额']):
            # 计算增值税利润差（保持原有逻辑不变）
            df_sales_profit['增值税利润差'] = np.where(
                df_sales_profit['票据事务费'] > 0,
                df_sales_profit[['车款（发票价）', '置换款', '返利合计']].sum(axis=1) - df_sales_profit[
                    ['提货价', '票据事务金额']].sum(axis=1),
                df_sales_profit[['车款（发票价）', '置换款', '返利合计']].sum(axis=1) - df_sales_profit['提货价']
            )

            # 新增：处理汉唐增值税逻辑
            try:
                # 读取汉唐增值税处理数据（使用已配置的服务网络路径）
                df_vat = pd.read_excel(self.SERVICE_NET_PATH, sheet_name='汉唐_增值税处理')
                # 生成车系辅助列（处理空值避免拼接异常）
                df_sales_profit['车系辅助'] = (
                        df_sales_profit['车系'].fillna('') +
                        df_sales_profit['车型'].fillna('') +
                        df_sales_profit['提货价'].fillna(0).astype('int').astype('str')
                )
                # 获取有效车系辅助列表
                df_vat_list = df_vat['辅助列'].dropna().unique().tolist()
                self.logger.debug(f"符合条件的车系辅助列表: {len(df_vat_list)}个")
            except Exception as e:
                self.logger.error(f"汉唐增值税数据处理失败: {str(e)}，将使用默认税费计算逻辑")
                df_vat_list = []  # 空列表确保后续条件判断不生效

            # 转换销售日期为datetime类型（兼容异常日期格式）
            df_sales_profit['销售日期'] = pd.to_datetime(df_sales_profit['销售日期'], errors='coerce')

            # 新税费计算逻辑
            # 条件：销售日期在2025/09/01之后且车系辅助在有效列表中
            condition = (df_sales_profit['销售日期'] >= '2025/09/01') & (df_sales_profit['车系辅助'].isin(df_vat_list))

            df_sales_profit['税费'] = np.where(
                condition,
                # 符合条件：利润差减10000后再计算税费（仅正数部分）
                np.where(
                    df_sales_profit['增值税利润差'] - 10000 > 0,
                    np.round((df_sales_profit['增值税利润差'] - 10000) / 1.13 * 0.13 * 1.12, 2),
                    0
                ),
                # 不符合条件：使用原有计算逻辑
                np.where(
                    df_sales_profit['增值税利润差'] > 0,
                    np.round(df_sales_profit['增值税利润差'] / 1.13 * 0.13 * 1.12, 2),
                    0
                )
            )
        else:
            # 字段不全时使用默认值（保持原有逻辑）
            df_sales_profit['增值税利润差'] = 0
            df_sales_profit['税费'] = 0

        if all(col in df_sales_profit.columns for col in ['促销费用', '装饰赠送合计']):
            df_sales_profit['固定支出'] = df_sales_profit[['促销费用', '装饰赠送合计']].sum(axis=1)
        else:
            df_sales_profit['固定支出'] = 0

        if all(col in df_sales_profit.columns for col in ['销售车价', '返利合计', '税费', '提货价']):
            df_sales_profit['毛利'] = df_sales_profit[['销售车价', '返利合计']].sum(axis=1) - df_sales_profit[
                ['税费', '提货价']].sum(axis=1)
        else:
            df_sales_profit['毛利'] = 0

        self.logger.debug("销售毛利基础计算完成")
        return df_sales_profit

    """处理调拨数据（合并到毛利表）"""

    def _process_transfer_data(self, df_diao, df_sales_profit):
        if df_diao.empty:
            self.logger.debug("调车结算数据为空，跳过处理")
            return pd.DataFrame()

        df_transfer = df_diao.copy()
        if '调拨费' not in df_transfer.columns:
            self.logger.debug("调拨数据缺少调拨费字段，无法处理")
            return pd.DataFrame()

        # 数据处理
        df_transfer['结算日期'] = pd.to_datetime(df_transfer["结算日期"])
        df_transfer = df_transfer.drop(columns=['调拨费']).sort_values(by='结算日期', ascending=False)
        df_transfer = df_transfer.drop_duplicates(subset=['车架号'], keep='first')

        # 合并销售数据补充字段
        if not df_sales_profit.empty and all(
                col in df_sales_profit.columns for col in ['车架号', '销售日期', '车系', '车型', '车辆配置', '调拨费']):
            df_transfer_merge = pd.merge(
                df_transfer, df_sales_profit[['车架号', '销售日期', '车系', '车型', '车辆配置', '调拨费']],
                on='车架号', how='left'
            )
        else:
            df_transfer_merge = df_transfer.copy()

        # 补充调拨数据默认值
        df_transfer_merge[['所属团队', '金融类型']] = '其他'
        df_transfer_merge['金融类型'] = '调出车'
        df_transfer_merge['调出车'] = '是'
        df_transfer_merge = df_transfer_merge.rename(columns={
            '车系': '车系1',
            '调出门店': '公司名称',
            '支付门店': '车主姓名'
        })
        df_transfer_merge['车系'] = '调拨车'

        # 处理车辆信息（截取空格后内容）
        if '车辆信息' in df_transfer_merge.columns:
            df_transfer_merge['车辆信息'] = df_transfer_merge['车辆信息'].apply(
                lambda x: x[x.find(" ") + 1:] if (x and x.find(" ") != -1) else x
            )

        df_transfer_merge['单车毛利'] = df_transfer_merge['调拨费']

        # 筛选调拨数据必要字段
        transfer_cols = [
            '公司名称', '销售日期', '车架号', '车系', '车系1', '车型', '车辆信息', '车辆配置',
            '所属团队', '金融类型', '车主姓名', '调拨费', '调出车', '单车毛利'
        ]
        valid_transfer_cols = self._get_valid_columns(df_transfer_merge, transfer_cols)
        df_transfer_final = df_transfer_merge[valid_transfer_cols]
        self.logger.debug(f"调拨数据：{len(df_transfer_final)}条")
        return df_transfer_final

    """最终毛利数据处理（合并二手车和调拨数据）"""

    def _finalize_profit_data(self, df_sales_profit, df_ers_rebate1, df_transfer_final):
        if df_sales_profit.empty and df_ers_rebate1.empty and df_transfer_final.empty:
            return pd.DataFrame()
        # 计算单车毛利
        if not df_sales_profit.empty:
            毛利构成字段 = ['毛利', '金融毛利', '上牌毛利', '二手车返利金额', '代开票支付费用',
                            '置换服务费', '回扣款', '票据事务费-公司', '返介绍费', '质损赔付金额',
                            '其他成本', '政府返回区补', '装饰收入', '调整项', '其它费用', '特殊事项']
            valid_profit_cols = self._get_valid_columns(df_sales_profit, 毛利构成字段)

            扣除项 = ['促销费用', '装饰赠送合计', '调拨费']
            valid_deduct_cols = self._get_valid_columns(df_sales_profit, 扣除项)

            df_sales_profit['单车毛利'] = df_sales_profit[valid_profit_cols].sum(axis=1) - df_sales_profit[
                valid_deduct_cols].sum(axis=1)

            # 标记调出类型（内部/外部）
            if all(col in df_sales_profit.columns for col in ['车主姓名', '所属团队']):
                df_sales_profit['调出类型'] = np.where(
                    ((df_sales_profit['车主姓名'].isin(self.COMPANIES)) | (
                            df_sales_profit['车主姓名'].str.len() <= 5)) &
                    (df_sales_profit['所属团队'] == '调拨'),
                    '内部调出',
                    np.where(
                        (~df_sales_profit['车主姓名'].isin(self.COMPANIES)) &
                        (df_sales_profit['车主姓名'].str.len() > 5) &
                        (df_sales_profit['所属团队'] == '调拨'),
                        '外部调出',
                        None
                    )
                )

            # 筛选最终毛利表字段
            profit_final_cols = [
                '公司名称', '订车日期', '入库日期', '收款日期', '销售日期', '车架号', '车系', '车辆配置',
                '车型', '外饰颜色', '所属团队', '调出类型', '销售人员', '邀约人员', '交付专员', '主播人员',
                '车主姓名', '联系电话', '定金金额', '指导价', '裸车成交价', '销售车价', '车款（发票价）',
                '提货价', '置换款', '精品款', '后返客户款项', '保险返利', '终端返利', '返利合计',
                '增值税利润差', '税费', '毛利', '购买方式', '金融类型', '金融性质', '金融方案',
                '首付金额', '贷款金额', '贷款期限', '返利系数', '金融返利', '厂家贴息金额',
                '经销商贴息金额', '金融税费', '金融服务费', '金融毛利', '上牌费', '上牌成本',
                '上牌毛利', '二手车返利金额', '置换服务费', '促销费用', '赠送装饰项目', '装饰收入',
                '装饰成本', '套餐明细', '保养升级成本', '装饰赠送合计', '其他成本', '返介绍费',
                '回扣款', '代开票支付费用', '调拨费', '票据事务费', '票据事务费-公司', '其它费用',
                '特殊事项', '政府返回区补', '质损赔付金额', '调整项', '单车毛利', '开票门店',
                '退代金券', '退成交车辆定金（未抵扣）', '退按揭押金', '退置换补贴保证金'
            ]
            valid_final_cols = self._get_valid_columns(df_sales_profit, profit_final_cols)
            df_sales_profit_final = df_sales_profit[valid_final_cols].copy()

            # 字段值清洗（空字符串/NaN替换为0）
            cols_to_clean = [
                '金融返利', '金融服务费', '金融毛利', '上牌费', '上牌毛利', '置换款', '保险返利',
                '终端返利', '返利合计', '精品款', '装饰成本', '裸车成交价', '保养升级成本',
                '票据事务费', '装饰收入', '其它费用', '特殊事项'
            ]
            valid_clean_cols = self._get_valid_columns(df_sales_profit_final, cols_to_clean)
            for col in valid_clean_cols:
                df_sales_profit_final[col] = df_sales_profit_final[col].replace('', 0).fillna(0)
        else:
            df_sales_profit_final = pd.DataFrame()

        # 合并二手车、调拨数据
        dfs_to_combine = [df_sales_profit_final]
        if not df_ers_rebate1.empty:
            dfs_to_combine.append(df_ers_rebate1)
        if not df_transfer_final.empty:
            dfs_to_combine.append(df_transfer_final)
        df_profit_combined = self._safe_concat(dfs_to_combine, axis=0, ignore_index=True, join='outer')

        # 补充收款日期（二手车用销售日期补值）
        if all(col in df_profit_combined.columns for col in ['二手车返利金额', '收款日期', '销售日期']):
            df_profit_combined['收款日期'] = np.where(
                df_profit_combined['二手车返利金额'] > 0,
                df_profit_combined['收款日期'].fillna(df_profit_combined['销售日期']),
                df_profit_combined['收款日期']
            )
            df_profit_combined['销售日期'] = df_profit_combined['销售日期'].fillna(df_profit_combined['收款日期'])

        self.logger.debug(f"综合毛利数据：{len(df_profit_combined)}条")
        return df_profit_combined

    # -------------------------- 数据写入方法 --------------------------
    """自定义DataFrame到MySQL数据类型映射（避免自动推断错误）"""

    def _get_sql_dtype(self, df):
        dtype_map = {}
        for col in df.columns:
            col_type = str(df[col].dtype)
            # 日期时间类型
            if col_type in ['datetime64[ns]', 'datetime64']:
                dtype_map[col] = DATETIME()
            # 金额类字段（用DECIMAL避免浮点误差）
            elif col_type in ['float64', 'float32']:
                if any(key in col for key in ['金额', '毛利', '费用', '价格', '成本', '返利', '补贴']):
                    dtype_map[col] = DECIMAL(18, 2)  # 18位整数+2位小数
                else:
                    dtype_map[col] = DECIMAL(10, 2)
            # 整数类型
            elif col_type in ['int64', 'int32', 'Int64']:
                dtype_map[col] = INTEGER()
            # 字符串类型（按最大长度适配）
            else:
                max_len = df[col].dropna().astype(str).str.len().max() if not df[col].dropna().empty else 50
                dtype_map[col] = VARCHAR(min(max_len + 20, 255))  # 最大255字符
        return dtype_map

    """为输出表添加主键约束"""

    def _add_primary_key(self, table_name, primary_keys):
        # 检查主键字段是否存在
        insp = inspect(self.output_engine)
        try:
            table_cols = [col['name'] for col in insp.get_columns(table_name)]
            missing_cols = [col for col in primary_keys if col not in table_cols]

            if missing_cols:
                self.logger.warning(f"表[{table_name}]缺少主键字段{missing_cols}，跳过主键添加")
                return

            # 检查是否已有主键
            constraints = insp.get_pk_constraint(table_name)
            if constraints['constrained_columns']:
                self.logger.debug(f"表[{table_name}]已存在主键：{constraints['constrained_columns']}")
                return

            # 添加主键约束
            with self.output_engine.connect() as conn:
                pk_sql = f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`{'`,`'.join(primary_keys)}`)"
                conn.execute(text(pk_sql))
                conn.commit()
                self.logger.debug(f"表[{table_name}]添加主键：{primary_keys}")
        except SQLAlchemyError as e:
            self.logger.error(f"表[{table_name}]添加主键失败：{str(e)}")

    """将处理后的数据写入输出库"""

    def write_to_output_db(self, df, table_name, primary_keys=None):
        if df.empty:
            self.logger.debug(f"数据为空，跳过表[{table_name}]写入")
            return

        try:
            # 写入数据（替换现有表）
            df.to_sql(
                name=table_name,
                con=self.output_engine,
                if_exists='replace',
                index=False,
                chunksize=1000,  # 批量写入避免内存溢出
                dtype=self._get_sql_dtype(df)
            )
            self.logger.info(f"表[{table_name}]写入：{len(df)}条")

            # 添加主键约束（若指定）
            if primary_keys and len(primary_keys) > 0:
                self._add_primary_key(table_name, primary_keys)
        except SQLAlchemyError as e:
            self.logger.error(f"表[{table_name}]写入失败：{str(e)}")
            raise

    """写入所有处理完成的数据表到输出库"""

    def _write_all_tables(self, df_profit_combined, df_dings, df_inventory_all, tui_dings_df, df_debit, df_salesAgg_clean, df_jingpin_final, df_inventory1):
        self.logger.info(f"开始写入输出数据库[{self.OUTPUT_MYSQL_CONFIG['database']}]")

        # 1. 销售毛利数据
        self.write_to_output_db(df_profit_combined, 'sales_data', )

        # 2. 订单数据
        df_dings['定金金额'] = np.where(df_dings['现定现交'] == '现定现交', df_dings['定金金额'] + 3000,df_dings['定金金额'])
        self.write_to_output_db(df_dings, 'order_data')

        # 3. 库存数据（未开票）
        df_inventory_uninvoiced = df_inventory_all[df_inventory_all['开票日期'].isna()] if not df_inventory_all.empty else pd.DataFrame()
        self.write_to_output_db(df_inventory_uninvoiced, 'inventory_data')

        # 4. 作废订单数据
        self.write_to_output_db(tui_dings_df, 'tuiding_data')

        # 5. 汇票数据
        self.write_to_output_db(df_debit, 'debit_data')

        # 6. 销售明细（开票日期）
        self.write_to_output_db(df_salesAgg_clean, 'sales_invoice_data')

        # 7. 精品销售数据
        df_profit_combined_1 = df_profit_combined[df_profit_combined["调出车"] != '是']
        df_jingpin_final = pd.merge(df_jingpin_final, df_profit_combined_1[['车架号', '车系']], on=['车架号'],how='left')
        self.write_to_output_db(df_jingpin_final, 'jingpin_data')

        # 8. 库存已售数据
        self.write_to_output_db(df_inventory1, 'sold_inventory')

    # -------------------------- 主流程控制方法 --------------------------
    def run(self):
        """主流程：数据读取→清洗→计算→写入输出库"""
        self.logger.info("=" * 40)
        self.logger.info("车易云商数据处理流程启动")
        self.logger.info("=" * 40)

        # 1. 读取基础数据
        self._read_basic_data()

        # 2. 字段映射与数据预处理
        self._preprocess_field_mapping()
        self.logger.info("字段映射完成，进入数据清洗阶段")

        # 解包处理后的数据（便于后续清洗）
        data = self.processed_data
        df_loan = data.get("按揭业务", pd.DataFrame())
        df_insurance = data.get("保险业务", pd.DataFrame())
        df_carcost = data.get("车辆成本管理", pd.DataFrame())
        df_salesAgg = data.get("车辆销售明细_开票日期", pd.DataFrame())
        df_diao = data.get("调车结算", pd.DataFrame())
        df_debit = data.get("汇票管理", pd.DataFrame())
        df_plan = data.get("计划车辆", pd.DataFrame())
        df_kaipiao = data.get("开票维护", pd.DataFrame())
        df_inventory = data.get("库存车辆查询", pd.DataFrame())
        df_inventory1 = data.get("库存车辆已售", pd.DataFrame())
        df_ershou = data.get("二手车成交", pd.DataFrame())
        df_ershou1 = data.get("二手车入库", pd.DataFrame())
        df_service = data.get("套餐销售", pd.DataFrame())
        df_unsold = data.get("未售订单", pd.DataFrame())
        # 修复SyntaxWarning：使用原始字符串
        df_unsold.to_csv(r"E:\powerbi_data\看板数据\cyy_old_data\未售订单.csv")
        df_books2 = data.get("成交订单", pd.DataFrame())
        tui_dings_df = data.get("作废订单", pd.DataFrame())
        df_books = data.get("衍生订单", pd.DataFrame())
        df_decoration = data.get("装饰订单", pd.DataFrame())
        df_sales_visit = data.get("销售回访", pd.DataFrame())

        # 3. 执行各模块数据清洗
        self._clean_insurance(df_insurance)  # 新车保险数据清洗
        df_Ers = self._clean_used_car(df_ershou, df_ershou1)  # 二手车数据清洗
        df_decoration_final, df_jingpin_final = self._clean_decoration(df_decoration)  # 装饰数据清洗
        df_service_final = self._clean_service_package(df_service)  # 套餐数据清洗
        df_carcost_clean = self._clean_car_cost(df_carcost)  # 车辆成本数据清洗
        df_loan_clean = self._clean_mortgage(df_loan)  # 按揭数据清洗
        df_inventory_clean = self._clean_inventory(df_inventory)  # 库存数据清洗
        df_inventory1_clean = self._clean_inventory(df_inventory1)  # 库存数据清洗
        df_debit_clean = self._clean_bill(df_debit, df_carcost_clean)  # 汇票数据清洗
        df_plan_clean = self._clean_planned_vehicles(df_plan, df_debit_clean)  # 计划车辆清洗
        df_inventory_all = self._clean_inventory_combined(df_inventory_clean, df_plan_clean)  # 库存+计划合并
        df_dings = self._clean_orders(df_books, df_books2, tui_dings_df)  # 订单数据清洗
        tui_dings_df_clean = self._clean_canceled_orders(tui_dings_df)  # 作废订单清洗
        df_salesAgg_clean = self._clean_sales_invoice(df_salesAgg)  # 销售明细清洗
        self.logger.info("数据清洗阶段完成")

        # 4. 毛利计算与数据合并
        self.logger.info("开始计算综合毛利数据")
        df_dings_clean = self._process_order_status(df_dings)  # 订单状态处理
        df_kaipiao_clean = self._process_invoice_data(df_kaipiao)  # 开票数据筛选
        df_ers_rebate1, df_ers_rebate2 = self._process_used_car_rebate(df_Ers, df_kaipiao_clean)  # 二手车返利处理
        df_sales_main = self._create_sales_main_data(df_salesAgg)  # 销售主数据创建

        # 毛利计算
        df_sales_profit = self._calculate_profit(df_sales_main, df_books2, df_service_final, df_carcost_clean,df_loan_clean, df_decoration_final, df_kaipiao_clean, df_ers_rebate2)

        # 调拨数据处理
        df_transfer_final = self._process_transfer_data(df_diao, df_sales_profit)

        # 最终毛利数据合并
        df_profit_combined = self._finalize_profit_data(df_sales_profit, df_ers_rebate1, df_transfer_final)

        # 5. 写入输出数据库
        self._write_all_tables(
            df_profit_combined, df_dings, df_inventory_all,
            tui_dings_df_clean, df_debit_clean, df_salesAgg_clean,
            df_jingpin_final, df_inventory1_clean)

        self.logger.info("=" * 40)
        self.logger.info("车易云商数据处理流程全部完成！")
        self.logger.info(f"输出数据库：{self.OUTPUT_MYSQL_CONFIG['database']}")
        self.logger.info("=" * 40)


if __name__ == "__main__":
    processor = CyysDataProcessor()
    processor.run()