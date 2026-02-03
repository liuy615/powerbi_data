# -*- coding: utf-8 -*-
import datetime
import logging
import os
import re
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import warnings
project_root = r"E:\powerbi_data"
sys.path.insert(0, project_root)
from config.cyys_data_processor.config import SOURCE_MYSQL_CONFIG, OUTPUT_MYSQL_CONFIG
warnings.filterwarnings('ignore', category=FutureWarning, message='.*Downcasting object dtype arrays.*')
# 全局显示配置：显示所有列
pd.set_option('display.max_columns', 100)

# -------------------------- 1. 修复路径转义：所有Windows路径加r前缀 --------------------------
log_dir = r"E:\powerbi_data\data\cyy_log\concat_log"  # 加r
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(message)s]',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{log_dir}/log1_{datetime.now().strftime('%Y_%m_%d')}.log", encoding='utf-8')
    ]
)


def standardize_date(date_str):
    if isinstance(date_str, str):
        date_str = date_str.strip()
        if "年" in date_str and "月" in date_str and "日" not in date_str:
            date_str += "1日"
    return date_str


def convert_date(date_str):
    if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_str):
        return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
    elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
        return pd.to_datetime(date_str, format='%Y年%m月%d日')
    return pd.NaT


class update_dashboard:
    def __init__(self):
        # 数据库配置（请确认已修改为实际环境信息）
        self.db_config = OUTPUT_MYSQL_CONFIG

        # 创建数据库引擎
        try:
            self.engine = create_engine(
                fr"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}?charset=utf8mb4",
                echo=False
            )
            logging.info("数据库引擎创建成功")
        except Exception as e:
            logging.error(f"数据库引擎创建失败: {str(e)}", exc_info=True)
            raise

        # 数据初始化（所有文件路径均加r前缀，且read_csv添加low_memory=False）
        self.df_books_cyy = self.Df_books_cyy()
        self.df_books_lock = self.Df_books_lock()
        self.df_sales_cyy = self.Df_sales_cyy()
        self.df_sales_lock = self.Df_sales_lock()
        self.df_sales_lock1 = self.Df_sales_lock1()
        self.df_tuis_lock = self.Df_tuis_lock()
        self.df_jingpins_cyy = self.Df_jingpins_cyy()
        self.df_jingpins_lock = self.Df_jingpins_lock()
        self.df_inventorys_cyy = self.Df_inventorys_cyy()
        self.df_tuis_cyy = self.Df_tuis_cyy()
        self.df_debits_cyy = self.Df_debits_cyy()
        self.df_debits_lock = self.Df_debits_lock()
        self.df_sales_cyy1 = self.Df_sales_cyy1()
        self.df_xcbx_lock = self.Df_xcbx_lock()
        self.df_xcbx_cyy = self.Df_xcbx_cyy()
        self.df_Ers_lock = self.Df_Ers_lock()
        self.df_Ers = self.Df_Ers()
        self.car_belongs = self.Car_belongs()
        self.df_books_unsold = self.Df_books_unsold()
        self.df_yingxiao = self.Df_yingxiao()
        self.df_salary = self.Df_salary()
        self.team_belongs = self.Team_belongs()
        self.df_plan_date_get = self.Df_plan_date_get()
        self.df_inventorys_lock = self.Df_inventory_lock()
        self.list_companys = ['成都新港建元汽车销售服务有限公司', '成都洪武盛世汽车销售服务有限公司',
                              '成都新港永初汽车服务有限公司', '成都新港海川汽车销售服务有限公司',
                              '成都新港先秦汽车服务有限公司', '成都新港治元汽车销售服务有限公司',
                              '成都新港建隆汽车销售服务有限公司', '成都上元盛世新能源汽车销售有限公司',
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
                              '贵州新港上元曦和汽车销售服务有限公司', '直播基地']

    # -------------------------- 2. 修复数据库读取：显式构造SQL语句，反引号包裹表名 --------------------------
    def _read_from_db(self, table_name: str) -> pd.DataFrame:
        """通用数据库读取方法：显式SQL语句，兼容MySQL表名格式"""
        try:
            logging.info(f"开始读取数据库表：{table_name}")
            # 关键修复：用SELECT * FROM `表名`（反引号包裹表名，MySQL语法）
            sql = f"SELECT * FROM `{table_name}`"
            df = pd.read_sql(sql, self.engine)
            logging.info(f"成功读取表 {table_name}，数据行数：{len(df)}")
            return df
        except SQLAlchemyError as e:
            logging.error(f"数据库操作失败（表：{table_name}，SQL：{sql}）：{str(e)}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"数据处理失败（表：{table_name}）：{str(e)}", exc_info=True)
            raise

    # 数据库读取方法
    def Df_books_cyy(self) -> pd.DataFrame:
        return self._read_from_db('order_data')

    def Df_sales_cyy(self) -> pd.DataFrame:
        return self._read_from_db('sales_data')

    def Df_inventorys_cyy(self) -> pd.DataFrame:
        return self._read_from_db('inventory_data')

    def Df_jingpins_cyy(self) -> pd.DataFrame:
        return self._read_from_db('jingpin_data')

    def Df_tuis_cyy(self) -> pd.DataFrame:
        return self._read_from_db('tuiding_data')

    def Df_debits_cyy(self) -> pd.DataFrame:
        return self._read_from_db('debit_data')

    def Df_sales_cyy1(self) -> pd.DataFrame:
        return self._read_from_db('sales_invoice_data')

    # -------------------------- 3. 修复所有文件路径：加r前缀 + 补充low_memory=False解决DtypeWarning --------------------------
    def Df_xcbx_cyy(self) -> pd.DataFrame:
        return pd.read_csv(r'E:\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv',encoding='utf-8', low_memory=False)

    def Df_salary(self) -> pd.DataFrame:
        return pd.read_excel(r"E:\WXWork\1688858189749305\WeDrive\成都永乐盛世\费效分析\销售\2025年人工效能分析表-销售.xlsx",sheet_name='看板用').replace("永乐盛世", "洪武盛世")

    def Car_belongs(self) -> pd.DataFrame:
        return pd.read_excel(r'E:\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx',sheet_name='补充车系')

    def Team_belongs(self) -> pd.DataFrame:
        return pd.read_excel(r'E:\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx',sheet_name='补充团队')

    # 新车保险未合并
    def Df_xcbx_lock(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/dashboard/新车保险台账.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_books_unsold(self) -> pd.DataFrame:
        return pd.read_csv(fr'E:/powerbi_data/看板数据/cyy_old_data/未售订单.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_yingxiao(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/dashboard/投放费用.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_Ers(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/dashboard/二手车.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_Ers_lock(self) -> pd.DataFrame:
        return pd.read_csv(r"E:/powerbi_data/看板数据/cyy_old_data/二手车台账.csv", low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_books_lock(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/cyy_old_data/定车.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_sales_lock(self) -> pd.DataFrame:
        # 修复DtypeWarning：补充low_memory=False
        return pd.read_csv(r'E:/powerbi_data/看板数据/cyy_old_data/销售毛利.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_sales_lock1(self) -> pd.DataFrame:
        # 修复DtypeWarning：补充low_memory=False
        return pd.read_csv(r'E:/powerbi_data/看板数据/cyy_old_data/销售.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_jingpins_lock(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/cyy_old_data/精品销售.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_tuis_lock(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/cyy_old_data/退定.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_debits_lock(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/cyy_old_data/三方台账.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_plan_date_get(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/cyy_old_data/计划车辆汇总.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory

    def Df_inventory_lock(self) -> pd.DataFrame:
        return pd.read_csv(r'E:/powerbi_data/看板数据/dashboard/库存存档.csv', low_memory=False).replace("永乐盛世", "洪武盛世")  # 加r + low_memory


    # -------------------------- 核心修复：切片后加.copy()解决SettingWithCopyWarning --------------------------
    def classify_inventory_duration(self, row):
        inventory_time = row['库存时间']
        vehicle_status = row['车辆状态']

        if vehicle_status in ["未发", "在途"]:
            return "未到库"
        elif inventory_time >= 180:
            return "180天以上"
        elif inventory_time >= 120:
            return "120-180天"
        elif inventory_time >= 90:
            return "90-120天"
        elif inventory_time >= 60:
            return "60-90天"
        elif inventory_time >= 30:
            return "30-60天"
        else:
            return "30天以下"

    def add_month_end_date(self, df, year_col='年份', month_col='月份'):
        df = df.copy()  # 入参后先加copy
        df['year'] = df[year_col].astype(str).str.extract(r'(\d+)')
        df['month'] = df[month_col].astype(str).str.extract(r'(\d+)')
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['day'] = 1
        df['当月第一天'] = pd.to_datetime(df[['year', 'month', 'day']],errors='coerce')
        df['日期'] = df['当月第一天'] + pd.offsets.MonthEnd(1)
        df = df.drop(columns=['year', 'month', 'day', '当月第一天'])
        return df

    # 合并sales表
    # 输入：销售毛利.csv 和 数据库的sales表
    # 输出：销售毛利1.csv
    def concat_newold_Sales_dashboad(self):
        # 1. 切片后加 .copy() 避免视图警告
        df_sales_lock = self.df_sales_lock[
            ['公司名称', '车架号', '车系', '外饰颜色', '车型', '指导价', '销售日期', '订车日期', '销售人员', '所属团队',
             '车主姓名', '联系电话', '购买方式', '销售车价', '车款（发票价）', '置换款', '精品款', '后返客户款项',
             '终端返利', '提货价', '增值税利润差', '税费', '毛利', '金融性质', '返利系数', '贷款金额', '贷款期限',
             '经销商贴息金额', '厂家贴息金额', '金融税费', '金融返利', '金融服务费', '金融毛利', '上牌费', '上牌成本',
             '上牌毛利', '临牌费', '临牌成本', '临牌毛利', '促销费用', '装饰成本', '二手车成交价', '二手车返利金额',
             '回扣款', '政府返回区补', '返客户区补', '开票价', '代开票支付费用', '单车毛利', '调出车', '金融类型',
             '贷款期限1']
        ].copy()  # 关键：加 .copy() 生成副本

        # 2. 切片后加 .copy() 避免视图警告
        df_sales_cyy = self.df_sales_cyy[
            ['公司名称', '销售日期', '订车日期', '收款日期', '车架号', '车系', '车系1', '车辆信息', '外饰颜色',
             '车辆配置', '车型', '所属团队', '调出类型', '销售人员', '主播人员', '车主姓名', '联系电话', '指导价',
             '销售车价', '车款（发票价）', '提货价', '置换款', '精品款', '后返客户款项', '保险返利', '终端返利',
             '返利合计', '增值税利润差', '税费', '毛利', '购买方式', '金融类型', '金融性质', '返利系数', '贷款金额',
             '贷款期限', '金融返利', '厂家贴息金额', '经销商贴息金额', '金融税费', '金融服务费', '金融毛利', '上牌费',
             '上牌成本', '上牌毛利', '二手车返利金额', '置换服务费', '促销费用', '装饰成本', '保养升级成本',
             '装饰赠送合计', '其他成本', '返介绍费', '回扣款', '代开票支付费用', '调拨费', '票据事务费-公司',
             '政府返回区补', '质损赔付金额', '单车毛利', '开票门店', '调出车', '装饰收入']
        ].copy()  # 关键：加 .copy() 生成副本

        # 3. 修复贷款期限：先处理 'None' 字符串+非数字值，再转int
        df_sales_cyy['销售日期'] = pd.to_datetime(df_sales_cyy['销售日期'], format='mixed', errors='coerce')
        df_sales_lock['销售日期'] = df_sales_lock['销售日期'].apply(standardize_date)

        # 处理贷款期限：替换'None'为NaN→处理逗号→转数值（无法转的设为NaN）→填充0→转int
        df_sales_cyy['贷款期限'] = df_sales_cyy['贷款期限'].apply(
            lambda x: np.nan if str(x).strip().lower() == 'none'  # 替换'None'为NaN
            else (str(x).split(',')[0] if isinstance(x, str) and ',' in str(x) else x))
        df_sales_cyy['贷款期限'] = pd.to_numeric(df_sales_cyy['贷款期限'], errors='coerce')  # 非数字转NaN
        df_sales_cyy['贷款期限1'] = df_sales_cyy['贷款期限'].fillna(0).astype(int)  # 填充0后转int
        df_sales_cyy['贷款期限1'] = df_sales_cyy['贷款期限1'].astype(str) + '期'

        df_sales_lock['销售日期'] = df_sales_lock['销售日期'].apply(convert_date)
        df_sales_lock = df_sales_lock[df_sales_lock['销售日期'] < '2025-04-01'].copy()  # 切片后加copy
        df_sales_cyy = df_sales_cyy.reset_index(drop=True)
        df_sales_lock = df_sales_lock.reset_index(drop=True)
        df_combined = pd.concat([df_sales_cyy, df_sales_lock], axis=0, join='outer', ignore_index=True)
        df_combined['车辆信息'] = np.where(df_combined['所属团队'] == '调拨',df_combined['车系'] + " " + df_combined['车辆配置'],df_combined['车辆信息'])
        df_combined['所属团队'] = np.where(df_combined['所属团队'] == '调拨', "其他", df_combined['所属团队'])
        df_combined['金融类型'] = np.where(df_combined['所属团队'] == '其他', '调出车', df_combined['金融类型'])
        df_combined['调出车'] = np.where(df_combined['所属团队'] == '其他', "是", "")
        df_combined['车辆配置'] = df_combined['车辆配置'].fillna(df_combined['车型'])
        df_combined['收款日期'] = np.where((df_combined['二手车返利金额'] > 0) | (df_combined['二手车返利金额'] < 0),df_combined['收款日期'].fillna(df_combined['销售日期']),df_combined['收款日期'])
        df_combined = pd.merge(df_combined, self.team_belongs[['公司名称', '服务网络']], on='公司名称', how='left')
        return df_combined

    # 合并二手车
    # 输入（二手车.csv） 和 （二手车台账.csv）
    # 输出：二手车1.csv
    def concat_newold_Ers_dashboad(self):
        # 切片后加.copy()避免视图警告
        df_Ers_lock = self.df_Ers_lock[['收购时间', '新车客户姓名', '联系电话', '旧车客户姓名', '旧车品牌', '收购价格', '二手车返利','二手车返利到账时间', '销售顾问', '归属团队']].copy()  # 关键修复：加copy
        # 切片后加.copy()避免视图警告
        df_Ers = self.df_Ers[['评估门店', '客户', '手机', '置换客户名称', '车型', '成交日期', '成交金额', '其他费用', '线索提供人','录入日期']].copy()  # 关键修复：加copy

        df_Ers.rename(columns={
            '评估门店': '归属团队',
            '客户': '旧车客户姓名',
            '手机': '联系电话',
            '置换客户名称': '新车客户姓名',
            '车型': '旧车品牌',
            '成交日期': '二手车返利到账时间',
            '成交金额': '收购价格',
            '其他费用': '二手车返利',
            '线索提供人': '销售顾问',
            '录入日期': '收购时间'}, inplace=True)
        df_combined = pd.concat([df_Ers_lock, df_Ers], axis=0, join='outer', ignore_index=True)
        return df_combined

    # 合并销售表
    # 输入：销售.csv 和 数据库的sales_invoice_data
    # 输出：销售1.csv
    def concat_newold_Sales_dashboad1(self):
        # 切片后加.copy()避免视图警告
        df_sales_lock1 = self.df_sales_lock1[
            ['城市', '车架号', '车系', '车型', '外饰颜色', '指导价', '到库日期', '销售顾问', '所属团队',
             '匹配定单归属门店', '客户姓名', '销售日期', '提货价', '定单日期', '定金金额', '当月定卖', '服务网络']
        ].copy()  # 关键修复：加copy
        # 切片后加.copy()避免视图警告
        df_sales_cyy1 = self.df_sales_cyy1[
            ['服务网络', '车架号', '车系', '车型', '车辆配置', '外饰颜色', '定金金额', '指导价', '提货价',
             '匹配定单归属门店', '到库日期', '定单日期', '销售日期', '所属团队', '销售顾问', '客户姓名']
        ].copy()  # 关键修复：加copy

        df_sales_cyy1 = df_sales_cyy1[~((df_sales_cyy1['所属团队'].isin(list(['调拨', '其他', '二手车返利']))))].copy()  # 切片后加copy
        df_inventorys_cyy = self.df_inventorys_cyy[['车架号', '到库日期']].copy()  # 切片后加copy
        df_inventorys_cyy.columns = ['车架号', '到库日期1']

        df_sales_cyy1['定单日期'] = pd.to_datetime(df_sales_cyy1['定单日期'], errors='coerce')
        df_sales_cyy1['销售日期'] = pd.to_datetime(df_sales_cyy1['销售日期'], errors='coerce')
        df_sales_cyy1['当月定卖'] = np.where(
            (df_sales_cyy1['定单日期'].dt.year == df_sales_cyy1['销售日期'].dt.year) & (
                    df_sales_cyy1['定单日期'].dt.month == df_sales_cyy1['销售日期'].dt.month), 1, 0)

        df_sales_lock1['销售日期'] = pd.to_datetime(df_sales_lock1['销售日期'], errors='coerce')
        df_sales_lock1 = df_sales_lock1[df_sales_lock1['销售日期'] < '2025-04-01'].copy()  # 切片后加copy
        df_sales_lock1 = df_sales_lock1[
            ['城市', '车架号', '车系', '车型', '外饰颜色', '指导价', '到库日期', '销售顾问', '所属团队',
             '匹配定单归属门店', '客户姓名', '销售日期', '提货价', '定单日期', '定金金额', '当月定卖', '服务网络']
        ].copy()  # 切片后加copy

        df_combined = pd.concat([df_sales_cyy1, df_sales_lock1], axis=0, join='outer', ignore_index=True)
        df_combined = pd.merge(df_combined, df_inventorys_cyy, on='车架号', how='left')
        df_combined['到库日期'] = df_combined['到库日期'].fillna(df_combined['到库日期1'])
        df_combined = df_combined[
            ['服务网络', '车架号', '车系', '车型', '车辆配置', '外饰颜色', '定金金额', '指导价', '提货价',
             '匹配定单归属门店', '到库日期', '定单日期', '销售日期', '所属团队', '销售顾问', '客户姓名', '当月定卖',
             '城市']
        ].copy()  # 切片后加copy

        df_combined['车辆配置'] = df_combined['车辆配置'].fillna(df_combined['车型'])
        return df_combined

    # 合并退订表
    # 输入：退定.csv 和 数据库的tuiding_data
    # 输出：退订1.csv 和 所有定单1.csv
    def concat_newold_Tuis_dashboad(self):
        df_books = self.concat_newold_Books_dashboad()
        # 切片后加.copy()避免视图警告
        df_tuis_lock = self.df_tuis_lock[['定单日期', '车系', '外饰颜色', '车型', '销售顾问', '所属团队', '定单归属门店', '客户姓名', '联系电话','退定日期', '非退定核算']].copy()  # 关键修复：加copy
        # 切片后加.copy()避免视图警告
        df_tuis_cyy = self.df_tuis_cyy[['订单门店', '业务渠道', '销售人员', '订单日期', '车系', '车型', '外饰颜色', '配置', '主播人员', '客户名称', '客户电话','作废类型', '退订原因', '退定日期', '非退定核算']].copy()  # 关键修复：加copy

        df_tuis_cyy = df_tuis_cyy[~df_tuis_cyy['作废类型'].isin(['错误录入','重复录入'])]
        df_tuis_cyy.rename(columns={'订单门店': '定单归属门店', '订单日期': '定单日期', '销售人员': '销售顾问','客户名称': '客户姓名', '客户电话': '联系电话', '业务渠道': '所属团队'},inplace=True)

        df_books['非退定核算'] = -1
        df_tuis_lock['退定日期'] = pd.to_datetime(df_tuis_lock['退定日期'], errors='coerce')
        df_tuis_lock = df_tuis_lock[df_tuis_lock['退定日期'] < '2025-04-01'].copy()  # 切片后加copy

        df_combined = pd.concat([df_tuis_cyy, df_tuis_lock], axis=0, join='outer', ignore_index=True)
        df_dings_all = pd.concat([df_books, df_combined], axis=0, join='outer', ignore_index=True)
        # 修复FutureWarning：fillna后加infer_objects
        df_dings_all['有效定单核算日期'] = df_dings_all['退定日期']
        df_dings_all['有效定单核算日期'] = df_dings_all['有效定单核算日期'].fillna(df_dings_all['定单日期'])
        return df_combined, df_dings_all

    # 合并采购表
    # 输入：数据库的inventory_data
    # 输出：库存1.csv
    def concat_newold_Inventorys_dashboad(self):
        # 切片后加.copy()避免视图警告
        df_inventorys_cyy = self.df_inventorys_cyy[
            ['采购订单号', '归属系统', '车系', '车型', '颜色', '配置', '车架号', '指导价', '提货价', '生产日期',
             '合格证状态', '发车日期', '到库日期', '库存天数', '车辆状态', '操作日期', '调入类型']
        ].copy()  # 关键修复：加copy

        df_inventorys_cyy.rename(columns={'库存天数': '库存时间', '归属系统': '归属系统1', '颜色': '外饰颜色'},inplace=True)
        df_inventorys_cyy['库存时长分类'] = df_inventorys_cyy.apply(self.classify_inventory_duration, axis=1)

        plan_date_map = (
            pd.concat([
                self.df_plan_date_get[['采购订单号', '计划日期']].rename(columns={'采购订单号': '采购订单号'}).copy(),
                self.df_plan_date_get[['销服订单号', '计划日期']].rename(columns={'销服订单号': '采购订单号'}).copy(),
                self.df_plan_date_get[['采购改单编号', '计划日期']].rename(columns={'采购改单编号': '采购订单号'}).copy()
            ]).dropna(subset=['采购订单号']).drop_duplicates(subset=['采购订单号']))

        df_inventorys_cyy0 = pd.merge(df_inventorys_cyy,plan_date_map,on='采购订单号',how='left')
        df_cleaned = df_inventorys_cyy0.dropna(subset=['到库日期', '计划日期']).copy()  # 切片后加copy
        df_cleaned['计划日期'] = df_cleaned['计划日期'].fillna(df_cleaned['发车日期'])
        df_cleaned['到库日期'] = pd.to_datetime(df_cleaned['到库日期'])
        df_cleaned['计划日期'] = pd.to_datetime(df_cleaned['计划日期'])
        df_cleaned['采购提前期'] = (df_cleaned['到库日期'] - df_cleaned['计划日期']).dt.days + 1
        df_inventorys_cyy0 = df_inventorys_cyy0.join(df_cleaned['采购提前期'])
        return df_inventorys_cyy0

    # 合并订单表
    # 输入：数据库的order_data
    # 输出：定车1.csv
    def concat_newold_Books_dashboad(self):
        # 切片后加.copy()避免视图警告
        df_books_cyy = self.df_books_cyy[['车架号', '订单日期', '定单日期', '销售人员', '定金金额', '审批状态', '定单归属门店', '所属团队', '车系', '外饰颜色', '车型', '配置', '定单状态', '主播人员', '联系电话', '联系电话2']].copy()  # 关键修复：加copy
        # 切片后加.copy()避免视图警告
        df_books_lock = self.df_books_lock[['定单日期', '车系', '外饰颜色', '车型', '车架号', '定单状态', '销售顾问', '所属团队', '定单归属门店','定金金额']].copy()  # 关键修复：加copy

        df_books_cyy.rename(columns={'订单日期': '定单日期', '定单日期': '订金日期', '销售人员': '销售顾问'},inplace=True)
        df_books_cyy = df_books_cyy[(df_books_cyy['审批状态'] == '审核通过')].copy()  # 切片后加copy

        df_books_lock['定单日期'] = pd.to_datetime(df_books_lock['定单日期'], errors='coerce')
        df_books_lock = df_books_lock[df_books_lock['定单日期'] < '2025-04-01'].copy()  # 切片后加copy

        df_combined = pd.concat([df_books_cyy, df_books_lock], axis=0, join='outer', ignore_index=True)
        df_combined['配置'] = df_combined['配置'].fillna(df_combined['车型'])
        return df_combined

    # 合并精品表
    # 输入：精品销售.csv 和 数据库的jingpin_data
    # 输出：精品销售1.csv
    # def concat_newold_jingpins_dashboad(self):
    #     # 切片后加.copy()避免视图警告
    #     df_jingpins_cyy = self.df_jingpins_cyy[['订单门店', '开票日期', '收款日期', '精品销售人员', '车架号', '车系', '客户名称', '联系电话', '物资明细', '销售总金额','总成本', '毛利润', '总次数']].copy()  # 关键修复：加copy
    #     # 切片后加.copy()避免视图警告
    #     df_jingpins_lock = self.df_jingpins_lock[['精品销售日期', '精品销售人员', '新车销售门店', '车型', '车架号', '客户姓名', '电话号码', '销售总金额','总成本', '毛利润', '总次数']].copy()  # 关键修复：加copy
    #
    #     team_sup = self.team_belongs[['公司名称', '板块']].copy()  # 切片后加copy
    #     service_net = self.car_belongs[['车系', '服务网络']].copy()  # 切片后加copy
    #
    #     df_jingpins_cyy.rename(columns={'收款日期': '精品销售日期', '订单门店': '新车销售门店', '联系电话': '电话号码','客户名称': '客户姓名'},inplace=True)
    #     df_jingpins_cyy['精品销售日期'] = pd.to_datetime(df_jingpins_cyy['精品销售日期'], errors='coerce')
    #     df_jingpins_lock['精品销售日期'] = pd.to_datetime(df_jingpins_lock['精品销售日期'], errors='coerce')
    #
    #     df_jingpins_lock = pd.merge(df_jingpins_lock, team_sup, how='left', left_on='新车销售门店',right_on='公司名称').copy()  # merge后加copy
    #     df_jingpins_cyy = df_jingpins_cyy[df_jingpins_cyy['精品销售日期'] >= '2025-04-01'].copy()  # 切片后加copy
    #     df_jingpins_lock1 = df_jingpins_lock[df_jingpins_lock['精品销售日期'] < '2025-04-01'].copy()  # 切片后加copy
    #     df_jingpins_lock2 = df_jingpins_lock[df_jingpins_lock['板块'] == '西河'].copy()  # 切片后加copy
    #
    #     df_combined = pd.concat([df_jingpins_cyy, df_jingpins_lock1, df_jingpins_lock2], axis=0, join='outer',ignore_index=True)
    #     df_combined = pd.merge(df_combined, service_net, how='left', on='车系').copy()  # merge后加copy
    #     df_combined['新车销售门店'] = np.where(df_combined['新车销售门店'] == '直播基地',df_combined['服务网络'] + '-' + df_combined['新车销售门店'],df_combined['新车销售门店'])
    #     return df_combined

    def concat_newold_jingpins_dashboad(self):
        df_jingpins_cyy = self.df_jingpins_cyy[['单据类型', '订单门店', '开票日期', '收款日期', '最早收款日期', '精品销售人员', '车架号', '车系', '客户名称', '联系电话', '物资明细', '销售总金额', '总成本', '毛利润', '总次数']]
        df_jingpins_lock = self.df_jingpins_lock[['精品销售日期', '精品销售人员', '新车销售门店', '车型', '车架号', '客户姓名', '电话号码', '销售总金额','总成本', '毛利润', '总次数']]
        team_sup = self.team_belongs[['公司名称', '板块']]
        service_net = self.car_belongs[['车系', '服务网络']]
        df_jingpins_cyy.rename(columns={'最早收款日期': '精品销售日期', '订单门店': '新车销售门店', '联系电话': '电话号码','客户名称': '客户姓名'}, inplace=True)
        df_jingpins_cyy['精品销售日期'] = pd.to_datetime(df_jingpins_cyy['精品销售日期'], format='mixed')
        df_jingpins_lock = df_jingpins_lock.copy()
        df_jingpins_lock['精品销售日期'] = pd.to_datetime(df_jingpins_lock['精品销售日期'], format='mixed')
        df_jingpins_lock = pd.merge(df_jingpins_lock, team_sup, how='left', left_on='新车销售门店', right_on='公司名称')
        # 筛选 df_sales_lock 中日期在 2025 年 4 月 1 日之前的数据
        df_jingpins_cyy = df_jingpins_cyy[df_jingpins_cyy['精品销售日期'] >= '2025-04-01']
        df_jingpins_lock1 = df_jingpins_lock[df_jingpins_lock['精品销售日期'] < '2025-04-01']
        df_jingpins_lock2 = df_jingpins_lock[df_jingpins_lock['板块'] == '西河']
        # df_jingpins_locks = pd.concat([df_jingpins_lock1, df_jingpins_lock2], axis=0, join='outer', ignore_index=True)
        df_combined = pd.concat([df_jingpins_cyy, df_jingpins_lock1, df_jingpins_lock2], axis=0, join='outer',ignore_index=True)
        df_combined = pd.merge(df_combined, service_net, how='left', on='车系')
        df_combined['新车销售门店'] = np.where(df_combined['新车销售门店'] == '直播基地',df_combined['服务网络'] + '-' + df_combined['新车销售门店'],df_combined['新车销售门店'])
        return df_combined

    # 合并三方台账表
    # 输入：三方台账.csv 和 数据库的debit_data
    # 输出：三方台账1.csv
    def concat_newold_Debits_dashboad(self):
        # 切片后加.copy()避免视图警告
        df_debits_cyy = self.df_debits_cyy[
            ['合格证门店', '采购订单号', '车源门店', '开票日期', '保证金比例', '到期日期', '开票银行', '合格证号',
             '车架号', '提货价', '赎证日期', '赎证款', '是否赎证']
        ].copy()  # 关键修复：加copy
        # 切片后加.copy()避免视图警告
        df_debits_lock = self.df_debits_lock[
            ['订购日期', '采购订单号', '车架号', '赎证日期', '提货价', '赎证款', '保证金比例', '开票银行', '开票日期',
             '到期日期', '是否赎证', '最新到期日期', '归属系统1']
        ].copy()  # 关键修复：加copy

        df_debits_cyy.rename(columns={'合格证门店': '归属系统1'}, inplace=True)
        df_debits_cyy['最新到期日期'] = df_debits_cyy['到期日期']
        df_debits_cyy['是否赎证'] = df_debits_cyy['是否赎证'].astype('int')
        df_debits_lock['是否赎证'] = df_debits_lock['是否赎证'].astype('int')
        df_debits_lock = df_debits_lock[df_debits_lock['是否赎证'] == 1].copy()  # 切片后加copy

        df_debits_cyy['开票日期'] = pd.to_datetime(df_debits_cyy['开票日期'], errors='coerce')
        df_debits_cyy0 = df_debits_cyy[(df_debits_cyy['开票日期'] >= '2025-04-01') & (df_debits_cyy['是否赎证'] == 1)].copy()  # 切片后加copy
        df_debits_cyy1 = df_debits_cyy[df_debits_cyy['是否赎证'] == 0].copy()  # 切片后加copy

        df_combined = pd.concat([df_debits_cyy1, df_debits_cyy0, df_debits_lock], axis=0, join='outer',ignore_index=True)
        df_combined['开票银行'] = np.where(df_combined['开票银行'].str.contains('光大'), '光大',
                                           np.where(df_combined['开票银行'].str.contains('中信'), '中信',
                                                    np.where(df_combined['开票银行'].str.contains('兴业'), '兴业',
                                                             np.where(df_combined['开票银行'].str.contains('平安'),'平安',
                                                                      np.where(df_combined['开票银行'].str.contains('交通'),'交通',
                                                                                np.where(df_combined['开票银行'].str.contains('工商'), '工商',
                                                                                        np.where(df_combined['开票银行'].str.contains('招商'), '招商', df_combined['开票银行'])))))))
        return df_combined

    # 合并未售锁车表
    # 输入：未售订单.csv
    # 输出：未售锁车.csv
    def concat_unsoldBook_dashboad(self):
        df_unsoldBook_cyy = self.df_books_unsold.copy()  # 读取后加copy
        column_mapping = {
            "BillStateName": "订单状态",
            "OrganizeName": "订单公司",
            "CarBrandName": "品牌",
            "CarSeriesName": "车系",
            "CarModelsName": "整车型号",
            "CarStyleName": "款式",
            "CarDisposeName": "配置",
            "BodyColorName": "外饰颜色",
            "ColloCation": "录入日期",
            "RegistrationTime": "录入日期",
            "EstimatedDeliveryDate": "预交日期",
            "BillCode": "合同单号",
            "DepartName": "部门",
            "SalesConsultantName": "销售人员",
            "DeliveryPersonName": "交付专员",
            "TVanchorName": "主播人员",
            "CusName": "客户",
            "CusPhone": "客户电话",
            "CusPhone2": "客户电话2",
            "LYOrganizeName": "来源公司",
            "FrameNumber": "车架号",
            "EngineNumber": "发动机",
            "OrderCode": "计划单号",
            "LockStartTime": "锁库日期",
            "LockEndTime": "到期日期",
            "ReportingTime": "汇报日期",
            "InvoiceDate": "开票日期",
            "DeliveryTime": "销售日期",
            "DRemarks": "订单备注",
            "CRemarks": "车辆备注",
            "LogisticsRemarks": "物流备注",
            "DistributionChannelName": "来源",
            "CusDistribuName": "客户来源",
            "InStoreDate": "入库日期",
            "RegisterDate": "生产日期",
            "Weekandweek": "车辆状态",
            "ApprovalTypeName": "ApprovalTypeName",
            "CertOrganizeName": "合格证门店",
            "OrgDeposit": "门店收取",
            "IsOrgDeposit": "门店收取状态",
            "FacDeposit": "厂家代收",
            "IsFacDeposit": "厂家代收状态",
            "MortgageState": "按揭状态",
            "OperaStatusName": "状态",
            "PublicCode": "公共单号",
            "Islogistics": "物流创建",
            "PaymentWay": "购车方式",
            "IsCoupon": "发券状态",
            "DepositType": "订金状态"
        }

        def map_status(x):
            if x == '计划':
                return '未发'
            elif x == '在途':
                return '在途'
            elif pd.notnull(x):
                return '在库'
            else:
                return None

        df_unsoldBook_cyy = df_unsoldBook_cyy.rename(columns=column_mapping)
        df_unsoldBook_cyy = df_unsoldBook_cyy[~df_unsoldBook_cyy['锁库日期'].isnull()].copy()  # 切片后加copy
        df_unsoldBook_cyy['车辆状态'] = df_unsoldBook_cyy['车辆状态'].apply(map_status)
        df_unsoldBook_cyy = df_unsoldBook_cyy[['订单公司', '车系', '外饰颜色', '整车型号', '配置', '销售人员', '交付专员', '主播人员', '客户', '客户电话','客户电话2', '车架号', '计划单号', '锁库日期', '来源', '入库日期', '车辆状态']].copy()  # 切片后加copy
        df_unsoldBook_cyy.columns = ['公司名称', '车系', '外饰颜色', '车型', '配置', '销售人员', '交付专员', '主播人员','客户姓名', '联系电话', '联系电话2', '车架号', '计划单号', '锁库日期', '所属团队','入库日期', '车辆状态']
        df_unsoldBook_cyy = df_unsoldBook_cyy[df_unsoldBook_cyy['车辆状态'].isin(['在库', '在途'])]
        return df_unsoldBook_cyy

    # 合并市场费用表
    # 输入：投放费用.csv
    # 输出：市场费用.csv
    def clean_yingxiao(self):
        df_yingxiao = self.df_yingxiao.copy()  # 读取后加copy
        df_yingxiao = self.add_month_end_date(df_yingxiao)
        df_yingxiao = df_yingxiao[df_yingxiao['项目分类'] != '随车'].copy()  # 切片后加copy
        df_yingxiao['费用合计'] = pd.to_numeric(df_yingxiao['费用合计'], errors='coerce').fillna(0)
        df_yingxiao1 = df_yingxiao.groupby(['日期', '归属门店']).agg({'费用合计': 'sum'}).reset_index()
        return df_yingxiao1

    # 合并人工效能表
    # 输入：2025年人工效能分析表-销售-新.xlsx
    # 输出：销售薪资.csv
    def clean_salary(self):
        df_salary = self.df_salary.copy()  # 读取后加copy
        excel_start_date = pd.Timestamp('1899-12-30')
        df_salary['年月'] = df_salary['年月'].apply(lambda x: excel_start_date + pd.Timedelta(days=x))
        df_salary['年月'] = pd.to_datetime(df_salary['年月'])
        df_salary['日期'] = df_salary['年月'] + pd.offsets.MonthEnd(0)
        cols_to_convert = ['月薪酬', '月社保']
        df_salary[cols_to_convert] = df_salary[cols_to_convert].apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0))
        df_salary['总薪酬'] = df_salary['月薪酬'] + df_salary['月社保']
        df_salary['总薪酬'] = pd.to_numeric(df_salary['总薪酬'], errors='coerce').fillna(0).astype(float)
        df_salary1 = df_salary.groupby(['日期', '门店']).agg({'总薪酬': 'sum'}).reset_index()
        return df_salary1

    def run(self):
        chexi = self.Car_belongs()
        df_yingxiao = self.clean_yingxiao()
        df_salary = self.clean_salary()
        df_sales = self.concat_newold_Sales_dashboad()
        df_sales1 = self.concat_newold_Sales_dashboad1()
        df_books = self.concat_newold_Books_dashboad()
        df_inventorys = self.concat_newold_Inventorys_dashboad()
        df_jingpins = self.concat_newold_jingpins_dashboad()
        df_tuis, df_dings_all = self.concat_newold_Tuis_dashboad()
        df_debits = self.concat_newold_Debits_dashboad()
        df_Ers = self.concat_newold_Ers_dashboad()
        df_unsoldBook = self.concat_unsoldBook_dashboad()
        df_inventory_lock = self.df_inventorys_lock[['车系', '配置', '车型', '颜色']].drop_duplicates().copy()  # 切片后加copy

        df_salary['总薪酬'] = pd.to_numeric(df_salary['总薪酬'], errors='coerce').fillna(0).astype(float)
        df_yingxiao['费用合计'] = pd.to_numeric(df_yingxiao['费用合计'], errors='coerce').fillna(0).astype(float)

        # 单独车系智驾操作
        df_sales['提货价'] = df_sales['提货价'].astype('float')
        df_sales['车系'] = np.where((df_sales['车系'] == "2025款海鸥") & (df_sales['提货价'] == 65800),'2025款 海鸥', df_sales['车系'])

        # 时间判断逻辑
        now = datetime.now()
        if now.hour < 20:
            df_sales['销售日期'] = df_sales['销售日期'].fillna(df_sales['收款日期'])
            df_sales['销售日期'] = pd.to_datetime(df_sales['销售日期'], format='mixed', errors='coerce')
            df_sales = df_sales[df_sales['销售日期'] < pd.Timestamp.today().normalize()].copy()  # 切片后加copy

        # 销售顾问在职天数计算
        df1_ = df_sales.copy()
        df1_['销售日期'] = pd.to_datetime(df1_['销售日期'], format='mixed', errors='coerce')
        df1_['在职天数'] = df1_.groupby('销售人员')['销售日期'].transform(lambda x: (x.max() - x.min()).days)
        df1_sorted = df1_.sort_values(by=['销售人员', '销售日期'], ascending=[True, False]).copy()  # 排序后加copy
        latest_stores = df1_sorted.drop_duplicates(subset='销售人员', keep='first').copy()  # drop_duplicates后加copy
        latest_stores = latest_stores[['销售人员', '公司名称', '在职天数']].copy()  # 切片后加copy
        latest_stores['在职月份'] = np.ceil(latest_stores['在职天数'] / 30)
        latest_stores['销售顾问辅助列'] = latest_stores['公司名称'].astype(str) + "-" + latest_stores['销售人员'].astype(str)

        # 销售台账匹配非智驾
        df_sales1['提货价'] = df_sales1['提货价'].astype('float')
        df_sales1['车系'] = np.where((df_sales1['车系'] == "2025款海鸥") & (df_sales1['提货价'] == 65800),'2025款 海鸥', df_sales1['车系'])
        df_sales1 = pd.merge(df_sales1, chexi[['车系', '类型']], on=['车系'], how='left').copy()  # merge后加copy

        # 销售毛利匹配非智驾
        df_sales['提货价'] = df_sales['提货价'].astype('float')
        df_sales['车系'] = np.where((df_sales['车系'] == "2025款海鸥") & (df_sales['提货价'] == 65800),'2025款 海鸥', df_sales['车系'])
        df_sales = pd.merge(df_sales, chexi[['车系', '类型']], on=['车系'], how='left').copy()  # merge后加copy

        # 定车匹配非智驾
        df_books = pd.merge(df_books, chexi[['车系', '类型']], on=['车系'], how='left').copy()  # merge后加copy

        # 库存匹配非智驾
        df_inventorys['提货价'] = df_inventorys['提货价'].astype('float')
        df_inventorys['车系'] = np.where((df_inventorys['车系'] == "2025款海鸥") & (df_inventorys['提货价'] == 65800),'2025款 海鸥', df_inventorys['车系'])
        df_inventorys = pd.merge(df_inventorys, chexi[['车系', '类型']], on=['车系'], how='left').copy()  # merge后加copy

        df_inventorys['提货价'] = df_inventorys['提货价'].astype('float')
        df_inventorys['车系'] = np.where((df_inventorys['车系'] == "2025款海鸥") & (df_inventorys['提货价'] == 65800),'2025款 海鸥', df_inventorys['车系'])

        # 切片后加copy
        df_carseris = pd.concat(
            [self.df_sales_cyy[['车系', '车型', '车辆配置']].copy(),
             self.df_books_cyy[['车系', '车型', '配置']].copy(),
             self.df_tuis_cyy[['车系', '车型', '配置']].copy(),
             self.df_inventorys_cyy[['车系', '车型', '配置']].copy()]
        )

        replacement_rules = {
            '豹5-': '',
            '豹8-': '',
            '宋L EV智驾版': '',
            'Z9GT': '',
            'DM-i': '',
            '23款混动': '',
            '钛3-': '',
        }

        def apply_replacements(s):
            if isinstance(s, str):
                for pattern, replacement in replacement_rules.items():
                    s = s.replace(pattern, replacement)
            return s

        df_books['配置'] = np.where(df_books['配置'].isnull(), df_books['车型'], df_books['配置'])
        df_dings_all['配置'] = np.where(df_dings_all['配置'].isnull(), df_dings_all['车型'], df_dings_all['配置'])
        df_tuis['配置'] = np.where(df_tuis['配置'].isnull(), df_tuis['车型'], df_tuis['配置'])

        # 切片后加copy
        df1 = df_sales[['车系', '车型', '外饰颜色']].copy()
        df1.columns = ['车系', '配置', '外饰颜色']
        df2 = df_books[['车系', '车型', '外饰颜色']].copy()
        df2.columns = ['车系', '配置', '外饰颜色']
        df3 = df_tuis[['车系', '车型', '外饰颜色']].copy()
        df3.columns = ['车系', '配置', '外饰颜色']
        df4 = df_inventorys[['车系', '车型', '外饰颜色']].copy()
        df4.columns = ['车系', '配置', '外饰颜色']
        df_inventory_lock.columns = ['车系', '车辆配置', '车型', '外饰颜色']

        df_carseris = pd.concat(
             [df_sales[['车系', '车辆配置', '车型', '外饰颜色']].copy(),df_inventory_lock.copy(),
             df1.copy(), df2.copy(), df3.copy(), df4.copy(),
             df_books[['车系', '车型', '配置', '外饰颜色']].copy(),
             df_tuis[['车系', '车型', '配置', '外饰颜色']].copy(),
             df_inventorys[['车系', '车型', '配置', '外饰颜色']].copy()])
        df_carseris['配置'] = df_carseris['配置'].fillna(df_carseris['车辆配置'])
        df_carseris['车型'] = df_carseris['车型'].fillna(df_carseris['配置'])
        df_carseris['辅助'] = df_carseris['车系'].astype(str) + df_carseris['配置'].astype(str)

        def standardize_text(text):
            if pd.isna(text):
                return ''
            text = str(text).strip().lower()
            text = re.sub(r'\s+', ' ', text)
            text = text.replace('km', 'KM').replace('kM', 'KM').replace('Km', 'KM').replace('plus', 'Plus').replace('e2', 'E2')
            return text

        df_carseris['辅助'] = df_carseris['辅助'].apply(standardize_text)
        df_carseris = df_carseris.drop_duplicates(subset=['辅助']).copy()  # drop_duplicates后加copy
        df_carseris = df_carseris[['车系', '车型', '配置', '外饰颜色']].copy()  # 切片后加copy
        df_carseris['车型'] = df_carseris['车型'].str.replace('\n', ' ', regex=True)
        df_carseris['车型'] = df_carseris['车型'].str.strip()
        df_carseris = df_carseris[~df_carseris['车系'].isin(['二手车返利', '调拨'])].copy()  # 切片后加copy

        # 所有输出路径加r
        df_sales.to_csv(r'E:\powerbi_data\看板数据\dashboard\销售毛利1.csv', index=False)
        df_books.to_csv(r'E:\powerbi_data\看板数据\dashboard\定车1.csv', index=False)
        df_inventorys.to_csv(r'E:\powerbi_data\看板数据\dashboard\库存1.csv', index=False)
        df_jingpins.to_csv(r'E:\powerbi_data\看板数据\dashboard\精品销售1.csv', index=False)
        df_tuis.to_csv(r'E:\powerbi_data\看板数据\dashboard\退订1.csv', index=False)
        df_dings_all.to_csv(r'E:\powerbi_data\看板数据\dashboard\所有定单1.csv', index=False)
        df_debits.to_csv(r'E:\powerbi_data\看板数据\dashboard\三方台账1.csv', index=False)
        df_sales1.to_csv(r'E:\powerbi_data\看板数据\dashboard\销售1.csv', index=False)
        df_Ers.to_csv(r'E:\powerbi_data\看板数据\dashboard\二手车1.csv', index=False)
        df_unsoldBook.to_csv(r'E:\powerbi_data\看板数据\dashboard\未售锁车.csv', index=False)
        df_salary.to_csv(r'E:\powerbi_data\看板数据\dashboard\销售薪资.csv', index=False)
        df_yingxiao.to_csv(r'E:\powerbi_data\看板数据\dashboard\市场费用.csv', index=False)
        df_carseris.to_csv(r'E:\powerbi_data\看板数据\dashboard\所有车系.csv', index=False)
        latest_stores.to_csv(r'E:\powerbi_data\看板数据\dashboard\辅助_销售顾问.csv', index=False)


if __name__ == "__main__":
    processor = update_dashboard()
    processor.run()