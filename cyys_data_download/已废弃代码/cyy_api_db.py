# -*- coding: utf-8 -*-
"""
    cyys.py
    爬取车易云商相关数据 - 数据库版本（重构优化版）
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import chardet
import requests
import json
import uuid
import os
from datetime import datetime, date
import pandas as pd
import logging
import time
import random
from DrissionPage import ChromiumOptions, ChromiumPage
from io import StringIO
import numpy as np
import re
from pymongo import MongoClient
from functools import lru_cache
import shutil
from sqlalchemy import create_engine, text
import sqlalchemy as sa

# 日志配置
log_dir = r"C:/Users/13111/code/logs/"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(message)s]',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{log_dir}/log_api{datetime.now().strftime('%Y_%m_%d')}.download_log", encoding='utf-8')
    ]
)

# 数据库配置
DB_URL = "postgresql+psycopg2://postgres:root@127.0.0.1:5432/cyys"

# 表名到数据框名称的映射
TABLE_TO_DF_MAP = {
    "car_sales_detail_invoice_with_delete": "车辆销售明细_开票日期",
    "inventory_car": "库存车辆查询", 
    "sold_inventory_with_delete": "库存车辆已售",
    "purchase_order": "计划车辆",
    "car_ticket": "汇票管理",
    "deal_order": "成交订单",
    "sale_preorder_with_delete": "衍生订单",
    "car_cost_management": "车辆成本管理",
    "decoration_order_with_delete": "装饰订单",
    "mortgage_business": "按揭业务",
    "abnormal_order": "作废订单",
    "package_sales_with_delete": "套餐销售",
    "car_shunting_settlement": "调车结算",
    "eval_deal_with_delete": "二手车成交", 
    "invoice_maintenance_with_delete": "开票维护",
    "insurance_business_with_delete": "保险业务",
    "sale_return_visit": "销售回访",
    "unsold_order": "未售订单"
}


class cyys:
    def __init__(self):
        self.tk = ""
        self._uuid = ""
        self.url = ""
        self.relogin_str = '{"loginstatus":-1,"Msg":"可能长时间没有操作，要继续使用请重新登录！"}'
        self.page = None
        self.df_vat = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='汉唐_增值税处理')
        self.list_companys = [
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
        # 初始化数据库连接
        self.engine = create_engine(DB_URL, pool_pre_ping=True)
        # 读取字段映射
        mapping_file = r'C:\Users\13111\code\cyys\字段对应.xlsx'
        if not os.path.exists(mapping_file):
            raise FileNotFoundError('字段对应.xlsx 不存在！')
        sheet1 = pd.read_excel(mapping_file, sheet_name='Sheet1')
        sheet1 = sheet1.dropna(subset=['数据库表名', '英文字段', '中文字段'])
        sheet1 = sheet1[['数据库表名', '英文字段', '中文字段']]
        self.col_mapping = (
            sheet1.groupby('数据库表名')
                  .apply(lambda x: x.set_index('英文字段')['中文字段'].to_dict())
                  .to_dict()
        )

    def load_data_from_db(self, table_name, condition=None):
        try:
            query = f"SELECT * FROM {table_name}"
            if condition:
                query += f" WHERE {condition}"
            df = pd.read_sql(query, self.engine)
            logging.info(f"从表 {table_name} 加载了 {len(df)} 条记录")
            return df
        except Exception as e:
            logging.error(f"从表 {table_name} 加载数据失败: {str(e)}")
            return pd.DataFrame()

    def rename_columns_using_mapping(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        if table_name not in self.col_mapping:
            logging.warning(f'表 {table_name} 在字段对应.xlsx 中未找到映射')
            return df
        mapper = self.col_mapping[table_name]
        exist = {k: v for k, v in mapper.items() if k in df.columns}
        if exist:
            df = df.rename(columns=exist)
            logging.info(f'表 {table_name} 重命名了 {len(exist)} 个字段')
        return df

    def load_all_data_from_db(self):
        data_dict = {}
        for table_name, df_name in TABLE_TO_DF_MAP.items():
            df = self.load_data_from_db(table_name)
            df = self.rename_columns_using_mapping(df, table_name)
            data_dict[df_name] = df
        return data_dict

    def send_md_to_person(self, number: str = "13111855638", msg: str = ""):
        try:
            data = {"touser": number, "msg": msg}
            res = requests.post('http://192.168.1.7/send_md_to_person', json=data, timeout=10)
            if res.status_code == 200:
                print(f"📢 通知发送成功")
            else:
                print(f"⚠️ 通知发送失败，状态码: {res.status_code}, 响应: {res.text}")
        except Exception as e:
            print(f"⚠️ 发送通知异常: {e}")
            with open("./logs/notify_fail.download_log", "a", encoding="utf-8") as f:
                f.write(f"{datetime.now()}: {msg}\n")

    def _to_numeric_safe(self, df, cols, fill_value=0):
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(fill_value)
        return df

    def _clean_deleted_records(self, df_dict):
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

    def _clean_insurance(self, df_insurance):
        df_insurance['保费总额'] = pd.to_numeric(df_insurance['保费总额'], errors='coerce').fillna(0)
        df_insurance['总费用_次数'] = df_insurance['保费总额'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        df_insurance.to_csv(
            fr"C:\Users\13111\Documents\WXWork\1688855282576011\WeDrive\成都永乐盛世\维护文件\新车保险台账-{datetime.now().year}.csv"
        )
        return df_insurance

    def _clean_used_cars(self, df_ershou):
        df_Ers = df_ershou[df_ershou['收款状态'] == '已收款'].copy()
        df_Ers.to_csv(r'C:\Users\13111\code\dashboard1\二手车.csv', index=False)
        return df_Ers
    def _clean_decoration_orders(self, df_decoration):
        df_decoration = df_decoration[
            # (df_decoration['是否无效'] == True) &
            (df_decoration['收款日期'] != "")&
            (df_decoration['收款日期'].notnull())
        ].copy()
        df_decoration = self._to_numeric_safe(df_decoration, ['销售合计','成本合计(含税)', '工时费','出/退/销数量'])
        df_decoration = df_decoration[~df_decoration['物资状态'].isin([
            '已退款','已退货','待退货','已换货','全退款','全退货','部分退款'
        ])]

        # === 赠送逻辑（保持不变）===
        condition_new = (df_decoration['单据类型'] == '新车销售')
        condition_other = (df_decoration['单据类型'].isin(['客户增购','换货销售','独立销售'])) & (df_decoration['销售合计'] == 0)
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

        # === 精品逻辑（按新规则聚合）===
        df_jingpin = df_decoration[~gift_mask].copy()
        df_jingpin = df_jingpin[~df_jingpin['销售顾问'].isin(['郑仁彬','刘红梅','衡珊珊','郝小龙'])].copy()
        df_jingpin['装饰赠送成本'] = df_jingpin[['成本合计(含税)','工时费']].sum(axis=1)

        # 构造物资明细（按订单编号）
        result_JP = df_jingpin.groupby('订单编号').apply(
            lambda x: ','.join(f"{name}*{qty}" for name, qty in zip(x['物资名称'], x['出/退/销数量']))
        ).reset_index(name='物资明细')

        df_jingpin = df_jingpin.merge(result_JP, on='订单编号', how='left')
        df_jingpin.rename(columns={'销售顾问': '精品销售人员'}, inplace=True)

        # 转换日期列（安全处理）
        df_jingpin['收款日期'] = pd.to_datetime(df_jingpin['收款日期'],format='mixed')
        df_jingpin['开票日期'] = pd.to_datetime(df_jingpin['开票日期'],format='mixed')

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
            '收款日期': join_dates,        # 拼接版：所有日期
            '客户名称': 'first',
            '联系电话': 'first',
            '物资明细': join_str,
            '装饰赠送成本': 'sum',
            '销售合计': 'sum',
            '出/退/销数量': 'sum'
        })

        # 新增最早收款日期（单独 min 聚合）
        earliest = grouped['收款日期'].min().reset_index()
        earliest.rename(columns={'收款日期': '最早收款日期'}, inplace=True)
        df_jingpin_result = df_jingpin_result.merge(earliest, on=['车架号', '精品销售人员'], how='left')
        df_jingpin_result['最早收款日期'] = df_jingpin_result['最早收款日期'].dt.strftime('%Y/%m/%d')

        # 最终列重命名与毛利计算
        df_jingpin_result.rename(columns={
            '装饰赠送成本': '总成本',
            '销售合计': '销售总金额',
            '出/退/销数量': '总次数'
        }, inplace=True)
        df_jingpin_result['毛利润'] = df_jingpin_result['销售总金额'] - df_jingpin_result['总成本']

        # 指定输出列顺序（含新字段）
        output_cols = [
            '单据类型', '订单门店', '开票日期', '收款日期', '最早收款日期', '精品销售人员',
            '车架号', '客户名称', '联系电话', '物资明细', '销售总金额', '总成本', '毛利润', '总次数'
        ]
        return df_decoration2, df_jingpin_result[output_cols]

    def _clean_service_packages(self, df_service):
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
        cols_to_convert = ['车辆成本_返介绍费','其他成本_退代金券','其他成本_退按揭押金']
        df_carcost[cols_to_convert] = df_carcost[cols_to_convert].apply(pd.to_numeric, errors='coerce').fillna(0)
        df_carcost.rename(columns={
            '车辆/订单门店': '公司名称',
            '车架号': '车架号',
            '车辆状态': '车辆状态',
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
        df_carcost['操作日期'] = pd.to_datetime(df_carcost['操作日期'], format='mixed')
        df_carcost.sort_values(by='操作日期', ascending=False, inplace=True)
        df_carcost.drop_duplicates(subset=['车架号'], keep='first', inplace=True)
        return df_carcost[[
            '公司名称','采购订单号','车架号','车辆状态','调整项','返介绍费','退成交车辆定金（未抵扣）',
            '政府返回区补','保险返利','终端返利','上牌成本','票据事务费-公司','代开票支付费用',
            '回扣款','退代金券','退按揭押金','退置换补贴保证金','质损赔付金额','其他成本','操作日期'
        ]]

    def _clean_loans(self, df_loan):
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
        df_loan['金融类型'] = np.where(
            df_loan['金融性质'].str.contains('非贴息'), '厂家非贴息贷',
            np.where(df_loan['金融性质'].str.contains('贴息'), '厂家贴息贷',
                     np.where(df_loan['金融方案'].isin(['交行信用卡中心五年两免-9%','建行5免2','5免2']), '无息贷', '非贴息贷'))
        )
        df_loan['返利系数'] = pd.to_numeric(df_loan['返利系数'].str.replace('%', ''), errors='coerce').fillna(0) / 100
        loan_cols = ['开票价','贷款金额','返利系数','金融返利','厂家贴息金额','经销商贴息金额','金融服务费']
        df_loan = self._to_numeric_safe(df_loan, loan_cols)
        df_loan['首付金额'] = df_loan['开票价'] - df_loan['贷款金额']
        df_loan['贷款期限'] = df_loan['贷款期限'].astype(str).apply(lambda x: re.sub(r'[\u4e00-\u9fa5]', '', x))
        df_loan['金融税费'] = df_loan['厂家贴息金额']/1.13*0.13*1.12 + df_loan['金融返利']/1.06*0.06*1.12
        df_loan['金融毛利'] = df_loan['金融返利'] - df_loan['经销商贴息金额'] - df_loan['金融税费']
        df_loan.sort_values(by=['车架号','收费状态'], ascending=True, inplace=True)
        df_loan.drop_duplicates(subset=['车架号'], keep='first', inplace=True)
        return df_loan

    def _clean_inventory_and_plan(self, df_inventory, df_inventory1, df_plan, df_debit, service_net, company_belongs):
        # 库存清洗
        df_inventory = self._rename_inventory(df_inventory)
        df_inventory1 = self._rename_inventory(df_inventory1)
        df_plan.rename(columns={'车型':'车系','整车型号':'车型','订单号':'采购订单号'}, inplace=True)
        df_plan = pd.merge(df_plan, df_debit[['采购订单号','提货价','开票银行','合格证门店','赎证日期','到期日期','保证金比例','赎证款']], on='采购订单号', how='left')
        df_plan['车辆状态'] = '未发'
        df_plan['开票银行'] = df_plan['开票银行'].fillna('公司')
        df_plan.rename(columns={'开票银行': '合格证状态', '门店': '归属系统'}, inplace=True)

        df_inventory_all = pd.concat([df_inventory, df_plan], axis=0, ignore_index=True)
        list_company = company_belongs['公司名称']
        df_inventory_all['调入类型'] = np.where(
            df_inventory_all['供应商'].isin(list_company),
            '内部调入',
            np.where(
                (~df_inventory_all['供应商'].isin(list_company)) & (df_inventory_all['供应商'] != '比亚迪') & (df_inventory_all['供应商'] != ""),
                '外部调入',
                None
            )
        )
        df_inventory_all = pd.merge(df_inventory_all, service_net[['车系', '服务网络']], how='left', on='车系')
        df_inventory_all['归属系统'] = np.where(
            df_inventory_all['归属系统'] == '直播基地',
            df_inventory_all['服务网络'] + '-' + df_inventory_all['归属系统'],
            df_inventory_all['归属系统']
        )
        return df_inventory_all, df_inventory, df_inventory1

    def _rename_inventory(self, df):
        df.rename(columns={
            '订单号':'采购订单号',
            '所属门店':'归属系统',
            '车系':'车系',
            '车型':'车型',
            '车架号':'车架号',
            '入库日期':'到库日期',
            '生产日期':'生产日期',
            '配车日期':'配车日期',
            '仓库地':'运输方式存放地点',
            '备注':'备注',
            '合格证':'合格证状态',
            '车辆状态':'车辆状态',
            '库存天数':'库存天数',
            '销售日期':'销售日期',
            '销售人员':'销售顾问',
            '订单客户':'客户姓名',
            '锁库日期':'锁库日期',
            '开票日期':'开票日期',
            '质损信息':'质损信息',
            '调拨日期':'调拨日期',
            '调拨记录':'调拨记录',
            '订单来源':'所属团队',
            '订单公司':'匹配定单归属门店',
            '合格证门店':'合格证门店',
            '赎证日期':'赎证日期',
            '出厂价格':'提货价',
            '厂家官价':'指导价'
        }, inplace=True)
        return df[[
            '车源门店','供应商','采购订单号','归属系统','匹配定单归属门店','合格证门店','所属团队','车系','车型','配置',
            '颜色','车架号','发动机号','指导价','提货价','生产日期','赎证日期','合格证状态','发车日期','到库日期',
            '库存天数','运输方式存放地点','车辆状态','调拨日期','调拨记录','锁库日期','销售日期','开票日期',
            '配车日期','销售顾问','客户姓名','质损信息','备注','操作日期'
        ]]

    def _clean_debit_and_merge(self, df_debit, df_carcost):
        df_debit.rename(columns={
            '车辆金额':'提货价','开票金额(含税)':'汇票金额','汇票开票日期':'开票日期','VIN码':'车架号',
            '计划单号':'采购订单号','开票银行':'开票银行','所属门店':'合格证门店','汇票到期日期':'到期日期',
            '首付比例':'保证金比例','赎证金额':'赎证款'
        }, inplace=True)
        df_debit['是否赎证'] = np.where(df_debit['是否结清'] == '已清', 1, 0)
        df_debit = df_debit[[
            '合格证门店', '车源门店', '开票日期', '保证金比例', '首付金额', '汇票金额', '到期日期',
            '开票银行', '汇票号', '合格证号', '采购订单号', '车架号', '提货价', '审核状态', '赎证日期', '赎证款',
            '首付单号', '赎证单号', '是否赎证','是否结清'
        ]]
        df_debit = pd.merge(df_debit, df_carcost[['采购订单号','车辆状态']], on='采购订单号', how='left')
        df_debit['车辆状态'] = df_debit['车辆状态'].fillna('未发')
        return df_debit

    def _clean_book_orders(self, df_books, df_books2, df_unsold, service_net):
        df_books.rename(columns={
            '订单日期': '订单日期',
            '计划单/车架号': '车架号',
            '订金日期': '定单日期',
            '开票日期': '销售日期',
            '订金状态': '订金状态',
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
        df_books = df_books[df_books['作废状态'] == False]

        df_books2['订单日期'] = pd.to_datetime(df_books2['订单日期'], format='mixed')
        df_books2.sort_values(by='订单日期', ascending=False, inplace=True)
        df_books2.rename(columns={'联系方式':'联系电话','联系方式2':'联系电话2'}, inplace=True)
        df_books2 = df_books2.drop_duplicates(subset=['车架号'], keep='first')
        df_sold = df_books2[['ID','联系电话','联系电话2','主播人员','车系','客户姓名','订单公司']].drop_duplicates()

        df_unsold.rename(columns={'客户电话':'联系电话','客户电话2':'联系电话2','客户':'客户姓名'}, inplace=True)
        df_unsold1 = df_unsold[['ID','联系电话','联系电话2','主播人员','车系','客户姓名','订单公司']]
        df_zhubolist = pd.concat([df_sold, df_unsold1], ignore_index=True).drop_duplicates()
        cols = ['联系电话', '联系电话2']
        df_zhubolist[cols] = (
            df_zhubolist[cols].replace('', pd.NA).fillna(0).astype('int64').astype('str').replace('0', '')
        )
        df_zhubolist['辅助列'] = df_zhubolist['联系电话'] + df_zhubolist['联系电话2'] + df_zhubolist['客户姓名'] + df_zhubolist['车系'] + df_zhubolist['订单公司']
        df_zhubolist = df_zhubolist.drop_duplicates(subset=['辅助列'], keep='first')

        df_dings = df_books[['ID','车架号', '订单日期', '定单日期','订金状态','审批状态', '销售人员', '销售日期', '定金金额', '定单归属门店', '所属团队', '车系', '外饰颜色','车型', '配置', '客户姓名','联系电话','联系电话2']]
        df_dings = pd.merge(df_dings, service_net[['车系', '服务网络']], how='left', on='车系')
        df_dings['定单归属门店'] = np.where(
            df_dings['定单归属门店'] == '直播基地',
            df_dings['服务网络'] + '-' + df_dings['定单归属门店'],
            df_dings['定单归属门店']
        )
        df_dings['辅助列'] = df_dings['联系电话'] + df_dings['联系电话2'] + df_dings['客户姓名'] + df_dings['车系'] + df_dings['定单归属门店']
        df_dings = pd.merge(df_dings, df_zhubolist[['辅助列','主播人员']], how='left', on='辅助列')
        df_dings['现定现交'] = np.where(
            (df_dings['定单日期'] == "") & (df_dings['销售日期'] != ""),
            '现定现交',
            np.where((df_dings['订金状态'] == "待收款") & (df_dings['定单日期'] != "") & (df_dings['销售日期'] != ""), '现定现交', None)
        )
        df_dings['定单状态'] = np.where((df_dings['销售日期']!=""), df_dings['销售日期'], None)
        df_dings['定金金额'] = np.where(df_dings['现定现交'] == '现定现交', 3000, df_dings['定金金额'])
        df_dings = df_dings.drop_duplicates()
        df_zhubo = df_dings[['车架号','主播人员']]
        return df_dings,df_zhubo

    def _clean_unsold_and_merge_host(self, df_unsold):
        # 已在 _clean_book_orders 中处理
        pass

    def _clean_void_orders(self, tui_dings_df, service_net):
        tui_dings_df = tui_dings_df[~tui_dings_df['退订类型'].isin(['重复录入','错误录入'])]
        tui_dings_df = pd.merge(tui_dings_df, service_net[['车系', '服务网络']], how='left', on='车系')
        tui_dings_df['订单门店'] = np.where(
            tui_dings_df['订单门店'] == '直播基地',
            tui_dings_df['服务网络'] + '-' + tui_dings_df['订单门店'],
            tui_dings_df['订单门店']
        )
        tui_dings_df['退定日期'] = pd.to_datetime(tui_dings_df['作废时间'], format='mixed', errors='coerce')
        tui_dings_df['定单日期'] = pd.to_datetime(tui_dings_df['订单日期'], format='mixed', errors='coerce')
        tui_dings_df['非退定核算'] = np.where(
            (tui_dings_df['定单日期'].dt.year == tui_dings_df['退定日期'].dt.year) &
            (tui_dings_df['定单日期'].dt.month == tui_dings_df['退定日期'].dt.month),
            0, 1
        )
        return tui_dings_df[[
            '订单门店','业务渠道','销售人员','主播人员','订单日期','车系','外饰颜色','车型','配置','客户名称',
            '客户电话','退订类型','退订原因','退定日期','非退定核算'
        ]]

    def _clean_sales_detail(self, df_salesAgg, service_net):
        df_salesAgg.rename(columns={
            '订单门店':'公司名称',
            '订单日期':'订车日期',
            '开票日期':'销售日期',
            '购车方式':'购买方式',
            '业务渠道':'所属团队',
            '销售人员':'销售人员',
            '分销/邀约人员':'邀约人员',
            '交付专员':'交付专员',
            '客户名称':'车主姓名',
            '车辆信息_车辆车系':'车系',
            '车辆信息_车辆车型':'车型',
            '车辆信息_车辆颜色':'外饰颜色',
            '车辆信息_车辆配置':'车辆配置',
            '车辆信息_车架号':'车架号',
            '订金信息_订金金额':'定金金额',
            '整车销售_厂家官价':'指导价',
            '整车销售_裸车成交价':'裸车成交价',
            '整车销售_开票价格':'车款（发票价）',
            '整车销售_票据事务金额':'票据事务金额',
            '整车销售_最终结算价':'提货价',
            '整车销售_调拨费':'调拨费',
            '其它业务_上牌费':'上牌费',
            '其它业务_置换补贴保证金':'置换款',
            '其它业务_精品款':'精品款',
            '其它业务_金融押金':'金融押金',
            '其它业务_保险押金':'保险押金',
            '其它业务_代金券':'代金券',
            '其它业务_其它押金':'其它押金',
            '其它业务_其它费用':'其它费用',
            '其它业务_特殊事项':'特殊事项',
            '其它业务_综合服务费':'金融服务费_顾问',
            '其它业务_票据事务费':'票据事务费',
            '其它业务_置换服务费':'置换服务费',
            '装饰业务_出库成本':'装饰成本',
            '其它业务_拖车费用':'拖车费用'
        }, inplace=True)
        df_salesAgg = df_salesAgg[(df_salesAgg['车架号'] != "") & (df_salesAgg['销售日期'] != "")]
        df_salesAgg = pd.merge(df_salesAgg, service_net[['车系', '服务网络']], how='left', on='车系')
        df_salesAgg['公司名称'] = np.where(
            df_salesAgg['公司名称'] == '直播基地',
            df_salesAgg['服务网络'] + '-' + df_salesAgg['公司名称'],
            df_salesAgg['公司名称']
        )
        df_salesAgg['销售日期'] = pd.to_datetime(df_salesAgg['销售日期'], format='mixed')
        df_salesAgg = df_salesAgg[df_salesAgg['销售日期'] > pd.to_datetime('2025-03-31')]
        df_salesAgg =df_salesAgg[['服务网络','公司名称','订车日期','入库日期','销售日期','车架号','车系','车型','车辆配置','外饰颜色','所属团队','销售人员','邀约人员','交付专员','车主姓名','联系电话','联系电话2','身份证号','定金金额','指导价','裸车成交价','车款（发票价）','提货价','调拨费','置换款','精品款','上牌费','购买方式','置换服务费','金融服务费_顾问','票据事务金额','票据事务费','代金券','金融押金','保险押金','其它押金','其它费用','特殊事项','拖车费用']]
        return df_salesAgg

    def _merge_main_sales_table(self, df_salesAgg, df_books2, df_service_aggregated, df_carcost, df_loan,
                               df_decoration2, df_kaipiao, df_Ers2, df_Ers2_archive):
        df_salesAgg1 = (
            df_salesAgg.merge(df_books2[['车架号','主播人员']], on='车架号', how='left')
            .merge(df_service_aggregated[['车架号','保养升级成本','套餐明细']], on='车架号', how='left')
            .merge(df_carcost[['车架号','调整项','返介绍费','退成交车辆定金（未抵扣）','政府返回区补','保险返利','终端返利',
                               '上牌成本','票据事务费-公司','代开票支付费用','回扣款','退代金券','退按揭押金','退置换补贴保证金','质损赔付金额','其他成本']],
                   on='车架号', how='left')
            .merge(df_loan[['车架号','金融类型','金融性质','首付金额','贷款金额','贷款期限','金融方案','返利系数','金融服务费','厂家贴息金额','经销商贴息金额','金融返利','金融税费','金融毛利']],
                   on='车架号', how='left')
            .merge(df_decoration2[['车架号','装饰成本','装饰收入','赠送装饰项目']], on='车架号', how='left')
            .merge(df_kaipiao, on='车架号', how='left')
            .merge(df_Ers2[['车架号','二手车返利金额1','收款日期']], on='车架号', how='left')
            .merge(df_Ers2_archive[['车架号','二手车返利金额']], on='车架号', how='left')
        )
        # 当购买方式为全款时，将金融相关字段设为空值
        financial_columns_to_clear = [
            '金融类型', '金融性质', '首付金额', '贷款金额', '贷款期限', '金融方案', 
            '返利系数', '金融服务费', '厂家贴息金额', '经销商贴息金额', 
            '金融返利', '金融税费', '金融毛利'
        ]

        # 将这些列转换为适当的空值（NaN）
        df_salesAgg1.loc[df_salesAgg1['购买方式'] == '全款', financial_columns_to_clear] = None

        # 确保所有参与财务计算的列都是数值类型
        financial_columns = [
        '定金金额', '指导价', '裸车成交价', '车款（发票价）', '提货价', '调拨费', '置换款', '精品款',
        '代金券', '其它押金', '其它费用', '特殊事项', '金融押金', '保险押金', '置换服务费', '金融服务费_顾问',
        '票据事务金额', '票据事务费', '调整项', '金融返利', '金融服务费', '金融毛利', '上牌费',
        '保险返利', '终端返利', '返利合计', '二手车返利金额', '代开票支付费用',
        '回扣款', '票据事务费-公司', '返介绍费', '质损赔付金额', '其他成本', '政府返回区补',
        '装饰收入', '保养升级成本', '装饰成本', '拖车费用', '上牌成本'
        ]
        for col in financial_columns:
            if col in df_salesAgg1.columns:
                df_salesAgg1[col] = pd.to_numeric(df_salesAgg1[col], errors='coerce').fillna(0)
        return df_salesAgg1

    def _handle_vat_logic(self, df_salesAgg1, df_vat):
        df_salesAgg1['车系辅助'] = df_salesAgg1['车系'] + df_salesAgg1['车型']
        df_vat['起始日期'] = pd.to_datetime(df_vat['起始日期'], format='mixed', errors='coerce')
        df_salesAgg1 = pd.merge(df_salesAgg1, df_vat[['辅助列','最终结算价（已抵扣超级置换）','抵扣金额','起始日期']],
                                left_on='车系辅助', right_on='辅助列', how='left')
        df_salesAgg1['抵扣金额'] = df_salesAgg1['抵扣金额'].fillna(0)
        df_salesAgg1['最终结算价（已抵扣超级置换）'] = pd.to_numeric(df_salesAgg1['最终结算价（已抵扣超级置换）']).fillna(0)
        df_salesAgg1['起始日期'] = df_salesAgg1['起始日期'].fillna(pd.Timestamp('1900-01-01'))
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
        return df_salesAgg1

    def _handle_diaobo_merge(self, df_diao, df_salesAgg1):
        df_diao = df_diao.drop(columns=['调拨费'], errors='ignore')
        df_diao = df_diao.sort_values(by=['结算日期'], ascending=False)
        df_diao = df_diao.drop_duplicates(subset=['车架号'], keep='first')
        df_diao.to_csv('调拨表.csv', index=False)
        df_diao1 = pd.merge(df_diao, df_salesAgg1[['车架号','销售日期','车系','车型','车辆配置','调拨费']],
                            on='车架号', how='left')
        df_diao1 = df_diao1[['调出门店','支付门店','调拨费','车架号','销售日期','车系','车型','车辆配置','车辆信息']]
        df_diao1[['所属团队','金融类型']] = '其他'
        df_diao1['金融类型'] = '调出车'
        df_diao1['调出车'] = '是'
        df_diao1.rename(columns={'车系': '车系1','调出门店': '公司名称','支付门店':'车主姓名'}, inplace=True)
        df_diao1['车系'] = '调拨车'
        df_diao1['车辆信息'] = df_diao1['车辆信息'].apply(lambda x: x[x.find(" ") + 1:] if x.find(" ") != -1 else x)
        df_diao1['单车毛利'] = df_diao1['调拨费']
        return df_diao1[['公司名称','销售日期','车架号','车系','车系1','车型','车辆信息','车辆配置','所属团队','金融类型','车主姓名','调拨费','调出车','单车毛利']]

    def _apply_promotion_logic(self, df_salesAgg1):

        
        df_salesAgg1['返利合计'] = df_salesAgg1['终端返利'] + df_salesAgg1['保险返利']
        df_salesAgg1['增值税利润差'] = np.where(
            df_salesAgg1['票据事务费'] > 0,
            df_salesAgg1[['车款（发票价）','置换款','返利合计']].sum(axis=1) - df_salesAgg1[['提货价','票据事务金额']].sum(axis=1),
            df_salesAgg1[['车款（发票价）','置换款','返利合计']].sum(axis=1) - df_salesAgg1['提货价']
        )
        df_salesAgg1 = self._handle_vat_logic(df_salesAgg1, self.df_vat)
        df_salesAgg1['后返客户款项'] = df_salesAgg1[['代金券','金融押金','保险押金','其它押金']].sum(axis=1)
        df_salesAgg1['促销费用'] = np.where(
            (df_salesAgg1['公司名称'].str.contains('贵州')) & (df_salesAgg1['所属团队'] != "调拨"),
            df_salesAgg1['后返客户款项'] + 200,
            df_salesAgg1['后返客户款项']
        )
        df_salesAgg1['二手车返利金额'] = np.where(
            (df_salesAgg1['二手车返利金额'] == "") | (df_salesAgg1['二手车返利金额'] == 0),
            df_salesAgg1['二手车返利金额1'],
            df_salesAgg1['二手车返利金额']
        )
        df_salesAgg1['金融服务费'] = np.where(
            (df_salesAgg1['金融服务费'].round(10) == 0) & (df_salesAgg1['购买方式'] != "全款"),
            df_salesAgg1['金融服务费_顾问'],
            df_salesAgg1['金融服务费']
        )
        df_salesAgg1['金融毛利'] = df_salesAgg1[['金融毛利','金融服务费']].sum(axis=1)
        df_salesAgg1['金融类型'] = np.where(df_salesAgg1['购买方式'] == '全款', '全款', df_salesAgg1['金融类型'])
        df_salesAgg1['上牌费'] = np.where(
            (df_salesAgg1['金融服务费_顾问'] > 0) & (df_salesAgg1['购买方式'] == "全款"),
            df_salesAgg1['金融服务费_顾问'] + df_salesAgg1['上牌费'],
            df_salesAgg1['上牌费']
        )
        df_salesAgg1['上牌毛利'] = df_salesAgg1[['上牌费','上牌成本']].sum(axis=1)
        df_salesAgg1['精品款'] = df_salesAgg1['票据事务金额']
        df_salesAgg1['装饰赠送合计'] = df_salesAgg1[['装饰成本','保养升级成本']].sum(axis=1)
        df_salesAgg1['销售车价'] = df_salesAgg1[['车款（发票价）','置换款','后返客户款项']].sum(axis=1) - df_salesAgg1['精品款']
        df_salesAgg1['固定支出'] = df_salesAgg1[['促销费用','装饰赠送合计']].sum(axis=1)
        df_salesAgg1['毛利'] = df_salesAgg1[['销售车价','返利合计']].sum(axis=1) - df_salesAgg1[['税费','提货价']].sum(axis=1)
        return df_salesAgg1

    def _finalize_and_export(self, df_salesAgg1, df_dings, df_inventory_all, tui_dings_df, df_debit,
                            df_salesAgg_, df_jingpin_result, df_inventory1, df_Ers1, df_diao2, df_inventory0_1):
        profit_cols_positive = ['毛利','金融毛利','上牌毛利','二手车返利金额','代开票支付费用','置换服务费','回扣款','票据事务费-公司','返介绍费','质损赔付金额','其他成本','政府返回区补','装饰收入','调整项','其它费用','特殊事项','拖车费用']
        profit_cols_negative = ['促销费用','装饰赠送合计']
        for col in profit_cols_positive + profit_cols_negative:
            if col in df_salesAgg1.columns:
                df_salesAgg1[col] = pd.to_numeric(df_salesAgg1[col], errors='coerce').fillna(0)
        df_salesAgg1['单车毛利'] = (
            df_salesAgg1[profit_cols_positive].sum(axis=1) -
            df_salesAgg1[profit_cols_negative].sum(axis=1) -
            pd.to_numeric(df_salesAgg1['调拨费'], errors='coerce').fillna(0)
        )
        df_salesAgg1['调出类型'] = np.where(
            ((df_salesAgg1['车主姓名'].isin(self.list_companys)) | (df_salesAgg1['车主姓名'].str.len() <= 5)) & (df_salesAgg1['所属团队'] == '调拨'),
            '内部调出',
            np.where(
                (~df_salesAgg1['车主姓名'].isin(self.list_companys)) & (df_salesAgg1['车主姓名'].str.len() > 5) & (df_salesAgg1['所属团队'] == '调拨'),
                '外部调出',
                None
            )
        )
        df_salesAgg2 = df_salesAgg1[[
            '服务网络','公司名称','订车日期','入库日期','收款日期','销售日期','车架号','车系','车辆配置','车型','外饰颜色',
            '所属团队','调出类型','销售人员','邀约人员','交付专员','主播人员','车主姓名','身份证号',
            '联系电话','联系电话2','定金金额','指导价','裸车成交价','销售车价','车款（发票价）','提货价',
            '置换款','精品款','后返客户款项','保险返利','终端返利','返利合计','增值税利润差','税费','毛利',
            '购买方式','金融类型','金融性质','金融方案','首付金额','贷款金额','贷款期限','返利系数',
            '金融返利','厂家贴息金额','经销商贴息金额','金融税费','金融服务费','金融毛利','上牌费',
            '上牌成本','上牌毛利','二手车返利金额','置换服务费','促销费用','赠送装饰项目','装饰收入',
            '装饰成本','套餐明细','保养升级成本','装饰赠送合计','其他成本','返介绍费','回扣款',
            '代开票支付费用','调拨费','票据事务费','票据事务费-公司','其它费用','特殊事项','政府返回区补',
            '质损赔付金额','调整项','单车毛利','开票门店','退代金券','退成交车辆定金（未抵扣）','退按揭押金','退置换补贴保证金','拖车费用'
        ]]

        df_salesAgg2 = pd.merge(df_salesAgg2, df_inventory0_1[['车架号','车源门店','供应商','发动机号']], on='车架号', how='left')
        df_salesAgg2 = df_salesAgg2.drop_duplicates()

        df_diao2 = df_diao2[(df_diao2['调拨费'] != 0) & (df_diao2['调拨费'].notnull())]
        df_Ers1['收款日期'] = pd.to_datetime(df_Ers1['收款日期'], format='mixed')
        df_salesAgg_combined = pd.concat([df_salesAgg2, df_Ers1, df_diao2], axis=0, ignore_index=True)
        df_salesAgg_combined['二手车返利金额'] = pd.to_numeric(df_salesAgg_combined['二手车返利金额'], errors='coerce').fillna(0)
        df_salesAgg_combined['收款日期'] = np.where(
            df_salesAgg_combined['二手车返利金额'] > 0,
            df_salesAgg_combined['收款日期'].fillna(df_salesAgg_combined['销售日期']),
            df_salesAgg_combined['收款日期']
        )
        df_salesAgg_combined['销售日期'] = df_salesAgg_combined['销售日期'].fillna(df_salesAgg_combined['收款日期'])
        df_jingpin_result = pd.merge(df_jingpin_result, df_salesAgg2[['车架号','车系']], on='车架号', how='left')

        # 导出 Excel
        outputfile = pd.ExcelWriter(r'C:\Users\13111\code\车易云商\cyy.xlsx')
        for name, df in [
            ('sales_data', df_salesAgg_combined.drop_duplicates()),
            ('book_data', df_dings.drop_duplicates()),
            ('inventory_data', df_inventory_all[(df_inventory_all['开票日期'].isna()) | (df_inventory_all['开票日期'] == "")]),
            ('tui_dings_df', tui_dings_df.drop_duplicates()),
            ('debit_df', df_debit.drop_duplicates()),
            ('sales_data1', df_salesAgg_.drop_duplicates()),
            ('df_jingpin_result', df_jingpin_result.drop_duplicates()),
            ('sold_inventorys', df_inventory1.drop_duplicates())
        ]:
            df.to_excel(outputfile, index=False, sheet_name=name)
        outputfile.close()
        logging.info('数据处理完成')
        df_inventory_all[(df_inventory_all['开票日期'].isna()) | (df_inventory_all['开票日期'] == "")].to_csv(
            r'C:\Users\13111\code\车易云商\inventory.csv', index=False
        )
        logging.info('库存数据处理完成')

        # 准备 MongoDB 导出
        df_salesAgg4 = df_salesAgg_combined.copy()
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
        df_salesAgg4 = df_salesAgg4[[
            '服务网络','车源门店','供应商','订单门店','订车日期','开票日期','收款日期','车架号','发动机号','车辆车系','车辆车型',
            '车辆颜色','业务渠道','销售人员','邀约人员','交付专员','主播人员','客户名称','身份证号','联系电话','联系电话2','订金金额',
            '厂家官价','裸车成交价','销售车价','开票价格','最终结算价',
            '置换补贴保证金','票据事务金额','后返客户款项','保险返利','终端返利',
            '厂家返利合计','增值税利润差','税费','毛利','购买方式','金融类型',
            '按揭渠道','按揭产品','首付金额','贷款总额','期限','返利系数','返利金额',
            '厂家贴息','公司贴息','金融税费','实收金融服务费','金融毛利','上牌费',
            '上牌服务费','上牌毛利','二手车成交价','二手车返利','置换服务费','促销费用','赠送装饰项目','装饰收入',
            '装饰成本','套餐明细','保养升级成本','装饰赠送合计','其他成本','返介绍费','合作返利',
            '综合结算服务费','调拨费','票据事务费','票据事务费-公司','其它费用','特殊事项','拖车费用','区补','质损赔付金额','调整项','单车毛利',
            '开票门店','调出类型','退代金券','退成交车辆定金（未抵扣）','退按揭押金','退置换补贴保证金'
        ]]

        # 类型转换
        float_columns = ['订金金额', '厂家官价','裸车成交价', '销售车价', '开票价格', '最终结算价',
            '置换补贴保证金', '票据事务金额', '保险返利', '终端返利', '厂家返利合计', '后返客户款项', '增值税利润差', '税费',
            '毛利', '返介绍费', '区补', '退代金券', '退成交车辆定金（未抵扣）', '退按揭押金', '退置换补贴保证金', '质损赔付金额',
            '首付金额', '贷款总额',  '实收金融服务费','厂家贴息', '公司贴息', '返利金额', '金融税费', '金融毛利', '上牌费', '上牌服务费', '上牌毛利',
            '二手车返利', '置换服务费', '促销费用', '保养升级成本', '装饰成本', '装饰赠送合计','其他成本','合作返利', '综合结算服务费', '调拨费', '票据事务费','票据事务费-公司', '单车毛利','二手车成交价','装饰收入','调整项','其它费用','特殊事项','拖车费用']
        string_columns = ['车源门店','供应商','订单门店', '订车日期', '开票日期','收款日期', '车架号','发动机号', '车辆车系', '车辆车型', '车辆颜色', '业务渠道', 
            '销售人员', '客户名称', '身份证号','联系电话','联系电话2','金融类型','购买方式', '按揭渠道','期限', '按揭产品', '赠送装饰项目','返利系数','套餐明细','开票门店','调出类型','邀约人员','交付专员','主播人员']
        try:
            df_salesAgg4[string_columns] = df_salesAgg4[string_columns].replace('nan', '').fillna('').astype('str')
        except Exception as e:
            logging.error(f"字符串列类型转换出错: {str(e)}")
        try:
            df_salesAgg4[float_columns] = df_salesAgg4[float_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype('str')
        except Exception as e:
            logging.error(f"数值列类型转换出错: {str(e)}")

        # 日期处理
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

        df_salesAgg4[['联系电话','联系电话2']] = clean_phone_series(df_salesAgg4[['联系电话','联系电话2']], keep_mobile_only=False, default='')

        start_date = datetime(2025, 4, 1)
        df_salesAgg4['开票日期'] = pd.to_datetime(df_salesAgg4['开票日期'], errors='coerce', format='mixed')
        df_salesAgg4['订单门店'] = np.where(df_salesAgg4['订单门店'].str.contains('直播基地'), '直播基地', df_salesAgg4['订单门店'])
        filtered_df = df_salesAgg4[df_salesAgg4['开票日期'] >= start_date].copy()
        filtered_df['开票日期'] = filtered_df['开票日期'].dt.strftime('%Y/%m/%d')
        filtered_df['订车日期'] = pd.to_datetime(filtered_df['订车日期'], errors='coerce', format='mixed').dt.strftime('%Y/%m/%d')

        filtered_df0 = filtered_df[filtered_df['业务渠道'].isin(['调拨', '其他'])]
        filtered_df0 = filtered_df0[['订单门店','订车日期','开票日期','车架号','车辆车系','车辆车型','车辆颜色','业务渠道','销售人员','邀约人员','交付专员','客户名称','身份证号',
                                    '联系电话','联系电话2','订金金额','厂家官价','裸车成交价','销售车价','开票价格','最终结算价','置换补贴保证金','票据事务金额','后返客户款项','保险返利',
                                    '终端返利','厂家返利合计','增值税利润差','税费','毛利','上牌费','上牌服务费','上牌毛利','质损赔付金额','单车毛利','开票门店']]
        df_jingpin_result['最早收款日期'] = pd.to_datetime(df_jingpin_result['最早收款日期'], errors='coerce', format='mixed')
        filtered_df_jingpin_result = df_jingpin_result[df_jingpin_result['最早收款日期'] >= start_date].copy()
        filtered_df_jingpin_result['收款日期'] = pd.to_datetime(filtered_df_jingpin_result['收款日期'], format='mixed', errors='coerce').dt.strftime('%Y/%m/%d')
        filtered_df_jingpin_result['订单门店'] = np.where(filtered_df_jingpin_result['订单门店'].str.contains('直播基地'), '直播基地', filtered_df_jingpin_result['订单门店'])

        self.export_to_mongodb(df_salesAgg4, filtered_df_jingpin_result, filtered_df0)
        df_salesAgg4.to_csv(r'C:\Users\13111\Documents\WXWork\1688855282576011\WeDrive\成都永乐盛世\维护文件\车易云毛利润表.csv', index=False)

    def export_to_mongodb(self, sales_data, jingpin_data, diaobo_data):
        try:
            client = MongoClient('mongodb://xg_wd:H91NgHzkvRiKygTe4X4ASw@192.168.1.7:27017/xg?authSource=xg&authMechanism=SCRAM-SHA-256')
            db = client['xg']
            db['sales_data3'].delete_many({})
            db['sales_data3'].insert_many(sales_data.to_dict('records'))
            db['jingpin_data'].delete_many({})
            db['jingpin_data'].insert_many(jingpin_data.fillna('').to_dict('records'))
            db['diao_data'].delete_many({})
            db['diao_data'].insert_many(diaobo_data.fillna('').to_dict('records'))
            self.send_md_to_person(msg=f"✅ **数据已成功写入 MongoDB 数据库**\n- 日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info("数据已成功写入 MongoDB 数据库")
        except Exception as e:
            self.send_md_to_person(msg=f"❌ **数据写入 MongoDB 数据库失败**\n- 日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n- 错误信息: {str(e)}")
            logging.error(f"导出到MongoDB失败: {str(e)}")

    def copy_file(self, source_path, destination_path):
        try:
            shutil.copy2(source_path, destination_path)
            logging.info(f"文件 {source_path} 已成功复制到 {destination_path}")
        except FileNotFoundError:
            logging.error(f"源文件 {source_path} 未找到。")
        except PermissionError:
            logging.error("没有足够的权限进行文件复制操作。")
        except Exception as e:
            logging.error(f"复制文件时发生错误: {e}")

    def run(self):
        # 1. 加载外部配置
        service_net = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充车系')
        company_belongs = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充团队')
        

        # 2. 加载原始数据
        logging.info("开始从数据库加载数据...")
        raw_data = self.load_all_data_from_db()
        print(raw_data)
        logging.info("数据加载完成")

        # 3. 清洗删除状态
        self._clean_deleted_records(raw_data)

        # 4. 各子表清洗
        df_insurance = self._clean_insurance(raw_data["保险业务"])
        df_Ers = self._clean_used_cars(raw_data["二手车成交"])
        df_decoration2, df_jingpin_result = self._clean_decoration_orders(raw_data["装饰订单"])
        df_service_aggregated = self._clean_service_packages(raw_data["套餐销售"])
        df_carcost = self._clean_vehicle_costs(raw_data["车辆成本管理"])
        df_loan = self._clean_loans(raw_data["按揭业务"])
        df_debit = self._clean_debit_and_merge(raw_data["汇票管理"], df_carcost)
        df_inventory_all, df_inventory, df_inventory1 = self._clean_inventory_and_plan(
            raw_data["库存车辆查询"], raw_data["库存车辆已售"], raw_data["计划车辆"], df_debit, service_net, company_belongs
        )
        df_dings ,df_zhubo = self._clean_book_orders(raw_data["衍生订单"], raw_data["成交订单"], raw_data["未售订单"], service_net)
        tui_dings_df = self._clean_void_orders(raw_data["作废订单"], service_net)
        df_salesAgg = self._clean_sales_detail(raw_data["车辆销售明细_开票日期"], service_net)

        # 5. 主表合并
        df_kaipiao = raw_data["开票维护"][raw_data["开票维护"]['单据类别'] == "车辆销售单"]
        df_kaipiao['下载时间'] = pd.to_datetime(df_kaipiao['下载时间'], format='mixed')
        df_kaipiao = df_kaipiao.sort_values(by=['车架号', '下载时间'], ascending=[True, False]).drop_duplicates(subset=['车架号'], keep='first')

        tichu = ['苏秀清','杜甯','周杨','李钰','易阳梅','黄毓香','王勇','钟鸣','刘前虎']
        df_Ers0 = df_Ers[(df_Ers['置换车架号'].notna()) & (df_Ers['置换车架号'] != '')]
        df_Ers1_ = pd.merge(df_Ers, df_kaipiao, how='left', left_on='置换车架号', right_on='车架号')
        df_Ers1_['置换车架号'] = np.where(df_Ers1_['开票门店'].isna(), np.nan, df_Ers1_['置换车架号'])
        df_Ers1 = df_Ers1_[((df_Ers1_['置换车架号'].isnull()) | (df_Ers1_['置换车架号'] == "")) & (~df_Ers1_['客户'].isin(tichu))].copy()
        df_Ers1 = df_Ers1[['评估门店','成交金额','其他费用','线索提供人','客户','车型','收款日期']]
        df_Ers1[['车系','车架号','所属团队']] = '二手车返利'
        df_Ers1['金融类型'] = '其他'
        df_Ers1['金融性质'] = '全款'
        df_Ers1.rename(columns={
            '评估门店':'公司名称','成交金额':'二手车成交价','其他费用':'二手车返利金额',
            '线索提供人':'销售人员','客户':'车主姓名','收款日期':'收款日期'
        }, inplace=True)
        df_Ers1['单车毛利'] = df_Ers1['二手车返利金额']

        df_Ers2 = df_Ers[(df_Ers['置换车架号'].notna()) & (df_Ers['置换车架号'] != '')].copy()
        df_Ers2.rename(columns={'车架号':'置换车架号_车牌','置换车架号':'车架号','其他费用':'二手车返利金额1'}, inplace=True)
        df_Ers2_archive = pd.read_csv(r'C:\Users\13111\code\dashboard\二手车返利存档.csv')

        df_salesAgg1 = self._merge_main_sales_table(
            df_salesAgg, df_zhubo, df_service_aggregated, df_carcost, df_loan,
            df_decoration2, df_kaipiao, df_Ers2, df_Ers2_archive
        )

        # 6. 专项逻辑
        
        df_salesAgg1 = self._apply_promotion_logic(df_salesAgg1)       
        df_diao2 = self._handle_diaobo_merge(raw_data["调车结算"], df_salesAgg1)

        # 7. 最终整理
        df_salesAgg_ = df_salesAgg1.copy()
        df_salesAgg_.rename(columns={
            '入库日期':'到库日期',
            '公司名称':'匹配定单归属门店',
            '订车日期':'定单日期',
            '销售人员':'销售顾问',
            '车主姓名':'客户姓名'
        }, inplace=True)
        df_salesAgg_ = df_salesAgg_[(df_salesAgg_['车架号'] != "") & (df_salesAgg_['销售日期'] != "")]
        df_salesAgg_ = df_salesAgg_[['服务网络','车架号','车系','车型','车辆配置','外饰颜色','定金金额','指导价','提货价','销售车价','匹配定单归属门店','到库日期','定单日期','销售日期','所属团队','销售顾问','客户姓名','身份证号','联系电话','联系电话2']]
        df_salesAgg_ = df_salesAgg_[(df_salesAgg_['所属团队'] != "调拨") & (df_salesAgg_['所属团队'].notna() & df_salesAgg_['所属团队'] != "")].drop_duplicates()

        df_inventory0_1 = pd.concat([df_inventory, df_inventory1], axis=0, ignore_index=True)

        # 8. 导出
        self._finalize_and_export(
            df_salesAgg1, df_dings, df_inventory_all, tui_dings_df, df_debit,
            df_salesAgg_, df_jingpin_result, df_inventory1, df_Ers1, df_diao2, df_inventory0_1
        )

        if self.page:
            self.page.quit()


if __name__ == "__main__":
    cyys = cyys()
    cyys.run()
    source_file = r'C:\Users\13111\code\车易云商\cyy.xlsx'
    destination_file = r'C:\Users\13111\Documents\WXWork\1688855282576011\WeDrive\成都永乐盛世\维护文件\cyy.xlsx'
    cyys.copy_file(source_file, destination_file)