# -*- coding: utf-8 -*-
"""
数据写入模块
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pymongo import MongoClient
import requests
from config.cyys_data_processor.config import MONGODB_URI, MONGODB_DB, NOTIFY_API_URL


class DataWriter:
    """数据写入类"""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def write_to_mysql(self, data_dict):
        """写入数据到MySQL"""
        logging.info("开始写入MySQL输出库...")

        for table_name, df in data_dict.items():
            if df is not None and not df.empty:
                self.db_manager.write_to_output_db(df, table_name)

        logging.info("MySQL数据写入完成")

    def export_to_mongodb(self, sales_data, jingpin_data, diaobo_data):
        """导出数据到MongoDB"""
        try:
            # 连接到 MongoDB 数据库
            client = MongoClient(MONGODB_URI)
            db = client[MONGODB_DB]

            # 写销售毛利表
            collection = db['sales_data3']
            collection.delete_many({})
            data_dict = sales_data.to_dict('records')
            collection.insert_many(data_dict)

            # 写精品表
            collection_jp = db['jingpin_data']
            collection_jp.delete_many({})
            jingpin_data = jingpin_data.fillna('')
            data_dict_jp = jingpin_data.to_dict('records')
            collection_jp.insert_many(data_dict_jp)

            # 写外部调拨表
            collection_diao = db['diao_data']
            collection_diao.delete_many({})
            diaobo_data = diaobo_data.fillna('')
            data_dict_diao = diaobo_data.to_dict('records')
            collection_diao.insert_many(data_dict_diao)

            logging.info("数据已成功写入 MongoDB 数据库")

            # 发送通知
            self.send_md_to_person(
                msg=f"✅ **数据已成功写入 MongoDB 数据库**\n- 日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        except Exception as e:
            logging.error(f"导出到MongoDB失败: {str(e)}")

            # 发送错误通知
            self.send_md_to_person(
                msg=f"❌ **数据写入 MongoDB 数据库失败**\n- 日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n- 错误信息: {str(e)}"
            )

    def send_md_to_person(self, number: str = "LiuYang01", msg: str = ""):
        """发送通知"""
        try:
            data = {"touser": number, "msg": msg}
            res = requests.post(NOTIFY_API_URL, json=data, timeout=10)
            if res.status_code == 200:
                logging.info(f"📢 通知发送成功")
            else:
                logging.warning(f"⚠️ 通知发送失败，状态码: {res.status_code}, 响应: {res.text}")
        except Exception as e:
            logging.error(f"⚠️ 发送通知异常: {e}")

    def prepare_mongodb_data(self, df_salesAgg_combined, df_jingpin_result):
        """准备MongoDB导出数据"""
        # 准备销售数据
        df_salesAgg = df_salesAgg_combined.copy()

        # 重命名列
        rename_dict = {
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
        }

        df_salesAgg.rename(columns=rename_dict, inplace=True)

        # 筛选需要的列
        columns_needed = [
            '服务网络','车源门店','供应商','订单门店','订车日期','开票日期','收款日期','车架号','发动机号','车辆车系','车辆车型',
            '车辆颜色','业务渠道','客户来源','销售人员','邀约人员','交付专员','主播人员','客户名称','身份证号','联系电话','联系电话2','订金金额',
            '厂家官价','裸车成交价','销售车价','开票价格','最终结算价','置换补贴保证金','票据事务金额','后返客户款项','保险返利','终端返利',
            '厂家返利合计','增值税利润差','税费','毛利','购买方式','金融类型','按揭渠道','按揭产品','首付金额','贷款总额','期限','返利系数','返利金额',
            '厂家贴息','公司贴息','金融税费','实收金融服务费','金融毛利','上牌费','上牌服务费','上牌毛利','二手车成交价','二手车返利','置换服务费','促销费用','赠送装饰项目','装饰收入',
            '装饰成本','套餐明细','保养升级成本','装饰赠送合计','其他成本','返介绍费','合作返利','综合结算服务费','调拨费','票据事务费','票据事务费-公司',
            '其它费用','特殊事项','拖车费用','特殊赠券成本','区补','质损赔付金额','调整项','单车毛利','开票门店','调出类型','退代金券','退成交车辆定金（未抵扣）','退按揭押金','退置换补贴保证金'
        ]

        # 只保留存在的列
        existing_columns = [col for col in columns_needed if col in df_salesAgg.columns]
        df_salesAgg = df_salesAgg[existing_columns]

        # 数据类型转换
        float_columns = ['订金金额', '厂家官价', '裸车成交价', '销售车价', '开票价格', '最终结算价',
                         '置换补贴保证金', '票据事务金额', '保险返利', '终端返利', '厂家返利合计', '后返客户款项',
                         '增值税利润差', '税费','毛利', '返介绍费', '区补', '退代金券', '退成交车辆定金（未抵扣）', '退按揭押金',
                         '退置换补贴保证金', '质损赔付金额','首付金额', '贷款总额', '实收金融服务费', '厂家贴息', '公司贴息', '返利金额', '金融税费',
                         '金融毛利', '上牌费', '上牌服务费', '上牌毛利','二手车返利', '置换服务费', '促销费用', '保养升级成本', '装饰成本', '装饰赠送合计', '其他成本',
                         '合作返利', '综合结算服务费', '调拨费', '票据事务费', '票据事务费-公司', '单车毛利','二手车成交价', '装饰收入', '调整项', '其它费用', '特殊事项', '拖车费用','特殊赠券成本']
        string_columns = ['车源门店', '供应商', '订单门店', '订车日期', '开票日期', '收款日期', '车架号', '发动机号','车辆车系', '车辆车型', '车辆颜色', '业务渠道','客户来源',
                          '销售人员', '客户名称', '身份证号', '联系电话', '联系电话2', '金融类型', '购买方式','按揭渠道', '期限', '按揭产品', '赠送装饰项目', '返利系数', '套餐明细', '开票门店',
                          '调出类型', '邀约人员', '交付专员', '主播人员']

        # 转换字符串列
        for col in string_columns:
            if col in df_salesAgg.columns:
                df_salesAgg[col] = df_salesAgg[col].replace('nan', '').fillna('').astype('str')

        # 转换数值列
        for col in float_columns:
            if col in df_salesAgg.columns:
                df_salesAgg[col] = pd.to_numeric(df_salesAgg[col], errors='coerce').fillna(0).round(2).astype(float)

        # 设置过滤条件
        start_date = datetime(2025, 4, 1)
        # 过滤精品数据
        df_jingpin_result['最早收款日期'] = pd.to_datetime(df_jingpin_result['最早收款日期'], errors='coerce',format='mixed')
        filtered_df_jingpin_result = df_jingpin_result[df_jingpin_result['最早收款日期'] >= start_date].copy()
        filtered_df_jingpin_result['订单门店'] = np.where(filtered_df_jingpin_result['订单门店'].str.contains('直播基地'), '直播基地',filtered_df_jingpin_result['订单门店'])

        # 准备调拨数据
        for col in ['订车日期', '开票日期']:
            df_salesAgg.loc[:, col] = pd.to_datetime(
                df_salesAgg[col],
                errors='coerce',
                format='mixed'
            )

        df_salesAgg['订单门店'] = np.where(
            df_salesAgg['订单门店'].str.contains('直播基地'),
            '直播基地',
            df_salesAgg['订单门店']
        )

        # 筛选调拨数据
        filtered_df = df_salesAgg[df_salesAgg['开票日期'] >= start_date]

        # 转换日期格式
        for col in ['订车日期', '开票日期']:
            filtered_df.loc[:, col] = filtered_df[col].apply(
                lambda x: x.strftime('%Y/%m/%d') if pd.notna(x) else None
            )

        # 对于原始数据也进行日期格式转换
        for col in ['订车日期', '开票日期']:
            df_salesAgg.loc[:, col] = df_salesAgg[col].apply(
                lambda x: x.strftime('%Y/%m/%d') if pd.notna(x) else None
            )

        # 继续处理其他筛选
        filtered_df = filtered_df[filtered_df['业务渠道'].isin(['调拨', '其他'])]

        filtered_df_columns = ['订单门店','订车日期','开票日期','车架号','车辆车系','车辆车型','车辆颜色','业务渠道','销售人员','邀约人员','交付专员','客户名称','身份证号',
                                    '联系电话','联系电话2','订金金额','厂家官价','裸车成交价','销售车价','开票价格','最终结算价','置换补贴保证金','票据事务金额','后返客户款项','保险返利',
                                    '终端返利','厂家返利合计','增值税利润差','税费','毛利','上牌费','上牌服务费','上牌毛利','质损赔付金额','单车毛利','开票门店']

        existing_filtered_columns = [col for col in filtered_df_columns if col in filtered_df.columns]
        filtered_df = filtered_df[existing_filtered_columns]
        return df_salesAgg, filtered_df_jingpin_result, filtered_df