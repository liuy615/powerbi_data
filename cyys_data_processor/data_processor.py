# -*- coding: utf-8 -*-
"""
核心数据处理模块
"""

import logging
import pandas as pd
import numpy as np
import re
from config.cyys_data_processor.config import COMPANIES, EXCLUDED_STAFF, INTERNAL_COMPANIES, USED_CAR_REBATE_PATH
from utils import DataUtils


class DataProcessor:
    """数据处理核心类（从cyy_api_db.py移植）"""

    def __init__(self, df_vat):
        self.df_vat = df_vat
        self.utils = DataUtils

    def _to_numeric_safe(self, df, cols, fill_value=0):
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(fill_value)
        return df

    """清洗保险数据"""
    def clean_insurance(self, df_insurance):
        df_insurance['保费总额'] = pd.to_numeric(df_insurance['保费总额'], errors='coerce').fillna(0)
        df_insurance['总费用_次数'] = df_insurance['保费总额'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        df_insurance.to_csv(fr"E:\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv")
        return df_insurance

    """清洗二手车数据"""
    def clean_used_cars(self, df_ershou):
        df_Ers = df_ershou[df_ershou['收款状态'] == '已收款'].copy()
        return df_Ers

    """清洗精品数据"""
    def clean_decoration_orders(self, df_decoration):
        df_decoration = df_decoration[
            (df_decoration['收款日期'] != "")&
            (df_decoration['收款日期'].notnull())
        ].copy()
        df_decoration = self._to_numeric_safe(df_decoration, ['销售合计','成本合计(含税)', '工时费','出/退/销数量'])
        df_decoration = df_decoration[~df_decoration['物资状态'].isin(['已退款','已退货','已换货','全退款','全退货', "部分退款"])]

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

    """清洗套餐销售数据"""
    def clean_service_packages(self, df_service):
        if df_service.empty:
            logging.warning("套餐销售数据为空，跳过清洗")
            return pd.DataFrame()

        df_service.rename(columns={'领取车架号/车牌号': '车架号'}, inplace=True)

        # 筛选有效套餐
        if not all(col in df_service.columns for col in ['套餐名称', '审批状态', '订单状态', '实售金额']):
            logging.warning("套餐数据缺少筛选字段，跳过清洗")
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

        # 聚合套餐数据 - 修复groupby.apply的方式
        if not all(col in df_service.columns for col in ['车架号', '套餐名称', '总次数']):
            return pd.DataFrame()

        def create_service_detail(group):
            items = []
            for name, qty in zip(group['套餐名称'], group['总次数']):
                if pd.notna(name) and pd.notna(qty):
                    items.append(f"{name}*{qty}")
            return ','.join(items)

        service_items_series = df_service.groupby('车架号').apply(create_service_detail)
        service_items = service_items_series.rename('套餐明细').reset_index()

        if '结算成本' in df_service.columns:
            df_service['保养升级成本'] = pd.to_numeric(df_service['结算成本'], errors='coerce')

            df_service_final = df_service.groupby('车架号')['保养升级成本'].sum().reset_index()
            df_service_final = df_service_final.merge(service_items, on='车架号', how='left')

            logging.info(f"套餐数据清洗完成：{len(df_service_final)}条有效记录")
            return df_service_final
        else:
            return pd.DataFrame()

    """清洗车辆成本数据"""
    def clean_vehicle_costs(self, df_carcost):
        if df_carcost.empty:
            logging.warning("车辆成本数据为空，跳过清洗")
            return pd.DataFrame()

        # 金额字段转换
        cost_cols = ['车辆成本_返介绍费', '其他成本_退代金券', '其他成本_退按揭押金']
        self.utils.convert_numeric_cols(df_carcost, cost_cols)

        # 字段重命名
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

        # 按车架号去重（保留最新操作记录）
        if all(col in df_carcost.columns for col in ['操作日期', '车架号']):
            df_carcost['操作日期'] = pd.to_datetime(df_carcost['操作日期'], errors='coerce')
            df_carcost.sort_values(by='操作日期', ascending=False, inplace=True)
            df_carcost.drop_duplicates(subset=['车架号'], keep='first', inplace=True)

        # 筛选必要字段
        required_cols = [
            '公司名称', '采购订单号', '车架号', '车辆状态', '调整项', '返介绍费',
            '退成交车辆定金（未抵扣）', '政府返回区补', '保险返利', '终端返利',
            '上牌成本', '票据事务费-公司', '代开票支付费用', '回扣款', '退代金券',
            '退按揭押金', '退置换补贴保证金', '质损赔付金额', '其他成本', '操作日期'
        ]
        df_carcost = df_carcost[self.utils.get_valid_columns(df_carcost, required_cols)]
        logging.info(f"车辆成本数据清洗完成：{len(df_carcost)}条去重记录")
        return df_carcost

    """清洗按揭业务数据"""
    """清洗按揭业务数据"""

    def clean_loans(self, df_loan):
        if df_loan.empty:
            logging.warning("按揭业务数据为空，跳过清洗")
            return pd.DataFrame()

        # 字段重命名
        df_loan.rename(columns={
            '按揭渠道': '金融性质',
            '贷款总额': '贷款金额',
            '期限': '贷款期限',
            '按揭产品': '金融方案',
            '实收金融服务费': '金融服务费',
            '厂家贴息': '厂家贴息金额',
            '公司贴息': '经销商贴息金额',
            '返利金额': '金融返利'
        }, inplace=True)

        # 划分金融类型
        if '金融性质' in df_loan.columns:
            df_loan['经销商贴息金额'] = pd.to_numeric(df_loan['经销商贴息金额'], errors='coerce')
            print(df_loan.columns)

            def determine_financial_type(row):
                # 修复：检查金融性质是否为None
                financial_nature = row['金融性质']
                if pd.isna(financial_nature) or financial_nature is None:
                    financial_nature = ''

                # 修复：检查金融方案是否为None
                financial_plan = row['金融方案']
                if pd.isna(financial_plan) or financial_plan is None:
                    financial_plan = ''
                if '非贴息' in str(financial_nature):
                    return '厂家非贴息贷'
                elif '贴息' in str(financial_nature):
                    return '厂家贴息贷'
                elif str(financial_plan) in ['交行信用卡中心5免2-9%', '建行5免2']:
                    return '无息贷'
                # elif pd.notna(row['经销商贴息金额']) and row['经销商贴息金额'] > 0:
                #     return '厂家贴息贷'
                else:
                    return '非贴息贷'

            df_loan['金融类型'] = df_loan.apply(determine_financial_type, axis=1)
            # def determine_financial_type(row):
            #     financial_nature = row['金融性质']
            #     if pd.isna(financial_nature) or financial_nature is None:
            #         financial_nature = ''
            #     if '非贴息' in financial_nature:
            #         return '厂家非贴息贷'
            #     elif '贴息' in financial_nature:
            #         return '厂家贴息贷'
            #     elif row['金融方案'] in ['交行信用卡中心5免2-9%', '建行5免2']:
            #         return '无息贷'
            #     elif pd.notna(row['经销商贴息金额']) and row['经销商贴息金额'] > 0:
            #         return '厂家非贴息贷'
            #     else:
            #         return '非贴息贷'
            #
            # df_loan['金融类型'] = df_loan.apply(determine_financial_type, axis=1)

        # 数据类型转换
        if '返利系数' in df_loan.columns:
            df_loan['返利系数'] = df_loan['返利系数'].astype(str).str.replace('%', '').astype(float,
                                                                                              errors='ignore') / 100

        float_cols = ['开票价', '贷款金额', '返利系数', '金融返利', '厂家贴息金额', '经销商贴息金额', '金融服务费']
        self.utils.convert_numeric_cols(df_loan, float_cols)

        # 计算衍生字段
        if all(col in df_loan.columns for col in ['开票价', '贷款金额']):
            df_loan['首付金额'] = df_loan['开票价'] - df_loan['贷款金额']

        if '贷款期限' in df_loan.columns:
            df_loan['贷款期限'] = df_loan['贷款期限'].astype(str).apply(
                lambda x: re.sub(r'[\u4e00-\u9fa5]', '', x) if pd.notna(x) else x)  # 移除中文

        if all(col in df_loan.columns for col in ['厂家贴息金额', '金融返利']):
            df_loan['金融税费'] = (df_loan['厂家贴息金额'] / 1.13 * 0.13 * 1.12) + (
                    df_loan['金融返利'] / 1.06 * 0.06 * 1.12)

        if all(col in df_loan.columns for col in ['金融返利', '经销商贴息金额', '金融税费']):
            df_loan['金融毛利'] = df_loan['金融返利'] - df_loan['经销商贴息金额'] - df_loan['金融税费']

        # 去重（按车架号保留第一条）
        if '车架号' in df_loan.columns:
            df_loan = df_loan[df_loan['车架号'].notna()]
            sort_cols = ['车架号', '收费状态'] if '收费状态' in df_loan.columns else ['车架号']
            df_loan.sort_values(by=sort_cols, ascending=True, inplace=True)
            df_loan = df_loan.drop_duplicates(subset=['车架号'], keep='first')

        logging.info(f"按揭数据清洗完成：{len(df_loan)}条有效记录")
        return df_loan

    """清洗汇票管理数据"""
    def clean_debit_and_merge(self, df_debit, df_carcost):
        if df_debit.empty:
            logging.warning("汇票管理数据为空，跳过清洗")
            return pd.DataFrame()

        # 字段重命名
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
        df_debit = df_debit[self.utils.get_valid_columns(df_debit, debit_cols)]
        logging.info(f"汇票数据清洗完成：{len(df_debit)}条记录")
        return df_debit

    """清洗库存和计划数据"""
    def clean_inventory_and_plan(self, df_inventory, df_inventory1, df_plan, df_debit, service_net, company_belongs):
        df_inventory = self.utils.rename_inventory(df_inventory)
        df_inventory1 = self.utils.rename_inventory(df_inventory1)

        df_plan.rename(columns={'车型': '车系', '整车型号': '车型', '订单号': '采购订单号'}, inplace=True)

        # 合并汇票数据
        if not df_debit.empty and '采购订单号' in df_debit.columns:
            merge_cols = ['采购订单号', '提货价', '开票银行', '合格证门店', '赎证日期', '到期日期',
                          '保证金比例', '赎证款']
            valid_merge_cols = self.utils.get_valid_columns(df_debit, merge_cols)
            df_plan = pd.merge(
                df_plan, df_debit[valid_merge_cols],
                on='采购订单号', how='left'
            )

        # 补充默认值
        df_plan['车辆状态'] = '未发'
        if '开票银行' in df_plan.columns:
            df_plan['开票银行'] = df_plan['开票银行'].fillna('公司')

        df_plan.rename(columns={'开票银行': '合格证状态', '门店': '归属系统'}, inplace=True)

        # 检查列名重复问题
        # 打印列名以便调试
        logging.info(f"df_inventory 列名: {list(df_inventory.columns)}")
        logging.info(f"df_plan 列名: {list(df_plan.columns)}")

        # 找出重复列名
        inventory_cols = set(df_inventory.columns)
        plan_cols = set(df_plan.columns)
        common_cols = inventory_cols.intersection(plan_cols)
        logging.info(f"共同列名: {common_cols}")

        # 检查是否有重复列名
        if len(df_inventory.columns) != len(set(df_inventory.columns)):
            duplicates = df_inventory.columns[df_inventory.columns.duplicated()].tolist()
            logging.warning(f"df_inventory 有重复列名: {duplicates}")
            # 删除重复列
            df_inventory = df_inventory.loc[:, ~df_inventory.columns.duplicated()]

        if len(df_plan.columns) != len(set(df_plan.columns)):
            duplicates = df_plan.columns[df_plan.columns.duplicated()].tolist()
            logging.warning(f"df_plan 有重复列名: {duplicates}")
            # 删除重复列
            df_plan = df_plan.loc[:, ~df_plan.columns.duplicated()]

        df_inventory_all = pd.concat([df_inventory, df_plan], axis=0, ignore_index=True)

        # 标记调入类型（内部/外部）
        if '供应商' in df_inventory_all.columns:
            df_inventory_all['调入类型'] = np.where(
                df_inventory_all['供应商'].isin(INTERNAL_COMPANIES), '内部调入',
                np.where(
                    (~df_inventory_all['供应商'].isin(INTERNAL_COMPANIES)) &
                    (df_inventory_all['供应商'] != '比亚迪') &
                    (df_inventory_all['供应商'].notnull()),
                    '外部调入', None
                )
            )

        # 合并服务网络（补充直播基地归属）
        if '车系' in df_inventory_all.columns and not service_net.empty:
            df_inventory_all = pd.merge(
                df_inventory_all, service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

            if all(col in df_inventory_all.columns for col in ['归属系统', '服务网络']):
                df_inventory_all['归属系统'] = np.where(
                    df_inventory_all['归属系统'] == '直播基地',
                    df_inventory_all['服务网络'] + '-' + df_inventory_all['归属系统'],
                    df_inventory_all['归属系统']
                )

        logging.info(f"库存+计划数据合并完成：共{len(df_inventory_all)}条记录")
        return df_inventory_all, df_inventory, df_inventory1

    """清洗订单数据"""
    def clean_book_orders(self, df_books, df_books2, df_unsold, service_net):
        if df_books.empty:
            logging.warning("衍生订单数据为空，跳过清洗")
            return pd.DataFrame(), pd.DataFrame()

        # 删除 df_books 中 ID 存在于 tui_dings_ids 中的行
        # 注意：这里需要先获取作废订单的ID，但在这个方法中我们没有tui_dings_df
        # 所以我们需要先检查是否有作废订单，如果没有就直接处理
        # 这里假设tui_dings_df已经在外部处理过了

        # 字段重命名
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
            '客户电话2': '联系电话2',
        }, inplace=True)

        # 过滤作废状态
        if '作废状态' in df_books.columns:
            df_books = df_books[df_books['作废状态'] == False]

        # 处理成交订单
        if not df_books2.empty:
            df_books2.rename(columns={'联系方式': '联系电话', '联系方式2': '联系电话2'}, inplace=True)

            # 获取主播人员信息
            if all(col in df_books2.columns for col in['ID', '联系电话', '联系电话2', '主播人员', '车系', '客户姓名', '订单公司']):
                df_sold = df_books2[['ID', '联系电话', '联系电话2', '主播人员', '车系', '客户姓名', '订单公司']].drop_duplicates()
            else:
                df_sold = pd.DataFrame()
        else:
            df_sold = pd.DataFrame()

        # 处理未售订单
        if not df_unsold.empty:
            df_unsold.rename(columns={'客户电话': '联系电话', '客户电话2': '联系电话2', '客户': '客户姓名'},inplace=True)
            if all(col in df_unsold.columns for col in['ID', '联系电话', '联系电话2', '主播人员', '车系', '客户姓名', '订单公司']):
                df_unsold1 = df_unsold[['ID', '联系电话', '联系电话2', '主播人员', '车系', '客户姓名', '订单公司']]
            else:
                df_unsold1 = pd.DataFrame()
        else:
            df_unsold1 = pd.DataFrame()

        # 合并主播列表
        if not df_sold.empty or not df_unsold1.empty:
            df_zhubolist = pd.concat([df_sold, df_unsold1], ignore_index=True).drop_duplicates()

            # 处理联系电话
            cols = ['联系电话', '联系电话2']
            if all(col in df_zhubolist.columns for col in cols):
                df_zhubolist[cols] = (df_zhubolist[cols].replace('', pd.NA).fillna(0).astype('int64').astype('str').replace('0', ''))

            # 创建辅助列
            df_zhubolist['辅助列'] = ''
            if '联系电话' in df_zhubolist.columns:
                df_zhubolist['辅助列'] += df_zhubolist['联系电话'].fillna('')
            if '联系电话2' in df_zhubolist.columns:
                df_zhubolist['辅助列'] += df_zhubolist['联系电话2'].fillna('')
            if '客户姓名' in df_zhubolist.columns:
                df_zhubolist['辅助列'] += df_zhubolist['客户姓名'].fillna('')
            if '车系' in df_zhubolist.columns:
                df_zhubolist['辅助列'] += df_zhubolist['车系'].fillna('')
            if '订单公司' in df_zhubolist.columns:
                df_zhubolist['辅助列'] += df_zhubolist['订单公司'].fillna('')

            df_zhubolist = df_zhubolist.drop_duplicates(subset=['辅助列'], keep='first')
        else:
            df_zhubolist = pd.DataFrame()

        # 筛选订单必要字段
        order_cols = [
            '车架号', '订单日期', '定单日期', '订金状态', '审批状态', '销售人员',
            '销售日期', '定金金额', '定单归属门店', '所属团队', '车系', '外饰颜色',
            '车型', '配置', '客户姓名', '联系电话', '联系电话2', '身份证号'
        ]

        valid_order_cols = self.utils.get_valid_columns(df_books, order_cols)
        df_dings = df_books[valid_order_cols].copy()

        # 补充直播基地归属
        if '车系' in df_dings.columns and not service_net.empty:
            df_dings = pd.merge(
                df_dings, service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

            if all(col in df_dings.columns for col in ['定单归属门店', '服务网络']):
                df_dings['定单归属门店'] = np.where(
                    df_dings['定单归属门店'] == '直播基地',
                    df_dings['服务网络'] + '-' + df_dings['定单归属门店'],
                    df_dings['定单归属门店']
                )

        # 匹配主播人员
        if not df_zhubolist.empty and '辅助列' in df_zhubolist.columns:
            # 创建df_dings的辅助列
            df_dings['辅助列'] = ''
            if '联系电话' in df_dings.columns:
                df_dings['辅助列'] += df_dings['联系电话'].fillna('')
            if '联系电话2' in df_dings.columns:
                df_dings['辅助列'] += df_dings['联系电话2'].fillna('')
            if '客户姓名' in df_dings.columns:
                df_dings['辅助列'] += df_dings['客户姓名'].fillna('')
            if '车系' in df_dings.columns:
                df_dings['辅助列'] += df_dings['车系'].fillna('')
            if '定单归属门店' in df_dings.columns:
                df_dings['辅助列'] += df_dings['定单归属门店'].fillna('')

            df_dings = pd.merge(df_dings, df_zhubolist[['辅助列', '主播人员']], how='left', on='辅助列')

            # 删除辅助列
            # df_dings = df_dings.drop(columns=['辅助列'], errors='ignore')

        # 处理现定现交
        df_dings['现定现交'] = np.where(
            (df_dings['定单日期'].isna()) & (df_dings['销售日期'].notna()),
            '现定现交',
            np.where((df_dings['订金状态'] == "待收款") & (df_dings['定单日期'].notna()) & (df_dings['销售日期'].notna()), '现定现交', None)
        )
        df_dings['定单状态'] = pd.to_datetime(np.where((df_dings['销售日期'].notna()), df_dings['销售日期'], None))
        df_dings['定金金额'] = np.where(df_dings['现定现交'] == '现定现交', 3000, df_dings['定金金额'])
        df_dings = df_dings.drop_duplicates()
        df_zhubo = df_dings[['车架号', '主播人员']]
        df_dings["身份证号"] = df_dings["身份证号"].astype("str")
        logging.info(f"订单数据清洗完成：{len(df_dings)}条记录")
        return df_dings, df_zhubo

    """清洗作废订单数据"""
    def clean_void_orders(self, tui_dings_df, service_net):
        if tui_dings_df.empty:
            logging.warning("作废订单数据为空，跳过清洗")
            return pd.DataFrame()

        # 过滤退订类型
        if '退订类型' in tui_dings_df.columns:
            tui_dings_df = tui_dings_df[~tui_dings_df['退订类型'].isin(['重复录入', '错误录入'])]

        # 补充直播基地归属
        if '车系' in tui_dings_df.columns and not service_net.empty:
            tui_dings_df = pd.merge(
                tui_dings_df, service_net[['车系', '服务网络']],
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
                (tui_dings_df['定单日期'].dt.year == tui_dings_df['退定日期'].dt.year) &
                (tui_dings_df['定单日期'].dt.month == tui_dings_df['退定日期'].dt.month),
                0, 1
            )

        # 筛选必要字段
        cancel_cols = ['订单门店', '业务渠道', '销售人员', '订单日期', '车系', '外饰颜色',
                       '车型', '配置', '主播人员', '客户名称', '客户电话', '作废类型',
                       '退订原因', '退定日期', '非退定核算']

        valid_cancel_cols = self.utils.get_valid_columns(tui_dings_df, cancel_cols)
        tui_dings_df = tui_dings_df[valid_cancel_cols]

        logging.info(f"作废订单数据清洗完成：{len(tui_dings_df)}条记录")
        return tui_dings_df

    """清洗销售明细数据"""
    def clean_sales_detail(self, df_salesAgg, service_net):
        if df_salesAgg.empty:
            logging.warning("销售明细（开票日期）数据为空，跳过清洗")
            return pd.DataFrame()

        df_salesAgg_clean = df_salesAgg.copy()

        # 重命名字段
        rename_dict = {
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
            '其它业务_特殊事项': '特殊事项',
            '其它业务_综合服务费': '金融服务费_顾问',
            '其它业务_票据事务费': '票据事务费',
            '其它业务_置换服务费': '置换服务费',
            '装饰业务_出库成本': '装饰成本',
            '其它业务_拖车费用': '拖车费用',
        }

        # 只重命名存在的列
        existing_rename = {k: v for k, v in rename_dict.items() if k in df_salesAgg_clean.columns}
        df_salesAgg_clean.rename(columns=existing_rename, inplace=True)

        # 过滤无效数据
        df_salesAgg_clean = df_salesAgg_clean[
            (df_salesAgg_clean['车架号'] != "") &
            (df_salesAgg_clean['销售日期'] != "")
            ].copy()

        # 补充直播基地归属
        if '车系' in df_salesAgg_clean.columns and not service_net.empty:
            df_salesAgg_clean = pd.merge(
                df_salesAgg_clean, service_net[['车系', '服务网络']],
                how='left', on='车系'
            )

            if all(col in df_salesAgg_clean.columns for col in ['公司名称', '服务网络']):
                df_salesAgg_clean['公司名称'] = np.where(
                    df_salesAgg_clean['公司名称'] == '直播基地',
                    df_salesAgg_clean['服务网络'] + '-' + df_salesAgg_clean['公司名称'],
                    df_salesAgg_clean['公司名称']
                )

        # 转换销售日期
        if '销售日期' in df_salesAgg_clean.columns:
            df_salesAgg_clean['销售日期'] = pd.to_datetime(df_salesAgg_clean['销售日期'], errors='coerce')

            # 过滤销售日期
            df_salesAgg_clean = df_salesAgg_clean[df_salesAgg_clean['销售日期'] > pd.to_datetime('2025-03-31')]

        # 筛选最终字段
        final_cols = [
            '服务网络', '公司名称', '订车日期', '入库日期', '销售日期', '车架号', '车系', '车型',
            '车辆配置', '外饰颜色', '所属团队', '销售人员', '邀约人员', '交付专员', '车主姓名',
            '联系电话', '联系电话2', '身份证号', '定金金额', '指导价', '裸车成交价', '车款（发票价）',
            '提货价', '调拨费', '置换款', '精品款', '上牌费', '购买方式', '置换服务费',
            '金融服务费_顾问', '票据事务金额', '票据事务费', '代金券', '金融押金', '保险押金',
            '其它押金', '其它费用', '特殊事项', '拖车费用'
        ]

        valid_final_cols = self.utils.get_valid_columns(df_salesAgg_clean, final_cols)
        df_salesAgg_clean = df_salesAgg_clean[valid_final_cols]

        logging.info(f"销售明细数据清洗完成：{len(df_salesAgg_clean)}条记录")
        return df_salesAgg_clean

    """合并主销售表"""
    def merge_main_sales_table(self, df_salesAgg, df_books2, df_service_aggregated, df_carcost, df_loan, df_decoration2, df_kaipiao, df_Ers2, df_Ers2_archive):
        df_salesAgg1 = df_salesAgg.copy()
        # 合并主播信息
        if not df_books2.empty and '车架号' in df_books2.columns and '主播人员' in df_books2.columns:
            df_salesAgg1 = df_salesAgg1.merge(
                df_books2[['车架号', '主播人员']],
                on='车架号', how='left'
            )

        # 合并套餐数据
        if not df_service_aggregated.empty and '车架号' in df_service_aggregated.columns:
            df_salesAgg1 = df_salesAgg1.merge(
                df_service_aggregated[['车架号', '保养升级成本', '套餐明细']],
                on='车架号', how='left'
            )

        # 合并成本数据
        if not df_carcost.empty and '车架号' in df_carcost.columns:
            cost_cols = [
                '车架号', '调整项', '返介绍费', '退成交车辆定金（未抵扣）', '政府返回区补',
                '保险返利', '终端返利', '上牌成本', '票据事务费-公司', '代开票支付费用',
                '回扣款', '退代金券', '退按揭押金', '退置换补贴保证金', '质损赔付金额', '其他成本'
            ]
            valid_cost_cols = self.utils.get_valid_columns(df_carcost, cost_cols)
            df_salesAgg1 = df_salesAgg1.merge(
                df_carcost[valid_cost_cols],
                on='车架号', how='left'
            )

        # 合并按揭数据
        if not df_loan.empty and '车架号' in df_loan.columns:
            loan_cols = [
                '车架号', '金融类型', '金融性质', '首付金额', '贷款金额', '贷款期限',
                '金融方案', '返利系数', '金融返利', '厂家贴息金额', '经销商贴息金额',
                '金融税费', '金融服务费', '金融毛利'
            ]
            valid_loan_cols = self.utils.get_valid_columns(df_loan, loan_cols)
            df_salesAgg1 = df_salesAgg1.merge(
                df_loan[valid_loan_cols],
                on='车架号', how='left'
            )

        # 合并装饰数据
        if not df_decoration2.empty and '车架号' in df_decoration2.columns:
            df_salesAgg1 = df_salesAgg1.merge(
                df_decoration2[['车架号', '装饰成本', '装饰收入', '赠送装饰项目']],
                on='车架号', how='left'
            )

        # 合并开票数据
        if not df_kaipiao.empty and '车架号' in df_kaipiao.columns:
            df_salesAgg1 = df_salesAgg1.merge(
                df_kaipiao,
                on='车架号', how='left'
            )

        # 合并二手车返利数据
        if not df_Ers2.empty and '车架号' in df_Ers2.columns:
            df_salesAgg1 = df_salesAgg1.merge(
                df_Ers2[['车架号', '二手车成交价', '二手车返利金额1', '收款日期']],
                on='车架号', how='left'
            )

        # 合并二手车返利存档
        if not df_Ers2_archive.empty and '车架号' in df_Ers2_archive.columns:
            df_salesAgg1 = df_salesAgg1.merge(
                df_Ers2_archive[['车架号', '二手车返利金额']],
                on='车架号', how='left'
            )

        # 当购买方式为全款时，将金融相关字段设为空值
        financial_columns_to_clear = [
            '金融类型', '金融性质', '首付金额', '贷款金额', '贷款期限', '金融方案',
            '返利系数', '金融服务费', '厂家贴息金额', '经销商贴息金额',
            '金融返利', '金融税费', '金融毛利'
        ]

        if '购买方式' in df_salesAgg1.columns:
            for col in financial_columns_to_clear:
                if col in df_salesAgg1.columns:
                    df_salesAgg1.loc[df_salesAgg1['购买方式'] == '全款', col] = None

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

    """处理增值税逻辑"""
    def handle_vat_logic(self, df_salesAgg1):
        # 创建车系辅助列
        df_salesAgg1['车系辅助'] = df_salesAgg1['车系'].fillna('') + df_salesAgg1['车型'].fillna('')

        # 处理增值税数据
        if not self.df_vat.empty:
            self.df_vat['起始日期'] = pd.to_datetime(self.df_vat['起始日期'], errors='coerce')

            df_salesAgg1 = pd.merge(
                df_salesAgg1,
                self.df_vat[['辅助列', '最终结算价（已抵扣超级置换）', '抵扣金额', '起始日期']],
                left_on='车系辅助', right_on='辅助列', how='left'
            )

        # 填充缺失值
        df_salesAgg1['抵扣金额'] = df_salesAgg1['抵扣金额'].fillna(0)
        df_salesAgg1['最终结算价（已抵扣超级置换）'] = pd.to_numeric(
            df_salesAgg1['最终结算价（已抵扣超级置换）'], errors='coerce'
        ).fillna(0)

        df_salesAgg1['起始日期'] = df_salesAgg1['起始日期'].fillna(pd.Timestamp('1900-01-01'))

        # 计算税费
        condition = (
                (df_salesAgg1['销售日期'] >= df_salesAgg1['起始日期']) &
                (df_salesAgg1['辅助列'] == df_salesAgg1['车系辅助']) &
                (df_salesAgg1['提货价'] <= df_salesAgg1['最终结算价（已抵扣超级置换）']) &
                (df_salesAgg1['置换款'] > 0)
        )

        # 确保增值税利润差存在
        if '增值税利润差' not in df_salesAgg1.columns:
            df_salesAgg1['增值税利润差'] = 0

        df_salesAgg1['税费'] = np.where(
            condition,
            np.where(
                df_salesAgg1['增值税利润差'] - df_salesAgg1['抵扣金额'] > 0,
                np.round((df_salesAgg1['增值税利润差'] - df_salesAgg1['抵扣金额']) / 1.13 * 0.13 * 1.12, 2),
                0
            ),
            np.where(
                df_salesAgg1['增值税利润差'] > 0,
                np.round(df_salesAgg1['增值税利润差'] / 1.13 * 0.13 * 1.12, 2),
                0
            )
        )

        return df_salesAgg1

    """处理调拨数据"""
    def handle_diaobo_merge(self, df_diao, df_salesAgg1):
        if df_diao.empty:
            logging.warning("调车结算数据为空，跳过处理")
            return pd.DataFrame()

        df_diao_clean = df_diao.copy()

        # 删除调拨费列（如果存在）
        if '调拨费' in df_diao_clean.columns:
            df_diao_clean = df_diao_clean.drop(columns=['调拨费'], errors='ignore')

        # 转换结算日期
        if '结算日期' in df_diao_clean.columns:
            df_diao_clean['结算日期'] = pd.to_datetime(df_diao_clean['结算日期'], errors='coerce')
            df_diao_clean = df_diao_clean.sort_values(by='结算日期', ascending=False)

        # 去重
        if '车架号' in df_diao_clean.columns:
            df_diao_clean = df_diao_clean.drop_duplicates(subset=['车架号'], keep='first')

        # 合并销售数据
        if not df_salesAgg1.empty and '车架号' in df_salesAgg1.columns:
            merge_cols = ['车架号', '销售日期', '车系', '车型', '车辆配置', '调拨费']
            valid_merge_cols = self.utils.get_valid_columns(df_salesAgg1, merge_cols)

            df_diao_clean = pd.merge(
                df_diao_clean,
                df_salesAgg1[valid_merge_cols],
                on='车架号', how='left'
            )

        # 补充调拨数据默认值
        df_diao_clean[['所属团队', '金融类型']] = '其他'
        df_diao_clean['金融类型'] = '调出车'
        df_diao_clean['调出车'] = '是'

        # 重命名
        rename_dict = {
            '车系': '车系1',
            '调出门店': '公司名称',
            '支付门店': '车主姓名'
        }
        existing_rename = {k: v for k, v in rename_dict.items() if k in df_diao_clean.columns}
        df_diao_clean.rename(columns=existing_rename, inplace=True)

        df_diao_clean['车系'] = '调拨车'

        # 处理车辆信息
        if '车辆信息' in df_diao_clean.columns:
            df_diao_clean['车辆信息'] = df_diao_clean['车辆信息'].apply(
                lambda x: x[x.find(" ") + 1:] if isinstance(x, str) and x.find(" ") != -1 else x
            )

        df_diao_clean['单车毛利'] = df_diao_clean['调拨费'].fillna(0)

        # 筛选调拨数据必要字段
        transfer_cols = [
            '公司名称', '销售日期', '车架号', '车系', '车系1', '车型', '车辆信息', '车辆配置',
            '所属团队', '金融类型', '车主姓名', '调拨费', '调出车', '单车毛利'
        ]

        valid_transfer_cols = self.utils.get_valid_columns(df_diao_clean, transfer_cols)
        df_diao_final = df_diao_clean[valid_transfer_cols]

        # 过滤无效数据
        df_diao_final = df_diao_final[
            (df_diao_final['调拨费'] != 0) &
            (df_diao_final['调拨费'].notnull())
            ]

        logging.info(f"调拨数据处理完成：{len(df_diao_final)}条有效记录")
        return df_diao_final

    """应用促销逻辑"""
    def apply_promotion_logic(self, df_salesAgg1):
        # 计算返利合计
        if '终端返利' in df_salesAgg1.columns and '保险返利' in df_salesAgg1.columns:
            df_salesAgg1['返利合计'] = df_salesAgg1['终端返利'] + df_salesAgg1['保险返利']
        else:
            df_salesAgg1['返利合计'] = 0

        # 计算增值税利润差
        if all(col in df_salesAgg1.columns for col in
               ['车款（发票价）', '置换款', '返利合计', '提货价', '票据事务金额', '票据事务费']):
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
        df_salesAgg1 = self.handle_vat_logic(df_salesAgg1)

        # 计算后返客户款项
        back_columns = ['代金券', '金融押金', '保险押金', '其它押金']
        existing_back_cols = [col for col in back_columns if col in df_salesAgg1.columns]

        if existing_back_cols:
            df_salesAgg1['后返客户款项'] = df_salesAgg1[existing_back_cols].sum(axis=1)
        else:
            df_salesAgg1['后返客户款项'] = 0

        # 计算促销费用（贵州区域加200）
        if '公司名称' in df_salesAgg1.columns and '所属团队' in df_salesAgg1.columns:
            df_salesAgg1['促销费用'] = np.where(
                (df_salesAgg1['公司名称'].str.contains('贵州', na=False)) &
                (df_salesAgg1['所属团队'] != "调拨"),
                df_salesAgg1['后返客户款项'] + 200,
                df_salesAgg1['后返客户款项']
            )
        else:
            df_salesAgg1['促销费用'] = df_salesAgg1['后返客户款项']

        # 合并二手车返利金额
        if '二手车返利金额' in df_salesAgg1.columns and '二手车返利金额1' in df_salesAgg1.columns:
            df_salesAgg1['二手车返利金额'] = np.where(
                (df_salesAgg1['二手车返利金额'] == "") |
                (df_salesAgg1['二手车返利金额'] == 0) |
                (df_salesAgg1['二手车返利金额'].isna()),
                df_salesAgg1['二手车返利金额1'],
                df_salesAgg1['二手车返利金额']
            )

        # 金融服务费补值
        if '金融服务费' in df_salesAgg1.columns and '金融服务费_顾问' in df_salesAgg1.columns and '购买方式' in df_salesAgg1.columns:
            df_salesAgg1['金融服务费'] = np.where(
                (df_salesAgg1['金融服务费'].round(10) == 0) &
                (df_salesAgg1['购买方式'] != "全款"),
                df_salesAgg1['金融服务费_顾问'],
                df_salesAgg1['金融服务费']
            )

        # 金融毛利计算
        if '金融毛利' in df_salesAgg1.columns and '金融服务费' in df_salesAgg1.columns:
            df_salesAgg1['金融毛利'] = df_salesAgg1[['金融毛利', '金融服务费']].sum(axis=1)

        # 金融类型补全
        if '购买方式' in df_salesAgg1.columns and '金融类型' in df_salesAgg1.columns:
            df_salesAgg1['金融类型'] = np.where(
                df_salesAgg1['购买方式'] == '全款',
                '全款',
                df_salesAgg1['金融类型']
            )

        # 上牌费计算
        if '金融服务费_顾问' in df_salesAgg1.columns and '购买方式' in df_salesAgg1.columns and '上牌费' in df_salesAgg1.columns:
            df_salesAgg1['上牌费'] = np.where(
                (df_salesAgg1['金融服务费_顾问'] > 0) &
                (df_salesAgg1['购买方式'] == "全款"),
                df_salesAgg1['金融服务费_顾问'] + df_salesAgg1['上牌费'],
                df_salesAgg1['上牌费']
            )

        # 上牌毛利
        if '上牌费' in df_salesAgg1.columns and '上牌成本' in df_salesAgg1.columns:
            df_salesAgg1['上牌毛利'] = df_salesAgg1['上牌费'] + df_salesAgg1['上牌成本']

        # 精品款用票据事务金额替换
        if '票据事务金额' in df_salesAgg1.columns:
            df_salesAgg1['精品款'] = df_salesAgg1['票据事务金额']

        # 装饰赠送合计
        if '装饰成本' in df_salesAgg1.columns and '保养升级成本' in df_salesAgg1.columns:
            df_salesAgg1['装饰赠送合计'] = df_salesAgg1[['装饰成本', '保养升级成本']].sum(axis=1)
        elif '装饰成本' in df_salesAgg1.columns:
            df_salesAgg1['装饰赠送合计'] = df_salesAgg1['装饰成本']
        elif '保养升级成本' in df_salesAgg1.columns:
            df_salesAgg1['装饰赠送合计'] = df_salesAgg1['保养升级成本']
        else:
            df_salesAgg1['装饰赠送合计'] = 0

        # 销售车价
        price_cols = ['车款（发票价）', '置换款', '后返客户款项']
        existing_price_cols = [col for col in price_cols if col in df_salesAgg1.columns]

        if existing_price_cols and '精品款' in df_salesAgg1.columns:
            df_salesAgg1['销售车价'] = df_salesAgg1[existing_price_cols].sum(axis=1) - df_salesAgg1['精品款']
        elif existing_price_cols:
            df_salesAgg1['销售车价'] = df_salesAgg1[existing_price_cols].sum(axis=1)
        else:
            df_salesAgg1['销售车价'] = 0

        # 固定支出
        if '促销费用' in df_salesAgg1.columns and '装饰赠送合计' in df_salesAgg1.columns:
            df_salesAgg1['固定支出'] = df_salesAgg1[['促销费用', '装饰赠送合计']].sum(axis=1)
        elif '促销费用' in df_salesAgg1.columns:
            df_salesAgg1['固定支出'] = df_salesAgg1['促销费用']
        elif '装饰赠送合计' in df_salesAgg1.columns:
            df_salesAgg1['固定支出'] = df_salesAgg1['装饰赠送合计']
        else:
            df_salesAgg1['固定支出'] = 0

        # 毛利
        if all(col in df_salesAgg1.columns for col in ['销售车价', '返利合计', '税费', '提货价']):
            df_salesAgg1['毛利'] = df_salesAgg1[['销售车价', '返利合计']].sum(axis=1) - df_salesAgg1[['税费', '提货价']].sum(axis=1)
        else:
            df_salesAgg1['毛利'] = 0
        return df_salesAgg1

    """最终整理和导出"""
    def finalize_and_export(self, df_salesAgg1, df_dings, df_inventory_all, tui_dings_df, df_debit, df_salesAgg_, df_jingpin_result, df_inventory1, df_Ers1, df_diao2, df_inventory0_1):
        profit_cols_positive = [
            '毛利', '金融毛利', '上牌毛利', '二手车返利金额', '代开票支付费用',
            '置换服务费', '回扣款', '票据事务费-公司', '返介绍费', '质损赔付金额',
            '其他成本', '政府返回区补', '装饰收入', '调整项', '其它费用', '特殊事项', '拖车费用'
        ]

        profit_cols_negative = ['促销费用', '装饰赠送合计']

        # 转换数值类型
        for col in profit_cols_positive + profit_cols_negative:
            if col in df_salesAgg1.columns:
                df_salesAgg1[col] = pd.to_numeric(df_salesAgg1[col], errors='coerce').fillna(0)

        # 计算单车毛利
        existing_positive = [col for col in profit_cols_positive if col in df_salesAgg1.columns]
        existing_negative = [col for col in profit_cols_negative if col in df_salesAgg1.columns]

        positive_sum = df_salesAgg1[existing_positive].sum(axis=1) if existing_positive else 0
        negative_sum = df_salesAgg1[existing_negative].sum(axis=1) if existing_negative else 0

        df_salesAgg1['单车毛利'] = positive_sum - negative_sum

        # 处理调拨费
        if '调拨费' in df_salesAgg1.columns:
            df_salesAgg1['调拨费'] = pd.to_numeric(df_salesAgg1['调拨费'], errors='coerce').fillna(0)
            df_salesAgg1['单车毛利'] = df_salesAgg1['单车毛利'] - df_salesAgg1['调拨费']

        # 标记调出类型
        if '车主姓名' in df_salesAgg1.columns and '所属团队' in df_salesAgg1.columns:
            df_salesAgg1['调出类型'] = np.where(
                ((df_salesAgg1['车主姓名'].isin(COMPANIES)) |
                 (df_salesAgg1['车主姓名'].str.len() <= 5)) &
                (df_salesAgg1['所属团队'] == '调拨'),
                '内部调出',
                np.where(
                    (~df_salesAgg1['车主姓名'].isin(COMPANIES)) &
                    (df_salesAgg1['车主姓名'].str.len() > 5) &
                    (df_salesAgg1['所属团队'] == '调拨'),
                    '外部调出',
                    None
                )
            )
        # 合并订车表中的身份证号
        df_dings = df_dings.drop_duplicates("车架号", keep="last")
        df_salesAgg1 = df_salesAgg1.merge(df_dings[["车架号", "身份证号"]], on='车架号', how="left")

        # 定义最终输出列
        final_columns = [
            '服务网络', '公司名称', '订车日期', '入库日期', '收款日期', '销售日期', '车架号', '车系', '车辆配置',
            '车型', '外饰颜色', '所属团队', '调出类型', '销售人员', '邀约人员', '交付专员', '主播人员',
            '车主姓名', '身份证号', '联系电话', '联系电话2', '定金金额', '指导价', '裸车成交价', '销售车价',
            '车款（发票价）', '提货价', '置换款', '精品款', '后返客户款项', '保险返利', '终端返利', '返利合计',
            '增值税利润差', '税费', '毛利', '购买方式', '金融类型', '金融性质', '金融方案', '首付金额',
            '贷款金额', '贷款期限', '返利系数', '金融返利', '厂家贴息金额', '经销商贴息金额', '金融税费',
            '金融服务费', '金融毛利', '上牌费', '上牌成本', '上牌毛利', "二手车成交价", '二手车返利金额', '置换服务费',
            '促销费用', '赠送装饰项目', '装饰收入', '装饰成本', '套餐明细', '保养升级成本', '装饰赠送合计',
            '其他成本', '返介绍费', '回扣款', '代开票支付费用', '调拨费', '票据事务费', '票据事务费-公司',
            '其它费用', '特殊事项', '政府返回区补', '质损赔付金额', '调整项', '单车毛利', '开票门店',
            '退代金券', '退成交车辆定金（未抵扣）', '退按揭押金', '退置换补贴保证金', '拖车费用'
        ]

        # 筛选存在的列
        existing_columns = [col for col in final_columns if col in df_salesAgg1.columns]
        df_salesAgg2 = df_salesAgg1[existing_columns].copy()

        # 合并库存信息
        if not df_inventory0_1.empty and '车架号' in df_inventory0_1.columns:
            inventory_cols = ['车架号', '车源门店', '供应商', '发动机号']
            valid_inventory_cols = self.utils.get_valid_columns(df_inventory0_1, inventory_cols)

            df_salesAgg2 = pd.merge(
                df_salesAgg2,
                df_inventory0_1[valid_inventory_cols],
                on='车架号', how='left'
            )

        df_salesAgg2_ = df_salesAgg2.copy().drop_duplicates()
        df_salesAgg2_.rename(columns={'公司名称': '匹配定单归属门店'}, inplace=True)
        df_salesAgg2_.to_csv(r'E:/WXWork/1688858189749305/WeDrive/成都永乐盛世/维护文件/车易云新车销售台账.csv',index=False)
        print(f"车易云新车销售台账备份完成！")

        # 处理调拨数据
        if not df_diao2.empty:
            df_diao2 = df_diao2[(df_diao2['调拨费'] != 0) &(df_diao2['调拨费'].notnull())]

        # 处理二手车数据
        if not df_Ers1.empty and '收款日期' in df_Ers1.columns:
            df_Ers1['收款日期'] = pd.to_datetime(df_Ers1['收款日期'], errors='coerce', format='mixed')

        # 合并所有数据
        dfs_to_concat = []
        if not df_salesAgg2.empty:
            dfs_to_concat.append(df_salesAgg2)
        if not df_Ers1.empty:
            dfs_to_concat.append(df_Ers1)
        else:
            # 创建一个空的DataFrame，列名为["收款日期"]
            df_empty = pd.DataFrame(columns=["收款日期"])
            dfs_to_concat.append(df_empty)
        if not df_diao2.empty:
            dfs_to_concat.append(df_diao2)

        if dfs_to_concat:
            df_salesAgg_combined = pd.concat(dfs_to_concat, axis=0, ignore_index=True)
        else:
            df_salesAgg_combined = pd.DataFrame()

        # 处理二手车返利金额和日期
        if '二手车返利金额' in df_salesAgg_combined.columns:
            df_salesAgg_combined['二手车返利金额'] = pd.to_numeric(df_salesAgg_combined['二手车返利金额'], errors='coerce').fillna(0)

            # 补充收款日期
            if '收款日期' in df_salesAgg_combined.columns and '销售日期' in df_salesAgg_combined.columns:
                df_salesAgg_combined['收款日期'] = np.where(
                    df_salesAgg_combined['二手车返利金额'] > 0,
                    df_salesAgg_combined['收款日期'].fillna(df_salesAgg_combined['销售日期']),
                    df_salesAgg_combined['收款日期']
                )

                df_salesAgg_combined['销售日期'] = df_salesAgg_combined['销售日期'].fillna(df_salesAgg_combined['收款日期'])

        # 合并精品结果和车系信息
        if not df_jingpin_result.empty and '车架号' in df_jingpin_result.columns:
            if not df_salesAgg2.empty and '车架号' in df_salesAgg2.columns and '车系' in df_salesAgg2.columns:
                df_jingpin_result = pd.merge(
                    df_jingpin_result,
                    df_salesAgg2[['车架号', '车系']],
                    on='车架号', how='left'
                )

        return df_salesAgg_combined, df_dings, df_inventory_all, tui_dings_df, df_debit, df_salesAgg_, df_jingpin_result, df_inventory1

    """处理二手车数据"""
    def process_used_car_data(self, df_Ers, df_kaipiao):
        tichu = EXCLUDED_STAFF
        # 处理有置换车架号的数据
        if '置换车架号' in df_Ers.columns:
            df_Ers0 = df_Ers[(df_Ers['置换车架号'].notna()) & (df_Ers['置换车架号'] != '')].copy()
        else:
            df_Ers0 = pd.DataFrame()

        # 合并开票数据
        if not df_Ers.empty and not df_kaipiao.empty and '置换车架号' in df_Ers.columns:
            df_Ers1_ = pd.merge(
                df_Ers, df_kaipiao,
                how='left', left_on='置换车架号', right_on='车架号'
            )

            if '开票门店' in df_Ers1_.columns:
                df_Ers1_['置换车架号'] = np.where(
                    df_Ers1_['开票门店'].isna(),
                    np.nan,
                    df_Ers1_['置换车架号']
                )

            # 筛选无置换车架号且客户不在排除名单中的数据
            df_Ers1 = df_Ers1_[
                ((df_Ers1_['置换车架号'].isnull()) | (df_Ers1_['置换车架号'] == "")) &
                (~df_Ers1_['客户'].isin(tichu))
                ].copy()

            # 选择需要的列
            ers_cols = ['评估门店', '成交金额', '其他费用', '线索提供人', '客户', '车型', '收款日期']
            valid_ers_cols = self.utils.get_valid_columns(df_Ers1, ers_cols)
            df_Ers1 = df_Ers1[valid_ers_cols]
        else:
            df_Ers1 = pd.DataFrame()

        # 添加默认值
        if not df_Ers1.empty:
            df_Ers1[['车系', '车架号', '所属团队']] = '二手车返利'
            df_Ers1['金融类型'] = '其他'
            df_Ers1['金融性质'] = '全款'

            # 重命名
            rename_dict = {
                '评估门店': '公司名称',
                '成交金额': '二手车成交价',
                '其他费用': '二手车返利金额',
                '线索提供人': '销售人员',
                '客户': '车主姓名'
            }

            existing_rename = {k: v for k, v in rename_dict.items() if k in df_Ers1.columns}
            df_Ers1.rename(columns=existing_rename, inplace=True)

            df_Ers1['单车毛利'] = pd.to_numeric(df_Ers1['二手车返利金额'], errors='coerce').fillna(0)

        # 处理有置换车架号的数据
        if not df_Ers0.empty:
            df_Ers2 = df_Ers0.copy()

            rename_dict = {
                '车架号': '置换车架号_车牌',
                '置换车架号': '车架号',
                '成交金额': '二手车成交价',
                '其他费用': '二手车返利金额1'
            }

            existing_rename = {k: v for k, v in rename_dict.items() if k in df_Ers2.columns}
            df_Ers2.rename(columns=existing_rename, inplace=True)
        else:
            df_Ers2 = pd.DataFrame()

        # 加载二手车返利存档
        try:
            df_Ers2_archive = pd.read_csv(USED_CAR_REBATE_PATH)
        except Exception as e:
            logging.error(f"二手车返利存档读取失败：{str(e)}")
            df_Ers2_archive = pd.DataFrame()

        return df_Ers1, df_Ers2, df_Ers2_archive