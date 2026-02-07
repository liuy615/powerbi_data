# -*- coding: utf-8 -*-
"""
仪表板数据生成器模块
包含所有数据合并和转换的具体实现
"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Tuple, Dict, Any, List

from config import FilePaths, REPLACE_MAPPING
from data_processor import DataProcessor, DataValidator


class DashboardGenerator:
    """仪表板数据生成器：生成所有仪表板所需的数据集"""
    
    def __init__(self, db_manager, file_paths: FilePaths):
        """
        初始化仪表板数据生成器
        
        Args:
            db_manager: 数据库管理器实例
            file_paths: 文件路径配置
        """
        self.db = db_manager
        self.paths = file_paths
        self.data_processor = DataProcessor()
        self.validator = DataValidator()
        
        # 数据缓存
        self._data_cache = {}
        
    def _load_from_cache_or_db(self, data_name: str, load_func, *args, **kwargs):
        """
        从缓存或加载函数获取数据
        
        Args:
            data_name: 数据名称（用于缓存键）
            load_func: 加载函数
            *args, **kwargs: 传递给加载函数的参数
            
        Returns:
            加载的数据
        """
        if data_name in self._data_cache:
            logging.info(f"从缓存加载数据: {data_name}")
            return self._data_cache[data_name]
        
        data = load_func(*args, **kwargs)
        self._data_cache[data_name] = data
        return data
    
    def _load_csv_with_replace(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        加载CSV文件并应用文本替换
        
        Args:
            file_path: CSV文件路径
            **kwargs: 传递给pd.read_csv的参数
            
        Returns:
            pd.DataFrame: 加载的数据
        """
        # 设置默认参数
        kwargs.setdefault('encoding', 'utf-8')
        kwargs.setdefault('low_memory', False)
        
        df = pd.read_csv(file_path, **kwargs)
        
        # 应用文本替换
        for old_text, new_text in REPLACE_MAPPING.items():
            df = df.replace(old_text, new_text)
            
        return df
    
    def _load_excel_with_replace(self, file_path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
        """
        加载Excel文件并应用文本替换
        
        Args:
            file_path: Excel文件路径
            sheet_name: 工作表名称
            **kwargs: 传递给pd.read_excel的参数
            
        Returns:
            pd.DataFrame: 加载的数据
        """
        df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
        
        # 应用文本替换
        for old_text, new_text in REPLACE_MAPPING.items():
            df = df.replace(old_text, new_text)
            
        return df
    
    # ========== 数据加载方法 ==========
    
    def load_books_cyy(self) -> pd.DataFrame:
        """加载数据库中的订单数据"""
        return self.db.read_table('order_data')
    
    def load_sales_cyy(self) -> pd.DataFrame:
        """加载数据库中的销售数据"""
        return self.db.read_table('sales_data')
    
    def load_inventorys_cyy(self) -> pd.DataFrame:
        """加载数据库中的库存数据"""
        return self.db.read_table('inventory_data')
    
    def load_jingpins_cyy(self) -> pd.DataFrame:
        """加载数据库中的精品数据"""
        return self.db.read_table('jingpin_data')
    
    def load_tuis_cyy(self) -> pd.DataFrame:
        """加载数据库中的退订数据"""
        return self.db.read_table('tuiding_data')
    
    def load_debits_cyy(self) -> pd.DataFrame:
        """加载数据库中的三方台账数据"""
        return self.db.read_table('debit_data')
    
    def load_sales_cyy1(self) -> pd.DataFrame:
        """加载数据库中的销售发票数据"""
        return self.db.read_table('sales_invoice_data')
    
    def load_xcbx_cyy(self) -> pd.DataFrame:
        """加载新车保险台账数据"""
        return self._load_csv_with_replace(self.paths.xcbx_cyy)
    
    def load_salary_data(self) -> pd.DataFrame:
        """加载薪资数据"""
        return self._load_excel_with_replace(
            self.paths.salary_excel,
            sheet_name='看板用'
        )
    
    def load_car_belongs(self) -> pd.DataFrame:
        """加载车辆归属数据"""
        return self._load_excel_with_replace(
            self.paths.car_belongs_excel,
            sheet_name='补充车系'
        )
    
    def load_team_belongs(self) -> pd.DataFrame:
        """加载团队归属数据"""
        return self._load_excel_with_replace(
            self.paths.team_belongs_excel,
            sheet_name='补充团队'
        )
    
    def load_xcbx_lock(self) -> pd.DataFrame:
        """加载新车保险历史数据"""
        return self._load_csv_with_replace(self.paths.xcbx_lock)
    
    def load_books_unsold(self) -> pd.DataFrame:
        """加载未售订单数据"""
        return self._load_csv_with_replace(self.paths.books_unsold)
    
    def load_yingxiao_data(self) -> pd.DataFrame:
        """加载营销费用数据"""
        return self._load_csv_with_replace(self.paths.yingxiao)
    
    def load_ers_data(self) -> pd.DataFrame:
        """加载二手车数据"""
        return self._load_csv_with_replace(self.paths.ers)
    
    def load_ers_lock(self) -> pd.DataFrame:
        """加载二手车历史数据"""
        return self._load_csv_with_replace(self.paths.ers_lock)
    
    def load_books_lock(self) -> pd.DataFrame:
        """加载订单历史数据"""
        return self._load_csv_with_replace(self.paths.books_lock)
    
    def load_sales_lock(self) -> pd.DataFrame:
        """加载销售毛利历史数据"""
        return self._load_csv_with_replace(self.paths.sales_lock)
    
    def load_sales_lock1(self) -> pd.DataFrame:
        """加载销售历史数据"""
        return self._load_csv_with_replace(self.paths.sales_lock1)
    
    def load_jingpins_lock(self) -> pd.DataFrame:
        """加载精品销售历史数据"""
        return self._load_csv_with_replace(self.paths.jingpins_lock)
    
    def load_tuis_lock(self) -> pd.DataFrame:
        """加载退订历史数据"""
        return self._load_csv_with_replace(self.paths.tuis_lock)
    
    def load_debits_lock(self) -> pd.DataFrame:
        """加载三方台账历史数据"""
        return self._load_csv_with_replace(self.paths.debits_lock)
    
    def load_plan_date_get(self) -> pd.DataFrame:
        """加载计划车辆数据"""
        return self._load_csv_with_replace(self.paths.plan_date_get)
    
    def load_inventory_lock(self) -> pd.DataFrame:
        """加载库存存档数据"""
        return self._load_csv_with_replace(self.paths.inventory_lock)
    
    # ========== 数据合并方法 ==========
    
    def concat_newold_sales_dashboard(self) -> pd.DataFrame:
        """
        合并新旧销售毛利数据
        
        输入：销售毛利历史数据 + 数据库销售数据
        输出：合并后的销售毛利数据
        """
        logging.info("开始合并销售毛利数据")
        
        # 1. 加载并处理历史销售数据
        df_sales_lock = self.load_sales_lock()
        
        # 选择需要的列
        sales_lock_columns = [
            '公司名称', '车架号', '车系', '外饰颜色', '车型', '指导价', '销售日期', '订车日期', '销售人员', '所属团队',
            '车主姓名', '联系电话', '购买方式', '销售车价', '车款（发票价）', '置换款', '精品款', '后返客户款项',
            '终端返利', '提货价', '增值税利润差', '税费', '毛利', '金融性质', '返利系数', '贷款金额', '贷款期限',
            '经销商贴息金额', '厂家贴息金额', '金融税费', '金融返利', '金融服务费', '金融毛利', '上牌费', '上牌成本',
            '上牌毛利', '临牌费', '临牌成本', '临牌毛利', '促销费用', '装饰成本', '二手车成交价', '二手车返利金额',
            '回扣款', '政府返回区补', '返客户区补', '开票价', '代开票支付费用', '单车毛利', '调出车', '金融类型', '贷款期限1'
        ]
        df_sales_lock = df_sales_lock[sales_lock_columns].copy()
        
        # 2. 加载并处理数据库销售数据
        df_sales_cyy = self.load_sales_cyy()
        
        sales_cyy_columns = [
            '公司名称', '销售日期', '订车日期', '收款日期', '车架号', '车系', '车系1', '车辆信息', '外饰颜色',
            '车辆配置', '车型', '所属团队', '调出类型', '销售人员', '主播人员', '车主姓名', '联系电话', '指导价',
            '销售车价', '车款（发票价）', '提货价', '置换款', '精品款', '后返客户款项', '保险返利', '终端返利',
            '返利合计', '增值税利润差', '税费', '毛利', '购买方式', '金融类型', '金融性质', '返利系数', '贷款金额',
            '贷款期限', '金融返利', '厂家贴息金额', '经销商贴息金额', '金融税费', '金融服务费', '金融毛利', '上牌费',
            '上牌成本', '上牌毛利', '二手车返利金额', '置换服务费', '促销费用', '装饰成本', '保养升级成本',
            '装饰赠送合计', '其他成本', '返介绍费', '回扣款', '代开票支付费用', '调拨费', '票据事务费-公司',
            '政府返回区补', '质损赔付金额', '单车毛利', '开票门店', '调出车', '装饰收入'
        ]
        df_sales_cyy = df_sales_cyy[sales_cyy_columns].copy()
        
        # 3. 处理贷款期限
        df_sales_cyy['销售日期'] = pd.to_datetime(df_sales_cyy['销售日期'], format='mixed', errors='coerce')
        df_sales_cyy['贷款期限'] = self.data_processor.process_loan_term(df_sales_cyy['贷款期限'])
        
        # 4. 处理历史数据日期
        df_sales_lock['销售日期'] = df_sales_lock['销售日期'].apply(self.data_processor.standardize_date)
        df_sales_lock['销售日期'] = df_sales_lock['销售日期'].apply(self.data_processor.convert_date)
        
        # 5. 筛选历史数据（2025年4月1日之前）
        df_sales_lock = df_sales_lock[df_sales_lock['销售日期'] < '2025-04-01'].copy()
        
        # 6. 合并数据
        df_combined = pd.concat([df_sales_cyy, df_sales_lock], axis=0, ignore_index=True)
        
        # 7. 数据清洗和补充
        # 7.1 处理车辆信息
        df_combined['车辆信息'] = np.where(
            df_combined['所属团队'] == '调拨',
            df_combined['车系'] + " " + df_combined['车辆配置'],
            df_combined['车辆信息']
        )
        
        # 7.2 处理所属团队
        df_combined['所属团队'] = np.where(
            df_combined['所属团队'] == '调拨',
            "其他",
            df_combined['所属团队']
        )
        
        # 7.3 处理金融类型和调出车标记
        df_combined['金融类型'] = np.where(
            df_combined['所属团队'] == '其他',
            '调出车',
            df_combined['金融类型']
        )
        df_combined['调出车'] = np.where(
            df_combined['所属团队'] == '其他',
            "是",
            ""
        )
        
        # 7.4 填充车辆配置
        df_combined['车辆配置'] = df_combined['车辆配置'].fillna(df_combined['车型'])
        
        # 7.5 处理收款日期
        df_combined['收款日期'] = np.where(
            (df_combined['二手车返利金额'] > 0) | (df_combined['二手车返利金额'] < 0),
            df_combined['收款日期'].fillna(df_combined['销售日期']),
            df_combined['收款日期']
        )
        
        # 8. 合并团队归属信息
        team_belongs = self.load_team_belongs()
        df_combined = pd.merge(
            df_combined,
            team_belongs[['公司名称', '服务网络']],
            on='公司名称',
            how='left'
        )
        
        logging.info(f"销售毛利数据合并完成，总行数：{len(df_combined)}")
        return df_combined
    
    def concat_newold_ers_dashboard(self) -> pd.DataFrame:
        """
        合并新旧二手车数据
        
        输入：二手车历史数据 + 当前二手车数据
        输出：合并后的二手车数据
        """
        logging.info("开始合并二手车数据")
        
        # 加载历史数据
        df_ers_lock = self.load_ers_lock()
        ers_lock_columns = [
            '收购时间', '新车客户姓名', '联系电话', '旧车客户姓名', '旧车品牌', '收购价格', '二手车返利',
            '二手车返利到账时间', '销售顾问', '归属团队'
        ]
        df_ers_lock = df_ers_lock[ers_lock_columns].copy()
        
        # 加载当前数据
        df_ers = self.load_ers_data()
        ers_columns = [
            '评估门店', '客户', '手机', '置换客户名称', '车型', '成交日期', '成交金额', '其他费用', '线索提供人', '录入日期'
        ]
        df_ers = df_ers[ers_columns].copy()
        
        # 重命名当前数据列以匹配历史数据
        column_mapping = {
            '评估门店': '归属团队',
            '客户': '旧车客户姓名',
            '手机': '联系电话',
            '置换客户名称': '新车客户姓名',
            '车型': '旧车品牌',
            '成交日期': '二手车返利到账时间',
            '成交金额': '收购价格',
            '其他费用': '二手车返利',
            '线索提供人': '销售顾问',
            '录入日期': '收购时间'
        }
        df_ers.rename(columns=column_mapping, inplace=True)
        
        # 合并数据
        df_combined = pd.concat([df_ers_lock, df_ers], axis=0, ignore_index=True)
        
        logging.info(f"二手车数据合并完成，总行数：{len(df_combined)}")
        return df_combined
    
    def concat_newold_sales_dashboard1(self) -> pd.DataFrame:
        """
        合并新旧销售数据（销售台账）
        
        输入：销售历史数据 + 数据库销售发票数据
        输出：合并后的销售台账数据
        """
        logging.info("开始合并销售台账数据")
        
        # 1. 加载历史销售数据
        df_sales_lock1 = self.load_sales_lock1()
        sales_lock1_columns = [
            '城市', '车架号', '车系', '车型', '外饰颜色', '指导价', '到库日期', '销售顾问', '所属团队',
            '匹配定单归属门店', '客户姓名', '销售日期', '提货价', '定单日期', '定金金额', '当月定卖', '服务网络'
        ]
        df_sales_lock1 = df_sales_lock1[sales_lock1_columns].copy()
        
        # 2. 加载数据库销售发票数据
        df_sales_cyy1 = self.load_sales_cyy1()
        sales_cyy1_columns = [
            '服务网络', '车架号', '车系', '车型', '车辆配置', '外饰颜色', '定金金额', '指导价', '提货价',
            '匹配定单归属门店', '到库日期', '定单日期', '销售日期', '所属团队', '销售顾问', '客户姓名'
        ]
        df_sales_cyy1 = df_sales_cyy1[sales_cyy1_columns].copy()
        
        # 3. 过滤无效团队
        invalid_teams = ['调拨', '其他', '二手车返利']
        df_sales_cyy1 = df_sales_cyy1[~df_sales_cyy1['所属团队'].isin(invalid_teams)].copy()
        
        # 4. 加载库存数据获取到库日期
        df_inventorys_cyy = self.load_inventorys_cyy()
        df_inventorys_cyy = df_inventorys_cyy[['车架号', '到库日期']].copy()
        df_inventorys_cyy.columns = ['车架号', '到库日期1']
        
        # 5. 处理数据库数据
        df_sales_cyy1['定单日期'] = pd.to_datetime(df_sales_cyy1['定单日期'], errors='coerce')
        df_sales_cyy1['销售日期'] = pd.to_datetime(df_sales_cyy1['销售日期'], errors='coerce')
        
        # 计算当月定卖
        df_sales_cyy1['当月定卖'] = np.where(
            (df_sales_cyy1['定单日期'].dt.year == df_sales_cyy1['销售日期'].dt.year) &
            (df_sales_cyy1['定单日期'].dt.month == df_sales_cyy1['销售日期'].dt.month),
            1, 0
        )
        
        # 6. 处理历史数据
        df_sales_lock1['销售日期'] = pd.to_datetime(df_sales_lock1['销售日期'], errors='coerce')
        df_sales_lock1 = df_sales_lock1[df_sales_lock1['销售日期'] < '2025-04-01'].copy()
        
        # 7. 合并数据
        df_combined = pd.concat([df_sales_cyy1, df_sales_lock1], axis=0, ignore_index=True)
        
        # 8. 补充到库日期
        df_combined = pd.merge(df_combined, df_inventorys_cyy, on='车架号', how='left')
        df_combined['到库日期'] = df_combined['到库日期'].fillna(df_combined['到库日期1'])
        
        # 9. 选择最终列
        final_columns = [
            '服务网络', '车架号', '车系', '车型', '车辆配置', '外饰颜色', '定金金额', '指导价', '提货价',
            '匹配定单归属门店', '到库日期', '定单日期', '销售日期', '所属团队', '销售顾问', '客户姓名', '当月定卖', '城市'
        ]
        df_combined = df_combined[final_columns].copy()
        
        # 10. 填充车辆配置
        df_combined['车辆配置'] = df_combined['车辆配置'].fillna(df_combined['车型'])
        
        logging.info(f"销售台账数据合并完成，总行数：{len(df_combined)}")
        return df_combined
    
    # 注意：由于代码长度限制，这里只展示了部分核心方法
    # 完整代码会包含所有原代码中的合并方法
    
    def concat_newold_tuis_dashboard(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        合并新旧退订数据
        
        返回:
            tuple: (退订数据, 所有订单数据)
        """
        logging.info("开始合并退订数据")
        
        # 加载订单数据
        df_books = self.concat_newold_books_dashboard()
        
        # 加载历史退订数据
        df_tuis_lock = self.load_tuis_lock()
        tuis_lock_columns = [
            '定单日期', '车系', '外饰颜色', '车型', '销售顾问', '所属团队', '定单归属门店', '客户姓名',
            '联系电话', '退定日期', '非退定核算'
        ]
        df_tuis_lock = df_tuis_lock[tuis_lock_columns].copy()
        
        # 加载数据库退订数据
        df_tuis_cyy = self.load_tuis_cyy()
        tuis_cyy_columns = [
            '订单门店', '业务渠道', '销售人员', '订单日期', '车系', '车型', '外饰颜色', '配置', '主播人员',
            '客户名称', '客户电话', '作废类型', '退订原因', '退定日期', '非退定核算'
        ]
        df_tuis_cyy = df_tuis_cyy[tuis_cyy_columns].copy()
        
        # 过滤无效作废类型
        invalid_types = ['错误录入', '重复录入']
        df_tuis_cyy = df_tuis_cyy[~df_tuis_cyy['作废类型'].isin(invalid_types)]
        
        # 重命名列
        column_mapping = {
            '订单门店': '定单归属门店',
            '订单日期': '定单日期',
            '销售人员': '销售顾问',
            '客户名称': '客户姓名',
            '客户电话': '联系电话',
            '业务渠道': '所属团队'
        }
        df_tuis_cyy.rename(columns=column_mapping, inplace=True)
        
        # 标记订单数据
        df_books['非退定核算'] = -1
        
        # 处理历史数据日期
        df_tuis_lock['退定日期'] = pd.to_datetime(df_tuis_lock['退定日期'], errors='coerce')
        df_tuis_lock = df_tuis_lock[df_tuis_lock['退定日期'] < '2025-04-01'].copy()
        
        # 合并退订数据
        df_combined_tuis = pd.concat([df_tuis_cyy, df_tuis_lock], axis=0, ignore_index=True)
        
        # 合并所有订单数据
        df_all_orders = pd.concat([df_books, df_combined_tuis], axis=0, ignore_index=True)
        
        # 计算有效订单核算日期
        df_all_orders['有效定单核算日期'] = df_all_orders['退定日期']
        df_all_orders['有效定单核算日期'] = df_all_orders['有效定单核算日期'].fillna(df_all_orders['定单日期'])
        
        logging.info(f"退订数据合并完成，退订行数：{len(df_combined_tuis)}，所有订单行数：{len(df_all_orders)}")
        return df_combined_tuis, df_all_orders
    
    def concat_newold_inventorys_dashboard(self) -> pd.DataFrame:
        """
        合并库存数据
        
        输入：数据库库存数据
        输出：处理后的库存数据
        """
        logging.info("开始处理库存数据")
        
        # 加载数据库库存数据
        df_inventorys_cyy = self.load_inventorys_cyy()
        inventory_columns = [
            '采购订单号', '归属系统', '车系', '车型', '颜色', '配置', '车架号', '指导价', '提货价', '生产日期',
            '合格证状态', '发车日期', '到库日期', '库存天数', '车辆状态', '操作日期', '调入类型'
        ]
        df_inventorys_cyy = df_inventorys_cyy[inventory_columns].copy()
        
        # 重命名列
        df_inventorys_cyy.rename(columns={
            '库存天数': '库存时间',
            '归属系统': '归属系统1',
            '颜色': '外饰颜色'
        }, inplace=True)
        
        # 分类库存时长
        df_inventorys_cyy['库存时长分类'] = df_inventorys_cyy.apply(
            self.data_processor.classify_inventory_duration,
            axis=1
        )
        
        # 加载计划日期数据
        df_plan_date = self.load_plan_date_get()
        
        # 合并不同订单号的计划日期
        plan_date_map = pd.concat([
            df_plan_date[['采购订单号', '计划日期']].rename(columns={'采购订单号': '采购订单号'}).copy(),
            df_plan_date[['销服订单号', '计划日期']].rename(columns={'销服订单号': '采购订单号'}).copy(),
            df_plan_date[['采购改单编号', '计划日期']].rename(columns={'采购改单编号': '采购订单号'}).copy()
        ]).dropna(subset=['采购订单号']).drop_duplicates(subset=['采购订单号'])
        
        # 合并计划日期
        df_inventorys_merged = pd.merge(
            df_inventorys_cyy,
            plan_date_map,
            on='采购订单号',
            how='left'
        )
        
        # 清理数据并计算采购提前期
        df_cleaned = df_inventorys_merged.dropna(subset=['到库日期', '计划日期']).copy()
        df_cleaned['计划日期'] = df_cleaned['计划日期'].fillna(df_cleaned['发车日期'])
        
        # 转换日期类型
        df_cleaned['到库日期'] = pd.to_datetime(df_cleaned['到库日期'])
        df_cleaned['计划日期'] = pd.to_datetime(df_cleaned['计划日期'])
        
        # 计算采购提前期
        df_cleaned['采购提前期'] = (df_cleaned['到库日期'] - df_cleaned['计划日期']).dt.days + 1
        
        # 合并采购提前期
        df_final = df_inventorys_merged.join(df_cleaned['采购提前期'])
        
        logging.info(f"库存数据处理完成，总行数：{len(df_final)}")
        return df_final
    
    def concat_newold_books_dashboard(self) -> pd.DataFrame:
        """
        合并新旧订单数据
        
        输入：订单历史数据 + 数据库订单数据
        输出：合并后的订单数据
        """
        logging.info("开始合并订单数据")
        
        # 加载数据库订单数据
        df_books_cyy = self.load_books_cyy()
        books_cyy_columns = [
            '车架号', '订单日期', '定单日期', '销售人员', '定金金额', '审批状态', '定单归属门店', '所属团队',
            '车系', '外饰颜色', '车型', '配置', '定单状态', '主播人员', '联系电话', '联系电话2'
        ]
        df_books_cyy = df_books_cyy[books_cyy_columns].copy()
        
        # 加载历史订单数据
        df_books_lock = self.load_books_lock()
        books_lock_columns = [
            '定单日期', '车系', '外饰颜色', '车型', '车架号', '定单状态', '销售顾问', '所属团队', '定单归属门店', '定金金额'
        ]
        df_books_lock = df_books_lock[books_lock_columns].copy()
        
        # 重命名数据库列
        df_books_cyy.rename(columns={
            '订单日期': '定单日期',
            '定单日期': '订金日期',
            '销售人员': '销售顾问'
        }, inplace=True)
        
        # 过滤审批通过的数据
        df_books_cyy = df_books_cyy[df_books_cyy['审批状态'] == '审核通过'].copy()
        
        # 处理日期
        df_books_lock['定单日期'] = pd.to_datetime(df_books_lock['定单日期'], errors='coerce')
        
        # 按日期分割数据
        df_books_cyy = df_books_cyy[df_books_cyy['定单日期'] > '2025-04-01'].copy()
        df_books_lock = df_books_lock[df_books_lock['定单日期'] < '2025-04-01'].copy()
        
        # 合并数据
        df_combined = pd.concat([df_books_cyy, df_books_lock], axis=0, ignore_index=True)
        
        # 填充配置列
        df_combined['配置'] = df_combined['配置'].fillna(df_combined['车型'])
        
        logging.info(f"订单数据合并完成，总行数：{len(df_combined)}")
        return df_combined
    
    def concat_newold_jingpins_dashboard(self) -> pd.DataFrame:
        """
        合并新旧精品销售数据
        
        输入：精品销售历史数据 + 数据库精品数据
        输出：合并后的精品销售数据
        """
        logging.info("开始合并精品销售数据")
        
        # 加载数据库精品数据
        df_jingpins_cyy = self.load_jingpins_cyy()
        jingpins_cyy_columns = [
            '单据类型', '订单门店', '开票日期', '收款日期', '最早收款日期', '精品销售人员', '车架号', '车系',
            '客户名称', '联系电话', '物资明细', '销售总金额', '总成本', '毛利润', '总次数'
        ]
        df_jingpins_cyy = df_jingpins_cyy[jingpins_cyy_columns].copy()
        
        # 加载历史精品数据
        df_jingpins_lock = self.load_jingpins_lock()
        jingpins_lock_columns = [
            '精品销售日期', '精品销售人员', '新车销售门店', '车型', '车架号', '客户姓名', '电话号码', '销售总金额',
            '总成本', '毛利润', '总次数'
        ]
        df_jingpins_lock = df_jingpins_lock[jingpins_lock_columns].copy()
        
        # 加载团队和车辆信息
        team_belongs = self.load_team_belongs()
        service_net = self.load_car_belongs()
        
        # 重命名数据库列
        df_jingpins_cyy.rename(columns={
            '最早收款日期': '精品销售日期',
            '订单门店': '新车销售门店',
            '联系电话': '电话号码',
            '客户名称': '客户姓名'
        }, inplace=True)
        
        # 处理日期
        df_jingpins_cyy['精品销售日期'] = pd.to_datetime(df_jingpins_cyy['精品销售日期'], format='mixed')
        df_jingpins_lock['精品销售日期'] = pd.to_datetime(df_jingpins_lock['精品销售日期'], format='mixed')
        
        # 合并团队信息
        df_jingpins_lock = pd.merge(
            df_jingpins_lock,
            team_belongs[['公司名称', '板块']],
            how='left',
            left_on='新车销售门店',
            right_on='公司名称'
        )
        
        # 按日期和板块筛选数据
        df_jingpins_cyy = df_jingpins_cyy[df_jingpins_cyy['精品销售日期'] >= '2025-04-01'].copy()
        df_jingpins_lock1 = df_jingpins_lock[df_jingpins_lock['精品销售日期'] < '2025-04-01'].copy()
        df_jingpins_lock2 = df_jingpins_lock[df_jingpins_lock['板块'] == '西河'].copy()
        
        # 合并所有数据
        df_combined = pd.concat(
            [df_jingpins_cyy, df_jingpins_lock1, df_jingpins_lock2],
            axis=0,
            ignore_index=True
        )
        
        # 合并服务网络信息
        df_combined = pd.merge(df_combined, service_net[['车系', '服务网络']], how='left', on='车系')
        
        # 处理直播基地的门店名称
        df_combined['新车销售门店'] = np.where(
            df_combined['新车销售门店'] == '直播基地',
            df_combined['服务网络'] + '-' + df_combined['新车销售门店'],
            df_combined['新车销售门店']
        )
        
        logging.info(f"精品销售数据合并完成，总行数：{len(df_combined)}")
        return df_combined
    
    def concat_newold_debits_dashboard(self) -> pd.DataFrame:
        """
        合并新旧三方台账数据
        
        输入：三方台账历史数据 + 数据库三方台账数据
        输出：合并后的三方台账数据
        """
        logging.info("开始合并三方台账数据")
        
        # 加载数据库三方台账数据
        df_debits_cyy = self.load_debits_cyy()
        debits_cyy_columns = [
            '合格证门店', '采购订单号', '车源门店', '开票日期', '保证金比例', '到期日期', '开票银行', '合格证号',
            '车架号', '提货价', '赎证日期', '赎证款', '是否赎证'
        ]
        df_debits_cyy = df_debits_cyy[debits_cyy_columns].copy()
        
        # 加载历史三方台账数据
        df_debits_lock = self.load_debits_lock()
        debits_lock_columns = [
            '订购日期', '采购订单号', '车架号', '赎证日期', '提货价', '赎证款', '保证金比例', '开票银行', '开票日期',
            '到期日期', '是否赎证', '最新到期日期', '归属系统1'
        ]
        df_debits_lock = df_debits_lock[debits_lock_columns].copy()
        
        # 重命名数据库列
        df_debits_cyy.rename(columns={'合格证门店': '归属系统1'}, inplace=True)
        
        # 添加最新到期日期列
        df_debits_cyy['最新到期日期'] = df_debits_cyy['到期日期']
        
        # 处理是否赎证列
        df_debits_cyy['是否赎证'] = df_debits_cyy['是否赎证'].astype('int')
        df_debits_lock['是否赎证'] = df_debits_lock['是否赎证'].astype('int')
        
        # 筛选已赎证的历史数据
        df_debits_lock = df_debits_lock[df_debits_lock['是否赎证'] == 1].copy()
        
        # 处理数据库数据日期
        df_debits_cyy['开票日期'] = pd.to_datetime(df_debits_cyy['开票日期'], errors='coerce')
        
        # 按日期和赎证状态分割数据库数据
        df_debits_cyy0 = df_debits_cyy[
            (df_debits_cyy['开票日期'] >= '2025-04-01') & (df_debits_cyy['是否赎证'] == 1)
        ].copy()
        
        df_debits_cyy1 = df_debits_cyy[df_debits_cyy['是否赎证'] == 0].copy()
        
        # 合并所有数据
        df_combined = pd.concat(
            [df_debits_cyy1, df_debits_cyy0, df_debits_lock],
            axis=0,
            ignore_index=True
        )
        
        # 标准化开票银行名称
        bank_mapping = {
            '光大': '光大',
            '中信': '中信',
            '兴业': '兴业',
            '平安': '平安',
            '交通': '交通',
            '工商': '工商',
            '招商': '招商'
        }
        
        def standardize_bank_name(bank_name):
            if not isinstance(bank_name, str):
                return bank_name
            
            for bank_key, bank_value in bank_mapping.items():
                if bank_key in bank_name:
                    return bank_value
            
            return bank_name
        
        df_combined['开票银行'] = df_combined['开票银行'].apply(standardize_bank_name)
        
        logging.info(f"三方台账数据合并完成，总行数：{len(df_combined)}")
        return df_combined
    
    def concat_unsold_book_dashboard(self) -> pd.DataFrame:
        """
        处理未售订单数据
        
        输入：未售订单数据
        输出：处理后的未售订单数据
        """
        logging.info("开始处理未售订单数据")
        
        # 加载未售订单数据
        df_unsold_book = self.load_books_unsold().copy()
        
        # 定义车辆状态映射函数
        def map_vehicle_status(status):
            if status == '计划':
                return '未发'
            elif status == '在途':
                return '在途'
            elif pd.notnull(status):
                return '在库'
            else:
                return None
        
        # 过滤有锁库日期的数据
        df_unsold_book = df_unsold_book[~df_unsold_book['锁库日期'].isnull()].copy()
        
        # 映射车辆状态
        df_unsold_book['车辆状态'] = df_unsold_book['车辆状态'].apply(map_vehicle_status)
        
        # 选择需要的列
        selected_columns = [
            '订单公司', '车系', '外饰颜色', '整车型号', '配置', '销售人员', '交付专员', '主播人员',
            '客户姓名', '联系电话', '联系电话2', '车架号', '计划单号', '锁库日期', '来源', '入库日期', '车辆状态'
        ]
        df_unsold_book = df_unsold_book[selected_columns].copy()
        
        # 重命名列
        column_mapping = {
            '订单公司': '公司名称',
            '整车型号': '车型',
            '来源': '所属团队'
        }
        df_unsold_book.rename(columns=column_mapping, inplace=True)
        
        # 过滤车辆状态
        valid_statuses = ['在库', '在途']
        df_unsold_book = df_unsold_book[df_unsold_book['车辆状态'].isin(valid_statuses)]
        
        logging.info(f"未售订单数据处理完成，总行数：{len(df_unsold_book)}")
        return df_unsold_book
    
    def clean_yingxiao_data(self) -> pd.DataFrame:
        """
        清洗营销费用数据
        
        输入：营销费用数据
        输出：处理后的营销费用数据
        """
        logging.info("开始清洗营销费用数据")
        
        # 加载营销费用数据
        df_yingxiao = self.load_yingxiao_data().copy()
        
        # 添加月末日期
        df_yingxiao = self.data_processor.add_month_end_date(df_yingxiao)
        
        # 过滤非随车项目
        df_yingxiao = df_yingxiao[df_yingxiao['项目分类'] != '随车'].copy()
        
        # 转换费用合计为数值类型
        df_yingxiao['费用合计'] = pd.to_numeric(df_yingxiao['费用合计'], errors='coerce').fillna(0)
        
        # 按日期和门店分组汇总
        df_yingxiao_grouped = df_yingxiao.groupby(['日期', '归属门店']).agg({
            '费用合计': 'sum'
        }).reset_index()
        
        logging.info(f"营销费用数据清洗完成，总行数：{len(df_yingxiao_grouped)}")
        return df_yingxiao_grouped
    
    def clean_salary_data(self) -> pd.DataFrame:
        """
        清洗薪资数据
        
        输入：薪资数据
        输出：处理后的薪资数据
        """
        logging.info("开始清洗薪资数据")
        
        # 加载薪资数据
        df_salary = self.load_salary_data().copy()
        
        # 处理Excel日期（Excel日期是从1899-12-30开始的天数）
        excel_start_date = pd.Timestamp('1899-12-30')
        df_salary['年月'] = df_salary['年月'].apply(
            lambda x: excel_start_date + pd.Timedelta(days=x) if pd.notnull(x) else pd.NaT
        )
        
        # 转换为日期时间
        df_salary['年月'] = pd.to_datetime(df_salary['年月'])
        
        # 计算月末日期
        df_salary['日期'] = df_salary['年月'] + pd.offsets.MonthEnd(0)
        
        # 转换数值列
        numeric_columns = ['月薪酬', '月社保']
        df_salary[numeric_columns] = df_salary[numeric_columns].apply(
            lambda x: pd.to_numeric(x, errors='coerce').fillna(0)
        )
        
        # 计算总薪酬
        df_salary['总薪酬'] = df_salary['月薪酬'] + df_salary['月社保']
        df_salary['总薪酬'] = pd.to_numeric(df_salary['总薪酬'], errors='coerce').fillna(0).astype(float)
        
        # 按日期和门店分组汇总
        df_salary_grouped = df_salary.groupby(['日期', '门店']).agg({
            '总薪酬': 'sum'
        }).reset_index()
        
        logging.info(f"薪资数据清洗完成，总行数：{len(df_salary_grouped)}")
        return df_salary_grouped
    
    def generate_all_data(self) -> Dict[str, pd.DataFrame]:
        """
        生成所有仪表板数据
        
        返回:
            dict: 包含所有处理后的数据集的字典
        """
        logging.info("开始生成所有仪表板数据")
        
        # 加载基础数据
        car_belongs = self.load_car_belongs()
        
        # 处理各类数据
        processed_data = {
            'chexi': car_belongs,
            'df_yingxiao': self.clean_yingxiao_data(),
            'df_salary': self.clean_salary_data(),
            'df_sales': self.concat_newold_sales_dashboard(),
            'df_sales1': self.concat_newold_sales_dashboard1(),
            'df_books': self.concat_newold_books_dashboard(),
            'df_inventorys': self.concat_newold_inventorys_dashboard(),
            'df_jingpins': self.concat_newold_jingpins_dashboard(),
            'df_tuis': self.concat_newold_tuis_dashboard()[0],  # 退订数据
            'df_dings_all': self.concat_newold_tuis_dashboard()[1],  # 所有订单数据
            'df_debits': self.concat_newold_debits_dashboard(),
            'df_ers': self.concat_newold_ers_dashboard(),
            'df_unsold_book': self.concat_unsold_book_dashboard(),
            'df_inventory_lock': self.load_inventory_lock()[['车系', '配置', '车型', '颜色']]
                                   .drop_duplicates().copy()
        }
        
        # 数据后期处理
        self._post_process_data(processed_data)
        
        logging.info("所有仪表板数据生成完成")
        return processed_data
    
    def _post_process_data(self, data_dict: Dict[str, pd.DataFrame]):
        """
        数据后期处理
        
        Args:
            data_dict: 包含所有数据集的字典
        """
        logging.info("开始数据后期处理")
        
        # 1. 处理数值列
        data_dict['df_salary']['总薪酬'] = pd.to_numeric(
            data_dict['df_salary']['总薪酬'], errors='coerce'
        ).fillna(0).astype(float)
        
        data_dict['df_yingxiao']['费用合计'] = pd.to_numeric(
            data_dict['df_yingxiao']['费用合计'], errors='coerce'
        ).fillna(0).astype(float)
        
        # 2. 单独处理车系智驾操作
        data_dict['df_sales']['提货价'] = data_dict['df_sales']['提货价'].astype('float')
        data_dict['df_sales']['车系'] = np.where(
            (data_dict['df_sales']['车系'] == "2025款海鸥") & 
            (data_dict['df_sales']['提货价'] == 65800),
            '2025款 海鸥',
            data_dict['df_sales']['车系']
        )
        
        # 3. 时间判断逻辑
        now = datetime.now()
        if now.hour < 20:
            data_dict['df_sales']['销售日期'] = data_dict['df_sales']['销售日期'].fillna(
                data_dict['df_sales']['收款日期']
            )
            data_dict['df_sales']['销售日期'] = pd.to_datetime(
                data_dict['df_sales']['销售日期'], format='mixed', errors='coerce'
            )
            data_dict['df_sales'] = data_dict['df_sales'][
                data_dict['df_sales']['销售日期'] < pd.Timestamp.today().normalize()
            ].copy()
        
        # 4. 销售顾问在职天数计算
        self._calculate_sales_consultant_work_days(data_dict)
        
        # 5. 匹配非智驾类型
        self._match_non_smart_drive_types(data_dict)
        
        # 6. 生成车系配置数据
        self._generate_car_series_data(data_dict)
        
        logging.info("数据后期处理完成")
    
    def _calculate_sales_consultant_work_days(self, data_dict: Dict[str, pd.DataFrame]):
        """
        计算销售顾问在职天数
        
        Args:
            data_dict: 包含所有数据集的字典
        """
        df_sales_copy = data_dict['df_sales'].copy()
        df_sales_copy['销售日期'] = pd.to_datetime(
            df_sales_copy['销售日期'], format='mixed', errors='coerce'
        )
        
        # 计算在职天数
        df_sales_copy['在职天数'] = df_sales_copy.groupby('销售人员')['销售日期'].transform(
            lambda x: (x.max() - x.min()).days if len(x) > 1 else 0
        )
        
        # 排序并获取最新记录
        df_sales_sorted = df_sales_copy.sort_values(
            by=['销售人员', '销售日期'], ascending=[True, False]
        ).copy()
        
        latest_records = df_sales_sorted.drop_duplicates(subset='销售人员', keep='first').copy()
        
        # 计算在职月份
        latest_records['在职月份'] = np.ceil(latest_records['在职天数'] / 30)
        
        # 创建辅助列
        latest_records['销售顾问辅助列'] = (
            latest_records['公司名称'].astype(str) + "-" + 
            latest_records['销售人员'].astype(str)
        )
        
        # 选择需要的列
        assistant_data = latest_records[['销售人员', '公司名称', '在职天数', '在职月份', '销售顾问辅助列']].copy()
        data_dict['assistant_sales_consultant'] = assistant_data
    
    def _match_non_smart_drive_types(self, data_dict: Dict[str, pd.DataFrame]):
        """
        匹配非智驾类型
        
        Args:
            data_dict: 包含所有数据集的字典
        """
        car_belongs = data_dict['chexi']
        
        # 销售台账匹配非智驾
        data_dict['df_sales1']['提货价'] = data_dict['df_sales1']['提货价'].astype('float')
        data_dict['df_sales1']['车系'] = np.where(
            (data_dict['df_sales1']['车系'] == "2025款海鸥") & 
            (data_dict['df_sales1']['提货价'] == 65800),
            '2025款 海鸥',
            data_dict['df_sales1']['车系']
        )
        data_dict['df_sales1'] = pd.merge(
            data_dict['df_sales1'],
            car_belongs[['车系', '类型']],
            on=['车系'],
            how='left'
        ).copy()
        
        # 销售毛利匹配非智驾
        data_dict['df_sales']['提货价'] = data_dict['df_sales']['提货价'].astype('float')
        data_dict['df_sales']['车系'] = np.where(
            (data_dict['df_sales']['车系'] == "2025款海鸥") & 
            (data_dict['df_sales']['提货价'] == 65800),
            '2025款 海鸥',
            data_dict['df_sales']['车系']
        )
        data_dict['df_sales'] = pd.merge(
            data_dict['df_sales'],
            car_belongs[['车系', '类型']],
            on=['车系'],
            how='left'
        ).copy()
        
        # 订单匹配非智驾
        data_dict['df_books'] = pd.merge(
            data_dict['df_books'],
            car_belongs[['车系', '类型']],
            on=['车系'],
            how='left'
        ).copy()
        
        # 库存匹配非智驾
        data_dict['df_inventorys']['提货价'] = data_dict['df_inventorys']['提货价'].astype('float')
        data_dict['df_inventorys']['车系'] = np.where(
            (data_dict['df_inventorys']['车系'] == "2025款海鸥") & 
            (data_dict['df_inventorys']['提货价'] == 65800),
            '2025款 海鸥',
            data_dict['df_inventorys']['车系']
        )
        data_dict['df_inventorys'] = pd.merge(
            data_dict['df_inventorys'],
            car_belongs[['车系', '类型']],
            on=['车系'],
            how='left'
        ).copy()
    
    def _generate_car_series_data(self, data_dict: Dict[str, pd.DataFrame]):
        """
        生成车系配置数据
        
        Args:
            data_dict: 包含所有数据集的字典
        """
        logging.info("开始生成车系配置数据")
        
        # 准备数据源
        replacement_rules = {
            '豹5-': '',
            '豹8-': '',
            '宋L EV智驾版': '',
            'Z9GT': '',
            'DM-i': '',
            '23款混动': '',
            '钛3-': '',
        }
        
        # 处理配置列
        data_dict['df_books']['配置'] = np.where(
            data_dict['df_books']['配置'].isnull(),
            data_dict['df_books']['车型'],
            data_dict['df_books']['配置']
        )
        
        data_dict['df_dings_all']['配置'] = np.where(
            data_dict['df_dings_all']['配置'].isnull(),
            data_dict['df_dings_all']['车型'],
            data_dict['df_dings_all']['配置']
        )
        
        data_dict['df_tuis']['配置'] = np.where(
            data_dict['df_tuis']['配置'].isnull(),
            data_dict['df_tuis']['车型'],
            data_dict['df_tuis']['配置']
        )
        
        # 准备各数据源
        df1 = data_dict['df_sales'][['车系', '车型', '外饰颜色']].copy()
        df1.columns = ['车系', '配置', '外饰颜色']
        
        df2 = data_dict['df_books'][['车系', '车型', '外饰颜色']].copy()
        df2.columns = ['车系', '配置', '外饰颜色']
        
        df3 = data_dict['df_tuis'][['车系', '车型', '外饰颜色']].copy()
        df3.columns = ['车系', '配置', '外饰颜色']
        
        df4 = data_dict['df_inventorys'][['车系', '车型', '外饰颜色']].copy()
        df4.columns = ['车系', '配置', '外饰颜色']
        
        data_dict['df_inventory_lock'].columns = ['车系', '车辆配置', '车型', '外饰颜色']
        
        # 合并所有车系数据
        all_car_series = pd.concat([
            data_dict['df_sales'][['车系', '车辆配置', '车型', '外饰颜色']].copy(),
            data_dict['df_inventory_lock'].copy(),
            df1.copy(),
            df2.copy(),
            df3.copy(),
            df4.copy(),
            data_dict['df_books'][['车系', '车型', '配置', '外饰颜色']].copy(),
            data_dict['df_tuis'][['车系', '车型', '配置', '外饰颜色']].copy(),
            data_dict['df_inventorys'][['车系', '车型', '配置', '外饰颜色']].copy()
        ])
        
        # 处理配置和车型列
        all_car_series['配置'] = all_car_series['配置'].fillna(all_car_series['车辆配置'])
        all_car_series['车型'] = all_car_series['车型'].fillna(all_car_series['配置'])
        
        # 创建辅助列并进行标准化
        all_car_series['辅助'] = all_car_series['车系'].astype(str) + all_car_series['配置'].astype(str)
        all_car_series['辅助'] = all_car_series['辅助'].apply(self.data_processor.standardize_text)
        
        # 去重
        all_car_series = all_car_series.drop_duplicates(subset=['辅助']).copy()
        
        # 选择最终列并应用替换规则
        all_car_series = all_car_series[['车系', '车型', '配置', '外饰颜色']].copy()
        
        # 应用文本替换规则
        for col in ['车型', '配置']:
            all_car_series[col] = all_car_series[col].apply(
                lambda x: self.data_processor.apply_replacements(str(x), replacement_rules)
            )
        
        # 清理文本
        all_car_series['车型'] = all_car_series['车型'].str.replace('\n', ' ', regex=True)
        all_car_series['车型'] = all_car_series['车型'].str.strip()
        
        # 过滤无效车系
        invalid_series = ['二手车返利', '调拨']
        all_car_series = all_car_series[~all_car_series['车系'].isin(invalid_series)].copy()
        
        data_dict['all_car_series'] = all_car_series
        logging.info(f"车系配置数据生成完成，总行数：{len(all_car_series)}")
    
    def save_all_data_to_csv(self, data_dict: Dict[str, pd.DataFrame], output_dir: str):
        """
        保存所有数据到CSV文件
        
        Args:
            data_dict: 包含所有数据集的字典
            output_dir: 输出目录
        """
        logging.info("开始保存数据到CSV文件")
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 定义输出文件名映射
        output_mapping = {
            'df_sales': '销售毛利1.csv',
            'df_books': '定车1.csv',
            'df_inventorys': '库存1.csv',
            'df_jingpins': '精品销售1.csv',
            'df_tuis': '退订1.csv',
            'df_dings_all': '所有定单1.csv',
            'df_debits': '三方台账1.csv',
            'df_sales1': '销售1.csv',
            'df_ers': '二手车1.csv',
            'df_unsold_book': '未售锁车.csv',
            'df_salary': '销售薪资.csv',
            'df_yingxiao': '市场费用.csv',
            'all_car_series': '所有车系.csv',
            'assistant_sales_consultant': '辅助_销售顾问.csv'
        }
        
        # 保存每个数据集
        for data_key, file_name in output_mapping.items():
            if data_key in data_dict:
                output_path = os.path.join(output_dir, file_name)
                data_dict[data_key].to_csv(output_path, index=False, encoding='utf-8-sig')
                logging.info(f"已保存: {file_name} ({len(data_dict[data_key])}行)")
        
        logging.info("所有数据已保存到CSV文件")
    
    def save_all_data_to_database(self, data_dict: Dict[str, pd.DataFrame]):
        """
        保存所有数据到数据库
        
        Args:
            data_dict: 包含所有数据集的字典
        """
        logging.info("开始保存数据到数据库")
        
        # 定义表名映射
        table_mapping = {
            'df_sales': 'sales_profit',
            'df_books': 'orders',
            'df_inventorys': 'inventory',
            'df_jingpins': 'premium_sales',
            'df_tuis': 'refunds',
            'df_dings_all': 'all_orders',
            'df_debits': 'third_party_ledger',
            'df_sales1': 'sales',
            'df_ers': 'used_cars',
            'df_unsold_book': 'unsold_locked',
            'df_salary': 'sales_salary',
            'df_yingxiao': 'marketing_expenses',
            'all_car_series': 'all_car_series',
            'assistant_sales_consultant': 'assistant_sales_consultant'
        }
        
        # 保存每个数据集到数据库
        success_count = 0
        total_count = len(table_mapping)
        
        for data_key, table_name in table_mapping.items():
            if data_key in data_dict:
                try:
                    self.db.write_to_database(data_dict[data_key], table_name)
                    success_count += 1
                    logging.info(f"已保存到数据库表: {table_name}")
                except Exception as e:
                    logging.error(f"保存到数据库表失败 ({table_name}): {str(e)}")
        
        logging.info(f"数据库保存完成: {success_count}/{total_count} 个表成功保存")