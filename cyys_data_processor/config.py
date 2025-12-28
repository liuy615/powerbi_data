# -*- coding: utf-8 -*-
"""
配置文件：常量、路径配置
"""

import os
from datetime import datetime

# 日志配置
LOG_DIR = r"E:/pycharm_project/powerbi/data/log"

# 文件路径配置
SERVICE_NET_PATH = r'C:/Users/刘洋/Documents/WXWork/1688858189749305/WeDrive/成都永乐盛世/维护文件/看板部分数据源/各公司银行额度.xlsx'
MAPPING_EXCEL_PATH = r"/powerbi_data/data/字段对应 - csv.xlsx"
USED_CAR_REBATE_PATH = r"E:/powerbi_data/看板数据/cyy_old_data/二手车返利存档.csv"

# 数据库配置
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

# MongoDB配置
MONGODB_URI = 'mongodb://xg_wd:H91NgHzkvRiKygTe4X4ASw@192.168.1.7:27017/xg?authSource=xg&authMechanism=SCRAM-SHA-256'
MONGODB_DB = 'xg'

# API配置
NOTIFY_API_URL = 'http://192.168.1.7/send_md_to_person'

# 表映射
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

# 公司列表
COMPANIES = [
    '成都新港建元汽车销售服务有限公司',
    '成都永乐盛世汽车销售服务有限公司',
    '成都新港永初汽车服务有限公司',
    '成都新港海川汽车销售服务有限公司',
    '成都新港先秦汽车销售服务有限公司',
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

# 贵州公司列表
GUIZHOU_COMPANIES = ['贵州新港澜源', '贵州新港海之辇', '贵州新港浩蓝', '贵州新港蔚蓝', '贵州上元曦和', '贵州新港澜轩', '贵州上元臻智', '贵州上元坤灵']

# 排除员工名单
EXCLUDED_STAFF = ['苏秀清', '杜甯', '周杨', '李钰', '易阳梅', '黄毓香', '王勇', '钟鸣', '刘前虎']

# 内部公司名单
INTERNAL_COMPANIES = [
    '新港建元', '永乐盛世', '新港永初', '新港海川', '新港先秦', '新港治元', '新港建隆',
    '王朝网-直播基地', '文景初治', '新港建武', '文景海洋', '文景盛世', '新港澜阔', '鑫港鲲鹏',
    '新港澜舰', '新茂元大', '新港浩蓝', '新港澜轩', '贵州新港澜源', '贵州新港海之辇',
    '贵州新港浩蓝', '贵州新港蔚蓝', '上元臻智', '上元臻享', '贵州上元臻智', '腾势-直播基地',
    '宜宾上元臻智', '乐山上元臻智', '上元坤灵', '上元曦和', '贵州上元曦和', '贵州新港澜轩',
    '方程豹-直播基地', '宜宾上元曦和', '乐山上元曦和', '泸州上元坤灵', '新港澜洲'
]

# 表唯一键配置
TABLE_UNIQUE_KEY = {
    'mortgage_business': 'ID',
    'insurance_business': 'ID',
    'car_cost_management': 'OrderCode',
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