"""
数据处理和清洗
"""
import pandas as pd
import numpy as np
from datetime import datetime

class DataProcessor:
    """数据处理类"""

    def __init__(self):
        print(f"[{datetime.now()}] 初始化数据处理器")

    def clean_sales_data(self, df):
        """清洗销售数据"""
        print(f"[{datetime.now()}] 开始清洗销售数据，原始记录: {len(df)} 条")

        if df.empty:
            print(f"[{datetime.now()}] 警告: 销售数据为空")
            return pd.DataFrame()

        try:
            # 1. 选择需要的字段
            required_fields = ['服务网络', '公司名称', '销售日期', '车架号', '车系','所属团队', '销售人员', '主播人员', '销售车价', '提货价', '返利合计', '购买方式', '金融类型', '金融毛利', '上牌毛利','二手车返利金额', '单车毛利']

            # 检查字段是否存在
            available_fields = [col for col in required_fields if col in df.columns]
            missing_fields = [col for col in required_fields if col not in df.columns]



            if missing_fields:
                print(f"[{datetime.now()}] 警告: 缺少字段: {missing_fields}")

            # 创建新DataFrame，只包含可用的字段
            clean_df = df[available_fields].copy()
            clean_df = clean_df[~clean_df['所属团队'].isin(['调拨', '其他', '试驾'])]

            # 2. 基础数据清洗
            # 字符串字段去除空格
            string_columns = clean_df.select_dtypes(include=['object']).columns
            for col in string_columns:
                clean_df[col] = clean_df[col].astype(str).str.strip()

            # 数值字段转换
            numeric_fields = ['销售车价', '提货价', '返利合计', '金融毛利', '上牌毛利', '二手车返利金额', '单车毛利']
            for field in numeric_fields:
                if field in clean_df.columns:
                    # 去除逗号，转换为数值
                    clean_df[field] = pd.to_numeric(
                        clean_df[field].astype(str).str.replace(',', ''),
                        errors='coerce'
                    ).fillna(0)

            # 日期字段转换
            if '销售日期' in clean_df.columns:
                clean_df['销售日期'] = pd.to_datetime(clean_df['销售日期'], errors='coerce')

            # 3. 处理空值和异常
            # 去除车架号为空的记录
            if '车架号' in clean_df.columns:
                clean_df = clean_df[clean_df['车架号'].notna() & (clean_df['车架号'] != 'nan')]
                print(f"[{datetime.now()}] 清理后记录: {len(clean_df)} 条")

            print(f"[{datetime.now()}] 销售数据清洗完成")
            return clean_df

        except Exception as e:
            print(f"[{datetime.now()}] 清洗销售数据时出错: {e}")
            return pd.DataFrame()

    def clean_inventory_data(self, df):
        """清洗库存数据"""
        print(f"[{datetime.now()}] 开始清洗库存数据，原始记录: {len(df)} 条")

        if df.empty:
            print(f"[{datetime.now()}] 警告: 库存数据为空")
            return pd.DataFrame()

        try:
            # 1. 选择需要的字段
            required_fields = ['归属系统', '车系', '车型', '颜色', '到库日期','库存天数', '车辆状态', '服务网络']

            # 检查字段是否存在
            available_fields = [col for col in required_fields if col in df.columns]
            missing_fields = [col for col in required_fields if col not in df.columns]

            if missing_fields:
                print(f"[{datetime.now()}] 警告: 缺少字段: {missing_fields}")

            # 创建新DataFrame，只包含可用的字段
            clean_df = df[available_fields].copy()

            # 2. 基础数据清洗
            # 字符串字段去除空格
            string_columns = clean_df.select_dtypes(include=['object']).columns
            for col in string_columns:
                clean_df[col] = clean_df[col].astype(str).str.strip()

            # 数值字段转换
            if '库存天数' in clean_df.columns:
                clean_df['库存天数'] = pd.to_numeric(clean_df['库存天数'], errors='coerce').fillna(0).astype(int)

            # 日期字段转换
            if '到库日期' in clean_df.columns:
                clean_df['到库日期'] = pd.to_datetime(clean_df['到库日期'], errors='coerce')

            # 3. 处理空值
            # 去除车系为空的记录
            if '车系' in clean_df.columns:
                clean_df = clean_df[clean_df['车系'].notna() & (clean_df['车系'] != 'nan')]
                print(f"[{datetime.now()}] 清理后记录: {len(clean_df)} 条")

            print(f"[{datetime.now()}] 库存数据清洗完成")
            return clean_df

        except Exception as e:
            print(f"[{datetime.now()}] 清洗库存数据时出错: {e}")
            return pd.DataFrame()

    def generate_summary(self, sales_df, inventory_df):
        """生成数据摘要"""
        print("\n" + "="*50)
        print("数据清洗摘要")
        print("="*50)

        print(f"\n销售数据:")
        print(f"  总记录数: {len(sales_df)}")
        if not sales_df.empty:
            if '公司名称' in sales_df.columns:
                print(f"  公司数量: {sales_df['公司名称'].nunique()}")
            if '车系' in sales_df.columns:
                print(f"  车系数量: {sales_df['车系'].nunique()}")
            if '单车毛利' in sales_df.columns:
                print(f"  平均毛利: {sales_df['单车毛利'].mean():.2f}")

        print(f"\n库存数据:")
        print(f"  总记录数: {len(inventory_df)}")
        if not inventory_df.empty:
            if '车系' in inventory_df.columns:
                print(f"  车系数量: {inventory_df['车系'].nunique()}")
            if '库存天数' in inventory_df.columns:
                print(f"  平均库存天数: {inventory_df['库存天数'].mean():.2f}")
            if '车辆状态' in inventory_df.columns:
                in_stock = inventory_df['车辆状态'].str.contains('在库', na=False).sum()
                print(f"  在库车辆数: {in_stock}")

        print("="*50 + "\n")