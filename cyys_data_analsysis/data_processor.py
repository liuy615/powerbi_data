# -*- coding: utf-8 -*-
"""
数据处理模块
包含所有数据清洗、转换和合并逻辑
"""

import re
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Tuple, Optional, Dict, Any

class DataProcessor:
    """数据处理类：包含所有数据清洗和转换方法"""
    
    @staticmethod
    def standardize_date(date_str: str) -> str:
        """
        标准化日期字符串
        
        处理包含中文的日期格式，如"2025年1月"补全为"2025年1月1日"
        
        Args:
            date_str: 原始日期字符串
            
        Returns:
            str: 标准化后的日期字符串
        """
        if isinstance(date_str, str):
            date_str = date_str.strip()
            # 如果包含年、月但不包含日，补全日
            if "年" in date_str and "月" in date_str and "日" not in date_str:
                date_str += "1日"
        return date_str
    
    @staticmethod
    def convert_date(date_str: str) -> pd.Timestamp:
        """
        转换日期字符串为pandas时间戳
        
        支持格式：
        1. YYYY-MM-DD HH:MM:SS
        2. YYYY年MM月DD日
        
        Args:
            date_str: 日期字符串
            
        Returns:
            pd.Timestamp: 转换后的时间戳，转换失败返回NaT
        """
        if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_str):
            return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
        elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
            return pd.to_datetime(date_str, format='%Y年%m月%d日')
        return pd.NaT
    
    @staticmethod
    def classify_inventory_duration(row: pd.Series) -> str:
        """
        根据库存时间分类车辆
        
        Args:
            row: 包含库存时间和车辆状态的数据行
            
        Returns:
            str: 库存时长分类
        """
        inventory_time = row['库存时间']
        vehicle_status = row['车辆状态']
        
        # 根据库存天数和车辆状态进行分类
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
    
    @staticmethod
    def add_month_end_date(df: pd.DataFrame, year_col: str = '年份', month_col: str = '月份') -> pd.DataFrame:
        """
        添加月末日期列
        
        根据年份和月份列计算当月的最后一天
        
        Args:
            df: 输入DataFrame
            year_col: 年份列名
            month_col: 月份列名
            
        Returns:
            pd.DataFrame: 添加了月末日期的DataFrame
        """
        # 创建副本避免修改原数据
        df = df.copy()
        
        # 提取年份和月份数字
        df['year'] = df[year_col].astype(str).str.extract(r'(\d+)')
        df['month'] = df[month_col].astype(str).str.extract(r'(\d+)')
        
        # 转换为数值类型
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        
        # 创建月初日期，然后计算月末
        df['day'] = 1
        df['当月第一天'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
        df['日期'] = df['当月第一天'] + pd.offsets.MonthEnd(1)
        
        # 清理临时列
        df = df.drop(columns=['year', 'month', 'day', '当月第一天'])
        
        return df
    
    @staticmethod
    def process_loan_term(loan_term_series: pd.Series) -> pd.Series:
        """
        处理贷款期限列
        
        将'None'字符串和包含逗号的字符串转换为标准格式
        
        Args:
            loan_term_series: 贷款期限列
            
        Returns:
            pd.Series: 处理后的贷款期限列
        """
        # 创建副本
        result = loan_term_series.copy()
        
        def process_single_value(x):
            if pd.isna(x):
                return np.nan
                
            x_str = str(x).strip()
            
            # 处理'None'字符串
            if x_str.lower() == 'none':
                return np.nan
            
            # 处理包含逗号的情况（取第一个值）
            if ',' in x_str:
                return x_str.split(',')[0]
            
            return x_str
        
        # 应用处理函数
        result = result.apply(process_single_value)
        
        # 转换为数值类型
        result = pd.to_numeric(result, errors='coerce')
        
        # 填充NaN为0，转换为整数，添加'期'后缀
        result = result.fillna(0).astype(int)
        result = result.astype(str) + '期'
        
        return result
    
    @staticmethod
    def standardize_text(text: str) -> str:
        """
        标准化文本
        
        统一大小写、空格和特定词汇格式
        
        Args:
            text: 原始文本
            
        Returns:
            str: 标准化后的文本
        """
        if pd.isna(text):
            return ''
        
        # 转换为小写并去除空格
        text = str(text).strip().lower()
        
        # 统一空格
        text = re.sub(r'\s+', ' ', text)
        
        # 统一特定词汇格式
        text = text.replace('km', 'KM').replace('km', 'KM').replace('km', 'KM')
        text = text.replace('plus', 'Plus').replace('e2', 'E2')
        
        return text
    
    @staticmethod
    def apply_replacements(text: str, replacement_rules: Dict[str, str]) -> str:
        """
        应用文本替换规则
        
        Args:
            text: 原始文本
            replacement_rules: 替换规则字典
            
        Returns:
            str: 替换后的文本
        """
        if isinstance(text, str):
            for pattern, replacement in replacement_rules.items():
                text = text.replace(pattern, replacement)
        return text


class DataValidator:
    """数据验证类：检查数据质量和完整性"""
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, df_name: str) -> bool:
        """
        验证DataFrame的基本完整性
        
        Args:
            df: 要验证的DataFrame
            df_name: DataFrame名称（用于日志）
            
        Returns:
            bool: 是否通过验证
        """
        try:
            # 检查DataFrame是否为空
            if df.empty:
                logging.warning(f"DataFrame '{df_name}' 为空")
                return False
            
            # 检查是否有重复行
            duplicate_count = df.duplicated().sum()
            if duplicate_count > 0:
                logging.info(f"DataFrame '{df_name}' 有 {duplicate_count} 个重复行")
            
            # 检查缺失值
            missing_counts = df.isnull().sum()
            total_missing = missing_counts.sum()
            
            if total_missing > 0:
                logging.info(f"DataFrame '{df_name}' 有 {total_missing} 个缺失值")
                
                # 列出缺失值最多的列
                top_missing = missing_counts.sort_values(ascending=False).head(5)
                for col, count in top_missing.items():
                    if count > 0:
                        percentage = (count / len(df)) * 100
                        logging.info(f"  列 '{col}': {count} 个缺失值 ({percentage:.2f}%)")
            
            return True
            
        except Exception as e:
            logging.error(f"验证DataFrame '{df_name}' 时出错: {str(e)}")
            return False
    
    @staticmethod
    def check_required_columns(df: pd.DataFrame, required_columns: list, df_name: str) -> bool:
        """
        检查DataFrame是否包含必需的列
        
        Args:
            df: 要检查的DataFrame
            required_columns: 必需列列表
            df_name: DataFrame名称（用于日志）
            
        Returns:
            bool: 是否包含所有必需列
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logging.warning(f"DataFrame '{df_name}' 缺少以下列: {missing_columns}")
            return False
        
        return True