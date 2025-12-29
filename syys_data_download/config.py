import pandas as pd
from typing import List, Dict, Any, Set
from datetime import datetime
import re


class Config:
    """配置类，存储所有配置信息"""

    # 跳过检核的文件列表
    SKIP_FILES = [
        "腾豹-双流、交大、羊犀、天府-贴膜升级登记表-最新年.xlsx",
        "两网-西门自店-贴膜升级登记表-最新年.xlsx",

    ]

    # 标准表头列表
    STANDARD_HEADERS = [
        "序号", "月份", "推送日期", "新车销售店名", "车型", "车架号（后6位）",
        "客户姓名", "是否送龙膜/高等级膜", "是否有满意度风险", "是否有效客户",
        "是否收劵", "膜升级金额", "其它施工项目", "其它项目金额", "合计升级金额",
        "三方返还佣金", "合作三方公司名称", "备注", "4S店交付", "推送月", "实际推送时间"
    ]

    # 新车销售店名列表
    VALID_STORE_NAMES = {
        "新港建武", "上元盛世", "新港澜阔", "新港澜舰", "新港澜洲", "文景海洋",
        "新茂元大", "贵州新港浩蓝", "贵州新港蔚蓝", "鑫港鲲鹏", "文景盛世",
        "新港浩蓝", "新港澜轩", "贵州新港澜源", "贵州新港海之辇", "新港建元",
        "永乐盛世", "新港先秦", "新港永初", "新港海川", "新港治元", "新港建隆",
        "直播基地", "上元臻享", "上元臻智", "上元臻盛", "贵州上元臻智",
        "乐山上元臻智", "绵阳新港鑫泽", "宜宾上元臻智", "上元弘川", "上元曦和",
        "上元坤灵", "贵州上元曦和", "贵州上元坤灵", "贵州新港澜轩", "乐山上元曦和",
        "泸州上元坤灵", "西藏上元曦和", "宜宾上元曦和", "上元星汉"
    }

    # 需要检核的字段及其规则类型
    CHECK_FIELDS = {
        "月份": "month_check",
        "推送日期": "date_check",
        "新车销售店名": "store_check",
        "车架号（后6位）": "vin_check"
    }


class RuleChecker:
    """规则检查器类"""

    @staticmethod
    def check_month(value: Any) -> tuple:
        """检查月份字段"""
        if pd.isna(value):
            return False, "月份为空"

        try:
            month = int(value)
            if 1 <= month <= 12:
                return True, ""
            else:
                return False, f"月份值{value}不在1-12范围内"
        except (ValueError, TypeError):
            # 尝试字符串转换
            str_value = str(value).strip()
            if str_value.isdigit() and 1 <= int(str_value) <= 12:
                return True, ""
            return False, f"月份格式错误: {value}"

    @staticmethod
    def check_date(value: Any) -> tuple:
        """检查日期字段"""
        if pd.isna(value):
            return False, "推送日期为空"

        try:
            # 尝试pandas日期转换
            pd.to_datetime(value, errors='raise')
            return True, ""
        except Exception as e:
            return False, f"日期格式错误: {value}"

    @staticmethod
    def check_store(value: Any) -> tuple:
        """检查销售店名字段"""
        if pd.isna(value):
            return False, "新车销售店名为空"

        store_name = str(value).strip()
        if store_name in Config.VALID_STORE_NAMES:
            return True, ""
        else:
            return False, f"销售店名'{store_name}'不在有效列表中"

    @staticmethod
    def check_vin(value: Any) -> tuple:
        """检查车架号后6位字段"""
        if pd.isna(value):
            return False, "车架号后6位为空"

        vin_str = str(value).strip()
        if len(vin_str) == 0:
            return False, "车架号后6位为空字符串"

        # 检查是否为6位（可能包含空格或其他字符）
        clean_vin = re.sub(r'\s+', '', vin_str)
        if len(clean_vin) != 6:
            return False, f"车架号后6位长度不为6: {vin_str}"

        # 检查是否包含非数字字母字符
        if not re.match(r'^[A-Za-z0-9]+$', clean_vin):
            return False, f"车架号后6位包含非法字符: {vin_str}"

        return True, ""

    @classmethod
    def check_field(cls, field_name: str, value: Any) -> tuple:
        """根据字段名调用对应的检查方法"""
        check_method = getattr(cls, f"check_{Config.CHECK_FIELDS[field_name]}", None)
        if check_method:
            return check_method(value)
        return True, ""