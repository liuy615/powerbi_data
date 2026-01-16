import pandas as pd
from typing import List, Dict, Any, Set
import re
from datetime import datetime


class Config:
    """配置类，存储所有配置信息"""

    # 自店贴膜文件列表
    自店贴膜_FILES = [
        "腾豹-双流、交大、羊犀、天府-贴膜升级登记表-最新年.xlsx",
        "两网-西门自店-贴膜升级登记表-最新年.xlsx",
        "两网-总部-贴膜升级登记表-最新年.xlsx",
        "方程豹-乐山上元曦和-贴膜升级登记表-最新年.xlsx",
        "方程豹-泸州上元坤灵-贴膜升级登记表-最新年.xlsx",
        "腾势-乐山上元臻智-贴膜升级登记表-最新年.xlsx",
    ]

    # 标准表头列表 - 三方贴膜模板
    STANDARD_HEADERS_三方贴膜 = [
        "序号",
        "月份",
        "推送日期",
        "新车销售店名",
        "车型",
        "车架号（后6位）",
        "客户姓名",
        "是否送龙膜/高等级膜",
        "是否有满意度风险",
        "是否有效客户",
        "是否收劵",
        "膜升级金额",
        "其它施工项目",
        "其它项目金额",
        "合计升级金额",
        "三方返还佣金",
        "合作三方公司名称",
        "备注"
    ]

    # 标准表头列表 - 自店贴膜模板
    STANDARD_HEADERS_自店贴膜 = [
        "序号",
        "月份",
        "推送日期",
        "到店日期",
        "精品顾问",
        "新车销售店名",
        "车型",
        "车架号（后6位）",
        "客户姓名",
        "是否算到店量",
        "是否送龙膜/高等级膜",
        "是否有满意度风险",
        "是否代办",
        "是否不推膜",
        "是否有效客户",
        "是否收劵",
        "膜升级具体内容",
        "膜升级金额",
        "膜升级成本",
        "膜升级毛利润",
        "其它施工项目",
        "其它项目金额",
        "其他项升级成本",
        "其他项升级毛利润",
        "合计升级金额",
        "合计升级毛利润",
        "合作三方公司名称",
        "备注"
    ]

    # 标准表头列表 - 投放市场费用模板
    STANDARD_HEADERS_投放市场费用 = [
        "年月",
        "归属门店",
        "项目大类",
        "项目分类",
        "费用金额",
        "具体明细",
        "项目明细",
        "支付日期",
        "备注"
    ]

    # 标准表头列表 - 新车三方延保模板
    STANDARD_HEADERS_新车三方延保 = [
        "新车销售店名",
        "延保销售日期",
        "购车日期",
        "车系",
        "车架号",
        "客户姓名",
        "电话号码1",
        "电话号码2",
        "延保销售人员",
        "延保期限",
        "金额",
        "是否录入厂家系统",
        "录入厂家系统日期",
        "比亚迪系统录入金额",
        "超期录入比亚迪系统违约金",
        "备注"
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

    # 需要检核的字段及其规则类型 - 三方贴膜模板
    CHECK_FIELDS_三方贴膜 = {
        "月份": "month_check",
        "推送日期": "date_check",
        "新车销售店名": "store_check",
        "车架号（后6位）": "vin_check"
    }

    # 需要检核的字段及其规则类型 - 自店贴膜模板
    CHECK_FIELDS_自店贴膜 = {
        "推送日期": "date_skip_check",
        "到店日期": "arrival_date_check",
        "精品顾问": "consultant_check",
        "新车销售店名": "store_check",
        "车架号（后6位）": "vin_check"
    }

    # 需要检核的字段及其规则类型 - 投放市场费用模板
    CHECK_FIELDS_投放市场费用 = {
        "归属门店": "store_check"
    }

    # 需要检核的字段及其规则类型 - 新车三方延保模板
    CHECK_FIELDS_新车三方延保 = {
        "新车销售店名": "store_check",
        "延保销售日期": "date_check",
        "车架号": "vin_check_extended"  # 车架号长度不限于6位
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
        """检查日期字段 - 通用日期检查"""
        if pd.isna(value):
            return False, "日期为空"

        try:
            # 尝试pandas日期转换
            pd.to_datetime(value, errors='raise')
            return True, ""
        except Exception as e:
            return False, f"日期格式错误: {value}"

    @staticmethod
    def check_date_skip(value: Any) -> tuple:
        """检查推送日期字段 - 自店贴膜模板（允许为空）"""
        # 允许为空
        if pd.isna(value):
            return True, ""

        try:
            # 尝试pandas日期转换
            pd.to_datetime(value, errors='raise')
            return True, ""
        except Exception as e:
            return False, f"推送日期格式错误: {value}"

    @staticmethod
    def check_arrival_date(value: Any) -> tuple:
        """检查到店日期字段"""
        if pd.isna(value):
            return False, "到店日期为空"

        try:
            # 尝试pandas日期转换
            pd.to_datetime(value, errors='raise')
            return True, ""
        except Exception as e:
            return False, f"到店日期格式错误: {value}"

    @staticmethod
    def check_consultant(value: Any) -> tuple:
        """检查精品顾问字段"""
        if pd.isna(value):
            return False, "精品顾问为空"

        consultant = str(value).strip()
        if len(consultant) == 0:
            return False, "精品顾问为空字符串"

        return True, ""

    @staticmethod
    def check_store(value: Any) -> tuple:
        """检查销售店名字段"""
        if pd.isna(value):
            return False, "门店名称为空"

        store_name = str(value).strip()
        if store_name in Config.VALID_STORE_NAMES:
            return True, ""
        else:
            return False, f"门店名称'{store_name}'不在有效列表中"

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

    @staticmethod
    def check_vin_extended(value: Any) -> tuple:
        """检查完整车架号字段（不限长度）"""
        if pd.isna(value):
            return False, "车架号为空"

        vin_str = str(value).strip()
        if len(vin_str) == 0:
            return False, "车架号为空字符串"

        # 移除空格
        clean_vin = re.sub(r'\s+', '', vin_str)

        # 检查是否包含非数字字母字符
        if not re.match(r'^[A-Za-z0-9]+$', clean_vin):
            return False, f"车架号包含非法字符: {vin_str}"

        # 车架号通常为17位，但这里不做严格限制
        if len(clean_vin) < 6:
            return False, f"车架号长度太短: {vin_str}"

        return True, ""

    @classmethod
    def check_field(cls, field_name: str, value: Any, data_source_type: str = "三方贴膜") -> tuple:
        """根据字段名调用对应的检查方法"""
        # 根据数据源类型选择检查字段映射
        if data_source_type == "三方贴膜":
            check_fields = Config.CHECK_FIELDS_三方贴膜
        elif data_source_type == "自店贴膜":
            check_fields = Config.CHECK_FIELDS_自店贴膜
        elif data_source_type == "投放市场费用":
            check_fields = Config.CHECK_FIELDS_投放市场费用
        elif data_source_type == "新车三方延保":
            check_fields = Config.CHECK_FIELDS_新车三方延保
        else:
            return True, ""  # 未知数据源类型，跳过检查

        if field_name in check_fields:
            check_method_name = f"check_{check_fields[field_name]}"
            check_method = getattr(cls, check_method_name, None)
            if check_method:
                return check_method(value)
        return True, ""

    @classmethod
    def check_date_logic(cls, push_date: Any, arrival_date: Any) -> tuple:
        """检查日期逻辑：推送日期必须小于到店日期（如果两者都有值）"""
        if pd.isna(push_date) or pd.isna(arrival_date):
            return True, ""  # 如果任一日期为空，则不检查逻辑

        try:
            # 尝试将两个日期转换为datetime对象
            push_dt = pd.to_datetime(push_date, errors='coerce')
            arrival_dt = pd.to_datetime(arrival_date, errors='coerce')

            if pd.isna(push_dt) or pd.isna(arrival_dt):
                return True, ""  # 如果转换失败，跳过逻辑检查

            if push_dt >= arrival_dt:
                return False, f"推送日期({push_date})必须小于到店日期({arrival_date})"

            return True, ""
        except Exception as e:
            # 如果发生异常，跳过逻辑检查
            return True, ""