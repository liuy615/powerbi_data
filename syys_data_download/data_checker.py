"""
数据检核类
用于检核每个字段的数据是否符合规范
"""
import pandas as pd
import re
from typing import Any, Tuple, Dict
from datetime import datetime


class DataChecker:
    """数据检核器 - 包含各种数据检核方法"""

    def __init__(self, valid_store_names: set = None):
        """
        初始化数据检核器
        
        参数:
            valid_store_names: 有效的门店名称集合
        """
        self.valid_store_names = valid_store_names or set()

    # ==================== 基础检核方法 ====================

    def check_not_empty(self, value: Any, field_name: str = "字段") -> Tuple[bool, str]:
        """检查字段是否为空"""
        if pd.isna(value):
            return False, f"{field_name}为空"
        
        str_value = str(value).strip()
        if len(str_value) == 0:
            return False, f"{field_name}为空字符串"
        
        return True, ""

    def check_month(self, value: Any) -> Tuple[bool, str]:
        """检查月份字段（1-12）"""
        if pd.isna(value):
            return False, "月份为空"

        try:
            month = int(value)
            if 1 <= month <= 12:
                return True, ""
            else:
                return False, f"月份值{value}不在1-12范围内"
        except (ValueError, TypeError):
            str_value = str(value).strip()
            if str_value.isdigit() and 1 <= int(str_value) <= 12:
                return True, ""
            return False, f"月份格式错误: {value}"

    def check_date(self, value: Any, allow_empty: bool = False) -> Tuple[bool, str]:
        """
        检查日期字段

        参数:
            value: 待检查的值
            allow_empty: 是否允许为空
        """
        if pd.isna(value):
            if allow_empty:
                return True, ""
            return False, "日期为空"

        try:
            pd.to_datetime(value, errors='raise')
            return True, ""

        except Exception:
            # 尝试处理中文年月格式，如“2025年11月”
            if isinstance(value, str):
                import re
                pattern = r'^(\d{4})年(\d{1,2})月$'
                match = re.match(pattern, value.strip())
                if match:
                    year = int(match.group(1))
                    month = int(match.group(2))
                    if 1 <= month <= 12:
                        return True, ""
            return False, f"日期格式错误: {value}"

    def check_store_name(self, value: Any) -> Tuple[bool, str]:
        """检查门店名称是否在有效列表中"""
        if pd.isna(value):
            return False, "门店名称为空"

        store_name = str(value).strip()
        if store_name in self.valid_store_names:
            return True, ""
        else:
            return False, f"门店名称'{store_name}'不在有效列表中"

    def check_vin_6(self, value: Any) -> Tuple[bool, str]:
        """检查车架号后6位"""
        if pd.isna(value):
            return False, "车架号后6位为空"

        vin_str = str(value).strip()
        if len(vin_str) == 0:
            return False, "车架号后6位为空字符串"

        # 移除空格
        clean_vin = re.sub(r'\s+', '', vin_str)
        if len(clean_vin) != 6:
            return False, f"车架号后6位长度不为6: {vin_str}"

        # 检查是否只包含数字和字母
        if not re.match(r'^[A-Za-z0-9]+$', clean_vin):
            return False, f"车架号后6位包含非法字符: {vin_str}"

        return True, ""

    def check_vin_full(self, value: Any) -> Tuple[bool, str]:
        """检查完整车架号"""
        if pd.isna(value):
            return False, "车架号为空"

        vin_str = str(value).strip()
        if len(vin_str) == 0:
            return False, "车架号为空字符串"

        # 移除空格
        clean_vin = re.sub(r'\s+', '', vin_str)

        # 检查是否只包含数字和字母
        if not re.match(r'^[A-Za-z0-9]+$', clean_vin):
            return False, f"车架号包含非法字符: {vin_str}"

        # 车架号长度检查（至少6位）
        if len(clean_vin) < 6:
            return False, f"车架号长度太短: {vin_str}"

        return True, ""

    def check_phone(self, value: Any, allow_empty: bool = True) -> Tuple[bool, str]:
        """检查电话号码格式"""
        if pd.isna(value):
            if allow_empty:
                return True, ""
            return False, "电话号码为空"

        phone_str = str(value).strip()
        if len(phone_str) == 0:
            if allow_empty:
                return True, ""
            return False, "电话号码为空"

        # 移除常见分隔符
        clean_phone = re.sub(r'[-\s()]', '', phone_str)

        # 检查是否为纯数字
        if not clean_phone.isdigit():
            return False, f"电话号码格式错误: {phone_str}"

        # 检查长度（手机11位，固话7-8位）
        if len(clean_phone) not in [7, 8, 11]:
            return False, f"电话号码长度不正确: {phone_str}"

        return True, ""

    def check_amount(self, value: Any, allow_empty: bool = True, allow_negative: bool = False) -> Tuple[bool, str]:
        """检查金额字段"""
        if pd.isna(value):
            if allow_empty:
                return True, ""
            return False, "金额为空"

        try:
            amount = float(value)
            if not allow_negative and amount < 0:
                return False, f"金额不能为负数: {value}"
            return True, ""
        except (ValueError, TypeError):
            return False, f"金额格式错误: {value}"

    # ==================== 逻辑检核方法 ====================

    def check_date_logic(self, date1: Any, date2: Any, date1_name: str = "日期1", date2_name: str = "日期2") -> Tuple[bool, str]:
        """检查日期逻辑：date1必须小于date2"""
        if pd.isna(date1) or pd.isna(date2):
            return True, ""  # 如果任一日期为空，跳过逻辑检查

        try:
            dt1 = pd.to_datetime(date1, errors='coerce')
            dt2 = pd.to_datetime(date2, errors='coerce')

            if pd.isna(dt1) or pd.isna(dt2):
                return True, ""  # 如果转换失败，跳过逻辑检查

            if dt1 >= dt2:
                return False, f"{date1_name}({date1})必须小于{date2_name}({date2})"

            return True, ""
        except Exception:
            return True, ""  # 发生异常时跳过逻辑检查

    # ==================== 可扩展的检核方法 ====================
    # 在此处添加新的检核方法，然后在主程序中调用

    def check_consultant_name(self, value: Any) -> Tuple[bool, str]:
        """检查顾问姓名（示例：可根据实际需求扩展）"""
        return self.check_not_empty(value, "精品顾问")

    def check_yes_no(self, value: Any, field_name: str = "字段") -> Tuple[bool, str]:
        """检查是/否字段"""
        if pd.isna(value):
            return False, f"{field_name}为空"

        str_value = str(value).strip()
        valid_values = {"是", "否", "yes", "no", "Y", "N", "y", "n"}

        if str_value in valid_values:
            return True, ""
        else:
            return False, f"{field_name}值'{str_value}'不是有效的是/否值"

    def check_null(self, value: Any, field_name: str = "字段") -> Tuple[bool, str]:
        """检查字段是否为空"""
        if pd.isna(value):
            return False, f"{field_name}为空"
        
        str_value = str(value).strip()
        if len(str_value) == 0:
            return False, f"{field_name}为空字符串"
        
        return True, ""