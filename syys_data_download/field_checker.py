"""
字段名检核类
用于检查表格是否包含所有必需的字段
"""
from typing import List, Tuple, Set


class FieldChecker:
    """字段名检核器"""

    def __init__(self):
        pass

    def check_fields(self, actual_fields: List[str], required_fields: List[str]) -> Tuple[bool, List[str], List[str]]:
        """
        检查实际字段是否包含所有必需字段
        
        参数:
            actual_fields: 实际表格的字段列表
            required_fields: 必需的字段列表
            
        返回:
            (是否通过, 缺失字段列表, 多余字段列表)
        """
        actual_set = set(actual_fields)
        required_set = set(required_fields)
        
        # 找出缺失的字段
        missing_fields = list(required_set - actual_set)
        
        # 找出多余的字段
        extra_fields = list(actual_set - required_set)
        
        # 如果没有缺失字段和多余字段，则通过检查
        is_valid = len(missing_fields) == 0 and len(extra_fields) == 0
        
        return is_valid, missing_fields, extra_fields

    def check_fields_subset(self, actual_fields: List[str], required_fields: List[str]) -> Tuple[bool, List[str]]:
        """
        检查实际字段是否包含所有必需字段（允许有多余字段）
        
        参数:
            actual_fields: 实际表格的字段列表
            required_fields: 必需的字段列表
            
        返回:
            (是否通过, 缺失字段列表)
        """
        actual_set = set(actual_fields)
        required_set = set(required_fields)
        
        # 找出缺失的字段
        missing_fields = list(required_set - actual_set)
        
        # 只要没有缺失字段就通过
        is_valid = len(missing_fields) == 0
        
        return is_valid, missing_fields
