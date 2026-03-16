"""
配置文件
存储所有配置信息，包括标准表头、检核规则等
"""


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
        "车架号",
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

    # 标准表头列表 - 投放市场费用模板_2025
    STANDARD_HEADERS_投放市场费用_2025 = [
        "年月",
        "归属门店",
        "项目大类",
        "项目分类",
        "费用金额",
        "具体项目",
        "核销发票税金",
        "核销发票金额",
        "费用合计",
        "备注"
    ]

    # 标准表头列表 - 投放市场费用模板_2026
    STANDARD_HEADERS_投放市场费用_2026 = [
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

    # 标准表头列表 - 特殊事项收入
    STANDARD_HEADERS_特殊事项收入 = [
        "业务时间",
        "归属门店",
        "车架号",
        "客户名称",
        "事项名称",
        "收付类型",
        "金额",
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
        "泸州上元坤灵", "西藏上元曦和", "宜宾上元曦和", "上元星汉", "文景初治", "洪武盛世"
    }

    @staticmethod
    def get_check_rules_三方贴膜(data_checker):
        """
        获取三方贴膜检核规则
        
        参数:
            data_checker: DataChecker实例
            
        返回:
            检核规则字典 {字段名: 检核方法}
        """
        return {
            "月份": data_checker.check_month,
            "推送日期": lambda v: data_checker.check_date(v, allow_empty=False),
            "新车销售店名": data_checker.check_store_name,
            "车架号（后6位）": data_checker.check_vin_6
        }

    @staticmethod
    def get_check_rules_自店贴膜(data_checker):
        """
        获取自店贴膜检核规则
        
        参数:
            data_checker: DataChecker实例
            
        返回:
            检核规则字典 {字段名: 检核方法}
        """
        return {
            "推送日期": lambda v: data_checker.check_date(v, allow_empty=True),
            "到店日期": lambda v: data_checker.check_date(v, allow_empty=False),
            "精品顾问": data_checker.check_consultant_name,
            "新车销售店名": data_checker.check_store_name,
            "车架号": data_checker.check_vin_full
        }

    @staticmethod
    def get_check_rules_投放市场费用(data_checker):
        """
        获取投放市场费用检核规则
        
        参数:
            data_checker: DataChecker实例
            
        返回:
            检核规则字典 {字段名: 检核方法}
        """
        return {
            "归属门店": data_checker.check_store_name,
            "年月": lambda v: data_checker.check_date(v, allow_empty=False),
        }

    @staticmethod
    def get_check_rules_新车三方延保(data_checker):
        """
        获取新车三方延保检核规则
        
        参数:
            data_checker: DataChecker实例
            
        返回:
            检核规则字典 {字段名: 检核方法}
        """
        return {
            "新车销售店名": data_checker.check_store_name,
            "延保销售日期": lambda v: data_checker.check_date(v, allow_empty=False),
            "车架号": data_checker.check_vin_full,
            "车系": data_checker.check_null,
            "客户姓名": data_checker.check_null,
            "金额": data_checker.check_null,
        }

    @staticmethod
    def get_check_rules_特殊事项收入(data_checker):
        """
        获取三方贴膜检核规则

        参数:
            data_checker: DataChecker实例

        返回:
            检核规则字典 {字段名: 检核方法}
        """
        return {
            "月份": data_checker.check_month,
            "推送日期": lambda v: data_checker.check_date(v, allow_empty=False),
            "新车销售店名": data_checker.check_store_name,
            "车架号（后6位）": data_checker.check_vin_6
        }
