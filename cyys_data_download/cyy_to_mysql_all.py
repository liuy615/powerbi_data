from common_code import RequestFunction, collect_nested_keys, WriteLog
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from webdav3.client import Client
from urllib.parse import unquote
from time import sleep
import tempfile
import random
import copy
import pymysql
import requests
from typing import Dict, List, Any, Optional, Tuple, Set, Union
import json


# 配置常量
class Config:
    # 企业微信机器人配置
    WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=229448fb-4f7e-4dbd-80c7-128309442439"

    # 数据库配置
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': '513921',
        'database': 'cyy_data',
        'charset': 'utf8mb4',
        'connect_timeout': 10,
        'autocommit': True
    }

    # WebDAV配置
    WEBDAV_CONFIG = {
        "webdav_hostname": "http://222.212.88.126:5005",
        "webdav_login": "zhufulin",
        "webdav_password": "150528Zfl@",
        "verify_ssl": False,
        "session_options": {
            "pool_connections": 5,
            "pool_maxsize": 5,
            "max_retries": 3
        },
        "disable_check": True
    }

    # API请求配置
    API_BASE_URL = "https://openapi.cyys361.com:5085"
    MAX_WORKERS = 3
    REQUEST_DELAY = (5, 10)  # 请求延迟范围(秒)
    PAGE_SIZE = 3000  # 每页数据量


class DataCenter:
    """数据中心配置类"""
    USER = '15884541263'
    PWD = 'uMhiC6vH9hv4J2zFPMs+LA=='

    @staticmethod
    def get_apis() -> Dict[str, Dict[str, Any]]:
        """
        获取全量API配置（不设置时间限制，获取所有数据）

        Returns:
            API配置字典
        """
        # API配置模板（移除所有时间相关参数）
        api_templates = {
            # '装饰_订单管理_装饰订单': {'url': f'{Config.API_BASE_URL}/api/Summary/AccessoryOrder','method': 'GET','payload': {"LikeType": "1","TimeType": "2","PageSize":3000,"PageNumber": 1}},
            # '销售_车辆销售_作废订单': {'url': f'{Config.API_BASE_URL}/api/Summary/AbnormalOrder','method': 'GET','payload': {"SalesContant": "1","state": "0,2","AuthDistrub": "0","TimeType": "2","yType": 2,"PageSize": 500,"PageNumber": 1}},
            # '销售_衍生_订单查询': {'url': f'{Config.API_BASE_URL}/api/Summary/SalePreorder','method': 'GET','payload': {"TimeType": "1","LikeType": "3","PageSize": 3000,"PageNumber": 1}},

            # '车辆销售明细表_开票日期': {'url': f'{Config.API_BASE_URL}/api/Summary/CarSalDetailSummary','method': 'GET','payload': {"TimeType": 3,"Type": "2","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '车辆成本管理': {'url': f'{Config.API_BASE_URL}/api/Summary/CarCost','method': 'GET','payload': {"OrgType": "1","CostType": "1","TimeType": "3","check": "2","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '套餐销售列表': {'url': f'{Config.API_BASE_URL}/api/AfterSales/SaleServicePackagePage','method': 'GET','payload': {"TimeType": "2","isShowPhone": "1","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '销售_车辆销售_成交订单': {'url': f'{Config.API_BASE_URL}/api/Summary/DealOrder','method': 'GET','payload': {"DateType": "4","SalesContant": "1","AuthDistrub": "0","LikeType": "3","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '保险业务': {'url': f'{Config.API_BASE_URL}/api/Home/InternalExport','method': 'GET','payload': {"Salescontant": "1","TimeType": "1","LikeType": '3',"PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '按揭业务': {'url': f'{Config.API_BASE_URL}/api/Summary/MortgageData','method': 'GET','payload': {"Salescontant": "1","IsMortgage": "","IsPledge": "","state": "12","DateType": "1","LikeType": '',"PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '评估管理_成交': {'url': f'{Config.API_BASE_URL}/api/Summary/GetUsedCarList','method': 'GET','payload': {"BillState": "3","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '评估管理_已入库': {'url': f'{Config.API_BASE_URL}/api/Summary/GetUsedCarList','method': 'GET','payload': {"BillState": "5","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '调车结算查询': {'url': f'{Config.API_BASE_URL}/api/Summary/GetShuntSettlementList','method': 'GET','payload': {"OrgType": "-1","OrganizeId": "","PayState": 2,"BillingState": "","CarState": "","Title": "","TimeType": "","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            '库存车辆已售': {'url': f'{Config.API_BASE_URL}/api/Summary/StoreCar','method': 'GET','payload': {"OrgType": "0","CarState": "2","TimeType": "3","ShowPriceFlag": "1","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '汇票管理': {'url': f'{Config.API_BASE_URL}/api/Summary/CarTicket','method': 'GET','payload': {'sortName': 'PledgeDate','sortOrder': 'desc','DateType': '1',"PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '销售回访': {'url': f'{Config.API_BASE_URL}/api/Summary/GetFollowUpList','method': 'GET','payload': {'TimeType': '3','Unvisit': '',"PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
            # '开票维护': {'url': f'{Config.API_BASE_URL}/api/Summary/GetInvoicesList','method': 'GET','payload': {"QueryType": "2","TimeType": "2","PageSize": Config.PAGE_SIZE,"PageNumber": 1}},
        }

        return api_templates


def should_fetch_next_page(current_items: list, page_size: int) -> bool:
    """
    判断是否满足翻页条件

    Args:
        current_items: 当前页获取的数据列表
        page_size: 每页预期数据量大小

    Returns:
        满足翻页条件返回True，否则返回False
    """
    if not current_items:
        return False
    return len(current_items) >= page_size


def flatten_data(data_list: List[Dict], parent_key: str = '', sep: str = '_') -> Tuple[List[Dict], List[str]]:
    """
    将嵌套的字典列表展平，用于处理API返回的嵌套结构数据

    Args:
        data_list: 嵌套字典的列表
        parent_key: 父键名，用于递归构建键名
        sep: 键名分隔符

    Returns:
        展平后的数据列表和所有字段名集合
    """
    flattened = []
    all_fields: Set[str] = set()

    for item in data_list:
        if not isinstance(item, dict):
            continue  # 跳过非字典类型的项

        flat_item = {}

        def _flatten(d: Dict, current_key: str = '') -> None:
            nonlocal flat_item, all_fields
            full_key = f"{parent_key}{sep}{current_key}" if parent_key and current_key else current_key or parent_key

            for k, v in d.items():
                new_key = f"{full_key}{sep}{k}" if full_key else k
                if isinstance(v, dict) and v:
                    _flatten(v, new_key)
                else:
                    flat_item[new_key] = v
                    all_fields.add(new_key)

        _flatten(item)
        flattened.append(flat_item)

    return flattened, list(all_fields)


def send_wechat_notification(content: str) -> bool:
    """
    发送企业微信机器人通知

    Args:
        content: 通知内容

    Returns:
        发送成功返回True，否则返回False
    """
    if not Config.WECHAT_WEBHOOK:
        print("未配置企业微信webhook，无法发送通知")
        return False

    try:
        data = {
            "msgtype": "text",
            "text": {
                "content": f"数据同步提醒：{content}\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
        }
        response = requests.post(Config.WECHAT_WEBHOOK, json=data, timeout=10)
        response.raise_for_status()
        result = response.json()

        if result.get("errcode") == 0:
            print("企微通知发送成功")
            return True
        else:
            print(f"企微通知发送失败: {result.get('errmsg')}")
            return False
    except Exception as e:
        print(f"发送企微通知出错: {str(e)}")
        return False


class DBOperator:
    """数据库操作类"""

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.connection: Optional[pymysql.Connection] = None
        self.last_error: Optional[str] = None  # 记录最后一次错误信息
        self.logger = WriteLog()
        self.connect()

    def connect(self) -> None:
        """连接数据库"""
        try:
            self.connection = pymysql.connect(**self.db_config)
            self.logger.log_info("数据库连接成功")
            self.last_error = None
        except Exception as e:
            error_msg = f"数据库连接失败: {str(e)}"
            self.logger.log_error(error_msg)
            self.last_error = error_msg
            raise

    def reconnect(self) -> None:
        """重新连接数据库"""
        if self.connection:
            self.connection.close()
        self.connect()

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Optional[List[Tuple]]:
        """执行查询语句"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                self.last_error = None
                return result
        except Exception as e:
            error_msg = f"查询执行失败: {str(e)}"
            self.logger.log_error(error_msg)
            self.last_error = error_msg
            self.reconnect()
            return None

    def execute_many(self, query: str, data_list: List[Tuple]) -> int:
        """执行批量插入/更新"""
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, data_list)
                self.connection.commit()
                self.last_error = None
                return cursor.rowcount
        except Exception as e:
            error_msg = f"批量操作失败: {str(e)}"
            self.logger.log_error(error_msg)
            self.last_error = error_msg
            self.connection.rollback()
            self.reconnect()
            return 0

    def get_last_error(self) -> Optional[str]:
        """获取最后一次错误信息"""
        return self.last_error

    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.logger.log_info("数据库连接已关闭")


class DataSaver:
    """数据保存类，处理不同API数据到数据库的保存（全部全量更新）"""

    # API名称到数据库表名的映射
    API_TABLE_MAPPING = {
        '按揭业务': 'mortgage_business',
        '保险业务': 'insurance_business',
        '车辆成本管理': 'car_cost_management',
        '车辆销售明细表_开票日期': 'car_sales_invoice_date',
        '调车结算查询': 'vehicle_transfer_settlement',
        '汇票管理': 'bill_management',
        '计划车辆': 'planned_vehicles',
        '开票维护': 'invoice_maintenance',
        '库存车辆查询': 'inventory_vehicle_query',
        '库存车辆已售': 'inventory_vehicle_sold',
        '评估管理_成交': 'evaluation_deal',
        '评估管理_已入库': 'evaluation_stored',
        '套餐销售列表': 'package_sales',
        '未售订单': 'unsold_orders',
        '销售_车辆销售_成交订单': 'sales_deal_orders',
        '销售_车辆销售_作废订单': 'sales_canceled_orders',
        '销售_衍生_订单查询': 'sales_derivative_orders',
        '装饰_订单管理_装饰订单': 'decoration_orders',
        '销售回访': 'car_sales_data',
    }

    # 各表的主键或唯一键字段（用于数据验证）
    TABLE_PRIMARY_KEYS = {
        'mortgage_business': 'ID',
        'insurance_business': 'ID',
        'car_cost_management': 'OrderCode',
        'car_sales_order_date': 'ID',
        'car_sales_invoice_date': 'ID',
        'vehicle_transfer_settlement': 'FrameNumber',
        'bill_management': 'ID',
        'planned_vehicles': 'OrderCode',
        'invoice_maintenance': 'ID',
        'inventory_vehicle_query': 'ID',
        'inventory_vehicle_sold': 'ID',
        'evaluation_deal': 'ID',
        'evaluation_stored': 'ID',
        'package_sales': 'SaleId',
        'unsold_orders': 'ID',
        'sales_deal_orders': 'CusPhone',
        'sales_canceled_orders': 'CusPhone',
        'sales_derivative_orders': 'ID',
        'decoration_orders': 'ID',
        'car_sales_data': 'ID',
    }

    def __init__(self, db_operator: DBOperator):
        self.db_operator = db_operator
        self.table_schema: Dict[str, Dict[str, str]] = {}
        self.last_error: Optional[str] = None  # 记录最后一次错误信息
        self.logger = WriteLog()

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """
        获取表结构信息

        Args:
            table_name: 表名

        Returns:
            表结构字典，字段名到类型的映射
        """
        if table_name in self.table_schema:
            return self.table_schema[table_name]

        query = f"DESCRIBE {table_name}"
        result = self.db_operator.execute_query(query)

        if not result:
            self.last_error = f"无法获取表 {table_name} 的结构信息"
            return None

        schema = {}
        for field_info in result:
            field_name = field_info[0]
            field_type = field_info[1].lower()
            schema[field_name] = field_type

        self.table_schema[table_name] = schema
        self.last_error = None
        return schema

    @staticmethod
    def convert_value_based_on_type(value: Any, field_type: str) -> Any:
        """
        根据字段类型转换值

        Args:
            value: 原始值
            field_type: 字段类型

        Returns:
            转换后的值
        """
        if value is None or value == '' or value == 'null':
            return None

        if 'int' in field_type:
            try:
                return int(value)
            except (ValueError, TypeError):
                return None

        if 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        if 'date' in field_type or 'time' in field_type:
            # 日期时间类型保持原样
            return value

        # 其他类型转为字符串
        return str(value) if value is not None else None

    def build_insert_query(self, table_name: str, data_list: List[Dict], fieldnames: List[str],
                           table_schema: Dict[str, str]) -> Tuple[Optional[str], Optional[List[Tuple]]]:
        """
        构建INSERT查询语句

        Args:
            table_name: 表名
            data_list: 数据列表
            fieldnames: 字段名列表
            table_schema: 表结构信息

        Returns:
            SQL查询语句和参数列表
        """
        if not data_list:
            return None, None

        # 过滤掉不存在于表结构中的字段
        valid_fields = [field for field in fieldnames if field in table_schema]
        if not valid_fields:
            self.last_error = f"没有找到与表 {table_name} 匹配的有效字段"
            self.logger.log_error(self.last_error)
            return None, None

        quoted_fields = [f"`{field}`" for field in valid_fields]
        placeholders = ', '.join(['%s'] * len(valid_fields))
        query = f"INSERT INTO {table_name} ({', '.join(quoted_fields)}) VALUES ({placeholders})"

        params_list = []
        for item in data_list:
            params = []
            for field in valid_fields:
                value = item.get(field)
                field_type = table_schema.get(field, 'varchar(255)')
                converted_value = self.convert_value_based_on_type(value, field_type)

                if isinstance(converted_value, (dict, list)):
                    converted_value = json.dumps(converted_value, ensure_ascii=False)

                params.append(converted_value)
            params_list.append(tuple(params))

        return query, params_list

    def save_full_update_data(self, api_name: str, data_list: List[Dict]) -> int:
        """
        全量更新数据到数据库（先删除所有数据，再插入新数据）

        Args:
            api_name: API名称
            data_list: 数据列表

        Returns:
            影响的行数
        """
        self.last_error = None

        if not data_list:
            msg = f"{api_name} 无数据可保存"
            self.logger.log_info(msg)
            self.last_error = msg
            return 0

        table_name = self.API_TABLE_MAPPING.get(api_name)
        if not table_name:
            msg = f"未找到API {api_name} 对应的表映射"
            self.logger.log_error(msg)
            self.last_error = msg
            return 0

        table_schema = self.get_table_schema(table_name)
        if not table_schema:
            self.last_error = f"无法获取表 {table_name} 的结构，无法保存数据"
            return 0

        try:
            flattened_data, fieldnames = flatten_data(data_list)

            # 构建插入查询
            query, params_list = self.build_insert_query(table_name, flattened_data, fieldnames, table_schema)

            if not query or not params_list:
                self.last_error = f"无法构建SQL查询，无法保存 {api_name} 数据"
                return 0

            # 先删除表中所有数据（全量更新核心逻辑）
            delete_query = f"DELETE FROM {table_name}"
            self.db_operator.execute_query(delete_query)
            self.logger.log_info(f"{api_name} - 表 {table_name} 已清空所有数据")

            # 执行批量插入
            affected_rows = self.db_operator.execute_many(query, params_list)

            # 检查是否有数据库错误
            if self.db_operator.get_last_error():
                self.last_error = self.db_operator.get_last_error()
                return 0

            self.logger.log_info(f"{api_name} 全量更新完成，插入 {affected_rows} 行数据")
            return affected_rows
        except Exception as e:
            error_msg = f"{api_name} 全量更新失败: {str(e)}"
            self.logger.log_error(error_msg)
            self.last_error = error_msg
            return 0

    def save_data(self, api_name: str, data_list: List[Dict]) -> int:
        """
        保存数据到数据库（所有表均采用全量更新）

        Args:
            api_name: API名称
            data_list: 数据列表

        Returns:
            影响的行数
        """
        self.last_error = None

        # 特殊处理调车结算查询
        if api_name == '调车结算查询':
            return self.save_vehicle_transfer_settlement(data_list)

        # 特殊处理开票维护
        elif api_name == '开票维护':
            return self.save_invoice_maintenance(data_list)

        # 其他表仍然使用全量更新
        else:
            return self.save_full_update_data(api_name, data_list)

    def get_last_error(self) -> Optional[str]:
        """获取最后一次错误信息"""
        return self.last_error

    def save_vehicle_transfer_settlement(self, data_list: List[Dict]) -> int:
        """
        保存调车结算查询数据 - 保留 SettlementTime 最新日期

        Args:
            data_list: 调车结算查询数据列表

        Returns:
            影响的行数
        """
        self.last_error = None

        if not data_list:
            msg = "调车结算查询无数据可保存"
            self.logger.log_info(msg)
            self.last_error = msg
            return 0

        table_name = 'vehicle_transfer_settlement'
        table_schema = self.get_table_schema(table_name)
        if not table_schema:
            self.last_error = f"无法获取表 {table_name} 的结构，无法保存数据"
            return 0

        try:
            flattened_data, fieldnames = flatten_data(data_list)

            # 按 FrameNumber 分组，保留 SettlementTime 最新的记录
            latest_records = {}
            for record in flattened_data:
                frame_number = record.get('FrameNumber')
                settlement_time = record.get('SettlementTime')

                if not frame_number:
                    continue

                # 如果该车架号还没有记录，或者当前记录的 SettlementTime 更新，则更新
                if frame_number not in latest_records:
                    latest_records[frame_number] = record
                else:
                    current_time = latest_records[frame_number].get('SettlementTime')
                    if settlement_time and current_time:
                        # 比较时间，保留最新的
                        try:
                            current_dt = datetime.fromisoformat(str(current_time).replace('Z', '+00:00'))
                            new_dt = datetime.fromisoformat(str(settlement_time).replace('Z', '+00:00'))
                            if new_dt > current_dt:
                                latest_records[frame_number] = record
                        except:
                            # 如果时间解析失败，保留当前记录
                            pass
                    elif settlement_time and not current_time:
                        # 新记录有时间，旧记录没有，使用新记录
                        latest_records[frame_number] = record

            deduplicated_data = list(latest_records.values())
            self.logger.log_info(f"调车结算查询去重后: 从 {len(flattened_data)} 条数据保留 {len(deduplicated_data)} 条")

            # 构建插入查询
            query, params_list = self.build_insert_query(table_name, deduplicated_data, fieldnames, table_schema)

            if not query or not params_list:
                self.last_error = "无法构建SQL查询，无法保存调车结算查询数据"
                return 0

            # 使用 ON DUPLICATE KEY UPDATE 来更新重复记录
            # 由于我们使用 FrameNumber 作为业务主键，需要确保有唯一索引
            update_fields = [f for f in fieldnames if f in table_schema and f not in ['ID', 'create_time']]
            update_clause = ', '.join([f"`{field}` = VALUES(`{field}`)" for field in update_fields])

            insert_update_query = f"{query} ON DUPLICATE KEY UPDATE {update_clause}"

            # 执行批量插入/更新
            affected_rows = self.db_operator.execute_many(insert_update_query, params_list)

            # 检查是否有数据库错误
            if self.db_operator.get_last_error():
                self.last_error = self.db_operator.get_last_error()
                return 0

            self.logger.log_info(f"调车结算查询保存完成，影响 {affected_rows} 行数据")
            return affected_rows

        except Exception as e:
            error_msg = f"调车结算查询保存失败: {str(e)}"
            self.logger.log_error(error_msg)
            self.last_error = error_msg
            return 0

    def save_invoice_maintenance(self, data_list: List[Dict]) -> int:
        """
        保存开票维护数据 - 如果有重复就覆盖原有的记录

        Args:
            data_list: 开票维护数据列表

        Returns:
            影响的行数
        """
        self.last_error = None

        if not data_list:
            msg = "开票维护无数据可保存"
            self.logger.log_info(msg)
            self.last_error = msg
            return 0

        table_name = 'invoice_maintenance'
        table_schema = self.get_table_schema(table_name)
        if not table_schema:
            self.last_error = f"无法获取表 {table_name} 的结构，无法保存数据"
            return 0

        try:
            flattened_data, fieldnames = flatten_data(data_list)

            # 构建插入查询
            query, params_list = self.build_insert_query(table_name, flattened_data, fieldnames, table_schema)

            if not query or not params_list:
                self.last_error = "无法构建SQL查询，无法保存开票维护数据"
                return 0

            # 使用 REPLACE INTO 来覆盖重复记录
            replace_query = query.replace('INSERT INTO', 'REPLACE INTO')

            # 执行批量替换
            affected_rows = self.db_operator.execute_many(replace_query, params_list)

            # 检查是否有数据库错误
            if self.db_operator.get_last_error():
                self.last_error = self.db_operator.get_last_error()
                return 0

            self.logger.log_info(f"开票维护保存完成，影响 {affected_rows} 行数据")
            return affected_rows

        except Exception as e:
            error_msg = f"开票维护保存失败: {str(e)}"
            self.logger.log_error(error_msg)
            self.last_error = error_msg
            return 0


class DataSyncManager:
    """数据同步管理器（全量更新版本）"""

    def __init__(self):
        """初始化数据同步管理器（无时间限制，全量更新）"""
        self.logger = WriteLog()
        self.requester = RequestFunction()
        self.token = self.get_token()

        # 初始化数据库连接
        self.db_operator = DBOperator(Config.DB_CONFIG)
        self.data_saver = DataSaver(self.db_operator)

        # 初始化WebDAV客户端
        self.client = Client(Config.WEBDAV_CONFIG)

    def get_token(self) -> str:
        """获取API访问令牌"""
        try:
            response = self.requester.request(
                url=f'{Config.API_BASE_URL}/api/Values/GetToken',
                method='GET',
                params={"Account": DataCenter.USER, "PassWord": DataCenter.PWD},
            )
            return response['data']['Msg']
        except Exception as e:
            error_msg = f"获取Token失败: {str(e)}"
            self.logger.log_error(error_msg)
            raise Exception(error_msg)

    def fetch_single_api(self, task_name: str, api_config: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        处理单个API的所有分页请求（获取全量数据）

        Args:
            task_name: 任务名称
            api_config: API配置

        Returns:
            包含所有数据的字典
        """
        url = api_config['url']
        method = api_config['method']
        payload = copy.deepcopy(api_config.get('payload', {}))

        all_data = []
        page_number = payload.get('PageNumber', 1)
        page_size = payload.get('PageSize', Config.PAGE_SIZE)

        try:
            while True:
                if 'PageNumber' in payload:
                    payload['PageNumber'] = page_number

                kwargs = {}
                if method.upper() == 'GET':
                    kwargs['params'] = payload
                else:
                    kwargs['json_data'] = payload

                payload['Token'] = self.token

                # 添加重试机制
                max_retries = 3
                retry_delay = 5  # 秒
                response = None
                last_exception = None

                for attempt in range(max_retries):
                    self.logger.log_info(f"{task_name} 第{page_number}页 - 重试第{attempt + 1}次")
                    try:
                        response = self.requester.request(
                            url=url,
                            method=method,
                            **kwargs
                        )

                        # 检查响应结构
                        if not response or not isinstance(response, dict):
                            raise KeyError(f"响应不是字典类型: {type(response)}")

                        if 'data' not in response:
                            raise KeyError("响应中缺少 'data' 字段")

                        # 进一步检查 data 结构
                        page_data = response['data']
                        if not page_data:
                            raise KeyError("data 字段为空")

                        # 检查嵌套的数据结构
                        if 'Msg' in page_data:
                            if 'Model' not in page_data['Msg']:
                                raise KeyError("data.Msg 中缺少 'Model' 字段")
                        else:
                            if 'Data' not in page_data:
                                raise KeyError("data 中缺少 'Data' 字段")

                        # 如果所有检查都通过，跳出重试循环
                        break

                    except KeyError as e:
                        # 专门处理数据结构错误
                        last_exception = e
                        if attempt < max_retries - 1:
                            self.logger.log_warning(
                                f"{task_name} 第{page_number}页第{attempt + 1}次请求数据结构错误: {str(e)}，{retry_delay}秒后重试")
                            sleep(retry_delay)
                        else:
                            raise Exception(
                                f"{task_name} 第{page_number}页请求数据结构错误，已重试{max_retries}次: {str(e)}")

                    except Exception as e:
                        last_exception = e
                        if attempt < max_retries - 1:
                            self.logger.log_warning(
                                f"{task_name} 第{page_number}页第{attempt + 1}次请求失败: {str(e)}，{retry_delay}秒后重试")
                            sleep(retry_delay)
                        else:
                            raise Exception(f"{task_name} 第{page_number}页请求失败，已重试{max_retries}次: {str(e)}")

                # 如果经过重试后仍然没有有效响应，抛出异常
                if not response or not isinstance(response, dict) or 'data' not in response:
                    error_msg = f"{task_name} 第{page_number}页请求返回无效数据"
                    if last_exception:
                        error_msg += f"，最后一次错误: {str(last_exception)}"
                    raise Exception(error_msg)

                # 提取数据（这里应该不会再有KeyError，因为上面已经验证过数据结构）
                page_data = response['data']
                if 'Msg' in page_data:
                    page_data = page_data['Msg']['Model']
                else:
                    page_data = page_data['Data']

                # 处理不同类型的数据结构
                if isinstance(page_data, dict):
                    page_data = page_data.get('list', page_data.get('items', []))

                if not isinstance(page_data, list):
                    raise Exception(
                        f"{task_name} 第{page_number}页返回数据格式不正确，预期列表类型，实际得到: {type(page_data)}")

                all_data.extend(page_data)

                if should_fetch_next_page(page_data, page_size):
                    page_number += 1
                else:
                    break

                # 添加随机延迟，避免请求过于频繁
                sleep(random.uniform(*Config.REQUEST_DELAY))

        except Exception as e:
            raise Exception(f"API请求过程出错: {str(e)}") from e

        return {task_name: all_data}

    def run_apis(self) -> Dict[str, List[Dict]]:
        """使用线程池并发处理API请求（全量更新）"""
        results = {}
        apis = DataCenter.get_apis()
        total_affected = 0  # 统计总影响行数
        success_tasks = 0  # 统计成功的任务数
        failed_tasks = []  # 记录失败的任务及详细错误
        row_count_notifications = []  # 记录每条数据的影响行数，用于最终汇总

        # 对容易失败的API进行特殊处理
        problematic_apis = ['销售_衍生_订单查询', '装饰_订单管理_装饰订单']  # 根据经验添加容易失败的API

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_task = {}
            for task_name, api_config in apis.items():
                # 对容易失败的API使用带重试的包装函数
                if task_name in problematic_apis:
                    future = executor.submit(self.fetch_single_api_with_extra_retry, task_name, api_config)
                else:
                    future = executor.submit(self.fetch_single_api, task_name, api_config)
                future_to_task[future] = task_name

            for future in as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results.update(result)
                    data_count = len(result[task_name])
                    self.logger.log_info(f"任务 {task_name} 完成, 获取{data_count}条全量数据")

                    # 保存数据并记录影响行数
                    affected = self.data_saver.save_data(task_name, result[task_name])

                    # 检查数据保存过程中是否有错误
                    if self.data_saver.get_last_error():
                        error_msg = f"任务 {task_name} 数据保存失败: {self.data_saver.get_last_error()}"
                        self.logger.log_error(error_msg)
                        failed_tasks.append(error_msg)
                    else:
                        total_affected += affected
                        success_tasks += 1
                        # 记录影响行数
                        row_count_msg = f"{task_name} 全量更新完成，插入 {affected} 行"
                        row_count_notifications.append(row_count_msg)

                except Exception as e:
                    # 捕获API请求和处理过程中的所有异常
                    error_msg = f"任务 {task_name} 处理失败: {str(e)}"
                    self.logger.log_error(error_msg)
                    failed_tasks.append(error_msg)

        # 所有任务完成后汇总并发送企微通知
        completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        notification_content = (
            f"【全量数据同步任务】已完成\n"
            f"完成时间: {completion_time}\n"
            f"同步类型: 全量更新（删除旧数据+插入新数据）\n"
            f"总任务数: {len(apis)}\n"
            f"成功任务数: {success_tasks}\n"
            f"失败任务数: {len(failed_tasks)}\n"
            f"总插入行数: {total_affected}\n"
        )

        # 添加各任务影响行数详情
        if row_count_notifications:
            notification_content += "\n各任务同步详情:\n" + "\n".join(row_count_notifications)

        # 添加失败任务详细信息
        if failed_tasks:
            notification_content += "\n失败任务详情:\n" + "\n".join(failed_tasks)

        # 发送汇总通知
        send_wechat_notification(notification_content)

        return results

    def fetch_single_api_with_extra_retry(self, task_name: str, api_config: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        对容易失败的API使用额外的重试机制
        """
        max_extra_retries = 2
        extra_retry_delay = 10  # 更长的重试间隔

        for extra_attempt in range(max_extra_retries + 1):
            try:
                return self.fetch_single_api(task_name, api_config)
            except Exception as e:
                if extra_attempt < max_extra_retries:
                    self.logger.log_warning(
                        f"{task_name} 额外重试第{extra_attempt + 1}次: {str(e)}，{extra_retry_delay}秒后重试")
                    sleep(extra_retry_delay)
                else:
                    self.logger.log_error(f"{task_name} 额外重试{max_extra_retries}次后仍然失败: {str(e)}")
                    raise

    def close(self) -> None:
        """关闭资源"""
        self.db_operator.close()


# 程序入口
if __name__ == "__main__":
    sync_manager = None
    try:
        print("=" * 60)
        print("【全量数据同步程序】启动（无时间限制，同步所有数据）")
        print("=" * 60)

        sync_manager = DataSyncManager()
        sync_manager.run_apis()
    except Exception as e:
        error_msg = f"全量数据同步主程序异常: {str(e)}"
        print(error_msg)
        send_wechat_notification(error_msg)
    finally:
        if sync_manager:
            sync_manager.close()
        print("=" * 60)
        print("【全量数据同步程序】执行结束")
        print("=" * 60)