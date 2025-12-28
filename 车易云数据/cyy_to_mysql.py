from common_code import RequestFunction, WriteLog
from concurrent.futures import ThreadPoolExecutor, as_completed
from webdav3.client import Client
from time import sleep
import random
import copy
import pymysql
import requests
from typing import Dict, Any, Optional, Tuple, Set
import json
from datetime import datetime, timedelta
from typing import List, Union


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
    def get_apis(time_option: int) -> Dict[str, Dict[str, Any]]:
        """
        根据时间选项获取API配置

        Args:
            time_option: 时间选项 1-当日 2-当月 3-全部数据

        Returns:
            API配置字典
        """
        time_range = get_time_range(time_option)
        time_params = {"beginTime": time_range[0], "endTime": time_range[1]} if time_range else {}
        # 解决套餐销售列表异常
        # time_params = {"beginTime": '2025/09/15', "endTime": '2025/09/30'} if time_range else ""
        print(time_params)

        # API配置模板
        api_templates = {
            '车辆销售明细表_开票日期': {'url': f'{Config.API_BASE_URL}/api/Summary/CarSalDetailSummary','method': 'GET','payload': {"TimeType": 3,"Type": "2","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '车辆成本管理': {'url': f'{Config.API_BASE_URL}/api/Summary/CarCost','method': 'GET','payload': {"OrgType": "1","CostType": "1","TimeType": "3","check": "2","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '装饰_订单管理_装饰订单': {'url': f'{Config.API_BASE_URL}/api/Summary/AccessoryOrder','method': 'GET','payload': {"LikeType": "1","TimeType": "2","PageSize": 3000,"PageNumber": 1,**time_params}},
            '套餐销售列表': {'url': f'{Config.API_BASE_URL}/api/AfterSales/SaleServicePackagePage','method': 'GET','payload': {"TimeType": "2","isShowPhone": "1","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '销售_车辆销售_成交订单': {'url': f'{Config.API_BASE_URL}/api/Summary/DealOrder','method': 'GET','payload': {"DateType": "4","SalesContant": "1","AuthDistrub": "0","LikeType": "3","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '销售_车辆销售_作废订单': {'url': f'{Config.API_BASE_URL}/api/Summary/AbnormalOrder','method': 'GET','payload': {"SalesContant": "1","state": "0,2","AuthDistrub": "0","TimeType": "2","yType": 2,"PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '保险业务': {'url': f'{Config.API_BASE_URL}/api/Home/InternalExport','method': 'GET','payload': {"Salescontant": "1","TimeType": "1","LikeType": '3',"PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '按揭业务': {'url': f'{Config.API_BASE_URL}/api/Summary/MortgageData','method': 'GET','payload': {"Salescontant": "1","IsMortgage": "","IsPledge": "","state": "12","DateType": "1","LikeType": '',"PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '评估管理_成交': {'url': f'{Config.API_BASE_URL}/api/Summary/GetUsedCarList','method': 'GET','payload': {"BillState": "3","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '评估管理_已入库': {'url': f'{Config.API_BASE_URL}/api/Summary/GetUsedCarList','method': 'GET','payload': {"BillState": "5","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '调车结算查询': {'url': f'{Config.API_BASE_URL}/api/Summary/GetShuntSettlementList','method': 'GET','payload': {"OrgType": "-1","OrganizeId": "","PayState": 2,"BillingState": "","CarState": "","Title": "","TimeType": "","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '销售_衍生_订单查询': {'url': f'{Config.API_BASE_URL}/api/Summary/SalePreorder','method': 'GET','payload': {"TimeType": "1","LikeType": "3","PageSize": 3000,"PageNumber": 1,**time_params}},
            '库存车辆已售': {'url': f'{Config.API_BASE_URL}/api/Summary/StoreCar','method': 'GET','payload': {"OrgType": "0","CarState": "2","TimeType": "3","ShowPriceFlag": "1","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '汇票管理': {'url': f'{Config.API_BASE_URL}/api/Summary/CarTicket','method': 'GET','payload': {'sortName': 'PledgeDate','sortOrder': 'desc','DateType': '1',"PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '销售回访': {'url': f'{Config.API_BASE_URL}/api/Summary/GetFollowUpList','method': 'GET','payload': {'TimeType': '3','Unvisit': '',"PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
            '开票维护': {'url': f'{Config.API_BASE_URL}/api/Summary/GetInvoicesList','method': 'GET','payload': {"QueryType": "2","TimeType": "2","PageSize": Config.PAGE_SIZE,"PageNumber": 1,**time_params}},
        }

        return api_templates


def get_time_range(time_option: Union[int, str]) -> List[str]:
    """
    获取时间范围参数

    Args:
        time_option: 时间选项
            1-当日 2-当月 4-全部数据（特定范围）
            '01'-'12'对应2025年1-12月

    Returns:
        时间范围列表 [开始时间, 结束时间]
    """
    current_date = datetime.now()
    today = current_date.strftime("%Y/%m/%d")

    # 1-返回当日数据（开始和结束均为当天）
    if time_option == 1:
        return [today, today]

    # 2-返回当月数据（从当月1日到当天）
    elif time_option == 2:
        first_day_of_month = current_date.replace(day=1).strftime("%Y/%m/%d")
        return [first_day_of_month, today]

    # '01'-'12'：返回2025年对应月份的完整范围（1日到当月最后一天）
    elif isinstance(time_option, str) and time_option in [f"{i:02d}" for i in range(1, 13)]:
        target_month = int(time_option)
        # 计算当月第一天
        first_day = datetime(2025, target_month, 1).strftime("%Y/%m/%d")
        # 计算当月最后一天（下个月1日减1天）
        if target_month == 12:
            next_month_first = datetime(2026, 1, 1)
        else:
            next_month_first = datetime(2025, target_month + 1, 1)
        last_day = (next_month_first - timedelta(days=1)).strftime("%Y/%m/%d")
        return [first_day, last_day]

    # 其他情况默认返回当月数据
    else:
        first_day_of_month = current_date.replace(day=1).strftime("%Y/%m/%d")
        return [first_day_of_month, today]


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
        self.connect()

    def connect(self) -> None:
        """连接数据库"""
        try:
            self.connection = pymysql.connect(**self.db_config)
            print("数据库连接成功")
            self.last_error = None
        except Exception as e:
            error_msg = f"数据库连接失败: {str(e)}"
            print(error_msg)
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
            print(error_msg)
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
            print(error_msg)
            self.last_error = error_msg
            self.connection.rollback()
            self.reconnect()
            return 0

    def execute_without_primary_key(self, table_name: str, data_list: List[Tuple], fieldnames: List[str]) -> int:
        """
        执行没有主键的批量插入（先清空再插入）

        Args:
            table_name: 表名
            data_list: 数据列表
            fieldnames: 字段名列表

        Returns:
            影响的行数
        """
        try:
            with self.connection.cursor() as cursor:
                # 开启事务
                self.connection.begin()

                # 清空表
                cursor.execute(f"DELETE FROM {table_name}")

                # 构建插入语句
                quoted_fields = [f"`{field}`" for field in fieldnames]
                placeholders = ', '.join(['%s'] * len(fieldnames))
                insert_query = f"INSERT INTO {table_name} ({', '.join(quoted_fields)}) VALUES ({placeholders})"

                # 执行批量插入
                cursor.executemany(insert_query, data_list)

                # 提交事务
                self.connection.commit()
                self.last_error = None
                return cursor.rowcount
        except Exception as e:
            error_msg = f"无主键表操作失败: {str(e)}"
            print(error_msg)
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
            print("数据库连接已关闭")


class DataSaver:
    """数据保存类，处理不同API数据到数据库的保存"""

    # API名称到数据库表名的映射
    API_TABLE_MAPPING = {
        '按揭业务': 'mortgage_business',
        '保险业务': 'insurance_business',
        '车辆成本管理': 'car_cost_management',
        '车辆销售明细表_订单日期': 'car_sales_order_date',
        '车辆销售明细表_开票日期': 'car_sales_invoice_date',
        '调车结算查询': 'vehicle_transfer_settlement',
        '汇票管理': 'bill_management',
        '开票维护': 'invoice_maintenance',
        '库存车辆已售': 'inventory_vehicle_sold',
        '评估管理_成交': 'evaluation_deal',
        '评估管理_已入库': 'evaluation_stored',
        '套餐销售列表': 'package_sales',
        '销售_车辆销售_成交订单': 'sales_deal_orders',
        '销售_车辆销售_作废订单': 'sales_canceled_orders',
        '销售_衍生_订单查询': 'sales_derivative_orders',
        '装饰_订单管理_装饰订单': 'decoration_orders',
        '销售回访': 'car_sales_data',
    }

    # 各表的主键或唯一键字段
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

    def save_data(self, api_name: str, data_list: List[Dict]) -> int:
        """
        保存数据到数据库

        Args:
            api_name: API名称
            data_list: 数据列表

        Returns:
            影响的行数
        """
        self.last_error = None

        if not data_list:
            msg = f"{api_name} 无数据可保存"
            print(msg)
            self.last_error = msg
            return 0

        table_name = self.API_TABLE_MAPPING.get(api_name)
        if not table_name:
            msg = f"未找到API {api_name} 对应的表映射"
            print(msg)
            self.last_error = msg
            return 0

        # 特殊处理删除日志表（没有主键，全量更新）
        if table_name == 'delete_log':
            return self.save_delete_log_data(api_name, data_list)

        table_schema = self.get_table_schema(table_name)
        if not table_schema:
            self.last_error = f"无法获取表 {table_name} 的结构，无法保存数据"
            return 0

        try:
            flattened_data, fieldnames = flatten_data(data_list)
            query, params_list = self.build_upsert_query(table_name, flattened_data, fieldnames, table_schema)

            if not query or not params_list:
                self.last_error = f"无法构建SQL查询，无法保存 {api_name} 数据"
                return 0

            affected_rows = self.db_operator.execute_many(query, params_list)

            # 检查是否有数据库错误
            if self.db_operator.get_last_error():
                self.last_error = self.db_operator.get_last_error()
                return 0

            print(f"{api_name} 数据保存完成，影响 {affected_rows} 行")
            return affected_rows
        except Exception as e:
            error_msg = f"{api_name} 数据处理失败: {str(e)}"
            print(error_msg)
            self.last_error = error_msg
            return 0

    def build_upsert_query(self, table_name: str, data_list: List[Dict], fieldnames: List[str],
                           table_schema: Dict[str, str]) -> Tuple[Optional[str], Optional[List[Tuple]]]:
        """
        构建UPSERT查询语句

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

        primary_key = self.TABLE_PRIMARY_KEYS.get(table_name)

        # 如果没有主键，返回None（这种情况应该不会发生，因为有主键的表才会调用这个方法）
        if not primary_key:
            self.last_error = f"表 {table_name} 未配置主键字段"
            print(self.last_error)
            return None, None

        # 检查主键是否存在于表结构中
        if primary_key not in table_schema:
            self.last_error = f"表 {table_name} 中不存在主键字段 {primary_key}"
            print(self.last_error)
            return None, None

        # 过滤掉不存在于表结构中的字段
        valid_fields = [field for field in fieldnames if field in table_schema]
        if not valid_fields:
            self.last_error = f"没有找到与表 {table_name} 匹配的有效字段"
            print(self.last_error)
            return None, None

        quoted_fields = [f"`{field}`" for field in valid_fields]
        placeholders = ', '.join(['%s'] * len(valid_fields))
        insert_clause = f"INSERT INTO {table_name} ({', '.join(quoted_fields)}) VALUES ({placeholders})"

        update_clause = ', '.join(
            [f"`{field}`=VALUES(`{field}`)" for field in valid_fields if field != primary_key]
        )

        query = f"{insert_clause} ON DUPLICATE KEY UPDATE {update_clause}"

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

    def get_last_error(self) -> Optional[str]:
        """获取最后一次错误信息"""
        return self.last_error


class DataSyncManager:
    """数据同步管理器"""

    def __init__(self, time_option: int = 2):
        """
        初始化数据同步管理器

        Args:
            time_option: 时间选项 1-当日 2-当月 3-全部数据
        """
        self.logger = WriteLog()
        self.requester = RequestFunction()
        self.token = self.get_token()
        self.time_option = time_option

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
        处理单个API的所有分页请求

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

                response = self.requester.request(
                    url=url,
                    method=method,
                    **kwargs
                )

                if not response or not isinstance(response, dict):
                    raise Exception(f"{task_name} 第{page_number}页请求返回无效数据")

                # 提取数据
                page_data = response['data']
                if 'Msg' in page_data:
                    page_data = page_data['Msg']['Model']
                else:
                    page_data = page_data['Data']

                # 处理不同类型的数据结构
                if isinstance(page_data, dict):
                    page_data = page_data.get('list', page_data.get('items', []))

                if not isinstance(page_data, list):
                    raise Exception(f"{task_name} 第{page_number}页返回数据格式不正确，预期列表类型")

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
        """使用线程池并发处理API请求"""
        results = {}
        apis = DataCenter.get_apis(self.time_option)
        total_affected = 0  # 统计总影响行数
        success_tasks = 0  # 统计成功的任务数
        failed_tasks = []  # 记录失败的任务及详细错误
        row_count_notifications = []  # 记录每条数据的影响行数，用于最终汇总

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_task = {
                executor.submit(
                    self.fetch_single_api,
                    task_name,
                    api_config
                ): task_name
                for task_name, api_config in apis.items()
            }

            for future in as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results.update(result)
                    data_count = len(result[task_name])
                    self.logger.log_info(f"任务 {task_name} 完成, 获取{data_count}条数据")

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
                        row_count_msg = f"{task_name} 数据保存完成，影响 {affected} 行"
                        row_count_notifications.append(row_count_msg)

                except Exception as e:
                    # 捕获API请求和处理过程中的所有异常
                    error_msg = f"任务 {task_name} 处理失败: {str(e)}"
                    self.logger.log_error(error_msg)
                    failed_tasks.append(error_msg)

        # 所有任务完成后汇总并发送企微通知
        completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_range_desc = {
            1: "当日",
            2: "当月",
            3: "25年04月以后"
        }.get(self.time_option, "未知")

        notification_content = (
            f"数据同步任务已完成\n"
            f"完成时间: {completion_time}\n"
            f"时间范围: {time_range_desc}\n"
            f"总任务数: {len(apis)}\n"
            f"成功任务数: {success_tasks}\n"
            f"失败任务数: {len(failed_tasks)}\n"
            f"总影响行数: {total_affected}\n"
        )

        # 添加各任务影响行数详情
        if row_count_notifications:
            notification_content += "\n各任务影响行数:\n" + "\n".join(row_count_notifications)

        # 添加失败任务详细信息
        if failed_tasks:
            notification_content += "\n失败任务详情:\n" + "\n".join(failed_tasks)

        # 发送汇总通知
        send_wechat_notification(notification_content)

        return results

    def close(self) -> None:
        """关闭资源"""
        self.db_operator.close()


# 程序入口
if __name__ == "__main__":
    # 1: 当日数据, 2: 当月数据, 3: 输入'01'-'12',获取当月的数据
    sync_manager = None
    try:
        # for i in [f"{month:02d}" for month in range(1, datetime.now().month+1)]:
            sync_manager = DataSyncManager(time_option=2)  # 默认下载当月数据
            sync_manager.run_apis()
    except Exception as e:
        error_msg = f"数据同步主程序异常: {str(e)}"
        print(error_msg)
        send_wechat_notification(error_msg)
    finally:
        if sync_manager:
            sync_manager.close()