import pandas as pd
import pymysql
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import os
import sys
import logging
import traceback


# 配置常量
class Config:
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

    # 删除日志对应关系文件路径
    DELETE_LOG_MAPPING_PATH = r"E:/powerbi_data/data/删除日志对应.xlsx"

    # API名称到数据库表名的映射
    API_TABLE_MAPPING = {
        '按揭业务': 'mortgage_business',
        '保险业务': 'insurance_business',
        '车辆成本管理': 'car_cost_management',
        '车辆销售明细表_订单日期': 'car_sales_order_date',
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

    # 各表的主键字段
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

    # 日志配置
    LOG_PATH = r"/powerbi_data\车易云数据\data\delete_log"
    LOG_LEVEL = logging.INFO


def setup_logger():
    """设置日志记录器，只记录删除成功的操作"""
    # 确保日志目录存在
    log_dir = os.path.dirname(Config.LOG_PATH)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 添加日期到日志文件名
    log_file = f"{Config.LOG_PATH}_{datetime.now().strftime('%Y%m%d')}.log"

    # 配置日志记录器
    logger = logging.getLogger('DeleteLogProcessor')
    logger.setLevel(logging.DEBUG)  # 设置最低级别为DEBUG，让所有消息都能通过

    # 如果已经配置过处理器，则清除以避免重复
    if logger.handlers:
        logger.handlers.clear()

    # 创建自定义过滤器
    class DeleteSuccessFilter(logging.Filter):
        """只允许删除成功和错误的日志通过"""

        def filter(self, record):
            # 只允许删除成功的日志通过（包含"删除了"但不包含"执行删除SQL"）
            if record.levelno == logging.INFO:
                msg = record.getMessage()
                # 只记录包含"删除了"且不包含"执行删除SQL"的消息
                if "删除了" in msg and "执行删除SQL" not in msg:
                    return True
                return False
            # 允许所有错误日志通过
            elif record.levelno >= logging.ERROR:
                return True
            return False

    # 创建文件处理器 - 只记录删除成功和错误
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)  # 设置级别为INFO
    file_handler.addFilter(DeleteSuccessFilter())  # 添加过滤器

    # 创建控制台处理器 - 显示所有信息
    console_handler = logging.StreamHandler()
    console_handler.setLevel(Config.LOG_LEVEL)

    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 将格式化器添加到处理器
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 将处理器添加到日志记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# 创建全局日志记录器
logger = setup_logger()


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
            logger.error(error_msg)
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
            logger.error(error_msg)
            self.last_error = error_msg
            self.reconnect()
            return None

    def execute_delete(self, table_name: str, condition: str, params: Optional[Tuple] = None) -> int:
        """
        执行删除语句

        Args:
            table_name: 表名
            condition: 删除条件
            params: 参数

        Returns:
            删除的行数
        """
        try:
            with self.connection.cursor() as cursor:
                query = f"DELETE FROM {table_name} WHERE {condition}"
                print(f"执行删除SQL: {query}")
                # 不在日志文件中记录SQL语句
                cursor.execute(query, params)
                self.connection.commit()
                self.last_error = None
                return cursor.rowcount
        except Exception as e:
            error_msg = f"删除操作失败: {str(e)}"
            print(error_msg)
            logger.error(error_msg)
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


class DeleteLogProcessor:
    """删除日志处理器"""

    def __init__(self):
        """初始化删除日志处理器"""
        self.db_operator = DBOperator(Config.DB_CONFIG)
        self.delete_log_mapping = self.load_delete_log_mapping()
        self.processed_count = 0
        self.deleted_count = 0
        self.errors = []

    def load_delete_log_mapping(self) -> Dict[str, List[Dict]]:
        """
        加载删除日志对应关系文件

        Returns:
            删除日志映射关系字典
        """
        mapping = {}
        try:
            if os.path.exists(Config.DELETE_LOG_MAPPING_PATH):
                df = pd.read_excel(Config.DELETE_LOG_MAPPING_PATH)
                for _, row in df.iterrows():
                    type_name = str(row['Type']).strip()
                    api_name = str(row['对应接口名称']).strip()
                    api_path = str(row['接口路径']).strip()

                    if type_name not in mapping:
                        mapping[type_name] = []

                    mapping[type_name].append({
                        'api_name': api_name,
                        'api_path': api_path
                    })
                print(f"成功加载删除日志映射关系，共 {len(mapping)} 种类型")
            else:
                print(f"警告: 删除日志映射文件不存在: {Config.DELETE_LOG_MAPPING_PATH}")
        except Exception as e:
            error_msg = f"加载删除日志映射文件失败: {str(e)}"
            print(error_msg)
            logger.error(error_msg)
            self.errors.append(error_msg)

        return mapping

    def parse_delete_log_contents(self, contents: str) -> Dict[str, Any]:
        """
        解析删除日志的Contents字段

        Args:
            contents: Contents字段内容

        Returns:
            解析后的字典
        """
        try:
            if contents and isinstance(contents, str):
                content_json = json.loads(contents)
                return content_json
        except Exception as e:
            print(f"解析Contents字段失败: {str(e)}，内容: {contents[:100] if contents else '空'}")

        return {}

    def get_delete_logs(self) -> List[Dict[str, Any]]:
        """
        从数据库获取删除日志数据

        Returns:
            删除日志列表
        """
        delete_logs = []

        try:
            # 查询删除日志表
            query = """
            SELECT 
                Title, 
                Type, 
                Contents, 
                WriteTime, 
                UserName
            FROM delete_log 
            ORDER BY WriteTime DESC
            """

            results = self.db_operator.execute_query(query)

            if results:
                for row in results:
                    title = row[0]
                    log_type = row[1]
                    contents = row[2]
                    write_time = row[3]
                    user_name = row[4]

                    # 解析Contents字段
                    content_data = self.parse_delete_log_contents(contents)

                    # 提取Logo_IdOrCode
                    logo_id_or_code = content_data.get('Logo_IdOrCode')

                    # 提取Arr数组中的信息
                    arr = content_data.get('Arr', [])
                    record_id = None
                    bill_code = None

                    if arr and len(arr) > 0:
                        first_item = arr[0]
                        record_id = first_item.get('ID')
                        bill_code = first_item.get('BillCode')

                    delete_logs.append({
                        'title': title,
                        'type': log_type,
                        'logo_id_or_code': logo_id_or_code,
                        'record_id': record_id,
                        'bill_code': bill_code,
                        'write_time': write_time,
                        'user_name': user_name,
                        'contents': contents
                    })

                print(f"成功获取 {len(delete_logs)} 条删除日志记录")
            else:
                print("没有找到删除日志数据")

        except Exception as e:
            error_msg = f"获取删除日志失败: {str(e)}"
            print(error_msg)
            logger.error(error_msg)
            self.errors.append(error_msg)

        return delete_logs

    def get_table_primary_key(self, table_name: str) -> Optional[str]:
        """
        获取表的主键字段

        Args:
            table_name: 表名

        Returns:
            主键字段名，如果找不到返回None
        """
        return Config.TABLE_PRIMARY_KEYS.get(table_name)

    def get_table_name_by_api(self, api_name: str) -> Optional[str]:
        """
        根据API名称获取对应的表名

        Args:
            api_name: API名称

        Returns:
            表名，如果找不到返回None
        """
        return Config.API_TABLE_MAPPING.get(api_name)

    def check_field_exists(self, table_name: str, field_name: str) -> bool:
        """
        检查表中是否存在指定字段

        Args:
            table_name: 表名
            field_name: 字段名

        Returns:
            是否存在
        """
        try:
            query = f"SHOW COLUMNS FROM {table_name} LIKE %s"
            result = self.db_operator.execute_query(query, (field_name,))
            return bool(result)
        except Exception as e:
            print(f"检查字段 {field_name} 是否存在失败: {str(e)}")
            return False

    def split_ids(self, id_string: str) -> List[str]:
        """
        将逗号分隔的ID字符串拆分为ID列表

        Args:
            id_string: 逗号分隔的ID字符串

        Returns:
            ID列表
        """
        if not id_string:
            return []

        # 去除空格并按逗号分割
        return [id_str.strip() for id_str in str(id_string).split(',') if id_str.strip()]

    def execute_delete_for_log(self, delete_log: Dict[str, Any]) -> int:
        """
        执行单条删除日志的删除操作

        Args:
            delete_log: 删除日志记录

        Returns:
            删除的行数
        """
        title = delete_log['title']
        logo_id_or_code = delete_log['logo_id_or_code']

        if not logo_id_or_code:
            print(f"跳过删除日志 {title}，Logo_IdOrCode为空")
            return 0

        # 根据Title找到对应的API配置
        if title not in self.delete_log_mapping:
            print(f"跳过删除日志 {title}，未找到对应的API映射")
            return 0

        total_deleted = 0

        for mapping in self.delete_log_mapping[title]:
            api_name = mapping['api_name']

            # 获取对应的表名
            table_name = self.get_table_name_by_api(api_name)
            if not table_name:
                print(f"跳过API {api_name}，未找到对应的表名")
                continue

            # 确定要删除的字段和值
            delete_field = None

            # 特殊情况处理
            if title == "删除装饰业务数据":
                # 装饰业务数据删除ID相同的行
                delete_field = "ID"
            elif title == "删除装饰出库数据":
                # 装饰出库数据删除OutId相同的行
                delete_field = "OutId"
            else:
                # 其他情况使用表的主键
                delete_field = self.get_table_primary_key(table_name)
                if not delete_field:
                    print(f"跳过表 {table_name}，未找到主键字段")
                    continue

            # 检查字段是否存在
            if not self.check_field_exists(table_name, delete_field):
                print(f"跳过表 {table_name}，字段 {delete_field} 不存在")
                continue

            # 处理多个ID的情况
            id_list = self.split_ids(logo_id_or_code)

            if not id_list:
                print(f"警告: Logo_IdOrCode '{logo_id_or_code}' 没有有效的ID")
                continue

            print(f"发现 {len(id_list)} 个ID需要删除: {', '.join(id_list)}")

            # 如果只有一个ID，使用等号条件
            if len(id_list) == 1:
                condition = f"{delete_field} = %s"
                params = (id_list[0],)

                deleted_rows = self.db_operator.execute_delete(
                    table_name,
                    condition,
                    params
                )

                if deleted_rows > 0:
                    print(f"从 {table_name} 表中删除了 {deleted_rows} 行数据（{delete_field} = {id_list[0]}）")
                    # 将删除成功的记录写入日志文件
                    logger.info(f"从 {table_name} 表中删除了 {deleted_rows} 行数据（{delete_field} = {id_list[0]}）")
                    total_deleted += deleted_rows
                else:
                    print(f"在 {table_name} 表中未找到 {delete_field} = {id_list[0]} 的数据")
            else:
                # 多个ID，使用IN条件
                placeholders = ', '.join(['%s'] * len(id_list))
                condition = f"{delete_field} IN ({placeholders})"

                deleted_rows = self.db_operator.execute_delete(
                    table_name,
                    condition,
                    tuple(id_list)
                )

                if deleted_rows > 0:
                    print(f"从 {table_name} 表中删除了 {deleted_rows} 行数据（{delete_field} IN ({', '.join(id_list)})）")
                    # 将删除成功的记录写入日志文件
                    logger.info(
                        f"从 {table_name} 表中删除了 {deleted_rows} 行数据（{delete_field} IN ({', '.join(id_list)})）")
                    total_deleted += deleted_rows
                else:
                    print(f"在 {table_name} 表中未找到 {delete_field} IN ({', '.join(id_list)}) 的数据")

        return total_deleted

    def process_all_delete_logs(self) -> Dict[str, Any]:
        """
        处理所有删除日志

        Returns:
            处理结果统计
        """
        print("开始处理删除日志...")

        # 获取所有删除日志
        delete_logs = self.get_delete_logs()

        if not delete_logs:
            print("没有找到需要处理的删除日志")
            return {
                'total_logs': 0,
                'processed_logs': 0,
                'total_deleted': 0,
                'errors': self.errors
            }

        total_deleted = 0

        # 处理每条删除日志
        for i, delete_log in enumerate(delete_logs, 1):
            print(f"\n处理第 {i}/{len(delete_logs)} 条删除日志: {delete_log['title']}")
            print(f"Logo_IdOrCode: {delete_log['logo_id_or_code']}")
            print(f"操作时间: {delete_log['write_time']}")
            print(f"操作人: {delete_log['user_name']}")

            try:
                deleted_rows = self.execute_delete_for_log(delete_log)
                total_deleted += deleted_rows
                self.processed_count += 1

                if deleted_rows > 0:
                    self.deleted_count += deleted_rows
                    print(f"本条删除日志成功删除了 {deleted_rows} 行数据")
                else:
                    print("本条删除日志未删除任何数据")

            except Exception as e:
                error_msg = f"处理删除日志失败: {str(e)}"
                print(error_msg)
                logger.error(error_msg)
                self.errors.append(f"{delete_log['title']}: {error_msg}")

        return {
            'total_logs': len(delete_logs),
            'processed_logs': self.processed_count,
            'total_deleted': total_deleted,
            'errors': self.errors
        }

    def close(self):
        """关闭资源"""
        self.db_operator.close()


def main():
    """主函数"""
    print("=" * 50)
    print("删除日志处理程序")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    processor = None
    try:
        # 创建处理器
        processor = DeleteLogProcessor()

        # 处理所有删除日志
        result = processor.process_all_delete_logs()

        print("\n" + "=" * 50)
        print("删除日志处理完成")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

        # 输出统计信息
        print(f"总删除日志数: {result['total_logs']}")
        print(f"已处理日志数: {result['processed_logs']}")
        print(f"总删除行数: {result['total_deleted']}")

        if result['errors']:
            print(f"\n错误信息 ({len(result['errors'])} 个):")
            for error in result['errors']:
                print(f"  - {error}")
        else:
            print("\n所有操作执行成功，没有错误")

    except Exception as e:
        print(f"程序执行异常: {str(e)}")
        logger.error(f"程序执行异常: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        if processor:
            processor.close()

    print("\n程序执行完毕")


if __name__ == "__main__":
    main()