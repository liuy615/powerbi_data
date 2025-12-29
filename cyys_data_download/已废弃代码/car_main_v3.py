from common_code import RequestFunction, WriteLog
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from webdav3.client import Client
from urllib.parse import unquote
from time import sleep
import tempfile
import random
import copy
import csv
import os


def should_fetch_next_page(current_items: list, page_size: int) -> bool:
    """
    判断是否满足翻页条件
    :param current_items: 当前页获取的数据列表
    :param page_size: 每页预期数据量大小
    :return: 满足翻页条件返回True，否则返回False
    """
    # 如果当前页无数据，不需要翻页
    if not current_items:
        return False

    # 如果当前页数据量达到页面大小上限，认为需要继续翻页
    if len(current_items) >= page_size:
        return True

    # 其他情况不需要翻页
    return False


def start_end_date():
    # 获取当前日期并计算昨天日期
    current_date = datetime.now()
    yesterday = current_date - timedelta(days=0)

    # 计算昨天所在月份的第一天
    first_day_of_month = yesterday.replace(day=1)
    # result = [
    #     first_day_of_month.strftime("%Y/%m/%d"),
    #     current_date.strftime("%Y/%m/%d")
    # ]
    result = [
        "2025/09/01",
        "2025/09/30"
    ]
    return result


class DataCenter:
    User = '15884541263'
    Pwd = 'uMhiC6vH9hv4J2zFPMs+LA=='
    day = start_end_date()
    APIs = {
       # '销售回访':{'url': 'https://openapi.cyys361.com:5085/api/Summary/GetFollowUpList', 'method': 'GET', 'payload': {"BeginTime": "2025/04/01","EndTime":"","TimeType":"3","Unvisit":"","PageSize": 1000, "PageNumber": 1,}},
       #  '保险业务':{'url': 'https://openapi.cyys361.com:5085/api/Home/InternalExport', 'method': 'GET', 'payload': {"beginTime": "2025/04/01", "endTime": "", "Salescontant":"1", "TimeType":"1","LikeType":'3', "PageSize": 1000, "PageNumber": 1, }},
       #  '按揭业务': {'url': 'https://openapi.cyys361.com:5085/api/Summary/MortgageData', 'method': 'GET', 'payload': {"beginTime": "", "endTime": "", "Salescontant":"1","IsMortgage":"","IsPledge":"","state": "12", "DateType":"1","LikeType":'', "PageSize": 1000, "PageNumber": 1, }},
       #  '评估管理_成交': {'url': 'https://openapi.cyys361.com:5085/api/Summary/GetUsedCarList', 'method': 'GET', 'payload': {"beginTime": "", "endTime": "","BillState":"3","PageSize": 1000, "PageNumber": 1, }},
       #  '评估管理_已入库': {'url': 'https://openapi.cyys361.com:5085/api/Summary/GetUsedCarList', 'method': 'GET', 'payload': {"beginTime": "", "endTime": "","BillState":"5","PageSize": 1000, "PageNumber": 1, }},
       #  '调车结算查询': {'url': 'https://openapi.cyys361.com:5085/api/Summary/GetShuntSettlementList', 'method': 'GET', 'payload': {"OrgType": "-1", "OrganizeId": "", "PayState":2, "BillingState": "", "CarState": "", "Title": "", "TimeType": "", "beginTime": "", "endTime": "","PageSize": 1000, "PageNumber": 1, }},
       #  '计划车辆': {'url': 'https://openapi.cyys361.com:5085/api/Summary/PurchaseOrder', 'method': 'GET', 'payload': {"OrderState": "1", "PageSize": 1000, "PageNumber": 1, }},
       #  '未售订单': {'url': 'https://openapi.cyys361.com:5085/api/Summary/UnSaleCar', 'method': 'GET', 'payload': {'DateType': '1',"timetype": "1", "LikeType": "3", "PageSize": 1000, "PageNumber": 1, }},
       #  '销售_衍生_订单查询': {'url': 'https://openapi.cyys361.com:5085/api/Summary/SalePreorder', 'method': 'GET', 'payload': {"TimeType": "1", "LikeType": "3",  "PageSize": 1000, "PageNumber": 1}},
       #  '库存车辆查询': {'url': 'https://openapi.cyys361.com:5085/api/Summary/StoreCar', 'method': 'GET', 'payload': {"OrgType": "0", "TimeType": "3", "ShowPriceFlag": "1", "PageSize": 1000, "PageNumber": 1, }},
       #  '库存车辆已售': {'url': 'https://openapi.cyys361.com:5085/api/Summary/StoreCar', 'method': 'GET', 'payload': {"OrgType": "0", "CarState":"2","TimeType": "3", "ShowPriceFlag": "1", "PageSize": 1000, "PageNumber": 1, }},
       #  '汇票管理': {'url': 'https://openapi.cyys361.com:5085/api/Summary/CarTicket', 'method': 'GET', 'payload': {'sortName': 'PledgeDate', 'sortOrder': 'desc', 'DateType': '1', "PageSize": 1000, "PageNumber": 1, }},
       #  '车辆销售明细表_订单日期': {'url': 'https://openapi.cyys361.com:5085/api/Summary/CarSalDetailSummary', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "TimeType": 1, "Type": 2, "PageSize": 3000, "PageNumber": 1, }},
       #  '车辆销售明细表_开票日期': {'url': 'https://openapi.cyys361.com:5085/api/Summary/CarSalDetailSummary', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "TimeType": 3, "Type": "2", "PageSize": 3000, "PageNumber": 1, }},
       #  '车辆成本管理': {'url': 'https://openapi.cyys361.com:5085/api/Summary/CarCost', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "OrgType": "1", "CostType": "1", "TimeType": "3", "check": "2", "PageSize": 3000, "PageNumber": 1, }},
       #  '装饰_订单管理_装饰订单': {'url': 'https://openapi.cyys361.com:5085/api/Summary/AccessoryOrder', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "LikeType": "1", "TimeType": "2", "PageSize": 3000, "PageNumber": 1, }},
        '套餐销售列表': {'url': 'https://openapi.cyys361.com:5085/api/AfterSales/SaleServicePackagePage', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "TimeType": "2", "isShowPhone": "1", "PageSize": 3000, "PageNumber": 1, }},
        # '销售_车辆销售_成交订单': {'url': 'https://openapi.cyys361.com:5085/api/Summary/DealOrder', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "DateType": "4", "SalesContant": "1", "AuthDistrub": "0", "LikeType": "3", "PageSize": 3000, "PageNumber": 1, }},
        # '销售_车辆销售_作废订单': {'url': 'https://openapi.cyys361.com:5085/api/Summary/AbnormalOrder', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "SalesContant": "1", "state": "0,2", "AuthDistrub": "0", "TimeType": "2", "yType": 2, "PageSize": 3000, "PageNumber": 1, }},
        # '开票维护': {'url': 'https://openapi.cyys361.com:5085/api/Summary/GetInvoicesList', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1],"QueryType":"2","TimeType":"2","PageSize": 1000, "PageNumber": 1, }},
    }
    APIs_AddUp = {
        # '按揭业务': {'url': 'https://openapi.cyys361.com:5085/api/Summary/MortgageData', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "SalesContant": "1", "state": "1,5,14", "timetype": "1", "LikeType": "1", "PageSize": 3000, "PageNumber": 1, }},
        # '车辆销售明细表_订单日期': {'url': 'https://openapi.cyys361.com:5085/api/Summary/CarSalDetailSummary', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "TimeType": 1, "Type": 2, "PageSize": 3000, "PageNumber": 1, }},
        # '车辆销售明细表_开票日期': {'url': 'https://openapi.cyys361.com:5085/api/Summary/CarSalDetailSummary', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "TimeType": 3, "Type": "2", "PageSize": 3000, "PageNumber": 1, }},
        # '车辆成本管理': {'url': 'https://openapi.cyys361.com:5085/api/Summary/CarCost', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "OrgType": "1", "CostType": "1", "TimeType": "1", "check": "2", "PageSize": 3000, "PageNumber": 1, }},
        # '装饰_订单管理_装饰订单': {'url': 'https://openapi.cyys361.com:5085/api/Summary/AccessoryOrder', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "LikeType": "1", "TimeType": "2", "PageSize": 3000, "PageNumber": 1, }},
        # '套餐销售列表': {'url': 'https://openapi.cyys361.com:5085/api/AfterSales/SaleServicePackagePage', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "TimeType": "1", "isShowPhone": "1", "PageSize": 3000, "PageNumber": 1, }},
        # '销售_车辆销售_成交订单': {'url': 'https://openapi.cyys361.com:5085/api/Summary/DealOrder', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "DateType": "4", "SalesContant": "1", "AuthDistrub": "0", "LikeType": "3", "PageSize": 3000, "PageNumber": 1, }},
        # '销售_车辆销售_作废订单': {'url': 'https://openapi.cyys361.com:5085/api/Summary/AbnormalOrder', 'method': 'GET', 'payload': {"beginTime": day[0], "endTime": day[-1], "SalesContant": "1", "state": "0,2", "AuthDistrub": "0", "TimeType": "2", "yType": 2, "PageSize": 3000, "PageNumber": 1, }},
    }


class Main:
    def __init__(self):
        self.logger = WriteLog()
        self.requester = RequestFunction()
        self.token = self.get_token()['data']['Msg']
        self.client = Client({
            "webdav_hostname": "http://222.212.88.126:5005",
            # "webdav_login": "liuhongjia",
            # "webdav_login": "shihansong",
            "webdav_login": "zhufulin",
            "webdav_password": "150528Zfl@",
            # "webdav_password": "Xg888888",
            "verify_ssl": False,
            "session_options": {
                "pool_connections": 5,  # 连接池大小
                "pool_maxsize": 5,  # 最大连接数
                "max_retries": 3  # 自动重试次数
            },
            "disable_check": True  # 禁用目录存在性检查
        })

    def run(self):
        """使用线程池并发处理所有API请求"""
        results = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(
                    self.fetch_single_api,
                    task_name,
                    api_config
                ): task_name
                for task_name, api_config in DataCenter.APIs.items()
            }

            # 等待任务完成并收集结果
            for future in as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results.update(result)
                    self.logger.log_info(f"任务 {task_name} 完成, 获取{len(result[task_name])}条数据")
                except Exception as e:
                    self.logger.log_error(f"任务 {task_name} 失败: {str(e)}")

        # 返回所有结果（可根据需要处理结果）
        return results
    def run_AddUp(self):
        """使用线程池并发处理所有API请求"""
        results = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(
                    self.fetch_single_api,
                    task_name,
                    api_config
                ): task_name
                for task_name, api_config in DataCenter.APIs_AddUp.items()
            }

            # 等待任务完成并收集结果
            for future in as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results.update(result)
                    self.logger.log_info(f"任务 {task_name} 完成, 获取{len(result[task_name])}条数据")
                except Exception as e:
                    self.logger.log_error(f"任务 {task_name} 失败: {str(e)}")

        # 返回所有结果（可根据需要处理结果）
        return results

    def get_token(self):
        response = self.requester.request(
            url='https://openapi.cyys361.com:5085/api/Values/GetToken',
            method='GET',
            params={"Account": DataCenter.User, "PassWord": DataCenter.Pwd},
        )
        return response

    def fetch_single_api(self, task_name, api_config):
        """处理单个API的所有分页请求"""
        url = api_config['url']
        method = api_config['method']
        payload = copy.deepcopy(api_config.get('payload', {}))

        all_data = []  # 存储所有分页数据
        page_number = payload.get('PageNumber', 1)
        page_size = payload.get('PageSize', 1000)  # 默认分页大小

        while True:
            # 更新页码
            if 'PageNumber' in payload:
                payload['PageNumber'] = page_number

            # 根据请求方法确定参数传递方式
            kwargs = {}
            if method.upper() == 'GET':
                kwargs['params'] = payload
            else:  # POST/PUT等
                kwargs['json_data'] = payload

            # 添加token到请求头
            payload['Token'] = self.token

            # 发送请求
            response = self.requester.request(
                url=url,
                method=method,
                # headers=headers,
                **kwargs
            )

            # 检查响应是否有效
            if not response or not isinstance(response, dict):
                self.logger.log_error(f"{task_name} 第{page_number}页请求失败")
                break

            # 提取实际数据（根据实际API响应结构调整）
            page_data = response['data']
            if 'Msg' in page_data:
                page_data = page_data['Msg']['Model']
            else:
                page_data = page_data['Data']

            if isinstance(page_data, dict):
                page_data = page_data.get('list', page_data.get('items', []))

            # 合并数据
            if isinstance(page_data, list):
                all_data.extend(page_data)

            # 检查是否需要翻页
            if should_fetch_next_page(page_data, page_size):
                page_number += 1
            else:
                break
            sleep(random.uniform(5,7))
        return {task_name: all_data}

    def simple_upload(self, content, save_dir, filename):
        """
        增强版文件上传方法

        :param content: 要写入的二进制内容
        :param save_dir: 文件保存目录（自动创建）
        :param filename: 目标文件名（包含扩展名）
        :return: 上传是否成功
        """

        try:
            # 创建保存目录（如果不存在）
            os.makedirs(save_dir, exist_ok=True)

            # 生成完整保存路径
            save_path = os.path.join(save_dir, filename)

            # 创建并写入临时文件
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_path = tmp_file.name

            # 执行上传操作
            self.client.upload(save_path, tmp_path)
            self.logger.log_info(f"[ 成功 ] 文件已保存至：{save_path}")
            return True
        except Exception as e:
            self.logger.log_error(f"[ 失败 ] 上传错误：{unquote(str(e))}, {filename}")
            return False

        finally:
            # 清理临时文件
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                    print(f"[ 清理 ] 临时文件已删除{filename}")
                except Exception as clean_e:
                    self.logger.log_error(f"[ 警告 ] 清理失败：{str(clean_e)}")
                    # send_wecom_robot_message(f"[警告] 清理失败：{str(clean_e)}")
def flatten_data(data_list):
    """
    展开数据中的嵌套字典，将嵌套键转换为平级字段
    :param data_list: 原始数据列表（每个元素是字典）
    :return: 展开后的新数据列表和字段名集合
    """
    flattened_data = []
    all_keys = set()
    
    for item in data_list:
        new_item = {}
        for key, value in item.items():
            # 如果是字典类型，则展开嵌套字段
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    # 创建新字段名: 原字段名_嵌套键名
                    new_key = f"{key}_{sub_key}"
                    new_item[new_key] = sub_value
                    all_keys.add(new_key)
            else:
                new_item[key] = value
                all_keys.add(key)
        flattened_data.append(new_item)
    
    return flattened_data, all_keys
def delete_files_for_active_apis_by_periods(root_dir, periods, active_apis):
    """
    根据多个时间区间删除未注释的API接口文件夹中的文件
    
    :param root_dir: 根目录路径
    :param periods: 时间区间列表，每个元素为包含开始和结束日期的列表
    :param active_apis: 活跃(未注释)的API名称列表
    """
    for period in periods:
        # 获取时间段的年月作为关键字（使用结束日期的年月）
        keyword = period[-1][:7].replace('/', '-')
        print(f"开始处理时间段: {period[0]} 到 {period[-1]}")
        
        # 遍历指定目录及其所有子目录
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                # 检查文件名是否包含关键字
                if keyword in file:
                    # 获取文件所在的文件夹名称
                    folder_name = os.path.basename(root)
                    
                    # 检查文件夹名称是否在活跃API列表中
                    if folder_name in active_apis:
                        file_path = os.path.join(root, file)
                        try:
                            # 删除包含关键字的文件
                            os.remove(file_path)
                            print(f"已删除文件: {file_path}")
                        except Exception as e:
                            print(f"删除文件 {file_path} 时出错: {e}")
def process_multiple_periods():
    """
    处理多个时间区间的数据下载
    """
    # 定义多个时间区间
    periods = [
        # ["2025/04/01", "2025/04/30"],
        # ["2025/05/01", "2025/05/31"],
        # ["2025/06/01", "2025/06/30"],
        # ["2025/07/01", "2025/07/31"],
        # ["2025/08/01", "2025/08/31"],
        ["2025/09/01", "2025/09/30"],
        # ["2025/10/01", "2025/10/31"],
    ]
    
    # 获取未注释的API名称列表
    active_apis = list(DataCenter.APIs.keys())
    
    # 遍历每个时间区间
    for period in periods:
        print(f"开始处理时间段: {period[0]} 到 {period[-1]}")
        
        # 临时修改DataCenter.day以适应当前时间段
        DataCenter.day = period
        
        # 更新APIs中的payload参数
        for api_name, api_config in DataCenter.APIs.items():
            payload = api_config.get('payload', {})
            # 更新时间参数
            if 'beginTime' in payload:
                payload['beginTime'] = period[0]
            if 'endTime' in payload:
                payload['endTime'] = period[-1]
        
        # 执行数据下载
        main = Main()
        all_results = main.run()
        
        # 保存数据到CSV文件
        for api_name, data_list in all_results.items():
            if not data_list:
                continue
                
            # 处理嵌套字典
            flattened_data, fieldnames = flatten_data(data_list)
            sorted_fieldnames = sorted(fieldnames)  # 对字段名排序保证一致性

            # 写入CSV
            filename = f"D:/车易云_源数据/{api_name}/{api_name}{period[-1][:7].replace('/','-')}.csv"
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted_fieldnames)
                writer.writeheader()
                writer.writerows(flattened_data)
                
        print(f"时间段 {period[0]} 到 {period[-1]} 处理完成\n")
if __name__ == '__main__':

    main = Main()
    all_results = main.run()
    day_date = start_end_date()


    for api_name, data_list in all_results.items():
        if not data_list:
            continue

        # 处理嵌套字典
        flattened_data, fieldnames = flatten_data(data_list)
        sorted_fieldnames = sorted(fieldnames)  # 对字段名排序保证一致性

        # 写入CSV
        filename = f"E:/powerbi_data/看板数据/cyy原始数据/{api_name}/{api_name}{day_date[-1][:7].replace('/','-')}.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)


        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fieldnames)
            writer.writeheader()
            writer.writerows(flattened_data)


    # 批量处理多个时间区间的数据
    process_multiple_periods()
    


    #  #  删除多个月数据
    #  # 获取未注释的API名称列表
    # active_apis = list(DataCenter.APIs.keys())
    
    # # 多个时间区间
    # periods = [
    #     ["2025/04/01", "2025/04/30"],
    #     ["2025/05/01", "2025/05/31"],
    #     ["2025/06/01", "2025/06/30"],
    #     ["2025/07/01", "2025/07/31"],
    #     ["2025/08/01", "2025/08/31"],
    #     ["2025/09/01", "2025/09/30"]
    # ]