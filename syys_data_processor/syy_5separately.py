import sys
import pandas as pd
from pathlib import Path
import warnings
import pandas as pd
import pymysql
from dns.dnssecalgs import PrivateDSA
from pymysql import MySQLError
from typing import List, Dict, Any, Optional
project_root = r"E:\powerbi_data"
sys.path.insert(0, project_root)
from config.syy_5separately.config import SOURCE_MYSQL_CONFIG, OUTPUT_MYSQL_CONFIG
# 忽略警告信息
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 100)

class FilmUpgradeAnalyzer:
    """
    贴膜升级数据分析器

    功能：
    1. 处理多个Excel文件的推送数据
    2. 处理到店数据
    3. 合并推送和到店数据，按日期取并集
    """

    def __init__(self, push_file_paths=None, visit_file_path=None, target_stores=None):
        """
        初始化分析器

        Parameters:
        -----------
        push_file_paths : list
            推送数据文件路径列表
        visit_file_path : str
            到店数据文件路径
        """
        # 推送文件路径列表
        if push_file_paths is None:
            self.push_file_paths = [
                r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\方程豹-上元坤灵-贴膜升级登记表-最新年.xlsx",
                r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\腾势-上元臻智-贴膜升级登记表-最新年.xlsx",
                r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\方程豹-上元弘川-贴膜升级登记表-最新年.xlsx",
                r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\腾势-上元臻盛-贴膜升级登记表-最新年.xlsx",
                r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\方程豹-上元星汉-贴膜升级登记表-最新年.xlsx",
                r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\两网-西门-贴膜升级登记表-最新年.xlsx",
            ]
        else:
            self.push_file_paths = push_file_paths

        if target_stores is None:
            self.target_stores = ['上元臻盛', '上元臻智', '上元星汉', '上元弘川', '上元坤灵', '文景盛世', '文景海洋', '上元盛世', '洪武盛世', '新港海川', '新港永初', '永乐盛世', '文景初治']
        else:
            self.target_stores = target_stores

        # 成本文件路径
        if visit_file_path is None:
            self.visit_file_path = [r"E:/powerbi_data/看板数据/私有云文件本地/贴膜升级/腾豹-双流、交大、羊犀、天府-贴膜升级登记表-最新年.xlsx",
                                    r"E:/powerbi_data/看板数据/私有云文件本地/贴膜升级/两网-西门自店-贴膜升级登记表-最新年.xlsx",
                                    r"E:/powerbi_data/看板数据/私有云文件本地/贴膜升级/两网-总部-贴膜升级登记表-最新年.xlsx",
                                    ]
        else:
            self.visit_file_path = visit_file_path

        # 结果存储
        self.push_stats = None
        self.visit_stats = None
        self.combined_stats = None

    # 处理推送数据
    def process_push_data(self):
        """
        处理推送数据，统计每个店每日的推送量和返佣

        Returns:
        --------
        pd.DataFrame
            包含推送日期、新车销售店名、推送量、返佣的DataFrame
        """
        print("开始处理推送数据...")

        # 存储所有数据的列表
        all_data = []

        # 遍历所有文件
        for file_path in self.push_file_paths:
            try:
                # 读取Excel文件
                df = pd.read_excel(file_path, sheet_name="膜升级登记表")

                # 检查必需的列是否存在
                if '车架号' in df.columns:
                    required_columns = ['新车销售店名', '推送日期', '车架号', '是否有效客户', '其它施工项目', '三方返还佣金','赠送装饰']
                    missing_columns = [col for col in required_columns if col not in df.columns]
                elif '车架号（后6位）' in df.columns:
                    required_columns = ['新车销售店名', '推送日期', '车架号（后6位）', '是否有效客户', '其它施工项目', '三方返还佣金','赠送装饰']
                    missing_columns = [col for col in required_columns if col not in df.columns]
                else:
                    missing_columns.extend(['车架号', '车架号（后6位）'])

                if missing_columns:
                    raise ValueError(f"文件缺少必需的列: {missing_columns}")

                if missing_columns:
                    print(f"文件 {Path(file_path).name} 缺少列: {missing_columns}")
                    continue

                # 提取需要的列
                if '车架号' in df.columns:
                    df_subset = df[['新车销售店名', '车架号', '推送日期', '是否有效客户', '其它施工项目', '三方返还佣金', '赠送装饰']].copy()
                elif '车架号（后6位）' in df.columns:
                    df_subset = df[['新车销售店名', '车架号（后6位）', '推送日期', '是否有效客户', '其它施工项目', '三方返还佣金','赠送装饰']].rename(columns={'车架号（后6位）': '车架号'}).copy()

                # 确保推送日期是日期格式
                df_subset['推送日期'] = pd.to_datetime(df_subset['推送日期'], errors='coerce')

                # 过滤掉推送日期为空的行
                df_subset = df_subset.dropna(subset=['推送日期'])

                # 处理三方返还佣金，确保是数值类型
                df_subset['三方返还佣金'] = pd.to_numeric(df_subset['三方返还佣金'], errors='coerce').fillna(0)

                # 处理赠送装饰，标记是否有值
                df_subset['赠送装饰_有值'] = df_subset['赠送装饰'].notna() & (df_subset['赠送装饰'].astype(str).str.strip() != '')

                # 计算每行的返佣
                # 三方返还佣金 - (200 if 赠送装饰有值 else 0)
                df_subset['返佣'] = df_subset['三方返还佣金'] - df_subset['赠送装饰_有值'].apply(lambda x: 200 if x else 0)

                # 添加到总数据中
                all_data.append(df_subset)
                print(f"读取文件: {Path(file_path).name}, 行数: {len(df)},有效数据行数: {len(df_subset)}")

            except FileNotFoundError:
                print(f"文件不存在: {file_path}")
            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {e}")

        # 如果没有任何数据，返回空的DataFrame
        if not all_data:
            print("警告: 没有找到任何有效的推送数据")
            return pd.DataFrame(columns=['日期', '新车销售店名', '推送量', '返佣'])

        # 合并所有数据
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"合并后总数据行数: {len(combined_df)}")

        # 提取日期部分（去掉时间）
        combined_df['推送日期'] = combined_df['推送日期'].dt.date
        # combined_df.to_csv("combined_df.csv")

        return combined_df

    # 处理成本数据
    def extract_store_cost_data(self):
        """
        从Excel文件中提取指定门店的成本数据
        返回处理后的DataFrame
        """

        try:
            df = pd.DataFrame()
            # 遍历所有文件
            for file_path in self.visit_file_path:
                # 读取Excel文件
                df_for = pd.read_excel(file_path, sheet_name="汇总表")
                # 读取Excel文件中的汇总表
                df = pd.concat([df, df_for], axis=0)

            # 筛选指定门店的数据
            target_stores = self.target_stores
            df_filtered = df[df['新车销售店名'].isin(target_stores)].copy()

            # 只保留需要的列
            required_columns = ['新车销售店名', '年份', '月份', '人员成本', '场地租金水电', '洗车费', '耗材采购','维修费用']
            df_filtered = df_filtered[required_columns].copy()

            # 删除空行（年份或月份为空的行）
            df_filtered = df_filtered.dropna(subset=['年份', '月份'])

            # 计算总成本列（人员成本 + 场地租金水电 + 洗车费 + 耗材采购 + 维修费用）
            cost_columns = ['人员成本', '场地租金水电', '洗车费', '耗材采购', '维修费用']

            # 确保成本列为数值类型，非数值转为NaN
            for col in cost_columns:
                df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

            # 计算总成本
            df_filtered['总成本'] = df_filtered[cost_columns].sum(axis=1, min_count=1)

            # 重命名列，使其更清晰
            column_mapping = {
                '新车销售店名': '公司名称',
                '人员成本': '人员成本',
                '场地租金水电': '场地租金水电',
                '洗车费': '洗车费',
                '耗材采购': '耗材采购',
                '维修费用': '维修费用'
            }

            df_filtered = df_filtered.rename(columns=column_mapping)

            # 重新排列列顺序
            final_columns = ['公司名称', '年份', '月份', '总成本']

            df_final = df_filtered[final_columns].copy()
            print(df_final)
            return df_final

        except FileNotFoundError:
            print(f"错误: 找不到文件 {file_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"处理数据时发生错误: {str(e)}")
            return pd.DataFrame()




class DecorationOrdersExtractor:
    """装饰订单数据提取器 - 使用pymysql"""

    def __init__(self):
        """
        初始化数据库配置
        这里可以根据需要修改配置参数
        """
        # 主数据库连接配置
        self.db_config = SOURCE_MYSQL_CONFIG

        # 销售数据数据库配置（新增）
        self.sales_db_config = OUTPUT_MYSQL_CONFIG

        # 装饰订单字段映射关系
        self.field_mapping = {
            'ID': 'ID',
            'OutId': 'OutId',
            'ArticleRretCode': '订单编号',
            'FrameNumber': '车架号',
            'InvoiceDate': '开票日期',
            'RecuDate': '收款日期',
            'CusName': '客户名称',
            'CusPhone': '联系电话',
            'SalesConsultantName': '销售顾问',
            'AloneTypeName': '单据类型',
            'OrganizeName': '新车销售店名',
            'MaterialName': '物资名称',
            'OutStoreState': '物资状态',
            'Number': '出/退/销数量',
            'TotalManHourFee': '工时费',
            'TaxCostAmount': '成本合计(含税)',
            'TaxTotalPrice': '合计收款金额'
        }

        # 销售数据字段映射关系（新增）
        self.sales_field_mapping = {
            '销售日期': '销售日期',
            '公司名称': '公司名称',
            '车架号': '车架号',
            '销售人员': '销售人员',
            '交付专员': '交付专员',
            '赠送装饰项目': '赠送装饰项目'
        }

        # 筛选条件
        self.target_sales_consultants = ['郑仁彬', '刘红梅', '郝小龙', '衡珊珊', '蒲松涛', '陈玲玲', '黄维']
        self.target_organize_names = ['上元臻盛', '上元臻智', '上元星汉', '上元弘川', '上元坤灵', '文景盛世', '文景海洋', '上元盛世', '洪武盛世', '新港海川', '新港永初']

        # 数据库连接对象
        self.connection = None
        self.cursor = None
        self.sales_connection = None  # 新增
        self.sales_cursor = None  # 新增

    def update_config(self, config_updates: Dict[str, Any], config_type: str = 'main'):
        """
        更新数据库配置

        Args:
            config_updates: 需要更新的配置项字典
            config_type: 配置类型，'main'或'sales'
        """
        if config_type == 'main':
            self.db_config.update(config_updates)
            print("主数据库配置已更新")
        elif config_type == 'sales':
            self.sales_db_config.update(config_updates)
            print("销售数据库配置已更新")
        else:
            print(f"未知的配置类型: {config_type}")

    def update_filter_conditions(self, sales_consultants: Optional[List[str]] = None, organize_names: Optional[List[str]] = None,company_names: Optional[List[str]] = None):
        """
        更新筛选条件
        Args:
            sales_consultants: 新的销售顾问列表
            organize_names: 新的门店列表
            company_names: 新的公司名称列表
        """
        if sales_consultants:
            self.target_sales_consultants = sales_consultants
        if organize_names:
            self.target_organize_names = organize_names
        if company_names:
            self.target_organize_names = company_names
        print("筛选条件已更新")

    def connect(self, db_type: str = 'main') -> bool:
        """
        连接到MySQL数据库

        Args:
            db_type: 数据库类型，'main'或'sales'

        Returns:
            bool: 连接是否成功
        """
        try:
            if db_type == 'main':
                self.connection = pymysql.connect(**self.db_config)
                self.cursor = self.connection.cursor()
                print(f"成功连接到主数据库: {self.db_config['database']}")
                return True
            elif db_type == 'sales':
                self.sales_connection = pymysql.connect(**self.sales_db_config)
                self.sales_cursor = self.sales_connection.cursor()
                print(f"成功连接到销售数据库: {self.sales_db_config['database']}")
                return True
            else:
                print(f"未知的数据库类型: {db_type}")
                return False
        except MySQLError as e:
            print(f"连接数据库时出错: {e}")
            return False
        except Exception as e:
            print(f"发生未知错误: {e}")
            return False

    def disconnect(self, db_type: str = 'all'):
        """
        断开数据库连接

        Args:
            db_type: 数据库类型，'main'、'sales'或'all'
        """
        try:
            if db_type in ['main', 'all']:
                if self.cursor:
                    self.cursor.close()
                if self.connection:
                    self.connection.close()
                print("主数据库连接已关闭")

            if db_type in ['sales', 'all']:
                if self.sales_cursor:
                    self.sales_cursor.close()
                if self.sales_connection:
                    self.sales_connection.close()
                print("销售数据库连接已关闭")

        except Exception as e:
            print(f"关闭连接时出错: {e}")

    def test_connection(self, db_type: str = 'main') -> bool:
        """
        测试数据库连接

        Args:
            db_type: 数据库类型，'main'或'sales'

        Returns:
            bool: 连接测试是否成功
        """
        try:
            if db_type == 'main':
                if not self.connection or not self.connection.open:
                    return self.connect(db_type)
                self.cursor.execute("SELECT 1")
                result = self.cursor.fetchone()
                return result is not None
            elif db_type == 'sales':
                if not self.sales_connection or not self.sales_connection.open:
                    return self.connect(db_type)
                self.sales_cursor.execute("SELECT 1")
                result = self.sales_cursor.fetchone()
                return result is not None
            else:
                print(f"未知的数据库类型: {db_type}")
                return False
        except Exception as e:
            print(f"连接测试失败: {e}")
            return False

    def get_table_info(self, table_name: str = 'decoration_orders', db_type: str = 'main') -> Dict[str, Any]:
        """
        获取表的基本信息

        Args:
            table_name: 表名
            db_type: 数据库类型，'main'或'sales'

        Returns:
            Dict[str, Any]: 表信息字典
        """
        try:
            if db_type == 'main':
                cursor = self.cursor
            elif db_type == 'sales':
                cursor = self.sales_cursor
            else:
                print(f"未知的数据库类型: {db_type}")
                return {}

            # 获取表结构信息
            cursor.execute(f"DESCRIBE {table_name}")
            columns_info = cursor.fetchall()

            # 获取表记录数
            cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
            total_count = cursor.fetchone()['total']

            # 获取数据日期范围（尝试获取可能的日期列）
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            all_columns = cursor.fetchall()
            date_columns = [col['Field'] for col in all_columns if
                            'date' in col['Field'].lower() or 'time' in col['Field'].lower()]

            date_ranges = {}
            for col in date_columns:
                try:
                    cursor.execute(f"SELECT MIN({col}) as min_date, MAX({col}) as max_date FROM {table_name}")
                    date_range = cursor.fetchone()
                    date_ranges[col] = date_range
                except:
                    date_ranges[col] = {'min_date': None, 'max_date': None}

            return {
                'total_records': total_count,
                'columns': columns_info,
                'date_ranges': date_ranges
            }
        except Exception as e:
            print(f"获取表信息时出错: {e}")
            return {}

    def extract_data(self) -> pd.DataFrame:
        """
        提取并筛选装饰订单数据

        Returns:
            pd.DataFrame: 包含筛选数据的DataFrame
        """
        # 构建SELECT字段列表
        select_fields = ', '.join(self.field_mapping.keys())

        # 构建IN语句的参数占位符
        sales_placeholders = ', '.join(['%s'] * len(self.target_sales_consultants))
        organize_placeholders = ', '.join(['%s'] * len(self.target_organize_names))

        # 构建SQL查询语句
        sql = f"""
        SELECT {select_fields}
        FROM decoration_orders
        WHERE SalesConsultantName IN ({sales_placeholders})
        AND OrganizeName IN ({organize_placeholders})
        """

        # 组合参数
        params = self.target_sales_consultants + self.target_organize_names

        print("=" * 60)
        print("正在执行装饰订单数据提取...")
        print(f"筛选条件:")
        print(f"  销售顾问: {self.target_sales_consultants}")
        print(f"  门店名称: {self.target_organize_names}")
        print("=" * 60)

        try:
            # 执行查询
            self.cursor.execute(sql, params)
            data = self.cursor.fetchall()

            if not data:
                print("未找到符合条件的数据")
                return pd.DataFrame()

            # 转换为DataFrame
            df = pd.DataFrame(data)

            # 重命名列名为中文
            df = df.rename(columns=self.field_mapping)
            df = df.groupby('ID').apply(lambda x: x[x['OutId'] != 0] if (x['OutId'] != 0).any() else x).reset_index(drop=True)


            # 输出提取结果摘要
            print(f"成功提取 {len(df)} 条记录")

            return df

        except MySQLError as e:
            print(f"执行SQL查询时出错: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"提取数据时发生未知错误: {e}")
            return pd.DataFrame()

    def extract_sales_data(self, company_names: List[str] = None) -> pd.DataFrame:
        """
        提取销售数据

        Args:
            company_names: 公司名称列表，如果为None则使用默认列表

        Returns:
            pd.DataFrame: 包含销售数据的DataFrame
        """
        # 使用传入的公司名称列表，如果没有传入则使用默认列表
        if company_names is None:
            company_names = self.target_organize_names

        # 构建SELECT字段列表
        select_fields = ', '.join(self.sales_field_mapping.keys())

        # 构建IN语句的参数占位符
        company_placeholders = ', '.join(['%s'] * len(company_names))

        # 构建SQL查询语句
        sql = f"""
        SELECT {select_fields}
        FROM sales_data
        WHERE 公司名称 IN ({company_placeholders})
        """

        # 组合参数
        params = company_names

        print("=" * 60)
        print("正在执行销售数据提取...")
        print(f"筛选条件:")
        print(f"  公司名称: {company_names}")
        print("=" * 60)

        try:
            # 连接到销售数据库
            if not self.connect('sales'):
                print("销售数据库连接失败，请检查配置")
                return pd.DataFrame()

            # 测试连接
            if not self.test_connection('sales'):
                print("销售数据库连接测试失败")
                self.disconnect('sales')
                return pd.DataFrame()

            # 执行查询
            self.sales_cursor.execute(sql, params)
            data = self.sales_cursor.fetchall()

            if not data:
                print("未找到符合条件的数据")
                self.disconnect('sales')
                return pd.DataFrame()

            # 转换为DataFrame
            df = pd.DataFrame(data)

            # 输出提取结果摘要
            print(f"成功提取 {len(df)} 条记录")

            # 关闭销售数据库连接
            self.disconnect('sales')

            return df

        except MySQLError as e:
            print(f"执行SQL查询时出错: {e}")
            self.disconnect('sales')
            return pd.DataFrame()
        except Exception as e:
            print(f"提取销售数据时发生未知错误: {e}")
            self.disconnect('sales')
            return pd.DataFrame()

    def execute_sales_data_pipeline(self, company_names: List[str] = None) -> pd.DataFrame:
        """
        执行完整的销售数据提取流程

        Args:
            company_names: 公司名称列表，如果为None则使用默认列表

        Returns:
            pd.DataFrame: 提取的销售数据DataFrame
        """
        try:
            print("开始执行销售数据提取流程...")

            # 提取销售数据
            df = self.extract_sales_data(company_names)

            if df.empty:
                print("未提取到符合条件的销售数据")
                return df

            return df

        except Exception as e:
            print(f"执行销售数据流程时出错: {e}")
            return pd.DataFrame()

    def execute_full_pipeline(self) -> pd.DataFrame:
        """
        执行完整的装饰订单数据提取流程

        Returns:
            pd.DataFrame: 提取的数据DataFrame
        """
        try:
            print("开始执行装饰订单数据提取流程...")

            # 1. 连接数据库
            if not self.connect():
                print("数据库连接失败，请检查配置")
                return pd.DataFrame()

            # 2. 测试连接
            if not self.test_connection():
                print("数据库连接测试失败")
                self.disconnect()
                return pd.DataFrame()

            # 3. 获取表信息
            table_info = self.get_table_info()
            if table_info:
                print(f"表总记录数: {table_info.get('total_records', 0):,}")

            # 4. 提取数据
            df = self.extract_data()

            if df.empty:
                print("未提取到符合条件的数据")
                self.disconnect()
                return df

            # 5. 关闭连接
            self.disconnect()

            return df

        except Exception as e:
            print(f"执行完整流程时出错: {e}")
            self.disconnect()
            return pd.DataFrame()

    def process_sales_data(self):
        """
        处理装饰数据的方法

        参数:
        df: pandas DataFrame，包含销售数据

        返回:
        result_df: 处理后的分组统计结果
        """
        df = self.execute_full_pipeline()
        # 1. 筛选物资状态不等于[全退货、全退款、待退货]的数据
        exclude_status = ['已换货','已退货','已退款']
        df_filtered = df[~df['物资状态'].isin(exclude_status)].copy()
        df_filtered = df_filtered.dropna(subset=['收款日期'])
        df_filtered_csv = df_filtered.copy()

        # 2. 判断是否包含"膜"的关键字并计算新列
        df_filtered_csv['贴膜成本'] = df_filtered_csv.apply(lambda row: row['成本合计(含税)'] if '膜' in str(row['物资名称']) else 0, axis=1)
        df_filtered_csv['贴膜合计收款金额'] = df_filtered_csv.apply(lambda row: row['合计收款金额'] if '膜' in str(row['物资名称']) else 0, axis=1)
        df_filtered_csv['其他成本'] = df_filtered_csv.apply(lambda row: row['成本合计(含税)'] if '膜' not in str(row['物资名称']) else 0, axis=1)
        df_filtered_csv['其他合计收款金额'] = df_filtered_csv.apply(lambda row: row['合计收款金额'] if '膜' not in str(row['物资名称']) else 0, axis=1)
        df_filtered_csv['龙膜成本'] = df_filtered_csv.apply(lambda row: row['成本合计(含税)'] if '龙膜' in str(row['物资名称']) else 0, axis=1)
        df_filtered_csv['龙膜收款金额'] = df_filtered_csv.apply(lambda row: row['合计收款金额'] if '龙膜' in str(row['物资名称']) else 0, axis=1)

        # 3. 按【车架号】分组并聚合数据
        grouped = df_filtered_csv.groupby('车架号').agg({
            '销售顾问': 'first',
            '新车销售店名': 'first',
            '收款日期': 'first',
            '物资名称': lambda x: '，'.join(x.dropna().astype(str)),
            '成本合计(含税)': 'sum',
            '合计收款金额': 'sum',
            '贴膜成本': 'sum',
            '贴膜合计收款金额': 'sum',
            '其他成本': 'sum',
            '其他合计收款金额': 'sum',
            '龙膜成本': 'sum',
            '龙膜收款金额': 'sum',
        }).reset_index()

        # 4. 按指定顺序排列列
        grouped_df = grouped[['车架号', '收款日期', '销售顾问', '新车销售店名', '物资名称','成本合计(含税)', '合计收款金额', '贴膜成本', '贴膜合计收款金额','其他成本', '其他合计收款金额', '龙膜成本', '龙膜收款金额']].rename(columns={'收款日期':'到店日期'})

        # 显示结果
        # grouped_df.to_csv("装饰合并.csv")

        print(f"原始数据有 {len(df)} 行，合并后有 {len(grouped_df)} 行")

        return grouped_df


# 处理销售表、推送表和到店表，装饰表,以及成本表生成符合要求的汇总表
def merge_and_process_data(sales_df, push_df, zhaungshi_df, chengben_df):
    """
    处理销售表、推送表和到店表，装饰表,以及成本表生成符合要求的汇总表

    参数:
    sales_df: 销售表DataFrame
    push_df: 推送表DataFrame

    返回:
    处理后的DataFrame
    """

    # 1. 预处理推送表：去重，保留最新的一条
    if '推送日期' in push_df.columns:
        # 将推送日期转换为datetime格式，确保正确排序
        push_df['推送日期'] = pd.to_datetime(push_df['推送日期'], errors='coerce')
        # 按车架号分组，保留最新的一条（推送日期最大）
        push_df = push_df.drop_duplicates(subset=['车架号'], keep='first')
    else:
        # 如果没有推送日期，直接去重
        push_df = push_df.drop_duplicates(subset=['车架号'], keep='first')
    push_df['车架号后6位'] = push_df['车架号'].astype(str).str[-6:]
    push_df = push_df.drop_duplicates(subset=['车架号后6位'], keep='first')



    # 3. 为销售表添加车架号后6位列，用于匹配到店表
    sales_df = sales_df.copy()
    sales_df = sales_df[sales_df["车架号"]!="二手车返利"].drop_duplicates(subset=['车架号'], keep='first')
    sales_df['车架号后6位'] = sales_df['车架号'].astype(str).str[-6:]
    sales_df.to_csv("销售数据表.csv")

    # 4. 以销售表为主表，左连接推送表
    push_df.to_csv("推送表.csv")
    merged_df = pd.merge(
        sales_df,
        push_df[['车架号后6位', '推送日期', '返佣']],  # 只保留需要的列
        on='车架号后6位',
        how='left'
    )
    merged_df.to_csv("推送合并表.csv")


    # 5. 以销售表为主表，左连接cyy装饰表
    merged_df = pd.merge(
        merged_df,
        zhaungshi_df,
        on='车架号',
        how='left'
    )


    # 6. 以销售表为主表，左连接成本表
    # 从merged_df的销售日期中提取年份和月份
    merged_df['年份'] = merged_df['到店日期'].dt.year if hasattr(merged_df['到店日期'], 'dt') else pd.to_datetime(merged_df['到店日期']).dt.year
    merged_df['月份'] = merged_df['到店日期'].dt.month if hasattr(merged_df['到店日期'], 'dt') else pd.to_datetime(merged_df['到店日期']).dt.month

    # 使用提取的年份和月份进行合并成本
    merged_df = pd.merge(
        merged_df,
        chengben_df,
        on=['公司名称', '年份', '月份'],
        how='left'
    )
    # 创建一个标记，标记每个公司每个月的重复行
    mask = merged_df.duplicated(subset=['公司名称', '年份', '月份'], keep='first')
    # 将重复行的总成本设为0
    merged_df.loc[mask, '总成本'] = 0
    # 删除临时添加的年份和月份列
    merged_df = merged_df.drop(['年份', '月份'], axis=1)


    # 7. 新建列【是否赠送膜】
    merged_df['是否赠送膜'] = merged_df['赠送装饰项目'].fillna('').apply(
        lambda x: '是' if '膜' in str(x) else '否'
    )

    # 8. 新建列【是否推送】
    merged_df['是否推送'] = merged_df['推送日期'].apply(
        lambda x: '是' if pd.notnull(x) else '否'
    )

    # 9. 新建列【是否到店】
    merged_df['是否到店'] = merged_df['到店日期'].apply(
        lambda x: '是' if pd.notnull(x) else '否'
    )


    # 10. 新建列【是否免费贴膜】
    def is_free_film(row):
        # 首先检查是否有到店日期
        if pd.isnull(row['到店日期']):
            return '未到店'  # 没有到店

        # 然后检查贴膜合计收款金额是否为空或为0
        film_amount = row['贴膜合计收款金额']
        if pd.isnull(film_amount):
            return '是'  # 有到店记录，膜升级金额为空，算免费贴膜
        try:
            # 尝试转换为数值
            film_amount_num = float(film_amount)
            return '是' if film_amount_num == 0 else '否'
        except (ValueError, TypeError):
            # 如果无法转换为数值，检查是否为字符串"0"
            if str(film_amount).strip() == '0' or str(film_amount).strip() == '':
                return '是'
            else:
                return '否'

    merged_df['是否免费贴膜'] = merged_df.apply(is_free_film, axis=1)

    # 10. 新建列【是否其他升级_重复】
    def is_other_upgrade(row):
        # 检查其它项目金额是否有值且大于0
        if pd.notnull(row['其他合计收款金额']):
            try:
                if float(row['其他合计收款金额']) > 0:
                    return '是'
                else:
                    return '否'
            except:
                return '否'
        else:
            return '否'

    merged_df['是否其他升级_重复'] = merged_df.apply(is_other_upgrade, axis=1)

    # 11. 新建列【是否其他升级_不重复】
    def is_other_upgrade_non_repeat(row):
        # 检查其它项目金额是否有值且大于0，并且膜升级金额为空或为0
        other_condition = False
        film_condition = False

        # 检查其它项目金额
        if pd.notnull(row['其他合计收款金额']):
            try:
                if float(row['其他合计收款金额']) > 0:
                    other_condition = True
            except:
                pass

        # 检查膜升级金额
        if pd.isnull(row['贴膜合计收款金额']) or row['贴膜合计收款金额'] == 0:
            film_condition = True
        else:
            try:
                if float(row['贴膜合计收款金额']) == 0:
                    film_condition = True
            except:
                pass

        return '是' if (other_condition and film_condition) else '否'

    merged_df['是否其他升级_不重复'] = merged_df.apply(is_other_upgrade_non_repeat, axis=1)

    # 12. 新建列【近3月是否到店】
    def calculate_3month_checkin(row):
        # 如果到店日期为空，直接返回"否"
        if pd.isna(row['到店日期']):
            return "否"

        # 计算时间差（天数）
        time_diff = (row['到店日期'] - row['推送日期']).days

        # 判断是否在90天内
        if 0 <= time_diff <= 90:
            return "是"
        else:
            return "否"

    # 13. 新建列【是否龙膜赠送】
    merged_df['推送日期'] = pd.to_datetime(merged_df['推送日期'], errors='coerce')
    merged_df['到店日期'] = pd.to_datetime(merged_df['到店日期'], errors='coerce')
    merged_df['近3月是否到店'] = merged_df.apply(calculate_3month_checkin, axis=1)

    def check_longmo(row):
        # 处理NaN值
        if pd.isna(row['赠送装饰项目']):
            return '否'

        # 检查是否包含"龙膜"
        if '龙膜' in str(row['赠送装饰项目']):
            return '是'
        else:
            return '否'

    # 使用方法2
    merged_df['是否龙膜赠送'] = merged_df.apply(check_longmo, axis=1)

    # 14. 重新排列列的顺序，使输出更清晰
    column_order = ['销售日期', '推送日期', '到店日期', '公司名称', '车架号', '车架号后6位', '销售人员', '交付专员', '赠送装饰项目',
                    '销售顾问', '新车销售店名', '物资名称', '成本合计(含税)', '合计收款金额', '贴膜成本', '贴膜合计收款金额', '其他成本', '其他合计收款金额', '龙膜成本', '龙膜收款金额','返佣','总成本',
                    '是否赠送膜', '是否推送', '是否到店', '是否免费贴膜', '是否其他升级_重复', '是否其他升级_不重复', '近3月是否到店', '是否龙膜赠送']

    # 只保留存在的列
    column_order = [col for col in column_order if col in merged_df.columns]

    merged_df = merged_df[column_order]
    merged_df.to_csv("E:/powerbi_data/看板数据/dashboard/5家店贴膜升级单独处理.csv", index=False)

    return merged_df


# 执行主函数
if __name__ == "__main__":
    # cyy数据
    extractor = DecorationOrdersExtractor()
    # 装饰订单数据
    zhaungshi_df = extractor.process_sales_data()
    # 销售数据
    sales_df = extractor.execute_sales_data_pipeline()

    # syy数据
    analyzer = FilmUpgradeAnalyzer()
    # 成本数据
    chengben = analyzer.extract_store_cost_data()
    # 推送数据
    df_subset = analyzer.process_push_data()

    # 合并销售人员的数据
    result_df_data = merge_and_process_data(sales_df, df_subset, zhaungshi_df, chengben)
