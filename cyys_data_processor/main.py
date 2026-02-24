# -*- coding: utf-8 -*-
"""
主程序入口
"""
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 100)
project_root = r"E:\powerbi_data"
sys.path.insert(0, project_root)
from config.cyys_data_processor.config import LOG_DIR
from utils import DataUtils
from database import DatabaseManager
from data_loader import DataLoader
from data_processor import DataProcessor
from data_writer import DataWriter


class CyysDataProcessorApp:
    """车易云商数据处理应用主类"""

    def __init__(self):
        # 初始化日志
        self.logger = DataUtils.init_logger(LOG_DIR)

        # 初始化数据库管理器
        self.db_manager = DatabaseManager()

        # 初始化数据加载器
        self.data_loader = DataLoader(self.db_manager)

        # 获取外部数据
        external_data = self.data_loader.get_external_data()

        # 初始化数据处理器
        self.data_processor = DataProcessor(external_data['vat'])

        # 初始化数据写入器
        self.data_writer = DataWriter(self.db_manager)

        # 存储处理过程中的数据
        self.raw_data = {}
        self.processed_data = {}

    def backup_to_excel_simple(self, mysql_data, output_path=None):
        """
        将数据表简单备份到Excel文件

        参数:
            mysql_data: 包含DataFrame的字典
            output_path: 输出Excel文件路径
        """

        # 设置默认输出路径
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"数据库备份_{timestamp}.xlsx"

        try:
            # 使用ExcelWriter将多个DataFrame写入同一个Excel的不同sheet
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet_name, df in mysql_data.items():
                    if df is not None and not df.empty:
                        # 确保sheet名称不超过31个字符（Excel限制）
                        safe_sheet_name = sheet_name[:31]
                        df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    else:
                        print(f"注意: '{sheet_name}' 表数据为空，跳过")

            print(f"备份完成！文件已保存到: {output_path}")
            return output_path

        except Exception as e:
            print(f"备份失败: {str(e)}")
            return None

    def run(self):
        """主流程：数据读取→清洗→计算→写入输出库→导出到MongoDB"""
        self.logger.info("=" * 50)
        self.logger.info("车易云商数据处理流程启动（优化版）")
        self.logger.info("=" * 50)

        try:
            # 1. 连接数据库
            self.db_manager.connect()

            # 2. 加载外部配置
            external_data = self.data_loader.get_external_data()
            service_net = external_data['service_net']
            company_belongs = external_data['company_belongs']

            # 3. 加载原始数据
            self.logger.info("开始从数据库加载数据...")
            raw_data = self.data_loader.load_all_data()
            self.logger.info(f"数据加载完成：共{len(raw_data)}个数据表")

            # 5. 各子表清洗
            self.logger.info("开始数据清洗...")

            # 保险数据清洗
            df_insurance = self.data_processor.clean_insurance(raw_data["保险业务"])

            # 二手车数据清洗
            df_Ers = self.data_processor.clean_used_cars(pd.concat([raw_data["二手车成交"], raw_data["二手车入库"]], ignore_index=True))

            # 装饰订单数据清洗
            raw_data["装饰订单"] = raw_data["装饰订单"].groupby('ID').apply(lambda x: x[x['OutId'] != 0] if (x['OutId'] != 0).any() else x).reset_index(drop=True)
            df_decoration2, df_jingpin_result = self.data_processor.clean_decoration_orders(raw_data["装饰订单"])

            # 套餐销售数据清洗
            df_service_aggregated = self.data_processor.clean_service_packages(raw_data["套餐销售"])

            # 车辆成本管理数据清洗
            df_carcost = self.data_processor.clean_vehicle_costs(raw_data["车辆成本管理"])

            # 按揭业务数据清洗
            df_loan = self.data_processor.clean_loans(raw_data["按揭业务"])

            # 汇票管理数据清洗
            df_debit = self.data_processor.clean_debit_and_merge(raw_data["汇票管理"], df_carcost)

            # 库存和计划数据清洗
            df_inventory_all, df_inventory, df_inventory1 = self.data_processor.clean_inventory_and_plan(raw_data["库存车辆查询"], raw_data["库存车辆已售"], raw_data["计划车辆"],df_debit, service_net, company_belongs)

            # 订单数据清洗
            df_dings, df_zhubo = self.data_processor.clean_book_orders(raw_data["衍生订单"], raw_data["成交订单"], raw_data["未售订单"], service_net)
            raw_data["未售订单"].to_csv("E:/powerbi_data/看板数据/dashboard/未售订单.csv")
            # 作废订单数据清洗
            tui_dings_df = self.data_processor.clean_void_orders(raw_data["作废订单"], service_net)

            # 销售明细数据清洗
            df_salesAgg = self.data_processor.clean_sales_detail(raw_data["车辆销售明细_开票日期"], service_net)

            self.logger.info("数据清洗完成")

            # 6. 主表合并
            self.logger.info("开始主表合并...")
            # 开票数据筛选
            df_kaipiao = raw_data["开票维护"][raw_data["开票维护"]['单据类别'] == "车辆销售单"]
            df_kaipiao['下载时间'] = pd.to_datetime(df_kaipiao['下载时间'], format='mixed')
            df_kaipiao = df_kaipiao.sort_values(by=['车架号', '下载时间'], ascending=[True, False])
            df_kaipiao = df_kaipiao.drop_duplicates(subset=['车架号'], keep='first')

            # 处理二手车数据
            df_Ers1, df_Ers2, df_Ers2_archive = self.data_processor.process_used_car_data(df_Ers, df_kaipiao)

            # 合并主销售表
            df_salesAgg1 = self.data_processor.merge_main_sales_table(df_salesAgg, df_zhubo, df_service_aggregated, df_carcost, df_loan, df_decoration2, df_kaipiao, df_Ers2, df_Ers2_archive)

            # 7. 应用促销逻辑
            df_salesAgg1 = self.data_processor.apply_promotion_logic(df_salesAgg1)

            # 8. 处理调拨数据
            df_diao2 = self.data_processor.handle_diaobo_merge(raw_data["调车结算"], df_salesAgg1)

            # 9. 最终整理
            self.logger.info("最终数据整理...")
            # 创建销售明细副本
            df_salesAgg_ = df_salesAgg1.copy()
            df_salesAgg_.rename(columns={
                '入库日期': '到库日期',
                '公司名称': '匹配定单归属门店',
                '订车日期': '定单日期',
                '销售人员': '销售顾问',
                '车主姓名': '客户姓名'
            }, inplace=True)

            df_salesAgg_ = df_salesAgg_[(df_salesAgg_['车架号'] != "") & (df_salesAgg_['销售日期'] != "")]
            df_salesAgg_ = df_salesAgg_[[
                '服务网络', '车架号', '车系', '车型', '车辆配置', '外饰颜色', '定金金额', '指导价',
                '提货价', '销售车价', '匹配定单归属门店', '到库日期', '定单日期', '销售日期',
                '所属团队', '销售顾问', '客户姓名', '联系电话', '联系电话2'
            ]]

            df_salesAgg_ = df_salesAgg_[(df_salesAgg_['所属团队'] != "调拨") & (df_salesAgg_['所属团队'].notna() & df_salesAgg_['所属团队'] != "")]
            df_salesAgg_ = df_salesAgg_.drop_duplicates()

            # 合并库存数据
            # 检查并删除重复列名
            if len(df_inventory.columns) != len(set(df_inventory.columns)):
                # 删除重复列
                df_inventory = df_inventory.loc[:, ~df_inventory.columns.duplicated()]

            if len(df_inventory1.columns) != len(set(df_inventory1.columns)):
                # 删除重复列
                df_inventory1 = df_inventory1.loc[:, ~df_inventory1.columns.duplicated()]

            df_inventory0_1 = pd.concat([df_inventory, df_inventory1], axis=0, ignore_index=True)

            # 最终整理和导出
            (df_salesAgg_combined, df_dings, df_inventory_all, tui_dings_df, df_debit, df_salesAgg_, df_jingpin_result, df_inventory1) = self.data_processor.finalize_and_export(
                df_salesAgg1, df_dings, df_inventory_all, tui_dings_df, df_debit,df_salesAgg_, df_jingpin_result, df_inventory1, df_Ers1, df_diao2, df_inventory0_1)

            self.logger.info("数据处理完成")

            # 10. 准备写入MySQL的数据
            mysql_data = {
                'sales_data': df_salesAgg_combined.drop_duplicates(),
                'order_data': df_dings.drop_duplicates(),
                'inventory_data': df_inventory_all[(df_inventory_all['开票日期'].isna()) | (df_inventory_all['开票日期'] == "")],
                'tuiding_data': tui_dings_df.drop_duplicates(),
                'debit_data': df_debit.drop_duplicates(),
                'sales_invoice_data': df_salesAgg_.drop_duplicates(),
                'jingpin_data': df_jingpin_result.drop_duplicates(),
                'sold_inventory': df_inventory1.drop_duplicates()
            }

            # 检查并清理所有DataFrame的重复列名
            for table_name, df in mysql_data.items():
                if df is not None and not df.empty:
                    # 检查重复列名
                    if len(df.columns) != len(set(df.columns)):
                        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
                        self.logger.warning(f"表[{table_name}]存在重复列名：{duplicate_cols}，正在清理...")
                        # 删除重复列（保留第一个）
                        df = df.loc[:, ~df.columns.duplicated()]
                        mysql_data[table_name] = df
                        self.logger.info(f"表[{table_name}]重复列名清理完成")

            # 11. 写入MySQL
            self.data_writer.write_to_mysql(mysql_data)
            # self.backup_to_excel_simple(mysql_data, r"E:/WXWork/1688858189749305/WeDrive/成都永乐盛世/维护文件/cyy.xlsx")

            # 12. 准备MongoDB数据并导出
            self.logger.info("准备MongoDB数据...")
            df_salesAgg_mongo, df_jingpin_result_mongo, df_diao_mongo = self.data_writer.prepare_mongodb_data(df_salesAgg_combined, df_jingpin_result)
            df_salesAgg_mongo['订车日期'] = pd.to_datetime(df_salesAgg_mongo['订车日期'], errors='coerce',format='mixed').dt.strftime('%Y/%m/%d')
            df_salesAgg_mongo['开票日期'] = pd.to_datetime(df_salesAgg_mongo['开票日期'], errors='coerce',format='mixed').dt.strftime('%Y/%m/%d')
            df_salesAgg_mongo['收款日期'] = pd.to_datetime(df_salesAgg_mongo['收款日期'], errors='coerce',format='mixed').dt.strftime('%Y/%m/%d')
            df_jingpin_result_mongo['最早收款日期'] = pd.to_datetime(df_jingpin_result_mongo['最早收款日期'], errors='coerce',format='mixed').dt.strftime('%Y/%m/%d')
            df_diao_mongo['订车日期'] = pd.to_datetime(df_diao_mongo['订车日期'], errors='coerce',format='mixed').dt.strftime('%Y/%m/%d')
            df_diao_mongo['开票日期'] = pd.to_datetime(df_diao_mongo['开票日期'], errors='coerce',format='mixed').dt.strftime('%Y/%m/%d')
            df_salesAgg_mongo = df_salesAgg_mongo.replace({'nan': None, np.nan: None, 'NaN': None, 'NAN': None})
            df_salesAgg_mongo = df_salesAgg_mongo.drop_duplicates()
            df_jingpin_result_mongo = df_jingpin_result_mongo.replace({'nan': None, np.nan: None, 'NaN': None, 'NAN': None})
            df_diao_mongo = df_diao_mongo.replace({'nan': None, np.nan: None, 'NaN': None, 'NAN': None})
            # df_salesAgg_mongo.to_csv(r"E:/WXWork/1688858189749305/WeDrive/成都永乐盛世/维护文件/车易云毛利润表.csv")
            # 13. 导出到MongoDB
            self.data_writer.export_to_mongodb(
                df_salesAgg_mongo,
                df_jingpin_result_mongo,
                df_diao_mongo
            )

            self.logger.info("=" * 50)
            self.logger.info("车易云商数据处理流程全部完成！")
            self.logger.info("=" * 50)

        except Exception as e:
            self.logger.error(f"数据处理过程中发生错误：{str(e)}")
            raise
        finally:
            # 关闭数据库连接
            self.db_manager.close()



if __name__ == "__main__":
    app = CyysDataProcessorApp()
    app.run()