# -*- coding: utf-8 -*-
"""
主程序入口
协调所有模块，执行数据处理流程
"""

import logging
import warnings
import pandas as pd
from datetime import datetime

# 导入自定义模块
from config import setup_logging, FilePaths, COMPANY_LIST
from database_manager import DatabaseManager
from dashboard_generator import DashboardGenerator

# 配置日志和警告
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 100)
setup_logging()


def load_configurations():
    """
    加载所有配置
    
    返回:
        tuple: (数据库配置字典, 文件路径配置)
    """
    logging.info("开始加载配置")
    
    try:
        # 从配置文件导入数据库配置
        project_root = r"E:\powerbi_data"
        
        # 动态导入配置文件
        import sys
        sys.path.insert(0, project_root)
        
        from config.cyys_data_processor.config import SOURCE_MYSQL_CONFIG, OUTPUT_MYSQL_CONFIG
        from config.cyys_data_application.config import APP_DB_CONFIG
        
        # 创建文件路径配置
        file_paths = FilePaths()
        
        logging.info("配置加载成功")
        return SOURCE_MYSQL_CONFIG, APP_DB_CONFIG, file_paths
        
    except ImportError as e:
        logging.error(f"导入配置文件失败: {str(e)}")
        logging.error("请确保配置文件存在且路径正确")
        raise
    except Exception as e:
        logging.error(f"加载配置时发生错误: {str(e)}")
        raise


def main():
    """
    主程序流程
    
    1. 加载配置
    2. 初始化数据库连接
    3. 初始化数据处理器
    4. 生成所有仪表板数据
    5. 保存数据到文件和数据库
    """
    logging.info("=" * 60)
    logging.info("开始执行仪表板数据更新程序")
    logging.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 60)
    
    try:
        # 步骤1: 加载配置
        logging.info("步骤1: 加载配置")
        source_db_config, output_db_config, file_paths = load_configurations()
        
        # 步骤2: 初始化数据库管理器
        logging.info("步骤2: 初始化数据库管理器")
        db_manager = DatabaseManager(source_db_config, output_db_config)
        
        # 步骤3: 初始化仪表板数据生成器
        logging.info("步骤3: 初始化仪表板数据生成器")
        dashboard_gen = DashboardGenerator(db_manager, file_paths)
        
        # 步骤4: 生成所有仪表板数据
        logging.info("步骤4: 生成所有仪表板数据")
        all_data = dashboard_gen.generate_all_data()
        
        # 步骤5: 保存数据到CSV文件
        logging.info("步骤5: 保存数据到CSV文件")
        output_dir = file_paths.dashboard_dir
        dashboard_gen.save_all_data_to_csv(all_data, output_dir)
        
        # 步骤6: 保存数据到数据库
        logging.info("步骤6: 保存数据到数据库")
        dashboard_gen.save_all_data_to_database(all_data)
        
        # 步骤7: 输出统计信息
        logging.info("步骤7: 输出统计信息")
        print_statistics(all_data)
        
        logging.info("=" * 60)
        logging.info("仪表板数据更新程序执行完成")
        logging.info(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"程序执行失败: {str(e)}", exc_info=True)
        raise


def print_statistics(data_dict: dict):
    """
    打印数据统计信息
    
    Args:
        data_dict: 包含所有数据集的字典
    """
    print("\n" + "=" * 60)
    print("数据统计信息")
    print("=" * 60)
    
    for key, df in data_dict.items():
        if isinstance(df, pd.DataFrame):
            print(f"{key:30} : {len(df):8} 行, {len(df.columns):3} 列")
    
    print("=" * 60)


def run_with_validation():
    """
    带验证的运行函数
    
    执行主程序并进行基本验证
    """
    try:
        # 执行主程序
        main()
        
        # 验证输出文件
        validate_output_files()
        
    except KeyboardInterrupt:
        logging.warning("程序被用户中断")
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {str(e)}")
        raise


def validate_output_files():
    """
    验证输出文件
    
    检查输出文件是否成功创建
    """
    logging.info("开始验证输出文件")
    
    try:
        file_paths = FilePaths()
        output_dir = file_paths.dashboard_dir
        
        # 预期的输出文件列表
        expected_files = [
            '销售毛利1.csv',
            '定车1.csv',
            '库存1.csv',
            '精品销售1.csv',
            '退订1.csv',
            '所有定单1.csv',
            '三方台账1.csv',
            '销售1.csv',
            '二手车1.csv',
            '未售锁车.csv',
            '销售薪资.csv',
            '市场费用.csv',
            '所有车系.csv',
            '辅助_销售顾问.csv'
        ]
        
        # 检查每个文件
        missing_files = []
        for file_name in expected_files:
            file_path = f"{output_dir}/{file_name}"
            if not os.path.exists(file_path):
                missing_files.append(file_name)
            else:
                # 检查文件大小
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    logging.warning(f"文件为空: {file_name}")
                else:
                    logging.info(f"文件验证通过: {file_name} ({file_size} 字节)")
        
        # 输出结果
        if missing_files:
            logging.warning(f"以下文件缺失: {missing_files}")
        else:
            logging.info("所有输出文件验证通过")
            
    except Exception as e:
        logging.error(f"验证输出文件时发生错误: {str(e)}")


if __name__ == "__main__":
    """
    程序入口点
    
    直接运行此文件将执行整个数据处理流程
    """
    run_with_validation()