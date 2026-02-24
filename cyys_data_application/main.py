"""
主程序 - 数据清洗流程（清洗后数据存入cyy_app_data数据库）
"""
import pandas as pd
from config.cyys_data_application.config import SOURCE_DB_URL, APP_DB_URL, SOURCE_TABLES, APP_TABLES
from db_connector import DatabaseConnector
from data_processor import DataProcessor
from datetime import datetime

def main():
    """主函数 - 全量清洗"""
    print(f"[{datetime.now()}] 开始数据清洗流程")
    print(f"[{datetime.now()}] 源数据库: {SOURCE_DB_URL.split('/')[-1].split('?')[0]}")
    print(f"[{datetime.now()}] 目标数据库: {APP_DB_URL.split('/')[-1].split('?')[0]}")
    print("="*60)

    # 1. 初始化
    db = DatabaseConnector(SOURCE_DB_URL, APP_DB_URL)

    # 2. 连接数据库
    if not db.connect():
        print(f"[{datetime.now()}] 无法连接数据库，程序退出")
        return

    try:
        # 3. 初始化处理器
        processor = DataProcessor()

        # 5. 从源数据库加载原始数据
        print(f"[{datetime.now()}] 从源数据库加载原始数据...")
        raw_sales = db.load_source_data(SOURCE_TABLES['sales'])
        raw_inventory = db.load_source_data(SOURCE_TABLES['inventory'])

        # 6. 清洗数据
        print(f"[{datetime.now()}] 清洗数据...")
        clean_sales = processor.clean_sales_data(raw_sales)
        clean_inventory = processor.clean_inventory_data(raw_inventory)

        # 7. 保存清洗后的数据到应用数据库
        print(f"[{datetime.now()}] 保存清洗后的数据到应用数据库...")
        if not clean_sales.empty:
            db.save_app_data(clean_sales, APP_TABLES['sales'])

        if not clean_inventory.empty:
            db.save_app_data(clean_inventory, APP_TABLES['inventory'])

        # 8. 生成摘要报告
        processor.generate_summary(clean_sales, clean_inventory)

        print(f"[{datetime.now()}] 数据清洗流程完成!")
        print("="*60)

    except Exception as e:
        print(f"[{datetime.now()}] 处理过程中出错: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # 9. 关闭数据库连接
        db.close()

def incremental_update(date_from=None):
    """增量更新模式"""
    print(f"[{datetime.now()}] 开始增量更新")
    print(f"[{datetime.now()}] 目标数据库: {APP_DB_URL.split('/')[-1].split('?')[0]}")

    db = DatabaseConnector(SOURCE_DB_URL, APP_DB_URL)

    if not db.connect():
        return

    try:
        # 初始化处理器
        processor = DataProcessor()

        # 检查并创建应用表
        db.create_app_tables()

        # 增量条件
        if date_from:
            sales_where = f"销售日期 >= '{date_from}'"
            inventory_where = f"到库日期 >= '{date_from}'"
        else:
            # 默认更新最近7天的数据
            sales_where = "销售日期 >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"
            inventory_where = "到库日期 >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"

        # 从源数据库加载增量数据
        raw_sales = db.load_source_data(SOURCE_TABLES['sales'], sales_where)
        raw_inventory = db.load_source_data(SOURCE_TABLES['inventory'], inventory_where)

        # 清洗数据
        clean_sales = processor.clean_sales_data(raw_sales)
        clean_inventory = processor.clean_inventory_data(raw_inventory)

        # 保存数据到应用数据库（追加模式）
        if not clean_sales.empty:
            db.save_app_data_append(clean_sales, APP_TABLES['sales'])

        if not clean_inventory.empty:
            db.save_app_data_append(clean_inventory, APP_TABLES['inventory'])

        # 生成摘要
        processor.generate_summary(clean_sales, clean_inventory)

        print(f"[{datetime.now()}] 增量更新完成")

    except Exception as e:
        print(f"[{datetime.now()}] 增量更新出错: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db.close()

def test_connection():
    """测试数据库连接"""
    print(f"[{datetime.now()}] 测试数据库连接...")

    db = DatabaseConnector(SOURCE_DB_URL, APP_DB_URL)

    if db.connect():
        print(f"[{datetime.now()}] 数据库连接测试成功")

        # 测试源数据库
        try:
            test_sql = "SELECT 1 as test"
            result = pd.read_sql(test_sql, db.source_conn)
            print(f"[{datetime.now()}] 源数据库连接正常")
        except Exception as e:
            print(f"[{datetime.now()}] 源数据库测试失败: {e}")

        # 测试应用数据库
        try:
            test_sql = "SELECT 1 as test"
            result = pd.read_sql(test_sql, db.app_conn)
            print(f"[{datetime.now()}] 应用数据库连接正常")
        except Exception as e:
            print(f"[{datetime.now()}] 应用数据库测试失败: {e}")

        db.close()
    else:
        print(f"[{datetime.now()}] 数据库连接测试失败")

if __name__ == "__main__":
    import sys

    # 支持命令行参数
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "incremental":
            date_from = sys.argv[2] if len(sys.argv) > 2 else None
            incremental_update(date_from)

        elif command == "test":
            test_connection()

        elif command == "help":
            print("""
                用法:
                  python main.py [command]
                  
                命令:
                  (无参数)    - 执行全量数据清洗
                  incremental [date] - 执行增量数据清洗，可选指定起始日期
                  test        - 测试数据库连接
                  help        - 显示此帮助信息
                  
                示例:
                  python main.py                    # 全量清洗
                  python main.py incremental        # 增量清洗最近7天数据
                  python main.py incremental 2025-01-01  # 增量清洗从指定日期开始
                  python main.py test              # 测试数据库连接
                            """)

        else:
            print(f"[{datetime.now()}] 未知命令: {command}")
            print("使用 'python main.py help' 查看帮助")

    else:
        # 无参数，执行全量清洗
        main()