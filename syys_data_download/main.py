import sys
from file_processor import FileProcessor
from logger import DataCheckerLogger


def main():
    """主程序入口"""
    # 配置路径
    directories = [
        {
            "path": r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级",
            "name": "贴膜升级",
            "sheet_name": "膜升级登记表"  # 指定sheet名称
        },
        {
            "path": r"E:\powerbi_data\看板数据\私有云文件本地\投放市场费用",
            "name": "投放市场费用",
            "sheet_name": None  # 使用第一个sheet
        },
        {
            "path": r"E:\powerbi_data\看板数据\私有云文件本地\新车三方延保",
            "name": "新车三方延保",
            "sheet_name": None  # 使用第一个sheet
        }
    ]

    # 初始化日志记录器
    log_dir = r"E:\powerbi_data\data\私有云日志\check_logs"
    logger = DataCheckerLogger(log_dir=log_dir)

    try:
        # 初始化文件处理器
        processor = FileProcessor(logger)

        # 处理所有目录
        for directory in directories:
            logger.logger.info(f"开始检核目录: {directory['name']} - {directory['path']}")
            processor.process_directory(directory["path"], directory["sheet_name"])
            logger.logger.info(f"完成检核目录: {directory['name']}")

        # 输出总结
        logger.log_summary()

        # 保存错误报告
        logger.save_errors_to_excel()

        logger.logger.info("所有数据检核完成！")

    except Exception as e:
        logger.logger.error(f"程序执行失败: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())