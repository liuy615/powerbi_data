import sys
from file_processor import FileProcessor
from logger import DataCheckerLogger
from config import Config


def check_投放市场费用(processor: FileProcessor):
    """
    检核投放市场费用数据
    
    参数:
        processor: 文件处理器实例
    """
    processor.logger.logger.info("=" * 60)
    processor.logger.logger.info("开始检核: 投放市场费用")
    processor.logger.logger.info("=" * 60)
    
    # 获取检核规则
    check_rules = Config.get_check_rules_投放市场费用(processor.data_checker)
    
    processor.process_task(
        directory=r"E:\powerbi_data\看板数据\私有云文件本地\投放市场费用",
        file_filters=[".xlsx"],
        sheet_name="2025年",
        required_fields=Config.STANDARD_HEADERS_投放市场费用_2025,
        check_rules=check_rules,
        task_name="市场费用"
    )
    
    processor.logger.logger.info("完成检核: 投放市场费用\n")


def check_新车三方延保(processor: FileProcessor):
    """
    检核新车三方延保数据
    只检核延保销售日期在2025年以后的数据
    
    参数:
        processor: 文件处理器实例
    """
    processor.logger.logger.info("=" * 60)
    processor.logger.logger.info("开始检核: 新车三方延保")
    processor.logger.logger.info("=" * 60)
    
    # 获取检核规则
    check_rules = Config.get_check_rules_新车三方延保(processor.data_checker)
    
    processor.process_task(
        directory=r"E:\powerbi_data\看板数据\私有云文件本地\新车三方延保",
        file_filters=[".xlsx"],
        sheet_name=None,
        required_fields=Config.STANDARD_HEADERS_新车三方延保,
        check_rules=check_rules,
        task_name="新车三方延保",
        row_filter={
            "field": "延保销售日期",
            "condition": ">=",
            "value": "2025-01-01"
        }
    )
    
    processor.logger.logger.info("完成检核: 新车三方延保\n")


def check_自店贴膜(processor: FileProcessor):
    """
    检核自店贴膜数据
    
    参数:
        processor: 文件处理器实例
    """
    processor.logger.logger.info("=" * 60)
    processor.logger.logger.info("开始检核: 自店贴膜")
    processor.logger.logger.info("=" * 60)
    
    # 获取检核规则
    check_rules = Config.get_check_rules_自店贴膜(processor.data_checker)
    
    processor.process_task(
        directory=r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级",
        file_filters=["自店", ".xlsx"],
        sheet_name="膜升级登记表",
        required_fields=Config.STANDARD_HEADERS_自店贴膜,
        check_rules=check_rules,
        task_name="自店贴膜"
    )
    
    processor.logger.logger.info("完成检核: 自店贴膜\n")


def check_三方贴膜(processor: FileProcessor):
    """
    检核三方贴膜数据
    
    参数:
        processor: 文件处理器实例
    """
    processor.logger.logger.info("=" * 60)
    processor.logger.logger.info("开始检核: 三方贴膜")
    processor.logger.logger.info("=" * 60)
    
    # 获取检核规则
    check_rules = Config.get_check_rules_三方贴膜(processor.data_checker)
    
    processor.process_task(
        directory=r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级",
        file_filters=["三方", ".xlsx"],
        sheet_name="膜升级登记表",
        required_fields=Config.STANDARD_HEADERS_三方贴膜,
        check_rules=check_rules,
        task_name="三方贴膜"
    )
    
    processor.logger.logger.info("完成检核: 三方贴膜\n")


def check_特殊事项收入(processor: FileProcessor):
    """
    检核投放市场费用数据

    参数:
        processor: 文件处理器实例
    """
    processor.logger.logger.info("=" * 60)
    processor.logger.logger.info("开始检核: 特殊事项收入")
    processor.logger.logger.info("=" * 60)

    # 获取检核规则
    check_rules = Config.get_check_rules_特殊事项收入(processor.data_checker)

    processor.process_task(
        directory=r"E:\powerbi_data\看板数据\私有云文件本地\特殊事项收入",
        file_filters=[".xlsx"],
        sheet_name="登记表",
        required_fields=Config.STANDARD_HEADERS_特殊事项收入,
        check_rules=check_rules,
        task_name="特殊事项收入"
    )

    processor.logger.logger.info("完成检核: 投放市场费用\n")

def main():
    """主程序入口"""
    # 初始化日志记录器
    log_dir = r"E:\powerbi_data\data\私有云日志\check_logs"
    # 企业微信webhook地址（可选，如果不需要企微通知可设为None）
    wecom_webhook = None  # 示例: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
    logger = DataCheckerLogger(log_dir=log_dir, wecom_webhook=wecom_webhook)

    try:
        # 初始化文件处理器
        processor = FileProcessor(logger)

        logger.logger.info("\n" + "=" * 60)
        logger.logger.info("数据检核程序启动")
        logger.logger.info("=" * 60 + "\n")

        # 依次执行各个检核任务
        # check_投放市场费用(processor)
        # check_新车三方延保(processor)
        check_自店贴膜(processor)
        # check_三方贴膜(processor)
        # check_特殊事项收入(processor)

        # 输出总结
        logger.logger.info("\n" + "=" * 60)
        logger.logger.info("所有检核任务完成，生成总结报告")
        logger.logger.info("=" * 60 + "\n")
        logger.log_summary()

        # 保存错误报告
        logger.save_errors_to_excel()

        # 发送企微通知
        logger.send_wecom_notification()

        logger.logger.info("\n" + "=" * 60)
        logger.logger.info("数据检核程序结束")
        logger.logger.info("=" * 60)

    except Exception as e:
        logger.logger.error(f"程序执行失败: {str(e)}")
        import traceback
        logger.logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())