import datetime
import os
import re
import shutil
import logging
import time
import csv
import io  # 只保留这个，移除 from io import StringIO
from webdav3.client import Client
from urllib.parse import unquote
import tempfile

# 日志配置 (保持不变)
log_dir = r"E:/powerbi_data/代码执行/data/私有云日志/上传私有云"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(message)s]',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{log_dir}/log_{datetime.datetime.now().strftime('%Y_%m_%d')}.log", encoding='utf-8')
    ]
)


def simple_upload(content, save_dir, filename):
    """
    增强版文件上传方法

    :param content: 要写入的二进制内容
    :param save_dir: 文件保存目录（自动创建）
    :param filename: 目标文件名（包含扩展名）
    :return: 上传是否成功
    """
    client = Client({
        "webdav_hostname": "http://222.212.88.126:5005",
        "webdav_login": "wangdie",
        "webdav_password": "wd#123456",
        "verify_ssl": False,
        "session_options": {
            "pool_connections": 5,
            "pool_maxsize": 5,
            "max_retries": 3
        },
        "disable_check": True
    })

    try:
        # 创建保存目录（如果不存在）
        os.makedirs(save_dir, exist_ok=True)

        # 生成完整保存路径
        save_dir = save_dir.strip().replace('\\', '/').rstrip('/')
        filename = filename.replace('\\', '/').lstrip('/')  # 防止 filename 含路径
        save_path = f"{save_dir}/{filename}"

        # 创建并写入临时文件
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(content)
            tmp_path = tmp_file.name

        # 执行上传操作
        client.upload(save_path, tmp_path)
        logging.info(f"[ 成功 ] 文件已保存至：{save_path}")  # 修改此处
        return True
    except Exception as e:
        logging.error(f"[ 失败 ] 上传错误：{unquote(str(e))}, {filename}")  # 修改此处
        return False

    finally:
        # 清理临时文件
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
                logging.info(f"[ 清理 ] 临时文件已删除{filename}")  # 修改此处
            except Exception as clean_e:
                logging.error(f"[ 警告 ] 清理失败：{str(clean_e)}")  # 修改此处


# ========== 主要逻辑开始 ==========

# 1. 定义你要上传的本地文件列表 (请确保这些路径存在)
local_files = [
    # r'E:\powerbi_data\看板数据\dashboard\merged_nps_data.xlsx',
    # r'E:\powerbi_data\看板数据\dashboard\merged_quality_data.xlsx',
    # r'E:\powerbi_data\看板数据\dashboard\merged_sales_data.xlsx',
    # r'E:\powerbi_data\看板数据\dashboard\merged_tiche_data.xlsx',
    # r'E:\powerbi_data\看板数据\dashboard\merged_wes_data.xlsx',
    # r'E:\powerbi_data\看板数据\dashboard\三方台账1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\二手车1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\保赔无忧.csv',
    # r'E:\powerbi_data\看板数据\dashboard\全赔无忧.csv',
    # r'E:\powerbi_data\看板数据\dashboard\定车1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\市场费用.csv',
    # r'E:\powerbi_data\看板数据\dashboard\库存1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\库存存档.csv',
    # r'E:\powerbi_data\看板数据\dashboard\所有定单1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\所有车系.csv',
    # r'E:\powerbi_data\看板数据\dashboard\投放费用.csv',
    # r'E:\powerbi_data\看板数据\dashboard\新车三方延保台账.csv',
    # r'E:\powerbi_data\看板数据\dashboard\新车保险台账.csv',
    # r'E:\powerbi_data\看板数据\dashboard\未售锁车.csv',
    # r'E:\powerbi_data\看板数据\dashboard\特殊费用收入.csv',
    # r'E:\powerbi_data\看板数据\dashboard\精品销售1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\贴膜升级.csv',
    # r'E:\powerbi_data\看板数据\dashboard\贴膜升级1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\辅助_销售顾问.csv',
    # r'E:\powerbi_data\看板数据\dashboard\退订1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\销售1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\销售毛利1.csv',
    # r'E:\powerbi_data\看板数据\dashboard\销售薪资.csv',
    r"E:/powerbi_data/看板数据/dashboard/5家店贴膜升级单独处理.csv",
]

# 2. 定义 WebDAV 上的目标目录 (请替换为实际的 WebDAV 路径)
# 重要！这里不能使用 Z:\，必须使用 WebDAV 服务器上的相对路径。
# 例如，如果 Z:\ 映射到 WebDAV 根目录 /，则路径如下：
webdav_target_dir = "/信息部/信息部内部文件/售前看板数据源"

# 3. 遍历文件列表，逐个上传
for local_file_path in local_files:
    try:
        # 检查文件是否存在
        if not os.path.exists(local_file_path):
            logging.warning(f"[ 跳过 ] 本地文件不存在：{local_file_path}")
            continue

        # 读取文件内容 (以二进制模式)
        with open(local_file_path, 'rb') as f:
            file_content = f.read()

        # 获取文件名
        filename = os.path.basename(local_file_path)

        # 调用上传函数
        success = simple_upload(file_content, webdav_target_dir, filename)

        if success:
            logging.info(f"[ 完成 ] 成功上传：{filename}")
        else:
            logging.error(f"[ 失败 ] 上传失败：{filename}")

    except Exception as e:
        logging.error(f"[ 异常 ] 处理文件 {local_file_path} 时出错：{str(e)}")

logging.info("所有文件上传任务已完成。")