from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import numpy as np
import pandas as pd

def read_excel_files(directory, sheet_name):
    """
    读取指定目录下的 Excel 文件，将指定的 sheet 读取为 DataFrame 列表。
    """
    dfs = []
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        print(f"正在读取文件 {filename}...")
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
            df['From'] = os.path.basename(file_path).split('.')[0]
            df.columns = df.columns.str.replace('\n', '')
            dfs.append(df)
            print(f"文件 {filename} 读取成功，一共 {len(df)} 行。")
        except Exception as e:
            print(f"读取 {filename} 时发生错误: {e}")
    return dfs

def process_directory_files(directory, sheet_name):  # 修改函数名
    """
    处理单个目录，返回该目录下所有 Excel 文件的 DataFrame 合并结果。
    """
    dfs = read_excel_files(directory, sheet_name)
    if dfs:
        return pd.concat(dfs, axis=0)
    else:
        return None

def TMSJ():
    directories = [
        r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级",
    ]

    all_dfs = []
    sheet_name = '膜升级登记表'

    # 使用线程池并行处理不同目录
    with ThreadPoolExecutor() as executor:
        future_to_directory = {executor.submit(process_directory_files, directory, sheet_name): directory for directory in directories}
        for future in as_completed(future_to_directory):
            directory = future_to_directory[future]
            try:
                df_combined = future.result()
                if df_combined is not None:
                    all_dfs.append(df_combined)
            except Exception as e:
                print(f"处理目录 {directory} 时发生错误: {e}")

    if all_dfs:
        df_final = pd.concat(all_dfs, axis=0)
        df_final = df_final[df_final['到店日期'].notna()]
        print(df_final.head())
    else:
        df_final = pd.DataFrame()
    return df_final

def process_single_file(file_path, sheet_name):  # 修改函数名
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df
    except Exception as e:
        print(f"读取文件 {file_path} 时发生错误: {e}")
        return None

def TMSJ1():
    directories = [
        r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\方程豹-乐山上元曦和-贴膜升级登记表-最新年.xlsx",
        r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\腾势-乐山上元臻智-贴膜升级登记表-最新年.xlsx",
        r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\方程豹-泸州上元坤灵-贴膜升级登记表-最新年.xlsx",
        r"E:\powerbi_data\看板数据\私有云文件本地\贴膜升级\两网-总部-贴膜升级登记表-最新年.xlsx",
    ]

    all_dfs = []
    sheet_name = '汇总表'

    for directory in directories:
        df_combined = process_single_file(directory, sheet_name)  # 调用新函数
        if df_combined is not None:
            all_dfs.append(df_combined)

    if all_dfs:
        df_final = pd.concat(all_dfs, axis=0,join='outer')
        df_final = df_final.dropna(subset=['新车销售店名'], how='all')
        df_final['新车销售店名'] = df_final['新车销售店名'].replace('文景初治', '上元盛世')
    else:
        df_final = pd.DataFrame()
    return df_final

# 执行代码
df_TMSJ1 = TMSJ1()
df_TMSJ = TMSJ()
if not df_TMSJ.empty:
    df_TMSJ = df_TMSJ[['月份', '到店日期', '新车销售店名', '车型', '车架号（后6位）', '客户姓名', '是否送龙膜/高等级膜',
                       '是否有满意度风险', '是否有效客户', '是否收劵', '膜升级金额', '其它施工项目', '其它项目金额', '合计升级金额',
                       '合作三方公司名称', '备注', '精品顾问', '是否算到店量', '是否代办', '是否不推膜', '膜升级具体内容', '膜升级成本', '膜升级毛利润', '其他项升级成本',
                       '其他项升级毛利润', '合计升级毛利润','三方返还佣金','赠送装饰', 'From']]
    # 定义符合条件的新车销售店名列表
    target_stores = ['上元臻智', '上元臻享', '上元臻盛', '上元坤灵', '上元曦和', '上元弘川']

    # 定义一个函数来计算返利金额
    def calculate_rebate(row):
        store_name = row['新车销售店名']
        vehicle_model = row['车型']
        gift_detail = row['赠送装饰']

        # 检查新车销售店名是否在目标列表中
        if store_name in target_stores:
            if '腾势' in vehicle_model:
                rebate = 1000
            elif vehicle_model == '钛3':
                rebate = 400
            elif '豹5' in vehicle_model or '豹8' in vehicle_model:
                rebate = 800
            else:
                rebate = 0
            # 检查赠送明细是否不为空
            if pd.notna(gift_detail) and gift_detail.strip() != '' and rebate > 0:
                rebate = rebate - 200
            return rebate
        return 0

    # 应用函数到每一行并创建新列
    df_TMSJ['成都腾豹贴膜返利'] = df_TMSJ.apply(calculate_rebate, axis=1)
    df_TMSJ['新车销售店名'] = df_TMSJ['新车销售店名'].replace('文景初治', '上元盛世')
    df_TMSJ.to_csv(r'E:\powerbi_data\看板数据\dashboard\贴膜升级.csv', index=False)
df_TMSJ1.to_csv(r'E:\powerbi_data\看板数据\dashboard\贴膜升级1.csv', index=False)