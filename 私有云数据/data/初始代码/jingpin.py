from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import numpy as np
import pandas as pd

def read_excel_files(directory, sheet_name):
    """
    读取指定目录下的 Excel 文件，返回 DataFrame 列表。
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

def process_directory(directory, sheet_name):
    """
    处理单个目录，返回合并后的 DataFrame。
    """
    dfs = read_excel_files(directory, sheet_name)
    if dfs:
        return pd.concat(dfs, axis=0)
    else:
        return None

def jingpin():
    directories = [r"E:\powerbi_data\看板数据\私有云文件本地\精品销售"]

    all_dfs = []
    sheet_name = '精品销售台账'

    # 使用线程池并行处理不同目录
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_directory, directory, sheet_name) for directory in directories]
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_dfs.append(result)

    if all_dfs:
        df_final = pd.concat(all_dfs, axis=0)
        df_final = df_final[df_final['精品销售日期'].notna()]
        # Save or use df_final as needed
        print(df_final.head())  # Example: print the first few rows
    else:
        df_final = pd.DataFrame()
    return df_final

df_jingpin = jingpin()
df_jingpin.columns
df_jingpin = df_jingpin[['月份', '精品销售日期', '精品销售人员', '新车销售门店', '车型', '车架号', '客户姓名', '电话号码',
       '护板组合套装', '电池下护板', '发动机/电机下护板', '行车记录仪', '360全景影像', 'FSD减震器', '侧踏板',
       '挡泥板', '隐形ETC', '360软包脚垫', '电尾门', '龙膜', '车贴贴花', '备件类别', '销售总金额', '总成本', '毛利润', '毛利率', '人员提成', '备注', 'From']]

# 定义一个函数来替换中文并转换为 float
def clean_and_convert_to_float(series):
    # 使用正则表达式替换中文
    cleaned_series = series.astype(str).str.replace(r'[^\d.]', '', regex=True)
    # 将空字符串转换为 NaN，然后转换为 float
    cleaned_series = cleaned_series.replace('', 0).astype('float')
    return cleaned_series

# 选择需要处理的列
columns_to_convert = ['护板组合套装', '电池下护板', '发动机/电机下护板', '行车记录仪', '360全景影像', 'FSD减震器', '侧踏板', '挡泥板', '隐形ETC', '360软包脚垫', '电尾门', '龙膜', '车贴贴花']
# 应用函数到每列
df_jingpin[columns_to_convert] = df_jingpin[columns_to_convert].apply(clean_and_convert_to_float)

# 定义一个函数来计算每行的总次数
def count_items(row):
    count = 0
    items = ['护板组合套装', '电池下护板', '发动机/电机下护板', '行车记录仪', '360全景影像', 'FSD减震器', '侧踏板', '挡泥板', '隐形ETC', '360软包脚垫', '电尾门', '龙膜', '车贴贴花']
    for item in items:
        if row[item] > 0:
            if item == '护板组合套装':
                count += 2
            else:
                count += 1
    return count

# 应用函数到每一行，得到总次数
df_jingpin['总次数'] = df_jingpin.apply(count_items, axis=1)
df_jingpin['新车销售门店'] = df_jingpin['新车销售门店'].replace('文景初治', '上元盛世')
df_jingpin.to_csv(r"E:\powerbi_data\看板数据\cyy_old_data\精品销售.csv", index=False)