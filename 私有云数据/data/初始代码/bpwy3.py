import pandas as pd
import numpy as np
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# 新增列名标准化函数
def standardize_columns(df):
    """
    标准化列名：去除空格、转换为小写、替换空格为下划线
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(r'\\s+', '_', regex=True)
    return df
# 处理保赔无忧

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
            df['from'] = os.path.basename(file_path).split('.')[0]
            # 增强列名清洗逻辑
            df = standardize_columns(df)
            required_columns = [
                '序号', '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码', 
                '保赔无忧金额', '双保无忧金额', '终身保养金额',
                '销售顾问', '所属门店', '备注', '日期'
            ]
            
            # 检查并补全缺失列
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[required_columns]
            dfs.append(df)
            print(f"文件 {filename} 读取成功，一共 {len(df)} 行。列名: {list(df.columns)}")
        except Exception as e:
            print(f"读取 {filename} 时发生错误: {e}")
    return dfs

def process_dataframe(df):
    """
    处理 DataFrame，进行数据清洗和过滤。
    """
    df['开票日期'] = pd.to_datetime(df['开票日期'], format='mixed',errors='coerce')
    df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed',errors='coerce')
    # df[['开票日期','保赔销售日期']] = pd.to_datetime(df[['开票日期','保赔销售日期']], errors='coerce')
    df['开票日期'] = np.where(df['开票日期'] <= df['销售日期'], df['销售日期'], df['开票日期'])
    df['日期'] = df['开票日期'].fillna(df['销售日期'])
    df['日期'] = pd.to_datetime(df['日期'], format='mixed',errors='coerce')
    # df = df[df['日期'].dt.year == 2024]
    df['日期'] = df['日期'].dt.date
    df_Car = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充车系')
    df = pd.merge(df, df_Car[['车系', '服务网络']], how='left', on='车系')
    df['所属门店'] = np.where(df['所属门店'] == '直播基地', df['服务网络'] + '-' + df['所属门店'], df['所属门店'])
    return df[['序号', '车架号', '车系', '销售日期', '开票日期', '客户姓名', '手机号码', '保赔无忧金额','双保无忧金额','终身保养金额',
               '销售顾问', '所属门店', '备注',  '日期']]

def bpwy_result():
    directories = [
        r"E:\私有云文件本地\衍生产品"
    ]

    all_dfs = []

    with ThreadPoolExecutor() as executor:
        # 提交任务到线程池
        futures = [executor.submit(read_excel_files, directory, '登记表') for directory in directories]

        # 获取任务结果
        for future in as_completed(futures):
            dfs = future.result()
            if dfs:
                df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
                df_processed = process_dataframe(df_combined)
                all_dfs.append(df_processed)

    if all_dfs:
        df_final = pd.concat(all_dfs, axis=0, join='outer', ignore_index=True)
        # Save or use df_final as needed
        print(df_final.head())  # Example: print the first few rows
    return df_final
# 处理全保无忧

def read_excel_files_qbwy(directory, sheet_name):
    """
    读取指定目录下的 Excel 文件，返回 DataFrame 列表。
    """
    dfs = []
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        print(f"正在读取文件 {filename}...")
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
            df['from'] = os.path.basename(file_path).split('.')[0]
            # 增强列名清洗逻辑
            df = standardize_columns(df)
            required_columns = [
                '客户姓名','手机号码','身份证号','车架号','发动机号','车牌号','车系','新车开票价格','车损险保额','车辆类型',
                '车系网络','销售日期','全保无忧版本','全保无忧金额','起保日期','终止日期','销售顾问','所属门店','投保费用','from'
            ]
            
            # 检查并补全缺失列
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[required_columns]
            dfs.append(df)
            print(f"文件 {filename} 读取成功，一共 {len(df)} 行。列名: {list(df.columns)}")
        except Exception as e:
            print(f"读取 {filename} 时发生错误: {e}")
    return dfs

def process_dataframe_qbwy(df):
    """
    处理 DataFrame，进行数据清洗和过滤。
    """
    df['销售日期'] = pd.to_datetime(df['销售日期'], format='mixed',errors='coerce')
    df['销售日期'] = df['销售日期'].dt.date
    df_Car = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充车系')
    df = pd.merge(df, df_Car[['车系', '服务网络']], how='left', on='车系')
    df['所属门店'] = np.where(df['所属门店'] == '直播基地', df['服务网络'] + '-' + df['所属门店'], df['所属门店'])
    return df[[ '客户姓名','手机号码','身份证号','车架号','发动机号','车牌号','车系','新车开票价格','车损险保额','车辆类型',
                '车系网络','销售日期','全保无忧版本','全保无忧金额','起保日期','终止日期','销售顾问','所属门店','投保费用','from']]

def qpwy_result():
    directories = [
        r"E:\私有云文件本地\全保无忧"
    ]

    all_dfs = []

    with ThreadPoolExecutor() as executor:
        # 提交任务到线程池
        futures = [executor.submit(read_excel_files_qbwy, directory, '全保无忧登记表') for directory in directories]

        # 获取任务结果
        for future in as_completed(futures):
            dfs = future.result()
            if dfs:
                df_combined = pd.concat(dfs, axis=0, join='outer', ignore_index=True)
                df_processed = process_dataframe_qbwy(df_combined)
                all_dfs.append(df_processed)

    if all_dfs:
        df_final = pd.concat(all_dfs, axis=0, join='outer', ignore_index=True)
        # Save or use df_final as needed
        print(df_final.head())  # Example: print the first few rows
    return df_final
df_qbwy = qpwy_result()
df_qbwy1 = df_qbwy.drop_duplicates()
df_qbwy1 = df_qbwy[df_qbwy['所属门店'].notnull()]
df4 = bpwy_result()
df4['是否保赔'] = '是'
df4['所属门店'] = df4['所属门店'].replace('文景初治', '上元盛世')
df4['城市'] = np.where(df4['所属门店'].str.contains('贵州'), '贵州', '成都')
df4.drop_duplicates(inplace=True)
df4.dropna(subset='车架号', inplace=True)
def standardize_date(date_str):
    if isinstance(date_str, str):  # 确保输入是字符串
        date_str = date_str.strip()  # 去除空格
        if "年" in date_str and "月" in date_str and "日" not in date_str:
            # 如果日期缺少“日”，补充为“1日”
            date_str += "1日"
    return date_str 
def convert_date(date_str):
    if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', date_str):
        return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
    elif re.match(r'\d{4}年\d{1,2}月\d{1,2}日', date_str):
        return pd.to_datetime(date_str, format='%Y年%m月%d日')
    return pd.NaT
def process_xinchebaoxian(filedir, sheet_name):
    filenames = os.listdir(filedir)

    def read_single_file(filename):
        if '新车' in filename and filename.endswith('.xlsx'):
            file_path = os.path.join(filedir, filename)
            try:
                # 检查工作表是否存在
                with pd.ExcelFile(file_path) as xls:
                    if sheet_name in xls.sheet_names:
                        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                        df['From'] = os.path.basename(file_path).split('.')[0]
                        df.columns = df.columns.str.replace('\n', '')
                        return df
                    else:
                        print(f"Worksheet named '{sheet_name}' not found in file {filename}. Skipping this file.")
            except Exception as e:
                print(f"读取 {filename} 时发生错误: {e}")
        return None

    dfs = []
    with ThreadPoolExecutor() as executor:
        # 提交任务到线程池
        futures = [executor.submit(read_single_file, filename) for filename in filenames]

        # 获取任务结果
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                dfs.append(df)

    # 如果 dfs 为空，返回一个空的 DataFrame
    if not dfs:
        return pd.DataFrame()

    df_combined = pd.concat(dfs, axis=0, ignore_index=True)
    return df_combined

# def process_xinchebaoxian(filedir, keyword):
#     dfs = []
#     for root, dirs, files in os.walk(filedir):
#         for file in files:
#             if keyword in file:
#                 file_path = os.path.join(root, file)
#                 try:
#                     # 假设文件是 Excel 文件，你可以根据实际情况修改
#                     df = pd.read_excel(file_path)
#                     dfs.append(df)
#                 except Exception as e:
#                     print(f"读取文件 {file_path} 时出错: {e}")
#     if dfs:
#         return pd.concat(dfs, axis=0, ignore_index=True)
#     return pd.DataFrame()

def xinchebaoxianTZ():
    directories = [
        r'E:\私有云文件本地\新车保险台账'
    ]
    all_dfs = []

    for filedir in directories:
        dfs = process_xinchebaoxian(filedir, '新车台账明细')
        # 如果 dfs 不为空，则添加到 all_dfs
        if not dfs.empty:
            all_dfs.append(dfs)

    df_cyy = pd.read_csv(r"C:\Users\13111\Documents\WXWork\1688855282576011\WeDrive\成都永乐盛世\维护文件\新车保险台账-2025.csv")
    df_cyy = df_cyy[['出单日期', '保险公司简称', '所属门店', '车系', '车架号', '交强险保费', '业务人员','保费总额']]
    df_cyy.rename(columns={'出单日期': '签单日期', '保险公司简称': '保险公司', '车系': '车型', '所属门店': '归属公司', '业务人员': '销售顾问'}, inplace=True)
    df_Car = pd.read_excel(r'C:\Users\13111\Desktop\各公司银行额度.xlsx', sheet_name='补充车系')
    df_cyy = pd.merge(df_cyy, df_Car[['车系', '服务网络']], how='left', left_on='车型',right_on='车系')
    df_cyy['归属公司'] = np.where(df_cyy['归属公司'] == '直播基地', df_cyy['服务网络'] + '-' + df_cyy['归属公司'], df_cyy['归属公司'])
    if all_dfs:
        df_combined = pd.concat(all_dfs, axis=0, ignore_index=True)

        # 检查列名是否唯一
        def make_unique(column_names):
            unique_names = []
            count = {}
            for name in column_names:
                if name in count:
                    count[name] += 1
                    unique_names.append(f"{name}_{count[name]}")
                else:
                    count[name] = 0
                    unique_names.append(name)
            return unique_names

        df_combined.columns = make_unique(df_combined.columns)
        df_cyy.columns = make_unique(df_cyy.columns)

        df_combined1 = pd.concat([df_combined, df_cyy], axis=0, join='outer', ignore_index=True)
        df_combined1['归属公司'] = df_combined1['归属公司'].replace('文景初治', '上元盛世')
        df_combined1.dropna(subset=['保险公司'], inplace=True)
        df_filtered = df_combined1[['月份', '签单日期', '到期日期', '保险公司', '数据归属门店', '归属公司', '车型', '车牌号', '车架号', '被保险人',
                                    '交强险保费', '销售顾问', '是否为保赔无忧客户']]
        df_filtered['日期'] = df_filtered['签单日期']
        return df_filtered
    else:
        return pd.DataFrame() 

df3 = xinchebaoxianTZ()
df3['日期'] = pd.to_datetime(df3['日期'], errors='coerce').dt.date
df3 = df3.sort_values(by='日期', ascending=False)
df3.drop_duplicates(subset='车架号', inplace=True,keep='first')
df5 = pd.merge(df3, df4[['车架号', '是否保赔']], how='left', on='车架号')
df5['是否保赔'] = df5['是否保赔'].fillna('否')
# 筛选运营车出去
exclude_list = [1000, 1130, 1800]
df_except = df5[df5['保险公司'].str.contains('鼎和')]
df_except1 = df5[df5['交强险保费'].isin(exclude_list)]
df_excluded = pd.concat([df_except, df_except1], axis=0).drop_duplicates()
df_excluded = df_excluded[df_excluded['是否保赔'] == '否']

# 筛选出不包含在 df_excluded 中的行
diff_df = df5[~df5['车架号'].isin(df_excluded['车架号'])]
diff_df['城市'] = np.where(diff_df['归属公司'].str.contains('贵州'), '贵州', '成都')
diff_df = diff_df.drop_duplicates()
df4.to_csv(r'C:\Users\13111\code\dashboard\保赔无忧.csv', index=False)
diff_df.to_csv(r'C:\Users\13111\code\dashboard\新车保险台账.csv', index=False)
df_qbwy1.to_csv(r'C:\Users\13111\code\dashboard\全赔无忧.csv', index=False)