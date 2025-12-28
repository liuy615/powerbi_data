import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor

def process_file(file_path):
    print(f'处理文件: {os.path.basename(file_path)}')
    try:
        dfs = []
        # 读取 Excel 文件中的指定工作表数据
        with pd.ExcelFile(file_path) as xls:
            for sheet_name in xls.sheet_names:
                print(f'处理工作表: {sheet_name}')
                if sheet_name in ['2024年', '2025年']:
                    try:
                        data = pd.read_excel(xls, sheet_name=sheet_name)
                        data['From'] = os.path.basename(file_path).split('.')[0]
                        if not data.empty:
                            dfs.append(data)
                    except Exception as e:
                        print(f'读取 {sheet_name} 时出错: {e}')
            if dfs:
                df_combined = pd.concat(dfs, axis=0)
                return df_combined
            else:
                print(f'{os.path.basename(file_path)} 中没有找到任何数据')
                return None
    except Exception as e:
        print(f'读取 {file_path} 时出错: {e}')
        return None

def yingxiao_money():
    # 设置工作簿所在文件夹路径
    directorie = [r'E:\powerbi_data\看板数据\私有云文件本地\投放市场费用']

    all_file_paths = []
    for folder_path in directorie:
        print(f'处理文件夹: {folder_path}')
        # 遍历文件夹中的每个 Excel 文件
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.xlsx'):
                file_path = os.path.join(folder_path, file_name)
                all_file_paths.append(file_path)

    combined_data = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(process_file, all_file_paths)
        for result in results:
            if result is not None:
                combined_data.append(result)

    if combined_data:
        df_A = pd.concat(combined_data)
        df_A = df_A[['年份', '月份', '归属门店', '项目大类', '项目分类', '具体项目', '费用金额', '核销发票金额', '核销发票税金', '费用合计', '备注', 'From']]
        df_A['费用合计'] =  df_A['费用合计'].fillna(df_A['费用金额'])
        df_A['费用合计'] = pd.to_numeric(df_A['费用合计'], errors='coerce').fillna(0)
        return df_A
    else:
        print('没有找到任何数据')
        return pd.DataFrame()

# 调用函数
df_yx = yingxiao_money()
df_yx['归属门店'] = df_yx['归属门店'].replace('文景初治', '上元盛世')
df_yx.to_csv(r'E:\powerbi_data\看板数据\dashboard\投放费用.csv', index=False)