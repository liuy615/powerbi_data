from concurrent.futures import ThreadPoolExecutor, as_completed
import os

import chardet
import numpy as np
import pandas as pd 

def process_inventory(filedir, sheet_name):
    filenames = os.listdir(filedir)

    def read_single_file(filename):
        
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
def process_csv(filedir):
    # 只保留真正的 csv 文件
    filenames = [
        f for f in os.listdir(filedir)
        if f.lower().endswith('.csv')
    ]

    def read_single_csv(filename):
        file_path = os.path.join(filedir, filename)
        try:
            # 先探测编码
            with open(file_path, 'rb') as f:
                raw = f.read(10000)
                enc = chardet.detect(raw)['encoding'] or 'utf-8'

            # 明确用 CSV 引擎
            df = pd.read_csv(file_path,encoding=enc,engine='python',on_bad_lines='warn')

            df['From'] = os.path.basename(file_path).rsplit('.', 1)[0]
            df.columns = df.columns.str.replace('\n', '').str.strip()
            return df

        except Exception as e:
            # CSV 解析失败就直接放弃，不再尝试 Excel
            print(f"[CSV] 读取 {filename} 失败: {e}")
            return None

    dfs = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(read_single_csv, f) for f in filenames]
        for fut in as_completed(futures):
            res = fut.result()
            if res is not None:
                dfs.append(res)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
def main():
    all_dfs = []
    filedir = r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\库存存档'
    sheet_name = 'inventory_data'
    try:
        # 并行处理两种文件类型
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_excel = executor.submit(process_inventory, filedir, sheet_name)
            future_csv = executor.submit(process_csv, filedir)
            
            df = future_excel.result()
            df1 = future_csv.result()
            
        # 过滤空DataFrame并合并
        valid_dfs = [d for d in [df, df1] if not d.empty]
        if valid_dfs:
            # 统一列结构后再合并
            common_cols = list(set(valid_dfs[0].columns) & set(valid_dfs[1].columns))
            df_combined = pd.concat([d[common_cols] for d in valid_dfs], ignore_index=True)
            df_combined = df_combined[['归属系统','采购订单号','车架号','车系','车型','配置','颜色','指导价', '提货价', '车辆状态','库存天数','From']]
            df_combined['日期'] = df_combined['From'].str.extract(r'(\d{4}-\d{2}-\d{2})')
            df_combined1 = df_combined.copy()
            df_combined1 = df_combined1[['车系','提货价']].drop_duplicates()
            df_combined1['提货价'] = df_combined1['提货价'].astype(float)
            df_combined1 = df_combined1[(df_combined1['提货价'] > 0) & (df_combined1['提货价'].notnull())].sort_values(by='提货价',ascending=False).drop_duplicates(subset='车系',keep='first')
            df_combined1.columns = ['车系', '提货价1']
            df_combined2 = pd.merge(df_combined, df_combined1, on='车系', how='left')
            df_combined2['提货价'] = df_combined2['提货价'].fillna(df_combined2['提货价1'])
            df_combined2['提货价'] = np.where(df_combined2['提货价']==0, df_combined2['提货价1'], df_combined2['提货价'])
            df_combined2['提货价'] = df_combined2['提货价'].astype('float')
            df_combined2['车系'] = np.where((df_combined2['车系']=="2025款海鸥")&(df_combined2['提货价']==65800), '2025款 海鸥', df_combined2['车系'])
            df_combined2['车系'] = np.where((df_combined2['车系']=="2025款海鸥")&(df_combined2['提货价']==65800), '2025款 海鸥', df_combined2['车系'])
            df_combined2['归属系统'] = df_combined2['归属系统'].replace('文景初治', '上元盛世')
            df_combined2.to_csv(r'E:\powerbi_data\看板数据\dashboard\库存存档.csv')
    except Exception as e:
        print(f"主流程执行失败: {e}")
        raise

if __name__ == '__main__':
    main()
