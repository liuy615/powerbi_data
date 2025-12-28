from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pandas as pd


def sanfangYB(directories, sheet_name):
    """处理三方延保文件"""
    all_dfs = []

    def process_folder(directory):
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]
        dfs = []

        for file_path in files:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, dtype=str)
                df['From'] = os.path.basename(file_path).split('.')[0]
                df.columns = df.columns.str.replace('\n', '')
                dfs.append(df)
            except Exception as e:
                print(f"读取 {file_path} 时出错: {e}")

        if dfs:
            df_combined = pd.concat(dfs, axis=0)
            all_dfs.append(df_combined)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_folder, directory) for directory in directories]
        for future in as_completed(futures):
            future.result()

    if all_dfs:
        df_final = pd.concat(all_dfs, axis=0)
        df_final = df_final[df_final['延保销售日期'].notna()]
        return df_final
    else:
        print('没有找到任何数据')
        return pd.DataFrame()
directories_sanfang = [r"E:\powerbi_data\看板数据\私有云文件本地\新车三方延保"]
df_sanfang = sanfangYB(directories_sanfang,sheet_name='登记表')
df_sanfang =df_sanfang[['新车销售店名','延保销售日期','购车日期','车系','车架号','客户姓名','电话号码1','电话号码2','延保销售人员','延保期限','金额','是否录入厂家系统','录入厂家系统日期','比亚迪系统录入金额','超期录入比亚迪系统违约金','备注','From']]
df_sanfang['新车销售店名'] = df_sanfang['新车销售店名'].replace('文景初治', '上元盛世')
df_sanfang.to_csv(r'E:\powerbi_data\看板数据\dashboard\新车三方延保台账.csv',index=False)