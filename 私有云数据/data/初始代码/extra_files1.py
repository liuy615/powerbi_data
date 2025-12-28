import pandas as pd
import os

# 定义两组文件路径列表
sales_files = [
    r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年销量汇总表.xlsx',
    r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年销量汇总表.xlsx",
    r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\25年数据统计表.xlsx',
    r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\24年数据统计表.xlsx'
]

quality_files = [
    r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\25年数据统计表.xlsx',
    r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\24年数据统计表.xlsx',
    r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年数据汇总表.xlsx',
    r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年数据汇总表.xlsx']

quality_files1 = [
    r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\25年数据统计表.xlsx',
    r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\贵州地区\数据统计\24年数据统计表.xlsx',
    r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年数据汇总表.xlsx',
    r'E:\powerbi_data\看板数据\私有云文件本地\收集文件\2024年数据汇总表.xlsx']

wes_files = [
    r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年WES返利汇总表.xlsx"]


# 定义一个函数来处理文件列表和工作表名称
def process_files(file_list, sheet_name):
    dfs = []
    for file_path in file_list:
        if os.path.exists(file_path):
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                unpivot_df = pd.melt(df, id_vars=df.columns[0:2], var_name='属性', value_name='值')
                dfs.append(unpivot_df)
            except Exception as e:
                print(f"处理文件 {file_path} 中的 {sheet_name} 工作表时出错: {e}")
        else:
            print(f"文件 {file_path} 不存在。")
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        return merged_df
    else:
        print(f"未找到 {sheet_name} 工作表的有效数据。")

# 定义一个函数来处理文件列表和工作表名称
def process_files1(file_list, sheet_name):
    dfs = []
    for file_path in file_list:
        if os.path.exists(file_path):
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                unpivot_df = pd.melt(df, id_vars=df.columns[0:2], var_name='属性', value_name='值')
                dfs.append(unpivot_df)
            except Exception as e:
                print(f"处理文件 {file_path} 中的 {sheet_name} 工作表时出错: {e}")
        else:
            print(f"文件 {file_path} 不存在。")
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df = merged_df[~merged_df['属性'].str.contains("Unnamed")]
        return merged_df
    else:
        print(f"未找到 {sheet_name} 工作表的有效数据。")

def process_files2(file_list, sheet_name):
    dfs = []
    for file_path in file_list:
        if os.path.exists(file_path):
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                unpivot_df = pd.melt(df, id_vars=df.columns[0:2], var_name='属性', value_name='值')
                dfs.append(unpivot_df)
            except Exception as e:
                print(f"处理文件 {file_path} 中的 {sheet_name} 工作表时出错: {e}")
        else:
            print(f"文件 {file_path} 不存在。")
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        # merged_df = merged_df[~merged_df['属性'].str.contains("Unnamed")]
        return merged_df
    else:
        print(f"未找到 {sheet_name} 工作表的有效数据。")

def process_files3(file_list, sheet_name):
    dfs = []
    for file_path in file_list:
        if os.path.exists(file_path):
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                unpivot_df = pd.melt(df, id_vars=df.columns[0:2], var_name='属性', value_name='值')
                unpivot_df = unpivot_df[(unpivot_df['属性'].str.contains("得分"))&unpivot_df['公司名称'].notna()]
                dfs.append(unpivot_df)
            except Exception as e:
                print(f"处理文件 {file_path} 中的 {sheet_name} 工作表时出错: {e}")
        else:
            print(f"文件 {file_path} 不存在。")
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        # merged_df = merged_df[~merged_df['属性'].str.contains("Unnamed")]
        return merged_df
    else:
        print(f"未找到 {sheet_name} 工作表的有效数据。")

# 处理“销量目标”工作表
sales_output = r'E:\powerbi_data\看板数据\dashboard\merged_sales_data.xlsx'
df_sales_goal = process_files(sales_files, '销量目标')
df_sales_goal['公司名称'] = df_sales_goal['公司名称'] .replace('文景初治', '上元盛世')
df_sales_goal.to_excel(sales_output, index=False)
# 处理“服务品质”工作表
quality_output = r'E:\powerbi_data\看板数据\dashboard\merged_quality_data.xlsx'
df_service_score = process_files1(quality_files, '服务品质')
df_service_score['月份'] = df_service_score['月份'].replace('文景初治', '上元盛世')
df_service_score.to_excel(quality_output, index=False)
# 处理“nps”工作表
quality_output1 = r'E:\powerbi_data\看板数据\dashboard\merged_nps_data.xlsx'
df_nps = process_files2(quality_files1, 'NPS')
df_nps['公司名称'] = df_nps['公司名称'].replace('文景初治', '上元盛世')
df_nps.to_excel(quality_output1, index=False)
# 处理“提车”工作表
quality_output2 = r'E:\powerbi_data\看板数据\dashboard\merged_tiche_data.xlsx'
df_tiche = process_files2(quality_files1, '提车')
df_tiche['公司名称'] = df_tiche['公司名称'].replace('文景初治', '上元盛世')
df_tiche.to_excel(quality_output2, index=False)
# 处理“wes”工作表
wes_output = r'E:\powerbi_data\看板数据\dashboard\merged_wes_data.xlsx'
df_wes = process_files3(wes_files, '两网')
df_wes['公司名称'] = df_wes['公司名称'].replace('文景初治', '上元盛世')
df_wes.to_excel(wes_output, index=False)