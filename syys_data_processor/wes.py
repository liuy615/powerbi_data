import pandas as pd

df_wes = pd.read_excel(r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2026年WES返利汇总表.xlsx", sheet_name='两网')[["公司名称", "2025年1月得分", "2025年2月得分", "2025年3月得分", "2025年4月得分", "2025年5月得分", "2025年6月得分", "2025年7月得分", "2025年8月得分", "2025年9月得分", "2025年10月得分", "2025年11月得分", "2025年12月得分", "2025年1月折让系数", "2025年2月折让系数", "2025年3月折让系数", "2025年4月折让系数", "2025年5月折让系数", "2025年6月折让系数", "2025年7月折让系数", "2025年8月折让系数", "2025年9月折让系数", "2025年10月折让系数", "2025年11月折让系数", "2025年12月折让系数"]]
df_wes = df_wes.replace({"文景初治":"上元盛世", "永乐盛世":"洪武盛世"})
df_wes = df_wes.drop_duplicates(subset=['公司名称'], keep='last')

# 1. 识别得分列和折让列（包含“2025”和“得分”/“折让”）
score_cols = [col for col in df_wes.columns if '得分' in col]
allow_cols = [col for col in df_wes.columns if '折让系数' in col]

# 2. 对得分列进行melt
df_score = df_wes.melt(
    id_vars=['公司名称'],
    value_vars=score_cols,
    var_name='月份_原始',
    value_name='得分'
)
# 去除“得分”后缀，得到纯月份字符串（如“2025年1月”）
df_score['月份'] = df_score['月份_原始'].str.replace('得分', '', regex=False)

# 3. 对折让列进行melt
df_allow = df_wes.melt(
    id_vars=['公司名称'],
    value_vars=allow_cols,
    var_name='月份_原始',
    value_name='折让系数'
)
df_allow['月份'] = df_allow['月份_原始'].str.replace('折让系数', '', regex=False)

# 4. 按公司名称和月份合并得分与折让
df_final = pd.merge(
    df_score[['公司名称', '月份', '得分']],
    df_allow[['公司名称', '月份', '折让系数']],
    on=['公司名称', '月份'],
    how='outer'  # 保留所有组合，缺失值填NaN
)

# 5. 将月份转换为日期类型，以便按时间排序
df_final['日期'] = pd.to_datetime(
    df_final['月份'].str.replace('年', '-').str.replace('月', ''),
    format='%Y-%m'
)
df_final = df_final.sort_values(['公司名称', '日期']).drop('日期', axis=1)
df_final = df_final.reset_index(drop=True)

# 6. 对得分列保留两位小数
df_final['得分'] = df_final['得分'].round(2)
df_final['折让系数'] = df_final['折让系数'].round(1)

print(df_final)
df_final.to_csv(r'E:\powerbi_data\看板数据\dashboard\data_wes.csv', index=False)






























