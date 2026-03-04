import pandas as pd


def clean_wes(path, sheet_name, list_name):
    df_wes = pd.read_excel(path, sheet_name=sheet_name)[list_name]
    df_wes = df_wes.replace({"文景初治":"上元盛世", "永乐盛世":"洪武盛世"})
    df_wes = df_wes.drop_duplicates(subset=['公司名称'], keep='last')

    # 1. 识别得分列和折让列（包含“得分”/“折让”）
    score_cols = [col for col in df_wes.columns if '得分' in col]
    allow_cols = [col for col in df_wes.columns if '折让系数' in col]

    # 2. 对得分列进行melt
    df_score = df_wes.melt(
        id_vars=['公司名称'],
        value_vars=score_cols,
        var_name='月份_原始',
        value_name='得分'
    )
    # 去除“得分”后缀，得到纯月份字符串（如“2026年1月”）
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
    return df_final

def wes_run():
    path = r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年WES返利汇总表.xlsx"
    sheet_name = "两网"
    list_name = ["公司名称", "2025年1月得分", "2025年2月得分", "2025年3月得分", "2025年4月得分", "2025年5月得分", "2025年6月得分", "2025年7月得分", "2025年8月得分", "2025年9月得分", "2025年10月得分", "2025年11月得分", "2025年12月得分", "2025年1月折让系数", "2025年2月折让系数", "2025年3月折让系数", "2025年4月折让系数", "2025年5月折让系数", "2025年6月折让系数", "2025年7月折让系数", "2025年8月折让系数", "2025年9月折让系数", "2025年10月折让系数", "2025年11月折让系数", "2025年12月折让系数"]
    df_wes_2025 = clean_wes(path, sheet_name, list_name)

    path = r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2026年WES返利汇总表.xlsx"
    sheet_name = "两网"
    list_name = ["公司名称", "2026年1月得分", "2026年2月得分", "2026年3月得分", "2026年4月得分", "2026年5月得分", "2026年6月得分", "2026年7月得分", "2026年8月得分", "2026年9月得分", "2026年10月得分", "2026年11月得分", "2026年12月得分", "2026年1月折让系数", "2026年2月折让系数", "2026年3月折让系数", "2026年4月折让系数", "2026年5月折让系数", "2026年6月折让系数", "2026年7月折让系数", "2026年8月折让系数", "2026年9月折让系数", "2026年10月折让系数", "2026年11月折让系数", "2026年12月折让系数"]
    df_wes_2026 = clean_wes(path, sheet_name, list_name)
    df_wes = pd.concat([df_wes_2025, df_wes_2026])
    df_wes.to_csv(r'E:\powerbi_data\看板数据\dashboard\data_wes.csv', index=False)



def fuwupinzhi_2026():
    pass






def fuwupinzhi_2025():
    data = pd.read_excel(r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2025年数据汇总表.xlsx", sheet_name="服务品质", skiprows=[0, 1]).rename(columns={"1-2月激励":"2月激励"})
    
    # 定义需要筛选的公司名称列表
    company_list = {
        "新港建元", "永乐盛世", "新港永初", "新港海川", "新港先秦", "新港治元", "新港建隆", "上元盛世", 
        "新港建武", "文景海洋", "文景盛世", "新港澜阔", "鑫港鲲鹏", "新港澜舰", "新茂元大", "新港澜轩", 
        "新港浩蓝", "贵州新港蔚蓝", "贵州新港浩蓝", "贵州新港澜源", "贵州新港海之辇", "上元坤灵", 
        "上元曦和", "上元弘川", "上元星汉", "贵州上元曦和", "贵州新港澜轩", "贵州上元坤灵", "宜宾上元曦和", 
        "乐山上元曦和", "西藏上元曦和", "泸州上元坤灵", "上元臻智", "上元臻享", "上元臻盛", "贵州上元臻智", 
        "乐山上元臻智", "绵阳新港鑫泽", "德阳上元臻智", "宜宾上元臻智"
    }
    
    # 筛选公司名称
    data = data[data['公司名称'].isin(company_list)].replace("永乐盛世", "洪武盛世")
    
    # 筛选月份
    month_list = ["1月分数", "2月分数", "2月激励",	"3月分数", "3月激励", "4月分数", "4月激励",	"5月分数", "5月激励", "6月分数", "6月激励", "10月分数", "10月激励", "11月分数", "11月激励","12月分数", "12月激励"]
    quart_list = ["Q3分数", "Q3成绩", "Q3激励", "Q4分数", "Q4成绩", "Q4激励"]
    
    # 拆分为两个数据集
    # 月度数据：包含公司名称和月度列
    data_month = data[['公司名称'] + month_list].copy()
    
    # 季度数据：包含公司名称和季度列
    data_quart = data[['公司名称'] + quart_list].copy()
    
    # 将数值类型列保留两位小数
    for col in data_month.columns:
        if col != '公司名称' and pd.api.types.is_numeric_dtype(data_month[col]):
            data_month[col] = data_month[col].round(2)
    
    for col in data_quart.columns:
        if col != '公司名称' and pd.api.types.is_numeric_dtype(data_quart[col]):
            data_quart[col] = data_quart[col].round(2)
    
    print("月度数据：")
    data_month.to_csv(r'E:\powerbi_data\看板数据\dashboard\data_fwpz.csv', index=False)
    print("\n季度数据：")
    print(data_quart)







wes_run()
# fuwupinzhi_2025()






























