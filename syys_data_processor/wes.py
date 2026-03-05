import numpy as np
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


def data_change(df, year):
    dfs = []

    # 季度列表及对应的列名处理
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    for q in quarters:
        if q == 'Q1' or q == 'Q2':
            # Q1 和 Q2 没有成绩列，只有分数和激励
            sub = df[['公司名称', f'{q}分数', f'{q}激励']].copy()
            sub['成绩'] = np.nan  # 成绩设为空
        else:
            # Q3 和 Q4 有成绩列
            sub = df[['公司名称', f'{q}分数', f'{q}成绩', f'{q}激励']].copy()
            sub = sub.rename(columns={f'{q}成绩': '成绩'})

        # 统一分数和激励的列名
        sub = sub.rename(columns={f'{q}分数': '分数', f'{q}激励': '激励'})

        # 添加年季列（固定为2026年）
        sub['年季'] = f'{year}年{q}'

        # 调整列顺序
        sub = sub[['公司名称', '年季', '分数', '成绩', '激励']]

        dfs.append(sub)

    # 合并所有季度数据
    result = pd.concat(dfs, ignore_index=True)

    # 按照公司名称和季度排序（可选）
    # 提取季度数字以便排序
    result['季度序号'] = result['年季'].str.extract(r'Q(\d)').astype(int)
    result = result.sort_values(['公司名称', '季度序号']).drop('季度序号', axis=1).reset_index(drop=True)

    return result


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

    return data_month, data_quart


def month_to_quarters(df):
    # 假设 df 是您的原始数据 DataFrame
    # 请确保列名准确，例如 '公司名称', '1月分数', '2月分数', '2月激励', ...

    # 定义季度对应的列名
    quarters = {
        'Q1': {
            '分数': ['1月分数', '2月分数', '3月分数'],
            '激励': ['2月激励', '3月激励']
        },
        'Q2': {
            '分数': ['4月分数', '5月分数', '6月分数'],
            '激励': ['4月激励', '5月激励', '6月激励']
        },
        # 注意：数据中缺少7-9月，Q3不处理
        'Q4': {
            '分数': ['10月分数', '11月分数', '12月分数'],
            '激励': ['10月激励', '11月激励', '12月激励']
        }
    }

    # 创建结果DataFrame，保留公司名称
    result = df[['公司名称']].copy()

    # 遍历每个季度
    for q, cols in quarters.items():
        # 处理分数：将对应列转为数值，无法转换的变为NaN，然后求每行的平均值（忽略NaN），全NaN则填充0
        score_cols = cols['分数']
        score_df = df[score_cols].apply(pd.to_numeric, errors='coerce')
        result[f'{q}分数'] = score_df.mean(axis=1, skipna=True).fillna(0).round(2)

        # 处理激励：将对应列转为数值，求和（忽略NaN），全NaN则填充0
        incentive_cols = cols['激励']
        incentive_df = df[incentive_cols].apply(pd.to_numeric, errors='coerce')
        result[f'{q}激励'] = incentive_df.sum(axis=1, skipna=True).fillna(0)

    return result


def month_quarters_merge(df1, df2):
    # 假设 df1 和 df2 已存在
    # df1 列：公司名称、Q3分数、Q3成绩、Q3激励、Q4分数、Q4成绩、Q4激励
    # df2 列：公司名称、Q1分数、Q1激励、Q2分数、Q2激励、Q4分数、Q4激励

    def convert_incentive(val):
        """
        将激励列的文本转换为数值：
        - 如果是数字字符串（如 '-20000'、'10000'），转为对应整数/浮点数
        - 如果是文本（如 '合格'、'免考核'、'未出'），转为 0
        - 如果是空字符串或 NaN，保留 NaN
        """
        if pd.isna(val):  # 包括 None 和 NaN
            return np.nan
        if isinstance(val, (int, float)):  # 已经是数字
            return val
        if isinstance(val, str):
            s = val.strip()
            if s == '':  # 空字符串
                return np.nan
            try:
                # 尝试转换为数字（整数或浮点数）
                return pd.to_numeric(s)
            except (ValueError, TypeError):
                # 转换失败，说明是文本，返回 0
                return 0
        # 其他情况（如布尔值等），按需处理，这里返回 0 或 NaN
        return 0

    # 处理 df1 的激励列
    for col in ['Q3激励', 'Q4激励']:
        df1[col] = df1[col].apply(convert_incentive)

    # 以 df2 为主表左连接 df1
    merged = pd.merge(df2, df1, on='公司名称', how='left', suffixes=('', '_df1'))

    # 处理 Q4 分数：优先使用 df2 中非空且非 0 的值，否则使用 df1 的值，否则保留 df2 原值
    cond_q4_score_df2 = merged['Q4分数'].notna() & (merged['Q4分数'] != 0)
    cond_q4_score_df1 = merged['Q4分数_df1'].notna()
    merged['Q4分数'] = np.where(
        cond_q4_score_df2,
        merged['Q4分数'],
        np.where(cond_q4_score_df1, merged['Q4分数_df1'], merged['Q4分数'])
    )

    # 处理 Q4 激励，逻辑同上
    cond_q4_incent_df2 = merged['Q4激励'].notna() & (merged['Q4激励'] != 0)
    cond_q4_incent_df1 = merged['Q4激励_df1'].notna()
    merged['Q4激励'] = np.where(
        cond_q4_incent_df2,
        merged['Q4激励'],
        np.where(cond_q4_incent_df1, merged['Q4激励_df1'], merged['Q4激励'])
    )

    # 选择最终需要的列
    result = merged[[
        '公司名称',
        'Q1分数', 'Q1激励',
        'Q2分数', 'Q2激励',
        'Q3分数', 'Q3成绩', 'Q3激励',
        'Q4分数', 'Q4成绩', 'Q4激励',
    ]].copy()

    # 输出结果（可根据需要保存或显示）
    result['Q4分数'] = pd.to_numeric(result['Q4分数'], errors='coerce')
    result['Q4分数'] = result['Q4分数'].round(2)

    result = data_change(result, "2025")

    return result


def fuwupinzhi_2026():
    data = pd.read_excel(r"E:\powerbi_data\看板数据\私有云文件本地\收集文件\2026年数据汇总表.xlsx", sheet_name="服务品质", skiprows=[0, 1])
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
    data = data[data['公司名称'].isin(company_list)]
    quart_list = ['公司名称', "Q1分数", "Q1成绩", "Q1激励", "Q2分数", "Q2成绩", "Q2激励", "Q3分数", "Q3成绩", "Q3激励", "Q4分数", "Q4成绩", "Q4激励"]
    data = data[quart_list]
    result = data_change(data, "2026")

    return result


def fwpz_run():
    # 获取2025年服务品质的数据
    data_month, data_quart = fuwupinzhi_2025()
    fwpz_2025 = pd.merge(data_month, data_quart, on='公司名称', how='outer')
    fwpz_2025.to_csv(r'E:\powerbi_data\看板数据\dashboard\data_fwpz.csv', index=False)
    data_month = month_to_quarters(data_month)
    data_2025 = month_quarters_merge(data_quart, data_month)


    # 获取2026年服务品质的数据
    data_2026 = fuwupinzhi_2026()
    data = pd.concat([data_2025, data_2026])
    data.to_csv(r'E:\powerbi_data\看板数据\dashboard\事实表_服务品质.csv', index=False)



def run():
    wes_run()
    fwpz_run()


run()




























