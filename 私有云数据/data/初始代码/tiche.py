import pandas as pd
import os
from datetime import datetime
import warnings

# 屏蔽openpyxl的样式警告
warnings.filterwarnings("ignore", message="Workbook contains no default style, apply openpyxl's default")

# 公司名称映射关系
COMPANY_MAPPING = {
    "德阳新港永熙汽车销售服务有限公司": "德阳新港永熙",
    "宜宾新港海川汽车销售服务有限公司": "宜宾新港海川",
    "成都文景七虎汽车销售服务有限公司": "文景七虎",
    "成都新港文景海洋汽车销售服务有限公司": "文景海洋",
    "成都新港建武汽车销售服务有限公司": "新港建武",
    "成都新港澜舰汽车销售服务有限公司": "新港澜舰",
    "成都文景初治新能源汽车销售有限公司": "上元盛世",
    "贵州新港蔚蓝汽车销售服务有限责任公司": "贵州新港蔚蓝",
    "泸州新港帝虹汽贸有限公司": "泸州新港帝虹",
    "成都新港浩蓝汽车销售服务有限公司": "新港浩蓝",
    "贵州新港浩蓝汽车销售服务有限责任公司": "贵州新港浩蓝",
    "成都新港澜轩汽车销售服务有限公司": "新港澜轩",
    "德阳鑫港永宣汽车销售服务有限公司": "德阳鑫港永宣",
    "成都新港先秦汽车服务有限公司": "新港先秦",
    "成都新港建元汽车销售服务有限公司": "新港建元",
    "成都新港上元曦和汽车销售服务有限公司": "上元曦和",
    "西藏新港建元汽车销售服务有限公司": "西藏新港建元",
    "成都新港澜阔汽车销售服务有限公司": "新港澜阔",
    "成都新茂元大汽车销售服务有限公司": "新茂元大",
    "成都新港治元汽车销售服务有限公司": "新港治元",
    "成都新港上元臻享汽车销售服务有限公司": "上元臻享",
    "成都新港上元坤灵汽车销售服务有限公司": "上元坤灵",
    "成都文景盛世汽车销售服务有限公司": "文景盛世",
    "仁怀市新港春风汽车销售服务有限公司": "仁怀新港春风",
    "成都鑫港鲲鹏汽车销售服务有限公司": "鑫港鲲鹏",
    "成都新港建隆汽车销售服务有限公司": "新港建隆",
    "成都新港上元弘川汽车销售服务有限公司": "上元弘川",
    "成都新港上元臻盛汽车销售服务有限公司": "上元臻盛",
    "成都新港上元臻智汽车销售服务有限公司": "上元臻智",
    "贵州新港澜轩汽车销售有限责任公司": "贵州新港澜轩",
    "泸州新港上元坤灵汽车销售服务有限公司": "泸州上元坤灵",
    "成都永乐盛世汽车销售服务有限公司": "永乐盛世",
    "贵州新港上元曦和汽车销售服务有限公司": "贵州上元曦和",
    "贵州新港澜源汽车服务有限责任公司": "贵州新港澜源",
    "贵州新港海之辇汽车销售服务有限责任公司": "贵州新港海之辇",
    "德阳新港上元臻智汽车销售服务有限公司": "德阳上元臻智",
    "成都新港永初汽车服务有限公司": "新港永初",
    "贵州新港江涛汽车销售服务有限公司": "贵州新港江涛",
    "成都新港海川汽车销售服务有限公司": "新港海川",
    "贵州新港上元臻智汽车贸易有限公司": "贵州上元臻智",
    "乐山新港上元臻智汽车销售服务有限公司": "乐山上元臻智",
    "宜宾新港上元曦和汽车销售服务有限公司": "宜宾上元曦和",
    "乐山新港上元曦和汽车销售服务有限公司": "乐山上元曦和",
    "宜宾新港上元臻智汽车销售服务有限公司": "宜宾上元臻智",
    "广汉鑫港恒锐新能源汽车销售有限公司": "广汉鑫港恒锐",
    "什邡鑫港宏盛汽车销售服务有限公司": "什邡鑫港宏盛",
    "成都上元盛世汽车销售服务有限公司":'上元盛世',
    "绵阳新港鑫泽汽车销售服务有限公司":'绵阳新港鑫泽'
}


def process_sales_data(start_date, end_date):
    """
    处理销售数据的完整流程

    参数:
    folder_path (str): 包含所有数据文件的文件夹路径
    start_date (str): 筛选的开始日期，格式如"2025-07-26"
    end_date (str): 筛选的结束日期，格式如"2025-08-25"
    """
    # 合并所有数据
    combined_df = pd.read_excel(r"E:/私有云文件本地/厂家采购/整车采购明细导出.xlsx",sheet_name=0)
    # 2. 以"销服订单号"为主键进行去重
    combined_df = combined_df.drop_duplicates(subset=['销服订单号'], keep='last')

    # 3. 处理日期：如果"考核计划日期"为空，用"计划日期"填充
    date_column = '考核计划日期'
    backup_date_column = '计划日期'

    # 确保两列都是日期格式
    combined_df[date_column] = pd.to_datetime(combined_df[date_column], errors='coerce')
    combined_df[backup_date_column] = pd.to_datetime(combined_df[backup_date_column], errors='coerce')

    # 用计划日期填充考核计划日期的空值
    combined_df[date_column] = combined_df[date_column].fillna(combined_df[backup_date_column])

    # 保存去重后的数据
    combined_df.to_csv(r"C:\Users\13111\code\cyys\去重后的数据.csv", index=False)

    # 4. 筛选指定时间区间的数据
    # 修改结束时间，包含整个结束日期
    start_ts = pd.to_datetime(start_date, format='%Y-%m-%d')
    end_ts = pd.to_datetime(end_date, format='%Y-%m-%d') + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    date_filtered_data = combined_df[
        (combined_df[date_column] >= start_ts) &
        (combined_df[date_column] <= end_ts)
    ]
    date_filtered_data['公司名称'] = date_filtered_data['经销商名称'].map(COMPANY_MAPPING).fillna(date_filtered_data['经销商名称'])
    # 正确的方式：从原始字符串中提取年月
    date_filtered_data['属性'] = f"{end_date.split('-')[0]}年{int(end_date.split('-')[1])}月"
    # 5. 按经销商名称分组统计数量，并应用公司名称映射
    if not date_filtered_data.empty:
        # 分组统计
        result = date_filtered_data.groupby(['公司名称','属性']).size().reset_index(name='值')
        # 排序并调整列顺序
        result = result.sort_values('值', ascending=False)
        result = result[['公司名称','属性','值']]
    else:
        print(f"未找到{start_date}至{end_date}期间的数据")
        result = pd.DataFrame(columns=['公司名称','属性','值'])

    return result,date_filtered_data


# 使用示例
if __name__ == "__main__":
    tiche_history = pd.read_excel(r'C:\Users\13111\code\dashboard\merged_tiche_data.xlsx',sheet_name='Sheet1')
    # 1. 定义时间段清单，可随意增删
    date_ranges = [
        ("2025-07-26", "2025-08-25"),
        ("2025-08-26", "2025-09-25"),
        ("2025-09-26", "2025-10-25"),
    ]
    all_data = []
    all_df = []
    # 2. 逐个区间统计
    for start, end in date_ranges:
        res,df = process_sales_data(start, end)
        if res is not None:
            out = f"{start}至{end}经销商订单"
            
            # res.to_excel(out, index=False)
            all_data.append(res)
            all_df.append(df)
            print(f"✅ 已合并 {out}")
    all_datas = pd.concat(all_data)
    all_dfs = pd.concat(all_df)
    tiche_history = tiche_history[~tiche_history['属性'].isin(['2025年8月', '2025年9月', '2025年10月','2025年11月', '2025年12月'])]
    All_datas = pd.concat([all_datas, tiche_history], join='outer')
    # 使用 pd.to_numeric 处理可能的非数值情况，然后转换为整数
    All_datas['值'] = pd.to_numeric(All_datas['值'], errors='coerce').fillna(0).astype(int)
    All_datas = All_datas[All_datas['公司名称'].notna() & (~All_datas['公司名称'].isin(['方程豹','海洋网','王朝网','腾势','方程豹总计','海洋网总计','王朝网总计','腾势总计','总计','公司名称','贵州海洋网','地州两网','两网总计','腾豹总计','小计']))& (~All_datas['属性'].isin(['合计','总计'])) & (All_datas['值']>0)]
    All_datas.to_excel(r'C:\Users\13111\code\dashboard\tiche_concat.xlsx', index=False)
    all_dfs.to_excel(r'C:\Users\13111\code\dashboard\提车明细.xlsx', index=False)
    All_datas.to_excel(r'C:\Users\13111\Documents\WXWork\1688855282576011\WeDrive\成都永乐盛世\维护文件\年度提车.xlsx', index=False)