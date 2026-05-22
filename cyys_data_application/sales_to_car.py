# 读取销售毛利1.csv中的数据，以此为主表，用车架号匹配以下数据表，数据存放位置：E:\powerbi_data\看板数据\dashboard
# 1. 贴膜升级.csv中筛选有推送日期的，然后通过车架号匹配推送日期，三方返还佣金，数据存放位置：E:\powerbi_data\看板数据\dashboard
# 2. 新车三方延保台账.csv中匹配延保销售日期、车架号、金额，数据存放位置：E:\powerbi_data\看板数据\dashboard
# 3. 精品销售1.csv中匹配精品销售日期、车架号、毛利润，数据存放位置：E:\powerbi_data\看板数据\dashboard
# 4. 保赔无忧.csv中匹配日期、车架号、利润，数据存放位置：E:\powerbi_data\看板数据\dashboard
# 5. 新保驾乘险费率匹配.csv中匹配出单日期、车架号、新保驾乘险理论费率收入，数据存放位置：E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源
# 6. 新保商业险和交强险费率匹配.csv匹配出单日期、车架号、新保交强险理论费率收入、新保商业险理论费率收入，数据存放位置：E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源

import pandas as pd
import os

# ==================== 配置路径 ====================
BASE_DIR = r"E:\powerbi_data\看板数据\dashboard"
BASE_DIR_2 = r"E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源"
MAIN_FILE = os.path.join(r"E:\powerbi_data\看板数据\cyy_old_data", "销售毛利1.csv")  # 主表，如果放在别处请修改
OUTPUT_FILE = os.path.join(BASE_DIR, "销售毛利1.csv")

# ==================== 读取主表 ====================
main_df = pd.read_csv(MAIN_FILE, dtype={"车架号": str})  # 强制车架号为字符串
print(f"主表行数: {len(main_df)}")

# ==================== 辅助函数：安全左连接 ====================
def safe_left_merge(main, right, on, cols, right_name, dedup_on=None):
    """
    左连接并仅保留需要的列，同时处理车架号重复问题。
    如果附表存在同一车架号多条记录，默认保留第一条（可通过 dedup_on 指定排序去重）。
    """
    # 确保车架号类型一致
    right[on] = right[on].astype(str).str.strip()
    main[on] = main[on].astype(str).str.strip()

    # 若需要去重，按指定列排序后保留第一条
    if dedup_on and dedup_on in right.columns:
        right = right.sort_values(dedup_on).drop_duplicates(subset=[on], keep="last")
    else:
        # 无排序依据时，简单去重保留第一条
        right = right.drop_duplicates(subset=[on], keep="first")

    # 只取关键列
    right_sub = right[[on] + cols].copy()
    result = main.merge(right_sub, on=on, how="left")
    print(f"  匹配 [{right_name}]，附表去重后行数: {len(right_sub)}，新增列: {cols}")
    return result

# ==================== 1. 贴膜升级 ====================
file1 = os.path.join(BASE_DIR, "贴膜升级.csv")
df1 = pd.read_csv(file1, dtype={"车架号": str}).rename(columns={'车架号（后6位）': '车架号', '三方返还佣金': '贴膜升级毛利润'})
# 筛选有推送日期的记录
df1 = df1[df1["推送日期"].notna()].copy()
main_df = safe_left_merge(
    main_df, df1, on="车架号",
    cols=["推送日期", "贴膜升级毛利润"],
    right_name="贴膜升级"
)

# ==================== 2. 新车三方延保台账 ====================
file2 = os.path.join(BASE_DIR, "新车三方延保台账.csv")
df2 = pd.read_csv(file2, dtype={"车架号": str}).rename(columns={'金额': '新车三方延保毛利润'})
main_df = safe_left_merge(
    main_df, df2, on="车架号",
    cols=["延保销售日期", "新车三方延保毛利润"],
    right_name="新车三方延保"
)

# ==================== 3. 精品销售1 ====================
file3 = os.path.join(BASE_DIR, "精品销售1.csv")
df3 = pd.read_csv(file3, dtype={"车架号": str}).rename(columns={'毛利润': '精品毛利润'})
main_df = safe_left_merge(
    main_df, df3, on="车架号",
    cols=["精品销售日期", "精品毛利润"],
    right_name="精品销售"
)

# ==================== 4. 保赔无忧 ====================
file4 = os.path.join(BASE_DIR, "保赔无忧.csv")
df4 = pd.read_csv(file4, dtype={"车架号": str}).rename(columns={'日期': '保赔无忧日期', '利润':'保赔无忧利润'})
main_df = safe_left_merge(
    main_df, df4, on="车架号",
    cols=["保赔无忧日期", "保赔无忧利润"],
    right_name="保赔无忧"
)

# ==================== 5. 新保驾乘险费率匹配 ====================
file5 = os.path.join(BASE_DIR_2, "新保驾乘险费率匹配.csv")
df5 = pd.read_csv(file5, dtype={"车架号": str}).rename(columns={'出单日期': '驾乘险出单日期', '新保驾乘险理论费率收入': '驾乘险利润'})
main_df = safe_left_merge(
    main_df, df5, on="车架号",
    cols=["驾乘险出单日期", "驾乘险利润"],
    right_name="新保驾乘险"
)

# ==================== 6. 新保商业险和交强险费率匹配 ====================
file6 = os.path.join(BASE_DIR_2, "新保商业险和交强险费率匹配.csv")
df6 = pd.read_csv(file6, dtype={"车架号": str}).rename(columns={'出单日期': '商业险出单日期', '新保交强险理论费率收入': '交强险利润', '新保商业险理论费率收入': '商业险利润'})
main_df = safe_left_merge(
    main_df, df6, on="车架号",
    cols=["商业险出单日期", "交强险利润", "商业险利润"],
    right_name="新保商业险+交强险"
)

# ==================== 保存结果 ====================
main_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"\n完成！结果已保存至: {OUTPUT_FILE}")
print(f"最终表行数: {len(main_df)}，列数: {len(main_df.columns)}")