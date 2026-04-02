import pandas as pd

def generate_2026_summary(source_path: str) -> pd.DataFrame:
    """
    从源数据生成2026年的汇总表（新保保费、新保产值、新车驾意险保费、新车驾意险产值）
    """
    df_src = pd.read_excel(source_path, sheet_name='Sheet1')
    df_src['出单日期'] = pd.to_datetime(df_src['出单日期']).dt.date

    # 筛选2026年的数据（可选，但源数据可能只有2026年，不过还是明确筛选）
    df_src_2026 = df_src[df_src['出单日期'] >= pd.to_datetime('2026-01-01').date()]

    # 计算指标
    df_src_2026['新保车险收入'] = df_src_2026['新保商业险理论费率收入'] + df_src_2026['新保交强险理论费率收入']
    df_src_2026['新保非车险收入'] = (
        df_src_2026['新保驾乘险理论费率收入'] +
        (df_src_2026['新保阳光驾乘险保费'] - df_src_2026['新保阳光驾乘险成本']) +
        (df_src_2026['新保阳光意外险保费'] - df_src_2026['新保阳光意外险成本'])
    )
    df_src_2026['新保产值'] = df_src_2026['新保车险收入'] + df_src_2026['新保非车险收入']
    df_src_2026['新保保费'] = df_src_2026['新保商业险保费'] + df_src_2026['新保交强险保费']
    df_src_2026['新车驾意险保费'] = df_src_2026['新保驾乘险保费']
    df_src_2026['新车驾意险产值'] = df_src_2026['新保非车险收入']

    # 按日期、门店、保险公司汇总
    group_cols = ['出单日期', '所属门店', '保险公司']
    agg_dict = {
        '新保保费': 'sum',
        '新保产值': 'sum',
        '新车驾意险保费': 'sum',
        '新车驾意险产值': 'sum'
    }
    df_2026 = df_src_2026.groupby(group_cols, as_index=False).agg(agg_dict)

    # 重命名列
    df_2026.rename(columns={
        '出单日期': '签单日期',
        '所属门店': '归属公司',
        '保险公司': '保险公司'
    }, inplace=True)

    return df_2026


def merge_with_history(history_path: str, source_path: str, output_path: str = None):
    """
    读取历史表（仅保留2026年之前的数据），与2026年新生成的数据合并，输出到新文件
    """
    # 1. 读取历史表
    df_history = pd.read_excel(history_path)
    df_history['签单日期'] = pd.to_datetime(df_history['签单日期']).dt.date

    # 只保留2026年之前的数据（注意：若历史表中已存在2026年数据，则舍弃，以新数据为准）
    df_history_before_2026 = df_history[df_history['签单日期'] < pd.to_datetime('2026-01-01').date()]

    # 2. 生成2026年新数据
    df_2026 = generate_2026_summary(source_path)

    # 3. 合并（按日期排序）
    df_merged = pd.concat([df_history_before_2026, df_2026], ignore_index=True)
    df_merged.sort_values('签单日期', inplace=True)

    # 4. 保存
    if output_path is None:
        output_path = "新保数量产值成本表_合并.xlsx"
    df_merged.to_excel(output_path, index=False)
    print(f"合并完成，历史记录数：{len(df_history_before_2026)}，新数据记录数：{len(df_2026)}，合计：{len(df_merged)}")
    print(f"结果保存至：{output_path}")


# 示例调用
if __name__ == "__main__":
    merge_with_history(
        history_path=r"E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源\新保数量产值成本表.xlsx",
        source_path=r"E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源\保险事业部新保收入相关数据.xlsx",
        output_path=r"E:\powerbi_data\看板数据\私有云文件本地\data\售前看板数据源\新保数量产值成本表.xlsx"
    )