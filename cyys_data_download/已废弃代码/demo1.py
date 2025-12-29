import pandas as pd
import numpy as np
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

# 文件路径
file_path = r"C:\Users\刘洋\Documents\WXWork\1688858189749305\Cache\File\2025-12\data.xlsx"


def process_monthly_data(file_path):
    """
    处理月度数据，汇总成指定格式
    """
    print("开始处理月度汇总数据...")
    print("-" * 60)

    # 定义需要的中心列表（按照要求的顺序）
    centers = [
        '总裁办', '电商中心', '研发中心', '注会及税务师项目', '财务中心',
        '市场中心', '销售中心', '之了会计研究院', '教学中心', '继续教育学历提升项目',
        '之了专升本', '运营中心', '行政中心', '产品中心', '人力资源中心',
        '学员管理中心', '之了（云南）教育'
    ]

    # 月份名称（假设sheet1到sheet11对应1月到11月）
    months = [f"{i}月" for i in range(1, 12)]

    try:
        # 读取所有sheet
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        print(f"找到以下sheet: {sheet_names}")

        # 假设前11个sheet是月度数据
        monthly_sheets = sheet_names[:11] if len(sheet_names) >= 11 else sheet_names

        # 初始化汇总数据结构
        amount_data = []  # 金额数据
        count_data = []  # 打车次数数据
        people_data = []  # 用车人数数据

        # 处理每个月的sheet
        monthly_summaries = {}

        for i, sheet_name in enumerate(monthly_sheets):
            month_num = i + 1
            month_name = f"{month_num}月"
            print(f"正在处理 {sheet_name} ({month_name})...")

            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)

                # 标准化列名
                df.columns = [str(col).strip() for col in df.columns]

                # 检查必要的列是否存在，如果不存在则尝试重命名
                if '乘车人姓名' not in df.columns:
                    for col in df.columns:
                        if '姓名' in col or 'name' in col.lower():
                            df = df.rename(columns={col: '乘车人姓名'})
                            break

                if '企业应付金额' not in df.columns:
                    for col in df.columns:
                        if '金额' in col or '应付' in col:
                            df = df.rename(columns={col: '企业应付金额'})
                            break

                if '中心' not in df.columns:
                    for col in df.columns:
                        if '中心' in col or '部门' in col:
                            df = df.rename(columns={col: '中心'})
                            break

                # 只保留需要的列
                required_cols = ['乘车人姓名', '企业应付金额', '中心']
                available_cols = [col for col in required_cols if col in df.columns]

                if len(available_cols) >= 2:
                    # 确保金额是数值类型
                    if '企业应付金额' in df.columns:
                        df['企业应付金额'] = pd.to_numeric(df['企业应付金额'], errors='coerce')

                    # 清理中心名称
                    if '中心' in df.columns:
                        df['中心'] = df['中心'].astype(str).str.strip()

                    # 按中心汇总本月数据
                    monthly_data = {}

                    # 金额汇总
                    if '企业应付金额' in df.columns and '中心' in df.columns:
                        amount_sum = df.groupby('中心')['企业应付金额'].sum()
                        for center, amount in amount_sum.items():
                            if center not in monthly_data:
                                monthly_data[center] = {'amount': 0, 'count': 0, 'people': set()}
                            monthly_data[center]['amount'] = amount

                    # 打车次数汇总（按中心统计行数）
                    if '中心' in df.columns:
                        count_sum = df.groupby('中心').size()
                        for center, count in count_sum.items():
                            if center not in monthly_data:
                                monthly_data[center] = {'amount': 0, 'count': 0, 'people': set()}
                            monthly_data[center]['count'] = count

                    # 用车人数汇总（按中心去重统计乘车人姓名）
                    if '乘车人姓名' in df.columns and '中心' in df.columns:
                        # 确保姓名是字符串类型
                        df['乘车人姓名'] = df['乘车人姓名'].astype(str).str.strip()
                        people_count = df.groupby('中心')['乘车人姓名'].apply(lambda x: set(x))
                        for center, people_set in people_count.items():
                            if center not in monthly_data:
                                monthly_data[center] = {'amount': 0, 'count': 0, 'people': set()}
                            monthly_data[center]['people'] = people_set

                    monthly_summaries[month_name] = monthly_data
                    print(f"  {month_name}: 处理了 {len(df)} 条记录，涉及 {len(monthly_data)} 个中心")

                else:
                    print(f"  跳过 {sheet_name}: 缺少必要的数据列")

            except Exception as e:
                print(f"  处理 {sheet_name} 时出错: {e}")

        # 创建汇总表格
        print("\n创建汇总表格...")

        # 1. 金额汇总表
        amount_rows = []
        for center in centers:
            row = ['金额', center]
            for month in months:
                if month in monthly_summaries and center in monthly_summaries[month]:
                    value = monthly_summaries[month][center]['amount']
                    # 格式化为两位小数，如果是0则不显示
                    if pd.isna(value) or value == 0:
                        row.append('')
                    else:
                        row.append(round(value, 2))
                else:
                    row.append('')
            amount_rows.append(row)

        # 2. 打车次数汇总表
        count_rows = []
        for center in centers:
            row = ['打车次数', center]
            for month in months:
                if month in monthly_summaries and center in monthly_summaries[month]:
                    value = monthly_summaries[month][center]['count']
                    if pd.isna(value) or value == 0:
                        row.append('')
                    else:
                        row.append(int(value))
                else:
                    row.append('')
            count_rows.append(row)

        # 3. 用车人数汇总表
        people_rows = []
        for center in centers:
            row = ['用车人数', center]
            for month in months:
                if month in monthly_summaries and center in monthly_summaries[month]:
                    value = len(monthly_summaries[month][center]['people'])
                    if value == 0:
                        row.append('')
                    else:
                        row.append(value)
                else:
                    row.append('')
            people_rows.append(row)

        # 创建最终的DataFrame
        columns = ['类型', '中心'] + months

        amount_df = pd.DataFrame(amount_rows, columns=columns)
        count_df = pd.DataFrame(count_rows, columns=columns)
        people_df = pd.DataFrame(people_rows, columns=columns)

        # 在三个表之间添加空行
        empty_row = pd.DataFrame([[''] * len(columns)], columns=columns)

        # 合并所有数据
        final_df = pd.concat([
            amount_df,
            empty_row,
            count_df,
            empty_row,
            people_df
        ], ignore_index=True)

        # 添加汇总行（总计）
        summary_rows = []

        # 金额总计
        total_amount_row = ['金额', '总计']
        for i, month in enumerate(months):
            col_idx = i + 2  # 跳过类型和中心列
            month_total = amount_df[month].apply(lambda x: 0 if x == '' else x).sum()
            total_amount_row.append(round(month_total, 2) if month_total > 0 else '')

        # 次数总计
        total_count_row = ['打车次数', '总计']
        for i, month in enumerate(months):
            col_idx = i + 2
            month_total = count_df[month].apply(lambda x: 0 if x == '' else x).sum()
            total_count_row.append(int(month_total) if month_total > 0 else '')

        # 人数总计（需要去重，按中心合并所有月份的人数）
        # 这里我们计算每个中心在所有月份的总人数（去重）
        # 先创建中心到人员集合的映射
        center_people = {}
        for month in months:
            if month in monthly_summaries:
                for center, data in monthly_summaries[month].items():
                    if center not in center_people:
                        center_people[center] = set()
                    center_people[center].update(data['people'])

        # 计算每个月的总人数（按中心统计）
        total_people_row = ['用车人数', '总计']
        for month in months:
            month_people = set()
            if month in monthly_summaries:
                for center, data in monthly_summaries[month].items():
                    month_people.update(data['people'])
            total_people_row.append(len(month_people) if month_people else '')

        # 添加总计行
        summary_df = pd.DataFrame([total_amount_row, total_count_row, total_people_row], columns=columns)

        # 合并总计行到最终表格
        final_df = pd.concat([final_df, empty_row, summary_df], ignore_index=True)

        # 计算每个中心的年度总计（作为新列添加）
        print("\n计算年度总计...")

        # 为每个中心计算年度总计
        annual_summary = []

        for i, center in enumerate(centers):
            # 金额年度总计
            center_amount_total = 0
            for month in months:
                if month in monthly_summaries and center in monthly_summaries[month]:
                    center_amount_total += monthly_summaries[month][center]['amount']

            # 次数年度总计
            center_count_total = 0
            for month in months:
                if month in monthly_summaries and center in monthly_summaries[month]:
                    center_count_total += monthly_summaries[month][center]['count']

            # 人数年度总计（所有月份去重）
            center_people_set = set()
            for month in months:
                if month in monthly_summaries and center in monthly_summaries[month]:
                    center_people_set.update(monthly_summaries[month][center]['people'])
            center_people_total = len(center_people_set)

            annual_summary.append({
                '中心': center,
                '年度金额总计': round(center_amount_total, 2) if center_amount_total > 0 else 0,
                '年度次数总计': center_count_total,
                '年度人数总计': center_people_total
            })

        annual_df = pd.DataFrame(annual_summary)

        return final_df, amount_df, count_df, people_df, annual_df, monthly_summaries

    except Exception as e:
        print(f"处理文件时出错: {e}")
        return None, None, None, None, None, None


def save_results(final_df, annual_df, file_path):
    """
    保存结果到Excel文件
    """
    # 创建输出文件名
    input_path = Path(file_path)
    output_path = input_path.parent / f"{input_path.stem}_月度汇总.xlsx"

    print(f"\n正在保存结果到: {output_path}")

    # 使用ExcelWriter创建文件
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 保存汇总表
        final_df.to_excel(writer, sheet_name='月度汇总', index=False)

        # 保存年度统计
        annual_df.to_excel(writer, sheet_name='年度统计', index=False)

        # 调整列宽
        workbook = writer.book
        worksheet = writer.sheets['月度汇总']

        # 设置列宽
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 30)
            worksheet.column_dimensions[column_letter].width = adjusted_width

        # 为年度统计设置格式
        worksheet2 = writer.sheets['年度统计']
        for column in worksheet2.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 20)
            worksheet2.column_dimensions[column_letter].width = adjusted_width

    print(f"结果已保存到: {output_path}")
    return output_path


def generate_summary_report(monthly_summaries, centers, months):
    """
    生成汇总报告
    """
    report = []
    report.append("=" * 80)
    report.append("月度用车数据汇总报告")
    report.append("=" * 80)

    # 统计每个月的总数据
    report.append("\n月度总览:")
    report.append("-" * 80)
    report.append(f"{'月份':<8} {'总金额(¥)':<15} {'总次数':<10} {'总人数':<10} {'涉及中心数':<12}")
    report.append("-" * 80)

    for month in months:
        if month in monthly_summaries:
            month_data = monthly_summaries[month]
            total_amount = sum(data['amount'] for data in month_data.values())
            total_count = sum(data['count'] for data in month_data.values())

            # 计算总人数（所有中心去重）
            all_people = set()
            for data in month_data.values():
                all_people.update(data['people'])
            total_people = len(all_people)

            center_count = len(month_data)

            report.append(f"{month:<8} {total_amount:>15,.2f} {total_count:>10} {total_people:>10} {center_count:>12}")

    # 统计年度热门中心（按金额）
    report.append("\n\n年度热门中心（按金额排名）:")
    report.append("-" * 80)

    center_totals = {}
    for center in centers:
        center_total = 0
        for month in months:
            if month in monthly_summaries and center in monthly_summaries[month]:
                center_total += monthly_summaries[month][center]['amount']
        center_totals[center] = center_total

    # 按金额排序
    sorted_centers = sorted(center_totals.items(), key=lambda x: x[1], reverse=True)

    report.append(f"{'排名':<5} {'中心':<20} {'年度总金额(¥)':<15}")
    report.append("-" * 40)

    for i, (center, amount) in enumerate(sorted_centers[:10], 1):
        if amount > 0:
            report.append(f"{i:<5} {center:<20} {amount:>15,.2f}")

    # 统计年度用车最频繁的中心（按次数）
    report.append("\n\n年度用车最频繁中心（按次数排名）:")
    report.append("-" * 80)

    center_counts = {}
    for center in centers:
        center_count_total = 0
        for month in months:
            if month in monthly_summaries and center in monthly_summaries[month]:
                center_count_total += monthly_summaries[month][center]['count']
        center_counts[center] = center_count_total

    # 按次数排序
    sorted_centers_count = sorted(center_counts.items(), key=lambda x: x[1], reverse=True)

    report.append(f"{'排名':<5} {'中心':<20} {'年度总次数':<10}")
    report.append("-" * 40)

    for i, (center, count) in enumerate(sorted_centers_count[:10], 1):
        if count > 0:
            report.append(f"{i:<5} {center:<20} {count:>10}")

    # 计算总体统计
    report.append("\n\n年度总体统计:")
    report.append("-" * 80)

    total_all_amount = sum(center_totals.values())
    total_all_count = sum(center_counts.values())

    # 计算总人数（所有中心所有月份去重）
    all_people_all_months = set()
    for month in months:
        if month in monthly_summaries:
            for data in monthly_summaries[month].values():
                all_people_all_months.update(data['people'])
    total_all_people = len(all_people_all_months)

    report.append(f"年度总用车金额: ¥{total_all_amount:,.2f}")
    report.append(f"年度总用车次数: {total_all_count:,} 次")
    report.append(f"年度总用车人数: {total_all_people:,} 人")

    if total_all_count > 0:
        report.append(f"平均每次用车金额: ¥{total_all_amount / total_all_count:.2f}")

    if total_all_people > 0:
        report.append(f"人均用车次数: {total_all_count / total_all_people:.2f} 次/人")
        report.append(f"人均用车金额: ¥{total_all_amount / total_all_people:.2f}")

    return "\n".join(report)


def main():
    """
    主函数
    """
    print("开始处理月度汇总数据...")
    print("=" * 80)

    # 检查文件是否存在
    if not Path(file_path).exists():
        print(f"错误: 文件不存在 - {file_path}")
        print("请检查文件路径是否正确")
        return

    # 处理数据
    final_df, amount_df, count_df, people_df, annual_df, monthly_summaries = process_monthly_data(file_path)

    if final_df is not None:
        # 显示汇总表格的前几行
        print("\n汇总表格预览:")
        print("-" * 80)
        print(final_df.head(40).to_string(index=False))

        # 生成报告
        centers = [
            '总裁办', '电商中心', '研发中心', '注会及税务师项目', '财务中心',
            '市场中心', '销售中心', '之了会计研究院', '教学中心', '继续教育学历提升项目',
            '之了专升本', '运营中心', '行政中心', '产品中心', '人力资源中心',
            '学员管理中心', '之了（云南）教育'
        ]
        months = [f"{i}月" for i in range(1, 12)]

        report = generate_summary_report(monthly_summaries, centers, months)
        print(report)

        # 保存结果
        output_file = save_results(final_df, annual_df, file_path)

        print("\n" + "=" * 80)
        print("处理完成！")
        print(f"汇总表格已保存到: {output_file}")
        print("=" * 80)

        # 显示一些额外信息
        print("\n输出文件包含以下内容:")
        print("1. '月度汇总' sheet: 完整的月度汇总表格")
        print("2. '年度统计' sheet: 每个中心的年度总计")
        print("\n表格格式说明:")
        print("- 类型: 金额/打车次数/用车人数")
        print("- 中心: 部门名称")
        print("- 1月-11月: 各月份的数据")
        print("- 空行: 不同类型之间的分隔")
        print("- 总计行: 每列的汇总")

    else:
        print("数据处理失败，请检查文件格式和数据内容")


if __name__ == "__main__":
    main()