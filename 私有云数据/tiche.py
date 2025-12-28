import pandas as pd
import os
from datetime import datetime
import warnings
from typing import List, Tuple, Optional, Dict


class SalesDataProcessor:
    """
    销售数据处理器：用于处理整车采购数据、筛选时间区间、统计分析并与历史提车数据合并
    """

    def __init__(self, base_dir: str = r"E:\powerbi_data\看板数据\dashboard"):
        """
        初始化处理器，集中管理文件路径、配置参数和映射关系
        :param base_dir: 基础目录，输入输出文件将围绕此目录组织
        """
        # 1. 路径集中管理（输入输出文件集中）
        self.base_dir = base_dir
        self.raw_data_path = r"E:\powerbi_data\看板数据\私有云文件本地\厂家采购\整车采购明细导出.xlsx"  # 原始数据路径（固定）
        self.dedup_data_path = os.path.join(base_dir, "去重后的数据.csv")  # 去重后数据
        self.tiche_history_path = os.path.join(base_dir, "merged_tiche_data.xlsx")  # 历史提车数据
        self.output_tiche_concat = os.path.join(base_dir, "tiche_concat.xlsx")  # 合并结果
        self.output_detail = os.path.join(base_dir, "提车明细.xlsx")  # 提车明细
        # self.output_annual = os.path.join(
        #     r"C:\Users\13111\Documents\WXWork\1688855282576011\WeDrive\成都永乐盛世\维护文件",
        #     "年度提车.xlsx"
        # )  # 年度提车数据

        # 2. 核心配置参数
        self.company_mapping: Dict[str, str] = {
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
            "成都上元盛世汽车销售服务有限公司": "上元盛世",
            "绵阳新港鑫泽汽车销售服务有限公司": "绵阳新港鑫泽"
        }
        self.date_ranges: List[Tuple[str, str]] = [
            ("2025-07-26", "2025-08-25"),
            ("2025-08-26", "2025-09-25"),
            ("2025-09-26", "2025-10-25"),
        ]  # 日期范围配置
        self.exclude_attributes: List[str] = [
            '2025年8月', '2025年9月', '2025年10月', '2025年11月', '2025年12月'
        ]  # 需排除的历史提车属性
        self.exclude_companies: List[str] = [
            '方程豹', '海洋网', '王朝网', '腾势', '方程豹总计', '海洋网总计',
            '王朝网总计', '腾势总计', '总计', '公司名称', '贵州海洋网',
            '地州两网', '两网总计', '腾豹总计', '小计'
        ]  # 需排除的公司名称
        self.exclude_property: List[str] = ['合计', '总计']  # 需排除的属性值

        # 3. 初始化设置
        self._init_environment()
        self._suppress_warnings()

    def _init_environment(self) -> None:
        """初始化环境：确保输出目录存在"""
        os.makedirs(self.base_dir, exist_ok=True)
        print(f"【初始化】基础目录: {self.base_dir}")
        print(f"【初始化】原始数据路径: {os.path.basename(self.raw_data_path)}")
        print(f"【初始化】输出文件将保存至基础目录")

    def _suppress_warnings(self) -> None:
        """屏蔽openpyxl的样式警告"""
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default"
        )

    def _read_raw_data(self) -> pd.DataFrame:
        """读取原始整车采购数据"""
        if not os.path.exists(self.raw_data_path):
            raise FileNotFoundError(f"原始数据文件不存在: {self.raw_data_path}")

        try:
            df = pd.read_excel(self.raw_data_path, sheet_name=0)
            print(f"【数据读取】成功加载原始数据（{len(df)}行）")
            return df
        except Exception as e:
            raise RuntimeError(f"读取原始数据失败: {str(e)}") from e

    def _deduplicate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """对数据进行去重（按销服订单号）并保存去重后的数据"""
        # 按销服订单号去重，保留最后一条
        dedup_df = df.drop_duplicates(subset=['销服订单号'], keep='last').copy()
        # 保存去重后的数据
        dedup_df.to_csv(self.dedup_data_path, index=False)
        print(
            f"【数据去重】完成（去重前{len(df)}行，去重后{len(dedup_df)}行），已保存至: {os.path.basename(self.dedup_data_path)}")
        return dedup_df

    def _process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理日期列：用计划日期填充考核计划日期的空值"""
        date_col = '考核计划日期'
        backup_date_col = '计划日期'

        # 转换为日期格式
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df[backup_date_col] = pd.to_datetime(df[backup_date_col], errors='coerce')

        # 填充空值
        df[date_col] = df[date_col].fillna(df[backup_date_col])
        print(f"【日期处理】完成（{date_col}空值已用{backup_date_col}填充）")
        return df

    def _filter_by_date_range(self, df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
        """按日期范围筛选数据"""
        date_col = '考核计划日期'
        # 转换筛选日期（结束日期包含当天最后一秒）
        start_ts = pd.to_datetime(start_date, format='%Y-%m-%d')
        end_ts = pd.to_datetime(end_date, format='%Y-%m-%d') + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        # 筛选数据
        filtered_df = df[
            (df[date_col] >= start_ts) &
            (df[date_col] <= end_ts)
            ].copy()

        # 处理公司名称映射
        filtered_df['公司名称'] = filtered_df['经销商名称'].map(
            self.company_mapping
        ).fillna(filtered_df['经销商名称'])

        # 添加属性（年月）
        year, month = end_date.split('-')[0], int(end_date.split('-')[1])
        filtered_df['属性'] = f"{year}年{month}月"

        print(f"【日期筛选】{start_date}至{end_date}：{len(filtered_df)}行数据")
        return filtered_df

    def _group_and_stat(self, df: pd.DataFrame) -> pd.DataFrame:
        """按公司和属性分组统计数量"""
        if df.empty:
            return pd.DataFrame(columns=['公司名称', '属性', '值'])

        # 分组统计
        stat_df = df.groupby(['公司名称', '属性']).size().reset_index(name='值')
        # 排序并调整列顺序
        stat_df = stat_df.sort_values('值', ascending=False)[['公司名称', '属性', '值']]
        return stat_df

    def _read_tiche_history(self) -> pd.DataFrame:
        """读取历史提车数据并过滤指定属性"""
        if not os.path.exists(self.tiche_history_path):
            raise FileNotFoundError(f"历史提车数据文件不存在: {self.tiche_history_path}")

        try:
            df = pd.read_excel(self.tiche_history_path, sheet_name='Sheet1')
            # 过滤不需要的属性
            df_filtered = df[~df['属性'].isin(self.exclude_attributes)].copy()
            print(f"【历史数据】加载完成（原始{len(df)}行，过滤后{len(df_filtered)}行）")
            return df_filtered
        except Exception as e:
            raise RuntimeError(f"读取历史提车数据失败: {str(e)}") from e

    def _merge_with_history(self, current_data: pd.DataFrame, history_data: pd.DataFrame) -> pd.DataFrame:
        """合并当前统计数据与历史提车数据"""
        merged_df = pd.concat([current_data, history_data], join='outer', ignore_index=True)
        print(f"【数据合并】完成（当前数据{len(current_data)}行 + 历史数据{len(history_data)}行）")
        return merged_df

    def _clean_final_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗最终数据：处理值列、过滤无效行"""
        # 处理值列（转换为数值并填充空值）
        df['值'] = pd.to_numeric(df['值'], errors='coerce').fillna(0).astype(int)

        # 过滤无效行
        cleaned_df = df[
            (df['公司名称'].notna()) &
            (~df['公司名称'].isin(self.exclude_companies)) &
            (~df['属性'].isin(self.exclude_property)) &
            (df['值'] > 0)
            ].copy()

        print(f"【数据清洗】完成（清洗前{len(df)}行，清洗后{len(cleaned_df)}行）")
        return cleaned_df

    def _save_results(self, final_data: pd.DataFrame, detail_data: pd.DataFrame) -> None:
        """保存所有结果文件"""
        # 保存合并结果
        final_data.to_excel(self.output_tiche_concat, index=False)
        print(f"【结果保存】合并数据: {os.path.basename(self.output_tiche_concat)}")

        # 保存提车明细
        detail_data.to_excel(self.output_detail, index=False)
        print(f"【结果保存】提车明细: {os.path.basename(self.output_detail)}")

        # 保存年度提车数据
        # final_data.to_excel(self.output_annual, index=False)
        # print(f"【结果保存】年度提车数据: {os.path.basename(self.output_annual)}")

    def run(self) -> None:
        """执行完整的数据处理流程"""
        try:
            print("=" * 60)
            print("【销售数据处理器】开始运行")
            print("=" * 60)

            # 1. 读取并处理原始数据
            raw_df = self._read_raw_data()
            dedup_df = self._deduplicate_data(raw_df)
            date_processed_df = self._process_dates(dedup_df)

            # 2. 按日期范围处理并统计
            all_stat_data = []
            all_detail_data = []
            for start, end in self.date_ranges:
                filtered_df = self._filter_by_date_range(date_processed_df, start, end)
                stat_df = self._group_and_stat(filtered_df)
                all_stat_data.append(stat_df)
                all_detail_data.append(filtered_df)

            # 3. 合并统计结果和明细数据
            combined_stat = pd.concat(all_stat_data, ignore_index=True)
            combined_detail = pd.concat(all_detail_data, ignore_index=True)

            # 4. 处理历史数据并合并
            tiche_history = self._read_tiche_history()
            merged_all = self._merge_with_history(combined_stat, tiche_history)

            # 5. 清洗最终数据并保存
            cleaned_final = self._clean_final_data(merged_all)
            self._save_results(cleaned_final, combined_detail)

            print("\n" + "=" * 60)
            print("【销售数据处理器】运行完成")
            print("=" * 60)

        except Exception as e:
            print(f"\n【错误】处理流程中断: {str(e)}")
            raise


if __name__ == "__main__":
    # 实例化处理器并执行（默认使用基础目录，可自定义）
    processor = SalesDataProcessor()
    processor.run()