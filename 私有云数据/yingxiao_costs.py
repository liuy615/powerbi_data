import pandas as pd
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional


class YingxiaoMoneyProcessor:
    """营销投放费用数据处理器：多线程读取Excel、合并清洗后输出CSV"""

    def __init__(self, base_data_dir: str = r"E:\powerbi_data\看板数据"):
        # 路径配置（输入输出集中）
        self.input_dir = os.path.join(base_data_dir, "私有云文件本地", "投放市场费用")
        self.output_dir = os.path.join(base_data_dir, "dashboard")
        self.output_file = os.path.join(self.output_dir, "投放费用.csv")

        # 业务配置（常量集中）
        self.target_sheets = ["2024年", "2025年"]
        self.required_columns = [
            "年份", "月份", "归属门店", "项目大类", "项目分类", "具体项目",
            "费用金额", "核销发票金额", "核销发票税金", "费用合计", "备注", "From"
        ]
        self.store_map = {"文景初治": "上元盛世"}

        # 初始化（抑制警告+环境检查）
        self._suppress_warnings()
        self._init_check()

    def _suppress_warnings(self) -> None:
        """抑制openpyxl数据验证警告"""
        warnings.filterwarnings(
            "ignore",
            message="Data Validation extension is not supported and will be removed",
            category=UserWarning
        )

    def _init_check(self) -> None:
        """环境检查：确保输入目录存在，创建输出目录"""
        if not os.path.exists(self.input_dir):
            raise FileNotFoundError(f"输入目录不存在：{self.input_dir}")
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"【初始化完成】输入：{os.path.basename(self.input_dir)} | 输出：{os.path.basename(self.output_dir)}")

    # -------------------------- 核心逻辑（日志精简） --------------------------
    def _get_excel_files(self) -> List[str]:
        """获取所有Excel文件路径（精简日志：只输总数）"""
        excel_files = [
            os.path.join(self.input_dir, f)
            for f in os.listdir(self.input_dir)
            if f.endswith(".xlsx")
        ]
        print(f"【文件扫描】找到 {len(excel_files)} 个Excel文件")
        return excel_files

    def _process_single_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """处理单个文件（精简日志：只输结果，不输工作表检查细节）"""
        file_name = os.path.basename(file_path)
        try:
            dfs = []
            with pd.ExcelFile(file_path) as xls:
                # 只处理目标工作表，不打印每个工作表检查
                for sheet in self.target_sheets:
                    if sheet in xls.sheet_names:
                        df = pd.read_excel(xls, sheet)
                        df["From"] = file_name.split('.')[0]
                        if not df.empty:
                            dfs.append(df)

            if dfs:
                df_comb = pd.concat(dfs, axis=0, ignore_index=True)
                # 只打印关键结果：文件名+合并行数
                print(f"  处理{file_name}：成功（{len(df_comb)}行）")
                return df_comb
            else:
                print(f"  处理{file_name}：无目标工作表数据")
                return None
        except Exception as e:
            print(f"  处理{file_name}：失败（{str(e)[:50]}...）")  # 错误信息截断
            return None

    def _process_all_files(self, excel_files: List[str]) -> pd.DataFrame:
        """多线程处理所有文件（精简日志：合并结果只输总数）"""
        print(f"【多线程处理】启动（{len(excel_files)}个文件）")
        combined = []

        with ThreadPoolExecutor() as executor:
            results = executor.map(self._process_single_file, excel_files)
            for res in results:
                if res is not None:
                    combined.append(res)

        if not combined:
            raise ValueError("无有效数据可合并")

        df_all = pd.concat(combined, axis=0, ignore_index=True)
        print(f"【合并完成】总计 {len(df_all)} 行数据")
        return df_all

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据清洗（精简日志：合并步骤输出）"""
        # 筛选列
        exist_cols = [c for c in self.required_columns if c in df.columns]
        df_clean = df[exist_cols].copy()

        # 处理费用合计
        if "费用合计" in df_clean.columns and "费用金额" in df_clean.columns:
            df_clean["费用合计"] = df_clean["费用合计"].fillna(df_clean["费用金额"])
            df_clean["费用合计"] = pd.to_numeric(df_clean["费用合计"], errors="coerce").fillna(0)

        # 精简日志：只输最终结果
        print(f"【数据清洗】完成（{len(df_clean)}行，保留{len(exist_cols)}列）")
        return df_clean

    def _replace_store(self, df: pd.DataFrame) -> pd.DataFrame:
        """替换门店名称（精简日志）"""
        if "归属门店" in df.columns:
            df["归属门店"] = df["归属门店"].replace(self.store_map)
            print(f"【门店替换】完成（{list(self.store_map.keys())[0]}→{list(self.store_map.values())[0]}）")
        return df

    def _save_result(self, df: pd.DataFrame) -> None:
        """保存结果（精简日志）"""
        df.to_csv(self.output_file, index=False, encoding="utf-8-sig")
        print(f"【结果保存】成功：{os.path.basename(self.output_file)}（{len(df)}行）")

    # -------------------------- 主执行入口 --------------------------
    def run(self) -> None:
        """完整流程（紧凑日志串联）"""
        print("=" * 50)
        print("【营销投放费用处理】开始")
        print("=" * 50)

        try:
            excel_files = self._get_excel_files()
            df_all = self._process_all_files(excel_files)
            df_clean = self._clean_data(df_all)
            df_final = self._replace_store(df_clean)
            self._save_result(df_final)

            print("\n" + "=" * 50)
            print("【所有任务】执行完成！")
            print("=" * 50)
        except Exception as e:
            print(f"\n【错误】{str(e)}")
            raise


if __name__ == "__main__":
    processor = YingxiaoMoneyProcessor()
    processor.run()