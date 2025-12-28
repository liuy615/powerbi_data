# -*- coding: utf-8 -*-
"""
    cyys.py
    爬取车易云商相关数据
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from datetime import datetime
import pandas as pd
import logging
import numpy as np
import re
import shutil
pd.set_option('display.max_columns', 100)

# 日志配置
log_dir = r"E:\pycharm_project\powerbi\data\log"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(message)s]',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{log_dir}/log_api{datetime.now().strftime('%Y_%m_%d')}.log", encoding='utf-8')
    ]
)


encoding_cache = {}
class cyys:
    def __init__(self):
        self.tk = ""
        self._uuid = ""
        self.url = ""
        # 重新登录的字符串
        self.relogin_str = '{"loginstatus":-1,"Msg":"可能长时间没有操作，要继续使用请重新登录！"}'
        self.page = None
        self.list_companys = ['成都新港建元汽车销售服务有限公司', '成都永乐盛世汽车销售服务有限公司', '成都新港永初汽车服务有限公司', '成都新港海川汽车销售服务有限公司', '成都新港先秦汽车服务有限公司',
                '成都新港治元汽车销售服务有限公司', '成都新港建隆汽车销售服务有限公司', '成都文景初治新能源汽车销售有限公司', '成都新港建武汽车销售服务有限公司',
                '成都新港文景海洋汽车销售服务有限公司', '成都文景盛世汽车销售服务有限公司', '成都新港澜舰汽车销售服务有限公司', '成都新港澜阔汽车销售服务有限公司',
                '成都鑫港鲲鹏汽车销售服务有限公司', '成都新茂元大汽车销售服务有限公司', '成都新港澜轩汽车销售服务有限公司', '成都新港浩蓝汽车销售服务有限公司', '贵州新港蔚蓝汽车销售服务有限责任公司',
                '贵州新港浩蓝汽车销售服务有限责任公司', '贵州新港澜源汽车服务有限责任公司', '贵州新港海之辇汽车销售服务有限责任公司', '成都新港上元坤灵汽车销售服务有限公司', '乐山新港上元曦和汽车销售服务有限公司',
                '宜宾新港上元曦和汽车销售服务有限公司', '泸州新港上元坤灵汽车销售服务有限公司', '贵州新港上元臻智汽车贸易有限公司', '成都新港上元臻智汽车销售服务有限公司', '乐山新港上元臻智汽车销售服务有限公司', '宜宾新港上元臻智汽车销售服务有限公司',
                '成都新港上元臻享汽车销售服务有限公司', '成都新港上元曦和汽车销售服务有限公司', '贵州新港澜轩汽车销售有限责任公司', '贵州新港上元曦和汽车销售服务有限公司', '直播基地']
        pass

    def merge_csv_files(self, folder_path):
        """
        合并指定文件夹中的所有CSV文件（支持多线程加速）

        参数:
        folder_path: str - 包含CSV文件的文件夹路径
        """
        # 获取所有CSV文件路径
        csv_files = [os.path.join(folder_path, f)
                     for f in os.listdir(folder_path)
                     if f.endswith('.csv')]

        if not csv_files:
            print("未找到CSV文件")
            return pd.DataFrame()

        # 多线程读取CSV文件
        def read_csv(file_path):
            try:
                # 尝试多种编码
                encodings = ['utf-8', 'gbk', 'gb2312', 'latin-1']
                for encoding in encodings:
                    try:
                        return pd.read_csv(file_path, encoding=encoding)
                    except UnicodeDecodeError:
                        continue
                # 如果所有编码都失败，尝试使用错误处理
                return pd.read_csv(file_path, encoding='utf-8', errors='replace')
            except Exception as e:
                print(f"读取文件 {file_path} 出错: {str(e)}")
                return pd.DataFrame()

        dfs = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(read_csv, file): file for file in csv_files}
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    dfs.append(df)

        # 合并DataFrame
        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            return merged_df
        else:
            print("没有有效数据可合并")
            return pd.DataFrame()

    def run(self):
        service_net = pd.read_excel(r'C:\Users\刘洋\Documents\WXWork\1688858189749305\WeDrive\成都永乐盛世\维护文件\看板部分数据源\各公司银行额度.xlsx',sheet_name='补充车系')
        # 汇票管理
        df_debit = self.merge_csv_files(r'/看板数据/cyy原始数据/汇票管理')
        # 装饰订单
        df_decoration = self.merge_csv_files(r'/看板数据/cyy原始数据/装饰_订单管理_装饰订单')
        # 衍生订单
        df_books = self.merge_csv_files(r'/看板数据/cyy原始数据/销售_衍生_订单查询')
        # 库存车辆
        df_inventory = self.merge_csv_files(r'/看板数据/cyy原始数据/库存车辆查询')
        # 库存车辆已售
        df_inventory1 = self.merge_csv_files(r'/看板数据/cyy原始数据/库存车辆已售')
        # 车辆成本
        df_carcost = self.merge_csv_files(r'/看板数据/cyy原始数据/车辆成本管理')
        # 按揭业务
        df_loan = self.merge_csv_files(r'/看板数据/cyy原始数据/按揭业务')
        # 套餐销售
        df_service = self.merge_csv_files(r'/看板数据/cyy原始数据/套餐销售列表')
        # 成交订单
        df_books2 = self.merge_csv_files(r'/看板数据/cyy原始数据/销售_车辆销售_成交订单')
        # 车辆销售明细_开票日期
        df_salesAgg = self.merge_csv_files(r'/看板数据/cyy原始数据/车辆销售明细表_开票日期')
        # 计划车辆
        df_plan = self.merge_csv_files(r'/看板数据/cyy原始数据/计划车辆')
        # 作废订单
        tui_dings_df = self.merge_csv_files(r'/看板数据/cyy原始数据/销售_车辆销售_作废订单')
        # 调车结算
        df_diao = self.merge_csv_files(r'/看板数据/cyy原始数据/调车结算查询')
        # 二手车
        df_ershou = self.merge_csv_files(r'/看板数据/cyy原始数据/评估管理_成交')
        df_ershou1 = self.merge_csv_files(r'/看板数据/cyy原始数据/评估管理_已入库')
        # 开票维护
        df_kaipiao = self.merge_csv_files(r'/看板数据/cyy原始数据/开票维护')
        # 保险业务
        df_insurance = self.merge_csv_files(r'/看板数据/cyy原始数据/保险业务')
        # 1. 读取字段对照表
        mapping_df = pd.read_excel(r"E:/powerbi_data/代码执行/data/字段对应.xlsx", sheet_name="Sheet1")

        # 2. 构建映射字典：{df_name: {英文字段: 中文字段}}
        mapping_dict = {}
        for df_name, group in mapping_df.groupby("df_name"):
            mapping_dict[df_name] = dict(zip(group["英文字段"], group["中文字段"]))

        raw_data = {
            "车辆销售明细_开票日期": df_salesAgg,
            "库存车辆查询": df_inventory,
            "库存车辆已售": df_inventory1,
            "计划车辆": df_plan,
            "汇票管理":df_debit,
            "成交订单": df_books2,
            "衍生订单": df_books,
            "车辆成本管理": df_carcost,
            "装饰订单": df_decoration,
            "按揭业务": df_loan,
            "作废订单": tui_dings_df,
            "套餐销售": df_service,
            "调车结算": df_diao,
            "二手车成交":df_ershou,
            "二手车入库":df_ershou1,
            "开票维护":df_kaipiao,
            '保险业务': df_insurance,
        }
        processed_data = {}
        for df_name, df_raw in raw_data.items():
            if df_name in mapping_dict:
                rename_map = mapping_dict[df_name]
                existing_cols = [col for col in rename_map.keys() if col in df_raw.columns]
                df_processed = df_raw[existing_cols].rename(columns=rename_map)
                processed_data[df_name] = df_processed
            else:
                print(f"跳过 {df_name}：无映射规则")
        df_salesAgg = processed_data["车辆销售明细_开票日期"]
        df_inventory = processed_data["库存车辆查询"]
        df_inventory1 = processed_data["库存车辆已售"]
        df_plan = processed_data["计划车辆"]
        df_debit = processed_data["汇票管理"]
        df_books2 = processed_data["成交订单"]
        df_books = processed_data["衍生订单"]
        df_carcost = processed_data["车辆成本管理"]
        df_decoration = processed_data["装饰订单"]
        df_loan = processed_data["按揭业务"]
        tui_dings_df = processed_data["作废订单"]
        df_service = processed_data["套餐销售"]
        df_diao = processed_data["调车结算"]
        df_ershou = processed_data["二手车成交"]
        df_ershou1 = processed_data["二手车入库"]
        df_kaipiao = processed_data["开票维护"]
        df_insurance = processed_data['保险业务']
        print("表处理完结")
        # 清洗新车保险台账
        end_time = datetime.now().strftime("%Y/%m/%d")
        # 运行稳定后取消注释
        df_insurance.to_csv(fr"E:/powerbi_data/看板数据/dashboard/新车保险台账-{datetime.now().year}.csv")
        # 清洗二手车
        df_Ers = pd.concat([df_ershou,df_ershou1],axis=0)
        df_Ers =df_Ers[df_Ers['收款状态']=='已收款']
        df_Ers.to_csv(r"E:/powerbi_data/看板数据/dashboard/二手车.csv")
        # 清洗装饰订单明细
        df_decoration['销售合计'] = df_decoration['销售合计'].astype(float)
        df_decoration = df_decoration[~df_decoration['物资状态'].isin(['已退款','已退货','待退货','已换货'])]
        conditions = [
            (df_decoration['单据类型'] == '新车销售'),
            ((df_decoration['单据类型'].isin(list(['客户增购','换货销售','独立销售']))) & (df_decoration['销售合计'] == 0))
        ]
        combined_condition = conditions[0] | conditions[1]
        df_decoration1 = df_decoration[combined_condition]
        result = df_decoration1.groupby('车架号')['物资名称'].agg(','.join).reset_index()
        df_decoration1.loc[:, ['成本合计(含税)','工时费']] = df_decoration1[['成本合计(含税)','工时费']].astype(float)

        # 解决视图与副本警告
        df_decoration1 = df_decoration1.copy()
        df_decoration1['装饰赠送成本'] = df_decoration1[['成本合计(含税)', '工时费']].sum(axis=1)
        df_decoration2 = df_decoration1.groupby(['车架号'])[['装饰赠送成本', '销售合计']].sum().reset_index().merge(result, on='车架号', how='left')
        df_decoration2.rename(columns={
        '订单门店':'公司名称',
        '装饰赠送成本':'装饰成本',
        '销售合计':'装饰收入',
        '物资名称':'赠送装饰项目'},inplace=True)
        # 精品清洗
        df_jingpin = df_decoration[~combined_condition]
        df_jingpin = df_jingpin[~df_jingpin['销售顾问'].isin(['郑仁彬','刘红梅','衡珊珊'])]
        df_jingpin[['成本合计(含税)','工时费']] = df_jingpin[['成本合计(含税)','工时费']].astype(float)
        df_jingpin['出/退/销数量'] = df_jingpin['出/退/销数量'].astype(int)
        df_jingpin1 = df_jingpin.copy()
        result_JP = df_jingpin1.groupby('订单编号').apply(lambda x: ','.join(f"{name}*{qty}" for name, qty in zip(x['物资名称'], x['出/退/销数量'])), include_groups=False).reset_index(name='物资明细')
        df_jingpin['装饰赠送成本'] = df_jingpin[['成本合计(含税)','工时费']].sum(axis=1)
        # 根据 '订单门店'、'车架号' 和 '开票日期' 进行聚合
        df_jingpin2 = df_jingpin.copy()
        df_jingpin_result = df_jingpin2.groupby(['订单门店', '订单编号','车架号', '开票日期', '销售顾问', '客户名称', '联系电话'])[['装饰赠送成本', '销售合计', '出/退/销数量']].sum().reset_index().merge(result_JP, on='订单编号', how='left')
        df_jingpin_result.rename(
            columns={
                '装饰赠送成本': '总成本',
                '销售合计': '销售总金额',
                '开票日期':'开票日期',
                '销售顾问':'精品销售人员',
                '订单门店':'订单门店',
                '出/退/销数量':'总次数'
            }, inplace=True
        )
        df_jingpin_result['毛利润'] = df_jingpin_result['销售总金额'] - df_jingpin_result['总成本']
        df_jingpin_result = df_jingpin_result[['订单门店','开票日期','精品销售人员','车架号','客户名称','联系电话','物资明细','销售总金额','总成本','毛利润','总次数']]
        df_jingpin_result = df_jingpin_result.astype(str)
        print(len(df_jingpin_result))

        # 清洗套餐明细
        df_service.rename(columns={'领取车架号/车牌号':'车架号'},inplace=True)
        df_service = df_service[(df_service['套餐名称']!='保赔无忧')&(df_service['审批状态']!='审批驳回')&(df_service['订单状态']!='已退卡')&(df_service['订单状态']!='已登记')]
        df_service.loc[:, '实售金额'] = df_service['实售金额'].astype(float)
        df_service = df_service[~((df_service['套餐名称'].str.contains('终身保养')) & (df_service['实售金额'] > 0))]
        df_service = df_service[~(df_service['实售金额'] > 0)]
        df_service['车架号'] = df_service['车架号'].astype(str)
        df_service1 = df_service.copy()
        details_service = df_service1.groupby('车架号').apply(lambda x: ','.join(f"{name}*{qty}" for name, qty in zip(x['套餐名称'], x['总次数'])), include_groups=False).reset_index(name='套餐明细')
        df_service['结算成本'] = df_service['结算成本'].astype(float)
        df_service.rename(columns={'结算成本': '保养升级成本'}, inplace=True)
        df_service2 = df_service.copy()
        # 按照 '联系电话' 列进行聚合，计算每个联系电话的总 '保养升级成本'
        df_service_aggregated = df_service2.groupby('车架号')['保养升级成本'].sum().reset_index().merge(details_service,on='车架号',how='left')
        # 清洗车辆成本
        df_carcost[['车辆成本_返介绍费','其他成本_退代金券','其他成本_退按揭押金']] = df_carcost[['车辆成本_返介绍费','其他成本_退代金券','其他成本_退按揭押金']].astype(float)
        df_carcost.rename(columns={
        '车辆/订单门店':'公司名称',
        '车架号':'车架号',
        '车辆状态':'车辆状态',
        '采购成本_调整项':'调整项',
        '车辆成本_二手车返利':'二手车返利金额',
        '车辆成本_返介绍费':'返介绍费',
        '车辆成本_退成交车辆定金（未抵扣）':'退成交车辆定金（未抵扣）',
        '车辆成本_区补':'政府返回区补',
        '车辆成本_保险返利':'保险返利',
        '车辆成本_终端返利':'终端返利',
        '车辆成本_上牌服务费':'上牌成本',
        '车辆成本_票据事务费':'高开票税费',
        '车辆成本_票据事务费-公司':'票据事务费-公司',
        '车辆成本_综合结算服务费':'代开票支付费用',
        '车辆成本_合作返利':'回扣款',
        '车辆成本_其他成本':'其他成本',
        '其他成本_退代金券':'退代金券',
        '其他成本_退按揭押金':'退按揭押金',
        '其他成本_退置换补贴保证金':'退置换补贴保证金',
        '车辆采购成本_质损费':'质损赔付金额',
        '计划单号':'采购订单号'}, inplace=True)
        logging.info(df_carcost.columns)
        df_carcost['操作日期'] = pd.to_datetime(df_carcost['操作日期'])
        df_carcost.sort_values(by='操作日期',ascending=False,inplace=True)
        df_carcost.drop_duplicates(subset=['车架号'],keep='first',inplace=True)
        df_carcost=df_carcost[['公司名称','采购订单号','车架号','车辆状态','调整项','返介绍费','退成交车辆定金（未抵扣）','政府返回区补','保险返利','终端返利','上牌成本','票据事务费-公司','代开票支付费用','回扣款','退代金券','退按揭押金','退置换补贴保证金','质损赔付金额','其他成本','操作日期','操作日期']]
        # 清洗按揭数据
        df_loan.rename(columns={
        '按揭渠道':'金融性质',
        '贷款总额':'贷款金额',
        '期限':'贷款期限',
        '按揭产品':'金融方案',
        '返利系数':'返利系数',
        '实收金融服务费':'金融服务费',
        '厂家贴息':'厂家贴息金额',
        '公司贴息':'经销商贴息金额',
        '返利金额':'金融返利'
        }, inplace=True)
        df_loan['金融类型'] = np.where(df_loan['金融性质'].str.contains('非贴息'),'厂家非贴息贷',
                                np.where(df_loan['金融性质'].str.contains('贴息'),'厂家贴息贷','非贴息贷'))

        df_loan['返利系数'] = df_loan['返利系数'].str.replace('%', '').astype(float) / 100
        df_loan[['开票价','贷款金额','返利系数','金融返利','厂家贴息金额','经销商贴息金额','金融服务费']] = df_loan[['开票价','贷款金额','返利系数','金融返利','厂家贴息金额','经销商贴息金额','金融服务费']].astype(float)
        df_loan['首付金额'] = df_loan['开票价']-df_loan['贷款金额']
        def remove_chinese(text):
            return re.sub(r'[\u4e00-\u9fa5]', '', text)
        df_loan['贷款期限'] = df_loan['贷款期限'].astype(str)
        # 使用 apply 和 lambda 函数应用 remove_chinese 函数到 '贷款期限' 列
        df_loan['贷款期限'] = df_loan['贷款期限'].apply(remove_chinese)
        df_loan['金融税费'] = df_loan['厂家贴息金额']/1.13*0.13*1.12 + df_loan['金融返利']/1.06*0.06*1.12
        df_loan['金融毛利'] = df_loan['金融返利']-df_loan['经销商贴息金额']-df_loan['金融税费']
        df_loan.sort_values(by=['车架号','收费状态'],inplace=True,ascending=True)
        df_loan = df_loan[df_loan['车架号'].notna()]
        df_loan = df_loan.drop_duplicates(subset=['车架号'], keep='first')
        # 清洗库存车辆
        df_inventory.rename(columns={
        '订单号':'采购订单号',
        '所属门店':'归属系统',
        '车系':'车系',
        '车型':'车型',
        '车架号':'车架号',
        '入库日期':'到库日期',
        '生产日期':'生产日期',
        '配车日期':'配车日期',
        '仓库地':'运输方式存放地点',
        '备注':'备注',
        '合格证':'合格证状态',
        '车辆状态':'车辆状态',
        '库存天数':'库存天数',
        '销售日期':'销售日期',
        '销售人员':'销售顾问',
        '订单客户':'客户姓名',
        '锁库日期':'锁库日期',
        '开票日期':'开票日期',
        '质损信息':'质损信息',
        '调拨日期':'调拨日期',
        '调拨记录':'调拨记录',
        '入库公司':'入库公司',
        '订单来源':'所属团队',
        '订单公司':'匹配定单归属门店',
        '合格证门店':'合格证门店',
        '赎证日期':'赎证日期',
        '出厂价格':'提货价',
        '厂家官价':'指导价'
        }, inplace=True)
        df_inventory = df_inventory[['供应商','采购订单号','归属系统','入库公司','匹配定单归属门店','合格证门店','所属团队','车系','车型','配置','颜色','车架号','指导价','提货价','生产日期','赎证日期','合格证状态','发车日期','到库日期','库存天数','运输方式存放地点','车辆状态','调拨日期','调拨记录','锁库日期','销售日期','开票日期','配车日期','销售顾问','客户姓名','质损信息','备注','操作日期']]
        # 清洗库存已售车辆
        df_inventory1.rename(columns={
        '订单号':'采购订单号',
        '所属门店':'归属系统',
        '车系':'车系',
        '车型':'车型',
        '车架号':'车架号',
        '入库日期':'到库日期',
        '生产日期':'生产日期',
        '配车日期':'配车日期',
        '仓库地':'运输方式存放地点',
        '备注':'备注',
        '合格证':'合格证状态',
        '车辆状态':'车辆状态',
        '库存天数':'库存天数',
        '销售日期':'销售日期',
        '销售人员':'销售顾问',
        '订单客户':'客户姓名',
        '锁库日期':'锁库日期',
        '开票日期':'开票日期',
        '质损信息':'质损信息',
        '调拨日期':'调拨日期',
        '调拨记录':'调拨记录',
        '入库公司':'入库公司',
        '订单来源':'所属团队',
        '订单公司':'匹配定单归属门店',
        '合格证门店':'合格证门店',
        '赎证日期':'赎证日期',
        '出厂价格':'提货价',
        '厂家官价':'指导价'
        }, inplace=True)
        df_inventory1 = df_inventory1[['供应商','采购订单号','归属系统','入库公司','匹配定单归属门店','合格证门店','所属团队','车系','车型','配置','颜色','车架号','指导价','提货价','生产日期','赎证日期','合格证状态','发车日期','到库日期','库存天数','运输方式存放地点','车辆状态','调拨日期','调拨记录','锁库日期','销售日期','开票日期','配车日期','销售顾问','客户姓名','质损信息','备注','操作日期']]
        # 清洗汇票三方数据

        df_debit.rename(columns={'车辆金额':'提货价','开票金额(含税)':'汇票金额','汇票开票日期':'开票日期','VIN码':'车架号','计划单号':'采购订单号','开票银行':'开票银行','所属门店':'合格证门店','汇票到期日期':'到期日期','首付比例':'保证金比例','赎证金额':'赎证款'},inplace=True)
        df_debit['是否赎证'] = np.where(df_debit['是否结清']=='已清',1,0)
        df_debit = df_debit[['合格证门店', '车源门店', '开票日期', '保证金比例', '首付金额', '汇票金额', '到期日期',
            '开票银行', '汇票号', '合格证号', '采购订单号', '车架号', '提货价', '审核状态', '赎证日期', '赎证款',
            '首付单号', '赎证单号', '是否赎证','是否结清']]
        df_debit = pd.merge(df_debit,df_carcost[['采购订单号','车辆状态']],on='采购订单号',how='left')
        df_debit['车辆状态'] = df_debit['车辆状态'].fillna('未发')
        # 清洗计划车辆明细
        df_plan.rename(columns={'车型':'车系','整车型号':'车型','订单号':'采购订单号'},inplace=True)
        df_plan = pd.merge(df_plan,df_debit[['采购订单号','提货价','开票银行','合格证门店','赎证日期','到期日期','保证金比例','赎证款']],on='采购订单号',how='left')
        df_plan['车辆状态'] = '未发'
        df_plan['开票银行'] = df_plan['开票银行'].fillna('公司')
        df_plan.rename(columns={'开票银行':'合格证状态','门店':'归属系统'},inplace=True)
        df_inventory_all = pd.concat([df_inventory,df_plan],axis=0)
        list_company = ['新港建元','永乐盛世','新港永初','新港海川','新港先秦','新港治元','新港建隆','王朝网-直播基地','文景初治','新港建武','文景海洋','文景盛世','新港澜阔','鑫港鲲鹏','新港澜舰',
'新茂元大','新港浩蓝','新港澜轩','贵州新港澜源','贵州新港海之辇','贵州新港浩蓝','贵州新港蔚蓝','上元臻智','上元臻享','贵州上元臻智','腾势-直播基地','宜宾上元臻智','乐山上元臻智','上元坤灵',
'上元曦和','贵州上元曦和','贵州新港澜轩','方程豹-直播基地','宜宾上元曦和','乐山上元曦和','泸州上元坤灵','新港澜洲']
        df_inventory_all['调入类型'] = np.where(
            df_inventory_all['供应商'].isin(list_company),
            '内部调入',
            np.where(
                (~df_inventory_all['供应商'].isin(list_company)) & (df_inventory_all['供应商'] != '比亚迪')& (df_inventory_all['供应商'].notnull()),
                '外部调入',
                None  # 使用 np.nan 表示缺失值
            )
        )

        df_inventory_all = pd.merge(df_inventory_all, service_net[['车系', '服务网络']], how='left', on='车系')
        df_inventory_all['归属系统'] = np.where(df_inventory_all['归属系统'] == '直播基地', df_inventory_all['服务网络'] + '-' + df_inventory_all['归属系统'], df_inventory_all['归属系统'])
        # 清洗订单数据

        df_books.rename(columns={
        '订单日期':'订单日期',
        '计划单/车架号':'车架号',
        '订金日期':'定单日期',
        '开票日期':'销售日期',
        '订金状态':'订金状态',
        '订单订金':'定金金额',
        '车辆车系':'车系',
        '车辆车型':'车型',
        '车辆配置':'配置',
        '订单门店':'定单归属门店',
        '业务来源':'所属团队',
        '客户名称':'客户姓名',
        '客户电话':'联系电话'
        }, inplace=True)

        df_books2.rename(columns={'联系方式':'联系电话'},inplace=True)
        df_books2 = df_books2[['车架号','联系电话','主播人员']]
        df_books2 = df_books2.drop_duplicates()
        df_dings = df_books[['车架号', '订单日期', '定单日期','订金状态','审批状态', '销售人员', '销售日期', '定金金额', '定单归属门店', '所属团队', '车系', '外饰颜色','车型', '配置', '客户姓名', '联系电话']]
        df_dings['联系电话'] =df_dings['联系电话'].astype(str)
        df_dings = pd.merge(df_dings, service_net[['车系', '服务网络']], how='left', on='车系')
        df_dings['定单归属门店'] = np.where(df_dings['定单归属门店'] == '直播基地', df_dings['服务网络'] + '-' + df_dings['定单归属门店'], df_dings['定单归属门店'])
        # 清洗作废订单

        # 删除第一行和第二行
        tui_dings_df = pd.merge(tui_dings_df, service_net[['车系', '服务网络']], how='left', on='车系')
        tui_dings_df['订单门店'] = np.where(tui_dings_df['订单门店'] == '直播基地', tui_dings_df['服务网络'] + '-' + tui_dings_df['订单门店'], tui_dings_df['订单门店'])
        tui_dings_df['退定日期'] = pd.to_datetime(tui_dings_df['作废时间'], errors='coerce')
        tui_dings_df['定单日期'] = pd.to_datetime(tui_dings_df['订单日期'], errors='coerce')
        tui_dings_df['非退定核算'] = np.where((tui_dings_df['定单日期'].dt.year == tui_dings_df['退定日期'].dt.year) & (tui_dings_df['定单日期'].dt.month == tui_dings_df['退定日期'].dt.month), 0, 1)
        tui_dings_df = tui_dings_df[['订单门店','业务渠道','销售人员','订单日期','车系','外饰颜色','车型','配置','客户名称','客户电话','退订类型','退订原因','作废时间']]
        # 清洗销售明细数据
        df_salesAgg_ = df_salesAgg.copy()
        df_salesAgg_.rename(columns={
            '入库日期':'到库日期',
            '订单门店':'匹配定单归属门店',
            '订单日期':'定单日期',
            '开票日期':'销售日期',
            '业务渠道':'所属团队',
            '销售人员':'销售顾问',
            '客户名称':'客户姓名',
            '车辆信息_车辆车系':'车系',
            '车辆信息_车辆车型':'车型',
            '车辆信息_车辆颜色':'外饰颜色',
            '车辆信息_车辆配置':'车辆配置',
            '车辆信息_车架号':'车架号',
            '订金信息_订金金额':'定金金额',
            '整车销售_厂家官价':'指导价',
            '整车销售_最终结算价':'提货价'
            }
            ,inplace=True)
        df_salesAgg_ = df_salesAgg_[['车架号','车系','车型','车辆配置','外饰颜色','定金金额','指导价','提货价','匹配定单归属门店','到库日期','定单日期','销售日期','所属团队','销售顾问','客户姓名']]
        df_salesAgg_ = pd.merge(df_salesAgg_, service_net[['车系', '服务网络']], how='left', on='车系')
        df_salesAgg_['匹配定单归属门店'] = np.where(df_salesAgg_['匹配定单归属门店'] == '直播基地', df_salesAgg_['服务网络'] + '-' + df_salesAgg_['匹配定单归属门店'], df_salesAgg_['匹配定单归属门店'])
        df_salesAgg_  = df_salesAgg_[(df_salesAgg_['所属团队']!="调拨") & (df_salesAgg_['所属团队'].notna() & df_salesAgg_['所属团队'].notnull())]
        df_salesAgg.rename(columns={
            '订单门店':'公司名称',
            '订单日期':'订车日期',
            '开票日期':'销售日期',
            '购车方式':'购买方式',
            '业务渠道':'所属团队',
            '销售人员':'销售人员',
            '分销/邀约人员':'邀约人员',
            '交付专员':'交付专员',
            '客户名称':'车主姓名',
            '车辆信息_车辆车系':'车系',
            '车辆信息_车辆车型':'车型',
            '车辆信息_车辆颜色':'外饰颜色',
            '车辆信息_车辆配置':'车辆配置',
            '车辆信息_车架号':'车架号',
            '订金信息_订金金额':'定金金额',
            '整车销售_厂家官价':'指导价',
            '整车销售_裸车成交价':'裸车成交价',
            '整车销售_开票价格':'车款（发票价）',
            '整车销售_票据事务金额':'票据事务金额',
            '整车销售_最终结算价':'提货价',
            '整车销售_调拨费':'调拨费',
            '其它业务_上牌费':'上牌费',
            '其它业务_置换补贴保证金':'置换款',
            '其它业务_精品款':'精品款',
            '其它业务_金融押金':'金融押金',
            '其它业务_保险押金':'保险押金',
            '其它业务_代金券':'代金券',
            '其它业务_其它押金':'其它押金',
            '其它业务_其它费用':'其它费用',
            '其它业务_综合服务费':'金融服务费_顾问',
            '其它业务_票据事务费':'票据事务费',
            '其它业务_置换服务费':'置换服务费',
            '装饰业务_出库成本':'装饰成本',
            '其它业务_特殊事项':'特殊事项',
            }
            ,inplace=True)
        df_salesAgg = pd.merge(df_salesAgg, service_net[['车系', '服务网络']], how='left', on='车系')
        df_salesAgg['公司名称'] = np.where(df_salesAgg['公司名称'] == '直播基地', df_salesAgg['服务网络'] + '-' + df_salesAgg['公司名称'], df_salesAgg['公司名称'])

        # 处理综合毛利数据
        df_dings['现定现交'] = np.where(
            (df_dings['定单日期'].isnull()) & (df_dings['销售日期'].notnull()),
            '现定现交',
            np.where( (df_dings['订金状态']== "待收款")&(df_dings['定单日期'].notnull()) & (df_dings['销售日期'].notnull()), '现定现交',None))
        df_dings['定单状态'] = np.where((df_dings['销售日期'].notnull()), df_dings['销售日期'], None)
        df_dings = df_dings.drop_duplicates()
        df_dings1 = df_dings[(df_dings['车架号'] != '') & (df_dings['定单日期'] != '') & (df_dings['定单日期'].notna())]
        df_dings1 = df_dings.drop_duplicates()
        df_dings1 = df_dings.sort_values(by=['车架号', '销售日期'], ascending=[True, True])
        df_dings1 = df_dings1.drop_duplicates(subset=['车架号'], keep='first')
        df_kaipiao = df_kaipiao[df_kaipiao['单据类别'] == "车辆销售单"]
        df_kaipiao=df_kaipiao[['车架号','开票门店']].drop_duplicates()
        # 关联二手车的二手车返利到销售毛利
        tichu = list(['苏秀清','杜甯','周杨','李钰','易阳梅','黄毓香','王勇','钟鸣','刘前虎'])
        df_Ers1_ = pd.merge(df_Ers, df_kaipiao, how='left', left_on='置换车架号',right_on='车架号')
        df_Ers1_['置换车架号'] = np.where(df_Ers1_['开票门店'].isna(),np.nan, df_Ers1_['置换车架号'])
        df_Ers1 = df_Ers1_[(df_Ers1_['置换车架号'].isnull()) & (~df_Ers1_['客户'].isin(tichu))]
        df_Ers1 = df_Ers1[['评估门店','成交金额','其他费用','线索提供人','客户','车型','收款日期']]
        df_Ers1[['车系','车架号','所属团队']] = '二手车返利'
        df_Ers1['金融类型'] = '其他'
        df_Ers1['金融性质'] = '全款'
        df_Ers1.rename(columns={'评估门店':'公司名称','成交金额':'二手车成交价','其他费用':'二手车返利金额',\
                               '线索提供人':'销售人员','客户':'车主姓名','收款日期':'收款日期'},inplace=True)
        df_Ers1['单车毛利'] = df_Ers1['二手车返利金额']
        # 拼接有置换车架号的二手车数据
        df_Ers2_ = pd.read_csv(r"E:/powerbi_data/看板数据/dashboard/二手车返利存档.csv")
        df_Ers2 = df_Ers[df_Ers['置换车架号'].notna()]
        df_Ers2 = df_Ers2.rename(columns={'车架号':'置换车架号_车牌','置换车架号':'车架号','其他费用':'二手车返利金额1'})

        df_salesAgg =df_salesAgg[['公司名称','订车日期','入库日期','销售日期','车架号','车系','车型','车辆配置','外饰颜色','所属团队','销售人员','邀约人员','交付专员','车主姓名','定金金额','指导价','裸车成交价','车款（发票价）','提货价','调拨费','置换款','精品款','上牌费','购买方式','置换服务费','金融服务费_顾问','票据事务金额','票据事务费','代金券','金融押金','保险押金','其它押金','其它费用', '特殊事项']]
        df_salesAgg=df_salesAgg.dropna(subset='销售日期')
        df_salesAgg1 = pd.merge(df_salesAgg, df_books2[['车架号','联系电话','主播人员']], on=['车架号'], how='left').merge(df_service_aggregated[['车架号','保养升级成本','套餐明细']], on=['车架号'], how='left')\
            .merge(df_carcost[['车架号','调整项','返介绍费','退成交车辆定金（未抵扣）','政府返回区补','保险返利','终端返利','上牌成本','票据事务费-公司','代开票支付费用','回扣款','退代金券','退按揭押金','退置换补贴保证金','质损赔付金额','其他成本']],on='车架号',how='left').merge(df_loan[['车架号','金融类型','金融性质','首付金额','贷款金额','贷款期限','金融方案','返利系数','金融服务费','厂家贴息金额','经销商贴息金额','金融返利','金融税费','金融毛利']], on=['车架号'], how='left').merge(df_decoration2[['车架号','装饰成本','装饰收入','赠送装饰项目']],on='车架号',how='left').merge(df_kaipiao,on='车架号',how='left').merge(df_Ers2[['车架号','二手车返利金额1','收款日期']],on='车架号',how='left').merge(df_Ers2_[['车架号','二手车返利金额']],on='车架号',how='left')
        # 金额字段转换数值
        df_salesAgg1[['定金金额','指导价','裸车成交价','车款（发票价）','提货价','调拨费','上牌费','置换款','精品款','代金券','其它押金','其它费用','金融押金','保险押金','置换服务费','金融服务费_顾问','票据事务费','调整项','特殊事项']] \
        = df_salesAgg1[['定金金额','指导价','裸车成交价','车款（发票价）','提货价','调拨费','上牌费','置换款','精品款','代金券','其它押金','其它费用','金融押金','保险押金','置换服务费','金融服务费_顾问','票据事务费','调整项','特殊事项']].astype(float)
        df_salesAgg1['后返客户款项'] =  df_salesAgg1[['代金券','金融押金','保险押金','其它押金']].sum(axis=1)
        df_salesAgg1['二手车返利金额'] = df_salesAgg1['二手车返利金额'].fillna(df_salesAgg1['二手车返利金额1'])
        df_salesAgg1[['金融服务费','金融服务费_顾问']] = df_salesAgg1[['金融服务费','金融服务费_顾问']].replace('', 0).fillna(0).astype(float)
        df_salesAgg1['金融服务费'] = np.where((df_salesAgg1['金融服务费']==0) & (df_salesAgg1['购买方式']!="全款"),df_salesAgg1['金融服务费_顾问'],df_salesAgg1['金融服务费'])
        df_salesAgg1['金融毛利'] = df_salesAgg1[['金融毛利','金融服务费']].sum(axis=1)
        df_salesAgg1['金融类型'] = df_salesAgg1['金融类型'].fillna('全款')
        df_salesAgg1['上牌费'] = np.where((df_salesAgg1['金融服务费_顾问']>0) & (df_salesAgg1['购买方式']=="全款"),df_salesAgg1['金融服务费_顾问']+df_salesAgg1['上牌费'],df_salesAgg1['上牌费'])
        df_salesAgg1[['上牌成本','上牌费','票据事务金额','精品款','票据事务费','其它费用']]=df_salesAgg1[['上牌成本','上牌费','票据事务金额','精品款','票据事务费','其它费用']].replace('',0).fillna(0).astype(float)
        df_salesAgg1['上牌毛利'] =df_salesAgg1[['上牌费','上牌成本']].sum(axis=1)
        df_salesAgg1['精品款'] = df_salesAgg1['票据事务金额']
        df_salesAgg1['装饰赠送合计'] = df_salesAgg1[['装饰成本','保养升级成本']].sum(axis=1)
        df_salesAgg1['销售车价'] = df_salesAgg1[['车款（发票价）','置换款','后返客户款项']].sum(axis=1)-df_salesAgg1['精品款']
        list_gui = ['贵州新港澜源','贵州新港海之辇','贵州新港浩蓝','贵州新港蔚蓝','贵州上元曦和','贵州新港澜轩','贵州上元臻智']
        df_salesAgg1['促销费用'] = np.where((df_salesAgg1['公司名称'].isin(list_gui))&(df_salesAgg1['所属团队'] !="调拨"),df_salesAgg1['后返客户款项']+200,df_salesAgg1['后返客户款项'])
        df_salesAgg1['返利合计'] = df_salesAgg1['终端返利']+df_salesAgg1['保险返利']
        df_salesAgg1['增值税利润差'] = np.where(df_salesAgg1['票据事务费']>0,df_salesAgg1[['车款（发票价）','置换款','返利合计']].sum(axis=1)-df_salesAgg1[['提货价','票据事务金额']].sum(axis=1),df_salesAgg1[['车款（发票价）','置换款','返利合计']].sum(axis=1)-df_salesAgg1['提货价'])
        df_salesAgg1['税费'] = np.where(df_salesAgg1['增值税利润差']>0,round(df_salesAgg1['增值税利润差']/1.13*0.13*1.12,2),0)
        df_salesAgg1['固定支出'] =df_salesAgg1[['促销费用','装饰赠送合计']].sum(axis=1)
        df_salesAgg1['毛利'] = df_salesAgg1[['销售车价','返利合计']].sum(axis=1)-df_salesAgg1[['税费','提货价']].sum(axis=1)
        # 处理调拨表合并到毛利表
        # df_diao['调拨费'] = df_diao['调拨费'].astype('float')
        # df_diao = df_diao[df_diao['调拨费']<=0]
        df_diao = df_diao.drop(columns=['调拨费'])
        df_diao['结算日期'] = pd.to_datetime(df_diao['结算日期'])
        df_diao = df_diao.sort_values(by=['结算日期'], ascending=False)
        df_diao = df_diao.drop_duplicates(subset=['车架号'], keep='first')
        df_diao.to_csv('调拨表.csv', index=False)
        df_diao1 = pd.merge(df_diao, df_salesAgg1[['车架号', '销售日期', '车系', '车型', '车辆配置', '调拨费']],on=['车架号'], how='left')
        df_diao = df_diao.sort_values(by=['结算日期'],ascending=False)
        df_diao = df_diao.drop_duplicates(subset=['车架号'],keep='first')
        df_diao.to_csv("E:/powerbi_data/代码执行/data/调拨表.csv",index=False)
        df_diao1 = pd.merge(df_diao, df_salesAgg1[['车架号','销售日期','车系','车型','车辆配置']], on=['车架号'], how='left')
        df_diao1['调拨费'] = df_diao1['调拨费'].astype('float')
        df_diao1 = df_diao1[['调出门店','支付门店','调拨费','车架号','销售日期','车系','车型','车辆配置','车辆信息']]
        df_diao1[['所属团队','金融类型']] = '其他'
        df_diao1['金融类型'] = '调出车'
        df_diao1['调出车'] = '是'
        df_diao1.rename(columns={'车系': '车系1','调出门店': '公司名称','支付门店':'车主姓名'}, inplace=True)
        df_diao1['车系'] = '调拨车'
        df_diao1['车辆信息'] = df_diao1['车辆信息'].apply(lambda x: x[x.find(" ") + 1:] if x.find(" ") != -1 else x)
        df_diao1['单车毛利'] = df_diao1['调拨费']
        df_diao2 = df_diao1[['公司名称','销售日期','车架号','车系','车系1','车型','车辆信息','车辆配置','所属团队','金融类型','车主姓名','调拨费','调出车','单车毛利']]

        df_salesAgg1['单车毛利'] = df_salesAgg1[['毛利','金融毛利','上牌毛利','二手车返利金额','代开票支付费用','置换服务费','回扣款','票据事务费-公司','返介绍费','质损赔付金额','其他成本','政府返回区补','装饰收入','调整项','其它费用','特殊事项']].sum(axis=1)-df_salesAgg1[['促销费用','装饰赠送合计']].sum(axis=1)-df_salesAgg1['调拨费']
        df_salesAgg1['调出类型'] = np.where(
            ((df_salesAgg1['车主姓名'].isin(self.list_companys)) | (df_salesAgg1['车主姓名'].str.len() <= 5)) & (df_salesAgg1['所属团队'] == '调拨'),
            '内部调出',
            np.where(
                (~df_salesAgg1['车主姓名'].isin(self.list_companys)) & (df_salesAgg1['车主姓名'].str.len() > 5) & (df_salesAgg1['所属团队'] == '调拨'),
                '外部调出',
                None
            )
        )
        df_salesAgg2 = df_salesAgg1[['公司名称','订车日期','入库日期','收款日期','销售日期','车架号','车系','车辆配置','车型','外饰颜色','所属团队','调出类型','销售人员','邀约人员','交付专员','主播人员','车主姓名','联系电话','定金金额','指导价','裸车成交价','销售车价','车款（发票价）','提货价','置换款','精品款','后返客户款项','保险返利','终端返利','返利合计','增值税利润差','税费','毛利','购买方式','金融类型','金融性质','金融方案','首付金额','贷款金额','贷款期限','返利系数','金融返利','厂家贴息金额','经销商贴息金额','金融税费','金融服务费','金融毛利','上牌费','上牌成本','上牌毛利','二手车返利金额','置换服务费','促销费用','赠送装饰项目','装饰收入','装饰成本','套餐明细','保养升级成本','装饰赠送合计','其他成本','返介绍费','回扣款','代开票支付费用','调拨费','票据事务费','票据事务费-公司','其它费用','特殊事项', '政府返回区补','质损赔付金额','调整项','单车毛利','开票门店','退代金券','退成交车辆定金（未抵扣）','退按揭押金','退置换补贴保证金']]
        # 获取列名列表
        columns_to_process = ['金融返利', '金融服务费', '金融毛利', '上牌费', '上牌毛利', '置换款','保险返利', '终端返利', '返利合计', '精品款', '装饰成本', '裸车成交价','保养升级成本', '票据事务费', '装饰收入', '其它费用', '特殊事项']

        # 使用 .loc 进行赋值
        df_salesAgg2.loc[:, columns_to_process] = (df_salesAgg2[columns_to_process].replace('', 0).fillna(0))
        df_salesAgg_combined = pd.concat([df_salesAgg2,df_Ers1,df_diao2],join='outer',axis=0)
        df_salesAgg_combined['收款日期'] = np.where(df_salesAgg_combined['二手车返利金额']>0,df_salesAgg_combined['收款日期'].fillna(df_salesAgg_combined['销售日期']),df_salesAgg_combined['收款日期'])
        df_salesAgg_combined['销售日期'] = df_salesAgg_combined['销售日期'].fillna(df_salesAgg_combined['收款日期'])
        df_jingpin_result = pd.merge(df_jingpin_result, df_salesAgg2[['车架号','车系']], on=['车架号'], how='left')

        outputfile=pd.ExcelWriter(r"/看板数据/dashboard/cyy.xlsx")
        df_salesAgg2_= df_salesAgg2.copy()
        df_salesAgg2_.rename(columns={'公司名称':'匹配定单归属门店'},inplace=True)
        df_salesAgg_combined.to_excel(outputfile, index=None,sheet_name='sales_data')
        df_dings.to_excel(outputfile, index=None,sheet_name='book_data')
        df_inventory_all0 = df_inventory_all[df_inventory_all['开票日期'].isna()]
        df_inventory_all0.to_excel(outputfile, index=None,sheet_name='inventory_data')
        tui_dings_df.to_excel(outputfile, index=None,sheet_name='tui_dings_df')
        df_debit.to_excel(outputfile, index=None,sheet_name='debit_df')
        df_salesAgg_.to_excel(outputfile, index=None,sheet_name='sales_data1')
        df_jingpin_result.to_excel(outputfile, index=None,sheet_name='df_jingpin_result')
        df_inventory1.to_excel(outputfile, index=None,sheet_name='sold_inventorys')
        outputfile.close()
        logging.info('数据处理完成')
        df_inventory_all0.to_csv(r"E:\powerbi_data\看板数据\dashboard\inventory.csv")
        logging.info('库存数据处理完成')


if __name__ == "__main__":
    cyys = cyys()
    cyys.run()
    def copy_file(source_path, destination_path):
        try:
            shutil.copy2(source_path, destination_path)
            logging.info(f"文件 {source_path} 已成功复制到 {destination_path}")
        except FileNotFoundError:
            logging.error(f"源文件 {source_path} 未找到。")
        except PermissionError:
            logging.error("没有足够的权限进行文件复制操作。")
        except Exception as e:
            logging.error(f"复制文件时发生错误: {e}")

