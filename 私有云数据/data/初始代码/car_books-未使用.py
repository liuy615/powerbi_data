import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor

def process_file_for_sheet(file_path, target_sheet):
    """
    处理单个文件中的指定工作表
    
    Args:
        file_path: 文件路径
        target_sheet: 目标工作表名称
    
    Returns:
        DataFrame: 处理后的数据
    """
    print(f'处理文件: {os.path.basename(file_path)}, 工作表: {target_sheet}')
    try:
        with pd.ExcelFile(file_path) as xls:
            if target_sheet in xls.sheet_names:
                try:
                    data = pd.read_excel(xls, sheet_name=target_sheet)
                    data['From'] = os.path.basename(file_path).split('.')[0]
                    if not data.empty:
                        return data
                except Exception as e:
                    print(f'读取 {target_sheet} 时出错: {e}')
            else:
                print(f'文件 {os.path.basename(file_path)} 中未找到工作表 {target_sheet}')
        return None
    except Exception as e:
        print(f'读取 {file_path} 时出错: {e}')
        return None


def process_sheet_data(sheet_name):
    """
    处理指定工作表的所有数据
    
    Args:
        sheet_name: 工作表名称
    
    Returns:
        DataFrame: 合并后的数据
    """
    # 设置工作簿所在文件夹路径
    directories = [r'E:\powerbi_data\看板数据\私有云文件本地\订车台账']

    all_file_paths = []
    for folder_path in directories:
        print(f'处理文件夹: {folder_path}')
        # 遍历文件夹中的每个 Excel 文件
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.xlsx'):
                file_path = os.path.join(folder_path, file_name)
                all_file_paths.append(file_path)

    combined_data = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(lambda fp: process_file_for_sheet(fp, sheet_name), all_file_paths)
        for result in results:
            if result is not None:
                combined_data.append(result)

    if combined_data:
        df_combined = pd.concat(combined_data, axis=0, ignore_index=True)
        print(f'工作表 {sheet_name} 合并完成，共 {len(df_combined)} 行数据')
        return df_combined
    else:
        print(f'工作表 {sheet_name} 没有找到任何数据')
        return pd.DataFrame()


def process_multiple_sheets(sheet_names=['登记表', '厂家下发提车任务']):
    """
    分别处理多个工作表的数据
    
    Args:
        sheet_names: 需要处理的工作表名称列表
    
    Returns:
        dict: 每个工作表对应的数据DataFrame
    """
    result = {}
    for sheet_name in sheet_names:
        result[sheet_name] = process_sheet_data(sheet_name)
    return result

def other_income():
    """
    处理特殊事项收入数据，分别合并两个工作表
    
    Returns:
        tuple: (登记表数据, 厂家下发提车任务数据)
    """
    # 分别获取两个工作表的数据
    registration_data = process_sheet_data('门店订车计划')
    task_data = process_sheet_data('厂家下发提车任务')
    
    # 对登记表数据进行后续处理
    if not registration_data.empty:
        df_A = registration_data[['所属门店','车系','车型','外饰颜色','数量','计划下单日期','审批意见','备注','操作日期']]
        df_A['所属门店'] = df_A['所属门店'].replace('文景初治', '上元盛世')
        registration_processed = df_A
    else:
        registration_processed = pd.DataFrame()
    if not task_data.empty:
        df_A = task_data[['所属门店','期间','车系','车型','外饰颜色','任务量','备注','操作日期']]
        df_A['所属门店'] = df_A['所属门店'].replace('文景初治', '上元盛世')
        task_data = df_A
    else:
        task_data = pd.DataFrame()
    
    # 如果需要对厂家下发提车任务数据也进行处理，可以在这里添加
    # task_processed = task_data  # 这里可以根据需要进行处理
    
    return registration_processed, task_data

df_registration, df_task = other_income()


# 保存数据到CSV文件
if not df_registration.empty:
    df_registration.to_csv(r'E:\powerbi_data\看板数据\dashboard\订车_登记表.csv', index=False)


if not df_task.empty:
    df_task.to_csv(r'E:\powerbi_data\看板数据\dashboard\订车_厂家任务.csv', index=False)