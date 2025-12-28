from pymongo import MongoClient
import pandas as pd
import json
from bson import ObjectId
client = MongoClient('mongodb://xg_wd:H91NgHzkvRiKygTe4X4ASw@192.168.1.7:27017/xg?authSource=xg&authMechanism=SCRAM-SHA-256')

# 更新字段 没有就新增
def update_mongo_data(query, update_data):
    db = client.xg_BiUrlLink  # 选择数据库
    collection = db.UrlLink  # 选择集合
    try:
        result = collection.update_one(query, {"$set": update_data}, upsert=True)
        if result.upserted_id is not None:
            # 如果是新增的话, 需要生成不可逆的密码
            # 再次更新一次
            collection.update_one(query, {"$set": update_data})
            return {"code": 1, "msg": "新增成功"}
        
        elif result.modified_count > 0:
            return {"code": 0, "msg": "更新成功"}
        else:
            return {"code": -1, "msg": "数据无需修改"}
    except Exception as e:
        return {"code": -2, "msg": f"更新数据失败 ==> {e}"}

def excel_to_json(excel_path, sheet_name=0):
    """
    将 Excel 文件转换为指定 JSON 结构
    :param excel_path: Excel 文件路径
    :param sheet_name: 工作表名称/索引，默认为第一个工作表
    :return: 生成的 JSON 数据
    """
    # 读取 Excel 数据
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    print(df)
    
    result = {"售前结果": {}}
    
    for _, row in df.iterrows():
        parent = row['板块']
        child = row['子公司'] if pd.notna(row['子公司']) else None  # 改为None判断
        url = row['url'] if pd.notna(row['url']) else ""
        
        # 创建父节点（保持初始化逻辑）
        if parent not in result['售前结果']:
            result['售前结果'][parent] = {
                "url": url,        # 改为空字符串
                "childs": {}      # 子节点存储在这里
            }
        
        # 添加子节点逻辑（当子公司存在时）
        if child:
            result['售前结果'][parent]['childs'][child] = url  # 直接存储url
    
    return result
def excel_to_json1(excel_path, sheet_name=1):
    """
    将 Excel 文件转换为指定 JSON 结构
    :param excel_path: Excel 文件路径
    :param sheet_name: 工作表名称/索引，默认为第一个工作表
    :return: 生成的 JSON 数据
    """
    # 读取 Excel 数据
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    result = {"售前过程": {}}
    
    for _, row in df.iterrows():
        parent = row['板块']
        child = row['子公司'] if pd.notna(row['子公司']) else None  # 改为None判断
        url = row['url'] if pd.notna(row['url']) else ""
        
        # 创建父节点（保持初始化逻辑）
        if parent not in result['售前过程']:
            result['售前过程'][parent] = {
                "url": url,        # 改为空字符串
                "childs": {}      # 子节点存储在这里
            }
        
        # 添加子节点逻辑（当子公司存在时）
        if child:
            result['售前过程'][parent]['childs'][child] = url  # 直接存储url
    
    return result

# 使用示例
excel_path = r"映射.xlsx"  # 替换为实际路径
output = excel_to_json(excel_path,sheet_name=0)
print(json.dumps(output, indent=2, ensure_ascii=False))
output1 = excel_to_json1(excel_path,sheet_name=1)
print(json.dumps(output1, indent=2, ensure_ascii=False))

result = update_mongo_data({"_id": ObjectId("682414599c12ad36fb6c16bf")}, output)
result = update_mongo_data({"_id": ObjectId("682414599c12ad36fb6c16bf")}, output1)