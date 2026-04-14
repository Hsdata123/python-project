import pandas as pd
import re
from pathlib import Path
import os
import glob
folder_path = r"E:\python project"
file_pattern = "全域数据_素材分析_视频*.xlsx"
live_room = "椰子"
# 组合完整路径
full_pattern = os.path.join(folder_path,live_room, file_pattern)
file_paths = glob.glob(full_pattern)
# 存储处理后的每个文件的数据框
dfs_processed = []
print(file_paths)
for file_path in file_paths:
    file_name = os.path.join(folder_path, live_room)
    file_path = os.path.join(file_name,file_path)
    # 从文件名中提取日期
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", file_path)
    days = date_match.group(1) if date_match else "未知日期"
    
    # 读取Excel文件
    df = pd.read_excel(file_path, sheet_name="Sheet1")
    
    # 新增投放日期列
    df["投放日期"] = days
    
    # 新增产品名称列
    def get_product_name(name_series):
        # 检查整个系列中是否包含"椰子"
        if name_series.astype(str).str.contains("椰子").any():
            return "椰子"
        # 检查整个系列中是否包含"鱼子酱"
        elif name_series.astype(str).str.contains("鱼子酱").any():
            return "鱼子酱"
        else:
            return "其他"

    # 应用函数到整个系列
    df["产品名称"] = df["素材名称"].apply(lambda x: get_product_name(df["素材名称"]))
    
    # 确保“整体消耗”是数值类型
    df["整体消耗"] = pd.to_numeric(df["整体消耗"], errors="coerce")
    
    # 按“整体消耗”降序排列，取前20
    df_top20 = df.nlargest(20, "整体消耗")
    
    dfs_processed.append(df_top20)

# 合并两个数据框
df_merged = pd.concat(dfs_processed, ignore_index=True)

# 保存结果（可选）
df_merged.to_excel(f"合并后_消耗前20素材_{live_room}.xlsx", index=False)

print("处理完成！合并后数据已保存至 合并后_消耗前20素材.xlsx")
print(f"总行数：{len(df_merged)}")