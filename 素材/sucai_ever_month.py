import pandas as pd
import os
from functions import *

file_list = ['全域推广数据-投后数据-素材-2025-12-08 00_00_00-2025-12-14 23_59_59-7584258341355061275.xlsx']
file_path = r"D:\python project\python project\素材"
all_df = pd.DataFrame()
for file_name in file_list:
    file = os.path.join(file_path,file_name)
    df = pd.read_excel(file)
    df["产品名称"] = get_product_name(df["全域素材视频名称"])
    all_df = pd.concat([df,all_df])

def change_wrong_name(video_name):
    if "达人-健芳" in video_name:
        return video_name.replace("达人-健芳","达人健芳")
    else:
        return video_name


def fromWho_video(video_name):
    if "子鱼" in video_name:
        return "子鱼"
    elif "余倩" in video_name:
        return "余倩"
    elif "榆" in video_name:
        return "林晓榆"
    elif "B达" in video_name or "-达-" in video_name:
        return "曹宇达"
    elif "乐晴" in video_name:
        return "乐晴"
    elif "AI" in video_name:
        return "AI"
    elif "B无" in video_name:
        return "商务自传"
    else:
        return "其他"
def fro_j_video(video_name):
    if "凯练" in video_name:
        return "J凯练"
    elif "安褀" in video_name or "J安" in video_name or "安祺" in video_name:
        return "J安褀"
    elif "钰灵" in video_name:
        return "J钰灵"
    elif "伟健" in video_name:
        return "J伟健"
    elif "黎洁" in video_name:
        return "J黎洁"
    elif "马杰" in video_name:
        return "J马杰"
    elif "-宾" in video_name or "J宾" in video_name:
        return "J宾"
    elif "-帆" in video_name or "J帆" in video_name:
        return "J帆"
    elif "J学顺" in video_name:
        return "学顺"
    elif "-兴" in video_name or "可兴" in video_name or "-韦" in video_name:
        return "可兴"
    elif "J俊彬" in video_name:
        return "J俊彬"
    elif "陈坤" in video_name:
        return "J陈坤"
    else:
        return "其他"

def check_video_name(df):
    df["素材创建时间"] = pd.to_datetime(df["素材创建时间"], errors='coerce')
    
    # 首先检查日期是否早于标准执行日期
    df["是否可区分剪辑和编导"] = np.where(
        df["素材创建时间"] < "2025-10-13",
        "未执行命名标准",
        # 对于2025-10-13及之后的记录，检查其他条件
        np.where(
            ((df["剪辑"] != "") & (df["编导"] != "其他")),
            "是",
            "否"
        )
    )
    df["素材类型-达人-AI-编导&剪辑"] = np.where(
        df["编导"] == "AI","AI",
        np.where(
            df["编导"]=="其他","达人","编导&剪辑"
        )
    )
    return df
def check_top_video(df):
    df['按日是否爆款'] = np.where(
        df["整体消耗"] >= 4000,
    1,0
    )
    return df
def check_month_top_video(df):
    df['按月是否爆款'] = np.where(
        df["整体消耗"] >= 30000,
    1,0
    )
    return df

def check_month_new_video(df):
    # 提取分割后的第一部分
    first_parts = df["全域素材视频名称"].str.split("-").str[0]
    
    # 使用正则表达式确保只处理纯数字的情况
    # ^\d+$ 表示从开始到结束都是数字
    numeric_mask = first_parts.str.match(r'^\d+$', na=False)
    
    # 只对纯数字的部分进行转换和比较
    df['是否新素材'] = 0  # 默认设为0
    
    # 只对符合数字格式的部分进行处理
    numeric_parts = first_parts[numeric_mask]
    if not numeric_parts.empty:
        numeric_values = pd.to_numeric(numeric_parts, errors='coerce')
        # 将符合条件的设为1
        df.loc[numeric_mask & (numeric_values >= 1100), '是否新素材'] = 1
    
    return df

def sucai_label(df):
    def sucai_label_leix(x):
        if "配音展示" in x:
            return "配音展示"
        elif "出境口播" in x:
            return "出境口播"
        elif "对比测评" in x:
            return "对比测评"
        elif "营销号" in x:
            return "营销号"
        elif "店播IP" in x:
            return "店播IP"
        elif "轻剧情" in x:
            return "轻剧情"
        elif "打卡" in x:
            return "打卡"
        elif "溯源" in x:
            return "溯源"
        elif "live图" in x or "Iive图" in x:
            return "live图"
        elif "采访" in x:
            return "采访"
        elif "无" in x:
            return "无"
        else:
            return "未识别类型"

    def sucai_label_shijiao(x):
        if "商" in x:
            return "商"
        elif "用" in x:
            return "用"
        elif "专" in x:
            return "专"
        else:
            return "未识别视角"

    def sucai_label_xuqiu(x):
        if "价格" in x:
            return "价格"
        elif "情感" in x:
            return "情感"
        elif "品质" in x:
            return "品质"
        elif "健康" in x:
            return "健康"
        else:
            return "未识别需求"        
    
    df["素材类型"] = df["全域素材视频名称"].str.split("-").str[8]
    df["素材类型"] = df["素材类型"].apply(lambda x:sucai_label_leix(str(x)))

    df["素材需求"] = df["全域素材视频名称"].str.split("-").str[7]
    df["素材需求"] = df["素材需求"].apply(lambda x:sucai_label_xuqiu(str(x)))

    df["素材视角"] = df["全域素材视频名称"].str.split("-").str[6]
    df["素材视角"] = df["素材视角"].apply(lambda x:sucai_label_shijiao(str(x)))
    return df

def sucai_buisness(df):
    def switch_buisness_name(video_name):
        if "西西" in video_name:
            return "西西"
        elif "其其" in video_name:
            return "其其"
        elif "健芳" in video_name:
            return "健芳"
        elif "杨桃" in video_name:
            return "杨桃"
        else:
            return "非商务"
    df["素材商务"] = np.where(
        ((df["编导"] != "B无") & (df["编导"] != "其他")),
        "商务","非商务"
    )
    df["素材商务"] = df["全域素材视频名称"].apply(lambda x: switch_buisness_name(x))
    return df

all_df["整体消耗"] = all_df["整体消耗"].apply(lambda x:x.replace("-","0"))
all_df["整体消耗"] = all_df["整体消耗"].apply(lambda x:x.replace(",",""))
all_df["整体消耗"] = all_df["整体消耗"].astype('float64')
all_df["素材数"] = 1
all_df['全域素材视频名称'] =  all_df['全域素材视频名称'].apply(lambda x: change_wrong_name(x))
all_df = sucai_label(all_df)


all_df["编导"] = all_df['全域素材视频名称'].apply(lambda x: fromWho_video(x))
all_df["剪辑"] = all_df['全域素材视频名称'].apply(lambda x: fro_j_video(x))
all_df = sucai_buisness(all_df)
all_df = check_video_name(all_df)


all_df_month = all_df[all_df["日期"]=="全部"]
all_df_month = check_month_top_video(all_df_month)
all_df_month = check_month_new_video(all_df_month)

all_df = all_df[all_df["日期"]!="全部"]
all_df = check_top_video(all_df)
with pd.ExcelWriter('素材_月度.xlsx') as writer:
    all_df_month.to_excel(writer, sheet_name='月度汇总', index=False)
    all_df.to_excel(writer, sheet_name='日期明细', index=False)