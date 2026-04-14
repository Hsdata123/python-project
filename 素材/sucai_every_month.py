import pandas as pd
import os
import pymysql
from functions import *
from datetime import datetime,timedelta
month_first = (datetime.now()-timedelta(days=2)).strftime("%Y-%m-01")
yesterday = datetime.now()-timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'qianchuan_sucai',
    'charset': 'utf8mb4'
}

# 从数据库读取素材数据
def get_material_data_from_db():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        query = "SELECT * FROM t_material_data where `日期` between '{}' and '{}'".format(month_first,yesterday_str)

        df = pd.read_sql(query, connection)
        return df
    finally:
        connection.close()

# 从数据库读取数据
print("正在从数据库读取素材数据...")
all_df = get_material_data_from_db()

# all_df =   pd.read_excel("C:/Users/Administrator/Downloads/全域推广数据-投后数据-素材-2026-03-01 00_00_00-2026-03-31 23_59_59-7623977794367684646.xlsx")
print(f"从数据库获取到 {len(all_df)} 条素材数据")
print("素材名称" in all_df.columns)
if "素材名称" in all_df.columns or "素材视频名称" in all_df.columns:
    all_df = all_df.rename(columns={"素材名称":"全域素材视频名称","素材视频名称":"全域素材视频名称"})
all_df["产品名称"] = get_product_name(all_df["全域素材视频名称"])
 
def change_wrong_name(video_name):
    if pd.isna(video_name):
        return ""
    if "达人-健芳" in str(video_name):
        return str(video_name).replace("达人-健芳","达人健芳")
    else:
        return video_name


def fromWho_video(video_name):
    if pd.isna(video_name):
        return "其他"
    if "子鱼" in str(video_name):
        return "子鱼"
    elif "余倩" in str(video_name):
        return "余倩"
    elif "榆" in str(video_name):
        return "林晓榆"
    elif "乐晴" in str(video_name):
        return "乐晴"
    elif "AI" in str(video_name):
        return "AIGC"
    elif "B无" in str(video_name):
        return "商务自传"
    elif "天爆" in str(video_name):
        return "天爆"
    elif "腾飞" in str(video_name):
        return "腾飞"
    else:
        return "其他"

def fro_j_video(video_name):
    if pd.isna(video_name):
        return "其他"
    video_name = str(video_name)
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
    elif "J学顺" in video_name or "榆-李" in video_name:
        return "学顺"
    elif "-兴" in video_name or "可兴" in video_name or "-韦" in video_name:
        return "可兴"
    elif "J俊彬" in video_name:
        return "J俊彬"
    elif "陈坤" in video_name or "-坤" in video_name:
        return "J陈坤"
    elif "J无" in video_name or "]无" in video_name:
        return "J无"
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
            ((df["剪辑"].fillna("") != "") & (df["编导"].fillna("") != "其他")),
            "是",
            "否"
        )
    )
    df["素材类型-达人-AI-编导&剪辑"] = np.where(
        df["编导"] == "AIGC","AIGC",
        np.where(
            df["编导"]=="其他","达人","编导&剪辑"
        )
    )
    return df
def check_top_video(df):
    df['按日是否爆款'] = np.where(
        df["整体消耗"].fillna(0) >= 4000,
    1,0
    )
    return df
def check_month_top_video(df):
    df['按月是否爆款'] = np.where(
        df["整体消耗"].fillna(0) >= 30000,
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
        if pd.isna(x):
            return "未识别类型"
        if "配音展示" in str(x):
            return "配音展示"
        elif "出境口播" in str(x):
            return "出境口播"
        elif "对比测评" in str(x):
            return "对比测评"
        elif "营销号" in str(x):
            return "营销号"
        elif "店播IP" in str(x):
            return "店播IP"
        elif "轻剧情" in str(x):
            return "轻剧情"
        elif "打卡" in str(x):
            return "打卡"
        elif "溯源" in str(x):
            return "溯源"
        elif "live图" in str(x) or "Iive图" in str(x):
            return "live图"
        elif "采访" in str(x):
            return "采访"
        elif "无" in str(x):
            return "无"
        else:
            return "未识别类型"

    def sucai_label_shijiao(x):
        if pd.isna(x):
            return "未识别视角"
        if "商" in str(x):
            return "商"
        elif "用" in str(x):
            return "用"
        elif "专" in str(x):
            return "专"
        else:
            return "未识别视角"

    def sucai_label_xuqiu(x):
        if pd.isna(x):
            return "未识别需求"
        if "价格" in str(x):
            return "价格"
        elif "情感" in str(x):
            return "情感"
        elif "品质" in str(x):
            return "品质"
        elif "健康" in str(x):
            return "健康"
        else:
            return "未识别需求"

    df["素材类型"] = df["全域素材视频名称"].str.split("-").str[8]
    df["素材类型"] = df["素材类型"].apply(sucai_label_leix)

    df["素材需求"] = df["全域素材视频名称"].str.split("-").str[7]
    df["素材需求"] = df["素材需求"].apply(sucai_label_xuqiu)

    df["素材视角"] = df["全域素材视频名称"].str.split("-").str[6]
    df["素材视角"] = df["素材视角"].apply(sucai_label_shijiao)
    return df

def sucai_buisness(df):
    def switch_buisness_name(video_name):
        if pd.isna(video_name):
            return "非商务"
        if "西西" in str(video_name):
            return "商务西西"
        elif "其其" in str(video_name) or "达人琪琪" in str(video_name):
            return "商务其其"
        elif "健芳" in str(video_name):
            return "商务健芳"
        elif "杨桃" in str(video_name):
            return "商务杨桃"
        else:
            return "非商务"
    df["素材商务"] = np.where(
        ((df["编导"].fillna("") != "B无") & (df["编导"].fillna("") != "其他")),
        "商务","非商务"
    )
    df["素材商务"] = df["全域素材视频名称"].apply(lambda x: switch_buisness_name(x))
    df["剪辑"] = np.where(
        ((df["素材商务"]!="非商务") & (df["剪辑"].fillna("")=="J无")),
        df["素材商务"],df["剪辑"]
    )
    df["剪辑"] = np.where(
        ((df["全域素材视频名称"].str.contains("腾飞", na=False)) & (df["剪辑"]=="J无")),
        "腾飞",df["剪辑"]
    )
    return df

def sucai_level(x):
    if pd.isna(x):
        return ""
    if len(str(x).split("，"))>1:
        return "其他"
    else:
        return x

try:
    all_df["素材千川标签"] = all_df["素材评估"].apply(lambda x:sucai_level(x))
except Exception as e:
    print(e)
print(all_df.columns)

# 处理数据类型：将数据库读取的数据转换为字符串后再处理
def safe_str_replace(x):
    if pd.isna(x):
        return "0"
    return str(x).replace("-","0").replace(",","")

all_df["整体消耗"] = all_df["整体消耗"].apply(safe_str_replace)
all_df["整体消耗"] = all_df["整体消耗"].astype('float64')
all_df["素材数"] = 1
all_df['全域素材视频名称'] =  all_df['全域素材视频名称'].apply(lambda x: change_wrong_name(x))
all_df = sucai_label(all_df)


all_df["10秒播放率"] = all_df["10秒播放率"].apply(safe_str_replace)
all_df["10秒播放率"] = all_df["10秒播放率"].str.replace("%","", regex=False)
all_df["10秒播放率"] = all_df["10秒播放率"].astype('float64')
all_df["10秒播放率"] = all_df["10秒播放率"]/100

all_df["视频播放数"] = all_df["视频播放数"].apply(safe_str_replace)
all_df["视频播放数"] = all_df["视频播放数"].astype('float64')
all_df["10秒播放数"] = all_df["10秒播放率"]*all_df["视频播放数"]
all_df["编导"] = all_df['全域素材视频名称'].apply(lambda x: fromWho_video(x))
all_df["剪辑"] = all_df['全域素材视频名称'].apply(lambda x: fro_j_video(x))
all_df = sucai_buisness(all_df)
all_df = check_video_name(all_df)

try:
    all_df_month = all_df[all_df["日期"]=="全部"]
    all_df_month = check_month_top_video(all_df_month)
    all_df_month = check_month_new_video(all_df_month)
except:
    all_df_month = pd.DataFrame()
try:
    all_df = all_df[all_df["日期"]!="全部"]
except:
    pass
all_df = check_top_video(all_df)
with pd.ExcelWriter('素材_月度.xlsx') as writer:
    all_df_month.to_excel(writer, sheet_name='月度汇总', index=False)
    all_df.to_excel(writer, sheet_name='日期明细', index=False)