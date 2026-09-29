import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine 
from functions import *
from datetime import datetime,timedelta
month_first = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-01")
yesterday = datetime.now()-timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
# month_first = '2026-05-01'
# yesterday_str = "2026-07-31"

# 素材创建时间筛选起始日期（参数）
MATERIAL_CREATE_START_DATE = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-01")
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
data_source = "db"

# all_df =   pd.read_excel("D:/月度绩效/素材/1-3月明细_合并.xlsx")
# data_source = "excel"
print(f"从数据库获取到 {len(all_df)} 条素材数据")
print("素材名称" in all_df.columns)
if "素材名称" in all_df.columns or "素材视频名称" in all_df.columns:
    all_df = all_df.rename(columns={"素材名称":"全域素材视频名称","素材视频名称":"全域素材视频名称"})
all_df["产品名称"] = get_product_name(all_df["全域素材视频名称"])

# ========== 一次性修改：根据素材ID修改素材名称 ==========
MANUAL_ID_NAME_MAP = {
    "7673330686392877094": "人参-0813【A-种草-主页素材01】-J钰灵-B子鱼",
    "7673455109986304043": "人参-0813【A-种草-主页素材02】-J钰灵-B子鱼",
    "7673396118743629830": "人参-0813【A-种草-主页素材03】-J钰灵-B子鱼",
}
# 将素材ID转为字符串进行匹配
all_df['素材ID'] = all_df['素材ID'].astype(str)
for material_id, new_name in MANUAL_ID_NAME_MAP.items():
    mask = all_df['素材ID'] == material_id
    if mask.sum() > 0:
        old_name = all_df.loc[mask, '全域素材视频名称'].values[0]
        all_df.loc[mask, '全域素材视频名称'] = new_name
        print(f"素材ID {material_id}: '{old_name}' -> '{new_name}'")
    else:
        print(f"素材ID {material_id} 未在数据中找到")
# ========== 修改结束 ==========
 
def change_wrong_name(video_name):
    if pd.isna(video_name):
        return ""
    if "达人-健芳" in str(video_name):
        return str(video_name).replace("达人-健芳","达人健芳")
    else:
        return video_name

def update_material_name_by_id(df, material_id_col, name_col):
    """
    根据素材ID修改素材名称

    参数:
        df: 数据表DataFrame
        material_id_col: 素材ID列名
        name_col: 新素材名称列名（来自外部传入的名称值）

    返回:
        修改后的DataFrame
    """
    if material_id_col not in df.columns or name_col not in df.columns:
        print(f"列名错误：找不到 '{material_id_col}' 或 '{name_col}' 列")
        return df

    # 读取数据库中的素材ID和名称映射表
    connection = pymysql.connect(**DB_CONFIG)
    try:
        query = f"SELECT `素材ID`, `全域素材视频名称` FROM t_material_data"
        material_map_df = pd.read_sql(query, connection)
    finally:
        connection.close()

    if material_map_df.empty:
        print("警告：从数据库未获取到素材映射数据")
        return df

    # 将素材ID转为字符串以便匹配
    df[material_id_col] = df[material_id_col].astype(str)
    material_map_df['素材ID'] = material_map_df['素材ID'].astype(str)

    # 创建素材ID到原始名称的映射字典
    id_to_name = dict(zip(material_map_df['素材ID'], material_map_df['全域素材视频名称']))

    # 用外部传入的名称列值（name_col）直接覆盖 df 中的全域素材视频名称
    # 只对有有效ID和有效名称的记录进行修改
    mask = df[material_id_col].isin(id_to_name.keys()) & df[name_col].notna() & (df[name_col] != '')
    df.loc[mask, '全域素材视频名称'] = df.loc[mask, name_col]

    print(f"根据素材ID修改了 {mask.sum()} 条素材名称")
    return df


def fromWho_video(video_name, account_name=""):
    prefix = account_name if account_name else ""
    if pd.isna(video_name):
        return "其他" if not prefix else prefix + "-其他"
    if "梦新" in str(video_name):
        return prefix + "-B梦新" if prefix else "梦新"
    if "子鱼" in str(video_name):
        return prefix + "-B子鱼" if prefix else "子鱼"
    elif "余倩" in str(video_name):
        return prefix + "-B余倩" if prefix else "余倩"
    elif "榆" in str(video_name):
        return prefix + "-林晓榆" if prefix else "林晓榆"
    elif "乐晴" in str(video_name):
        return prefix + "-B乐晴" if prefix else "乐晴"
    elif "AIGC动态创意" in str(video_name):
        return prefix + "-AIGC" if prefix else "AIGC"
    elif "B无" in str(video_name):
        return prefix + "-商务自传" if prefix else "商务自传"
    elif "天爆" in str(video_name):
        return prefix + "-天爆" if prefix else "天爆"
    elif "腾飞" in str(video_name):
        return prefix + "-腾飞" if prefix else "腾飞"
    else:
        return prefix + "-其他" if prefix else "其他"

def fro_j_video(video_name, account_name=""):
    prefix = account_name if account_name else ""
    if pd.isna(video_name):
        return "其他" if not prefix else prefix + "-其他"
    video_name = str(video_name)
    if "佳慧" in str(video_name):
        return prefix + "-J佳慧" if prefix else "佳慧"
    if "凯练" in video_name:
        return prefix + "-J凯练" if prefix else "J凯练"
    elif "安褀" in video_name or "J安" in video_name or "安祺" in video_name:
        return prefix + "-J安褀" if prefix else "J安褀"
    elif "AIGC动态创意" in str(video_name):
        return prefix + "-AIGC" if prefix else "AIGC"
    elif "钰灵" in video_name:
        return prefix + "-J钰灵" if prefix else "J钰灵"
    elif "伟健" in video_name:
        return prefix + "-J伟健" if prefix else "J伟健"
    elif "学顺" in video_name or "榆-李" in video_name:
        return prefix + "-J学顺" if prefix else "J学顺"
    elif "J俊彬" in video_name:
        return prefix + "-J俊彬" if prefix else "J俊彬"
    elif "陈坤" in video_name or "-坤" in video_name:
        return prefix + "-J陈坤" if prefix else "J陈坤"
    elif "星骅" in video_name:
        return prefix + "-J星骅" if prefix else "J星骅"
    elif "J无" in video_name or "]无" in video_name:
        return prefix + "-J无" if prefix else "J无"
    else:
        return prefix + "-其他" if prefix else "其他"

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
        df["编导"].str.contains("AIGC", na=False),"AIGC",
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
        if "商" in str(x):
            return "商"
        elif "用" in str(x):
            return "用"
        elif "专" in str(x):
            return "专"
        else:
            return "未识别视角"
    # '价格': lambda name: '价格' in str(name),
    # '机制': lambda name: '机制' in str(name),
    # '情感': lambda name: '情感' in str(name),
    # '种草': lambda name: '种草' in str(name),
    # '图文': lambda name: '图文' in str(name),
    # '剧情': lambda name: '剧情' in str(name),
    def sucai_label_xuqiu(x):

        if "价格" in str(x):
            return "价格"
        if "情感" in str(x):
            return "情感"
        if "机制" in str(x):
            return "机制"
        if "种草" in str(x):
            return "种草"
        if "图文" in str(x) or "live图" in  str(x) or "Iive图" in str(x):
            return "图文"
        if "剧情" in str(x):
            return "剧情"
        if "痛点" in str(x):
            return "痛点"
        if "AIGC" in str(x):
            return "AIGC"
        else:
            return "其他"

    df["素材类型"] = df["全域素材视频名称"]
    df["素材类型"] = df["素材类型"].apply(sucai_label_leix)

    df["素材需求"] = df["全域素材视频名称"]
    df["素材需求"] = df["素材需求"].apply(sucai_label_xuqiu)

    df["素材视角"] = df["全域素材视频名称"]
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
all_df["编导"] = all_df.apply(lambda row: fromWho_video(row['全域素材视频名称'], row.get('账号名称', '')), axis=1)
all_df["剪辑"] = all_df.apply(lambda row: fro_j_video(row['全域素材视频名称'], row.get('账号名称', '')), axis=1)
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
# 输出到 Excel
with pd.ExcelWriter('素材_月度.xlsx') as writer:
    all_df_month.to_excel(writer, sheet_name='月度汇总', index=False)
    all_df.to_excel(writer, sheet_name='日期明细', index=False)
if data_source == "db":
    engine = create_engine('mysql+pymysql://root:123456@localhost/qianchuan_sucai?charset=utf8mb4')

    # 维度1：账户名称-编导-日期
    query_editor = f"""
    SELECT
        `账号名称`,
        `编导`,
        `日期`,
        '{yesterday_str}' AS `数据截止日期`,
        SUM(`整体成交金额`) AS `整体成交金额`,
        SUM(`整体消耗`) AS `整体消耗`,
        SUM(`整体成交订单数`) AS `整体成交订单数`,
        SUM(`整体展现次数`) AS `整体展现次数`,
        SUM(`视频播放数`) AS `视频播放数`,
        SUM(`视频完播数`) AS `视频完播数`,
        SUM(`视频播放数` * `3秒播放率`) AS `3秒播放次数`,
        SUM(`视频播放数` * `5秒播放率`) AS `5秒播放次数`,
        SUM(`视频播放数` * `10秒播放率`) AS `10秒播放次数`
    FROM qianchuan_sucai.t_sucai_daily_report
    WHERE 日期 BETWEEN '{month_first}' AND '{yesterday_str}'
    GROUP BY `账号名称`, `编导`, `日期`
    ORDER BY `账号名称`, `编导`, `日期`
    """

    # 维度2：账户名称-剪辑-日期
    query_clipper = f"""
    SELECT
        `账号名称`,
        `剪辑`,
        `日期`,
        '{yesterday_str}' AS `数据截止日期`,
        SUM(`整体成交金额`) AS `整体成交金额`,
        SUM(`整体消耗`) AS `整体消耗`,
        SUM(`整体成交订单数`) AS `整体成交订单数`,
        SUM(`整体展现次数`) AS `整体展现次数`,
        SUM(`视频播放数`) AS `视频播放数`,
        SUM(`视频完播数`) AS `视频完播数`,
        SUM(`视频播放数` * `3秒播放率`) AS `3秒播放次数`,
        SUM(`视频播放数` * `5秒播放率`) AS `5秒播放次数`,
        SUM(`视频播放数` * `10秒播放率`) AS `10秒播放次数`
    FROM qianchuan_sucai.t_sucai_daily_report
    WHERE 日期 BETWEEN '{month_first}' AND '{yesterday_str}'
    GROUP BY `账号名称`, `剪辑`, `日期`
    ORDER BY `账号名称`, `剪辑`, `日期`
    """

    # 执行查询
    with engine.connect() as conn:
        df_editor = pd.read_sql(query_editor, conn)
        df_clipper = pd.read_sql(query_clipper, conn)

    # UPSERT逻辑 - 使用 INSERT ... ON DUPLICATE KEY UPDATE
    def upsert_to_table(df, table_name, primary_keys):
        if df.empty:
            return
        cols = ', '.join([f'`{c}`' for c in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        update_parts = [f'`{c}`=VALUES(`{c}`)' for c in df.columns if c not in primary_keys]
        update_cols = ', '.join(update_parts)
        sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_cols}"

        connection = pymysql.connect(**DB_CONFIG) 
        try:
            with connection.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute(sql, tuple(row))
            connection.commit()
        finally:
            connection.close()
    print(df_editor)
    df_editor = df_editor.replace([np.nan, np.inf, -np.inf], None)
    df_clipper = df_clipper.replace([np.nan, np.inf, -np.inf], None)
    # 编导表 - 联合主键：账号名称, 编导, 日期
    upsert_to_table(df_editor, 't_sucai_editor_daily_report', ['账号名称', '编导', '日期'])
    print(f"维度1（账号名称-编导-日期）已保存到数据库，共 {len(df_editor)} 条")

    # 剪辑表 - 联合主键：账号名称, 剪辑, 日期
    upsert_to_table(df_clipper, 't_sucai_clipper_daily_report', ['账号名称', '剪辑', '日期'])
    print(f"维度2（账号名称-剪辑-日期）已保存到数据库，共 {len(df_clipper)} 条")

    # ========== 新增：筛选素材创建时间>=2026-05-01的版本 ==========
    # 维度1_new：账户名称-编导-日期（素材创建时间>=参数日期）
    query_editor_new = f"""
    SELECT
        `账号名称`,
        `编导`,
        `日期`,
        '{yesterday_str}' AS `数据截止日期`,
        SUM(`整体成交金额`) AS `整体成交金额`,
        SUM(`整体消耗`) AS `整体消耗`,
        SUM(`整体成交订单数`) AS `整体成交订单数`,
        SUM(`整体展现次数`) AS `整体展现次数`,
        SUM(`视频播放数`) AS `视频播放数`,
        SUM(`视频完播数`) AS `视频完播数`,
        SUM(`视频播放数` * `3秒播放率`) AS `3秒播放次数`,
        SUM(`视频播放数` * `5秒播放率`) AS `5秒播放次数`,
        SUM(`视频播放数` * `10秒播放率`) AS `10秒播放次数`
    FROM qianchuan_sucai.t_sucai_daily_report
    WHERE 日期 BETWEEN '{month_first}' AND '{yesterday_str}'
      AND `素材创建时间` >= '{MATERIAL_CREATE_START_DATE}'
    GROUP BY `账号名称`, `编导`, `日期`
    ORDER BY `账号名称`, `编导`, `日期`
    """

    # 维度2_new：账户名称-剪辑-日期（素材创建时间>=参数日期）
    query_clipper_new = f"""
    SELECT
        `账号名称`,
        `剪辑`,
        `日期`,
        '{yesterday_str}' AS `数据截止日期`,
        SUM(`整体成交金额`) AS `整体成交金额`,
        SUM(`整体消耗`) AS `整体消耗`,
        SUM(`整体成交订单数`) AS `整体成交订单数`,
        SUM(`整体展现次数`) AS `整体展现次数`,
        SUM(`视频播放数`) AS `视频播放数`,
        SUM(`视频完播数`) AS `视频完播数`,
        SUM(`视频播放数` * `3秒播放率`) AS `3秒播放次数`,
        SUM(`视频播放数` * `5秒播放率`) AS `5秒播放次数`,
        SUM(`视频播放数` * `10秒播放率`) AS `10秒播放次数`
    FROM qianchuan_sucai.t_sucai_daily_report
    WHERE 日期 BETWEEN '{month_first}' AND '{yesterday_str}'
      AND `素材创建时间` >= '{MATERIAL_CREATE_START_DATE}'
    GROUP BY `账号名称`, `剪辑`, `日期`
    ORDER BY `账号名称`, `剪辑`, `日期`
    """
    print(query_clipper_new)
    with engine.connect() as conn:
        df_editor_new = pd.read_sql(query_editor_new, conn)
        df_clipper_new = pd.read_sql(query_clipper_new, conn)

    df_editor_new = df_editor_new.replace([np.nan, np.inf, -np.inf], None)
    df_clipper_new = df_clipper_new.replace([np.nan, np.inf, -np.inf], None)

    upsert_to_table(df_editor_new, 't_sucai_editor_daily_report_new', ['账号名称', '编导', '日期'])
    print(f"维度1_new（账号名称-编导-日期，素材创建时间>={month_first}）已保存到数据库，共 {len(df_editor_new)} 条")

    upsert_to_table(df_clipper_new, 't_sucai_clipper_daily_report_new', ['账号名称', '剪辑', '日期'])
    print(f"维度2_new（账号名称-剪辑-日期，素材创建时间>={month_first}）已保存到数据库，共 {len(df_clipper_new)} 条")
    # ========== 新增结束 ==========

    # 新建表并写入数据
    all_df_month.to_sql(name='t_sucai_monthly_report', con=engine, if_exists='replace', index=False)
    all_df.to_sql(name='t_sucai_daily_report', con=engine, if_exists='replace', index=False)
    print("数据已保存到数据库表：t_sucai_monthly_report（月度汇总）, t_sucai_daily_report（日期明细）")

else:
    # 输出到 Excel
    with pd.ExcelWriter('素材_月度.xlsx') as writer:
        all_df_month.to_excel(writer, sheet_name='月度汇总', index=False)
        all_df.to_excel(writer, sheet_name='日期明细', index=False)
    print("数据已输出到 Excel 文件：素材_月度.xlsx")