# -*- coding: utf-8 -*-
import pandas as pd
import pymysql
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')

# 检查 expand_hours 函数
from data_functions import expand_hours

LIVE_ROOM_MAPPING = {
    "鱼子酱": "弹动官方旗舰店",
    "椰子": "弹动个人护理旗舰店"
}

def get_live_room_data_from_db(live_room):
    account_name = LIVE_ROOM_MAPPING.get(live_room)
    if not account_name:
        return {}
    conn = pymysql.connect(host='localhost', user='root', password='123456', database='baiyin_data', charset='utf8mb4')
    try:
        table_mapping = {
            '基本信息': 'basic_info',
            '流量&转化-转化漏斗': 'traffic_conversion_funnel',
            '流量分析-渠道分析': 'traffic_analysis_channel',
            '流量&转化-短视频引流': 'traffic_short_video',
            '互动&人群&售后': 'interaction_after_sale',
            '直播间总体数据': 'live_room_summary',
            '商品数据': 'product_data',
            'SKU数据': 'sku_data'
        }
        merged_data = {}
        for sheet_name, table_name in table_mapping.items():
            query = f"SELECT * FROM {table_name} WHERE 账号名称='{account_name}'"
            df = pd.read_sql(query, conn)
            merged_data[sheet_name] = df
        return merged_data
    finally:
        conn.close()

def day_of_week_chinese(df):
    weekday_map = {
        0: '星期一',
        1: '星期二',
        2: '星期三',
        3: '星期四',
        4: '星期五',
        5: '星期六',
        6: '星期日'
    }
    df['日期'] = pd.to_datetime(df['日期'])
    df['星期'] = df['日期'].dt.dayofweek.map(weekday_map)
    return df

def time_to_seconds(time_str):
    if pd.isna(time_str) or time_str == '':
        return 0
    minutes = 0
    seconds = 0
    if '分钟' in time_str:
        minutes = int(time_str.split('分钟')[0])
        if '秒' in time_str:
            seconds = int(time_str.split('分钟')[1].replace('秒', ''))
    elif '秒' in time_str:
        seconds = int(time_str.replace('秒', ''))
    return minutes * 60 + seconds

def process_live_time(data_dict):
    basic_info_df = data_dict.get('基本信息')
    if basic_info_df is None:
        raise ValueError("字典中未找到'基本信息'sheet")

    if '直播时间' not in basic_info_df.columns:
        raise ValueError("基本信息sheet中未找到'直播时间'字段")

    def extract_start_time(time_str):
        if pd.isna(time_str) or time_str == '':
            return None
        match = re.search(r'(\d{4}-\d{2}-\d{2}[_ ]\d{2}-\d{2}-\d{2})~', str(time_str))
        if match:
            return match.group(1).replace(' ', '_')
        return None

    def extract_end_time(time_str):
        if pd.isna(time_str) or time_str == '':
            return None
        match = re.search(r'~(\d{4}-\d{2}-\d{2}[_ ]\d{2}-\d{2}-\d{2})', str(time_str))
        if match:
            return match.group(1).replace(' ', '_')
        return None

    def extract_date(start_time):
        if pd.isna(start_time) or start_time == '':
            return None
        return start_time[:10] if len(start_time) >= 10 else None

    def standardize_time_format(time_str):
        if pd.isna(time_str) or time_str == '':
            return None
        return str(time_str).replace(' ', '_')

    basic_info_df = basic_info_df.copy()
    basic_info_df['直播开始时间'] = basic_info_df['直播时间'].apply(extract_start_time)
    basic_info_df['直播结束时间'] = basic_info_df['直播时间'].apply(extract_end_time)
    basic_info_df['日期'] = basic_info_df['直播开始时间'].apply(extract_date)

    end_time_to_start_time = {}
    end_time_to_date = {}

    for _, row in basic_info_df.iterrows():
        if pd.notna(row['直播结束时间']):
            end_time = row['直播结束时间']
            end_time_to_start_time[end_time] = row['直播开始时间']
            end_time_to_date[end_time] = row['日期']

    for sheet_name, df in data_dict.items():
        if sheet_name != '基本信息':
            if '直播结束时间' in df.columns:
                df = df.copy()
                df['标准化结束时间'] = df['直播结束时间'].apply(standardize_time_format)
                df['直播结束时间'] = df['标准化结束时间']
                df['直播开始时间'] = df['标准化结束时间'].map(end_time_to_start_time)
                df['日期'] = df['标准化结束时间'].map(end_time_to_date)
                df.drop('标准化结束时间', axis=1, inplace=True)
                data_dict[sheet_name] = df

    data_dict['基本信息'] = basic_info_df
    return data_dict

def process_columns(merged_data):
    merged_data_new = {}

    for sheet_name, df in merged_data.items():
        df_processed = df.copy()
        df_processed = df_processed.replace("-", "")
        for col in df_processed.columns:
            if '率' in col:
                df_processed.loc[:, col] = (
                    df_processed[col].astype(str)
                    .apply(lambda x: str(float(x.replace('%', '')) / 100) if '%' in x else x)
                    .replace(['nan', ''], '0')
                    .astype(float)
                )
            elif '金额' in col or '笔单价' in col or '消耗' in col or '人数' in col or '次数' in col or '件数' in col or '订单数' in col:
                df_processed.loc[:, col] = (
                    df_processed[col].astype(str)
                    .str.replace(r'[¥,，]', '', regex=True)
                    .replace(['nan', '', '-'], '0')
                    .astype(float)
                )
            elif '人均观看时长' in col:
                df_processed.loc[:, col] = df_processed[col].apply(time_to_seconds)
        merged_data_new[sheet_name] = df_processed
    return merged_data_new

# 测试
print('=== Getting data ===')
merged_data = get_live_room_data_from_db('椰子')

print('=== Processing live time ===')
merged_data = process_live_time(merged_data)

print('=== Processing columns ===')
merged_data = process_columns(merged_data)

print('\n=== Testing live_room_okr_data ===')
df = merged_data["基本信息"][["日期","直播结束时间",'直播开始时间','千次观看成交金额']].copy()
df["直播场次计数"] = 1
df = day_of_week_chinese(df)

# Merge 流量分析
traffic_analysis_overall = merged_data["流量分析-渠道分析"][merged_data["流量分析-渠道分析"]["渠道名称"]=="整体"][["直播结束时间",'直播开始时间',"千川消耗","人均观看时长","观看次数"]]
df = df.merge(traffic_analysis_overall, on=["直播结束时间",'直播开始时间'])
df["人均观看时长"] = df["人均观看时长"].replace(['nan',''],0).astype(float)
df['观看次数'] = df['观看次数'].apply(lambda x: float(str(x).replace('万', '')) * 10000 if '万' in str(x) else float(x))
df["千川消耗"] = df["千川消耗"].astype(float)

# Merge 流量漏斗
df = df.merge(merged_data["流量&转化-转化漏斗"][["直播结束时间",'直播开始时间',"自然流量观看人数","付费流量观看人数",
                                        "平均在线人数","直播间曝光人数","直播间观看人数","直播间曝光次数","商品曝光人数","商品点击人数","成交人数"]], on=["直播结束时间",'直播开始时间'])

# 确保所有数值列都转换为 float 类型
numeric_cols_from_funnel = ["自然流量观看人数","付费流量观看人数","平均在线人数","直播间曝光人数",
                            "直播间观看人数","直播间曝光次数","商品曝光人数","商品点击人数","成交人数"]
for col in numeric_cols_from_funnel:
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'[¥,，\-]', '', regex=True).replace(['nan', ''], '0').astype(float)

df["观看总时长"] = df.apply(lambda row:
                row["直播间观看人数"] * row["人均观看时长"],
                axis=1)

# Merge 互动售后
df = df.merge(merged_data["互动&人群&售后"][["直播结束时间",'直播开始时间',"退款人数","新增粉丝数"]], on=["直播结束时间",'直播开始时间'])

# 确保互动售后表中的数值列也转换为 float
for col in ["退款人数", "新增粉丝数"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'[¥,，\-]', '', regex=True).replace(['nan', ''], '0').astype(float)

target_columns = ["平均在线人数","直播间观看人数","直播间曝光人数","商品曝光人数","自然流量观看人数","付费流量观看人数","观看次数",
                        "直播间曝光次数","商品点击人数","成交人数","千次观看成交金额",
                        "退款人数","直播场次计数","观看总时长","新增粉丝数"]

df_day = df.groupby(['日期','直播开始时间','直播结束时间','星期'])[target_columns].sum(numeric_only=True).reset_index()

print(f'\nAfter groupby - df_day columns: {df_day.columns.tolist()}')
print(f'\nlive_room_okr_df shape: {df_day.shape}')

# 模拟 main 中的处理
live_room_okr_df = df_day.copy()
live_room_okr_df["日期"] = pd.to_datetime(live_room_okr_df["日期"])
live_room_okr_df.drop(columns="新增粉丝数", inplace=True)

print(f'\nAfter drop 新增粉丝数 - live_room_okr_df columns: {live_room_okr_df.columns.tolist()}')

# 生成 live_time_day_time_new
live_time_day_time = live_room_okr_df[["日期","直播开始时间","直播结束时间"]]
print(f'\nlive_time_day_time columns: {live_time_day_time.columns.tolist()}')
print(f'live_time_day_time sample:\n{live_time_day_time.head(3)}')

print('\nCalling expand_hours...')
live_time_day_time_new = pd.DataFrame([record for _, row in live_time_day_time.iterrows() for record in expand_hours(row)])
print(f'\nlive_time_day_time_new columns: {live_time_day_time_new.columns.tolist()}')
print(f'live_time_day_time_new shape: {live_time_day_time_new.shape}')
print(f'live_time_day_time_new sample:\n{live_time_day_time_new.head(3)}')

print('\n=== TEST COMPLETE ===')