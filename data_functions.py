import os
import glob
import calendar
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dateutil import parser
import numpy as np

def convert_watch_time_to_minutes(time_str):
    """
    将观看时间字符串直接转换为分钟单位的数值
    支持格式: "17秒", "1分钟2秒", "2分钟"等
    """
    if pd.isna(time_str) or time_str == '':
        return 0.0
    
    # 清理字符串中的空格
    time_str = str(time_str).strip()
    
    # 匹配分钟和秒
    minute_pattern = r'(\d+)分钟'
    second_pattern = r'(\d+)秒'
    
    total_seconds = 0
    
    # 提取分钟并转换为秒
    minute_match = re.search(minute_pattern, time_str)
    if minute_match:
        total_seconds += int(minute_match.group(1)) * 60
    
    # 提取秒
    second_match = re.search(second_pattern, time_str)
    if second_match:
        total_seconds += int(second_match.group(1))
    
    # 如果没有分钟只有秒
    if total_seconds == 0 and '分钟' not in time_str and second_match:
        total_seconds = int(second_match.group(1))
    
    # 转换为分钟并保留2位小数
    return round(total_seconds / 60, 2)

# 生成小时记录的函数
def expand_hours(row):
    try:
        start = datetime.strptime(row['直播开始时间'], '%Y-%m-%d %H-%M-%S')
    except:
        start = datetime.strptime(row['直播开始时间'], '%Y-%m-%d_%H-%M-%S')
    try:
        end = datetime.strptime(row['直播结束时间'], '%Y-%m-%d %H-%M-%S')
    except:
        end = datetime.strptime(row['直播结束时间'], '%Y-%m-%d_%H-%M-%S')
    
    # 调整到整点小时
    current = start.replace(minute=0, second=0, microsecond=0)
    end_hour = end.replace(minute=0, second=0, microsecond=0)
    if end > end_hour:
        end_hour += timedelta(hours=1)
    
    records = []
    while current < end_hour:
        records.append({
            '日期': row['日期'],
            '直播开始时间': row['直播开始时间'],
            '直播结束时间': row['直播结束时间'],
            '小时': current.strftime('%H'),
            '日期-小时': current.strftime('%Y-%m-%d %H:00')
        })
        current += timedelta(hours=1)
    
    return records
def process_columns(df):
    df_processed = df.copy()
    
    for col in df_processed.columns:
        if '率' in col:
            # 处理百分比列
            df_processed.loc[:, col] = (
                df_processed[col].astype(str)
                .apply(lambda x: str(float(x.replace('%', '')) / 100) if '%' in x else x)
                .replace(['nan', ''], '0')
                .astype(float)
            )
        else :
            try:
            # 处理金额列
                df_processed.loc[:, col] = (
                    df_processed[col].astype(str)
                    .str.replace(r'[¥,]', '', regex=True)
                    .replace(['nan', ''], '0')
                    .astype(float)
                )
            except:
                pass
    return df_processed

def auto_format_date(date_str):
    """自动检测日期格式并转换为 yyyy-mm-dd"""
    date_obj = parser.parse(date_str)
    return date_obj.strftime("%Y-%m-%d")

def return_year_month_day(date_str):
        input_date = datetime.strptime(date_str, '%Y-%m-%d')
        year = input_date.year
        month = input_date.month
        day = input_date.day
        return year,month,day
def ensure_max_5_weeks(date_str, week_start=1):
    """
    确保月份最多显示5周，支持yyyy-mm-dd格式字符串输入
    
    Args:
        date_str: yyyy-mm-dd格式的日期字符串
        week_start: 0=周日开始, 1=周一开始
    
    Returns:
        调整后的周列表，每个元素为(start_date, end_date)的元组
    """
    try:
        # 解析yyyy-mm-dd格式的字符串
        year,month,day = return_year_month_day(date_str)
        
        
        # 获取月份中的所有周范围
        first_day = datetime(year, month, 1)
        last_day = first_day + relativedelta(months=1) - timedelta(days=1)
        
        weeks = []
        current_date = first_day - timedelta(days=(first_day.weekday() + (1 if week_start == 1 else 0)) % 7)
        
        while current_date <= last_day:
            week_end = current_date + timedelta(days=6)
            if week_end >= first_day:  # 只包含与月份有交集的周
                weeks.append((current_date, week_end))
            current_date += timedelta(days=7)
        
        # 如果超过5周，合并首尾的短周
        if len(weeks) > 5:
            first_week_days = (weeks[0][1] - datetime(year, month, 1)).days + 1
            last_week_days = (datetime(year, month, calendar.monthrange(year, month)[1]) - weeks[-1][0]).days + 1
            
            if first_week_days <= 2:  # 首周少于3天则合并到下一周
                weeks = weeks[1:]
            elif last_week_days <= 2:  # 尾周少于3天则合并到前一周
                weeks = weeks[:-1]
        
        return weeks
        
    except ValueError as e:
        print(f"日期格式错误: {e}，请使用yyyy-mm-dd格式")
        return []
    except Exception as e:
        print(f"计算错误: {e}")
        return []

def merge_excel_files(folder_path, file_name_pattern):
    """
    合并多个相同字段的Excel和CSV文件到一个DataFrame
    
    参数:
    folder_path (str): 文件夹路径
    file_name_pattern (str): 文件名模式，支持通配符
    
    返回:
    pd.DataFrame: 合并后的DataFrame
    """
    # 构建Excel和CSV文件的路径模式
    excel_pattern = os.path.join(folder_path, f"{file_name_pattern}.xlsx")
    csv_pattern = os.path.join(folder_path, f"{file_name_pattern}.csv")
    
    # 获取匹配的文件列表
    excel_files = glob.glob(excel_pattern)
    csv_files = glob.glob(csv_pattern)
    file_list = excel_files + csv_files
    
    # 如果没有找到文件，返回空的DataFrame
    if not file_list:
        print(f"警告: 在路径 {folder_path} 下没有找到匹配 {file_name_pattern} 的Excel或CSV文件")
        return pd.DataFrame()
    
    # 存储所有DataFrame的列表
    dfs = []
    
    # 遍历所有文件
    for file_path in file_list:
        try:
            # 根据文件扩展名选择读取方式
            file_extension = os.path.splitext(file_path)[1].lower()
            
            if file_extension == '.xlsx':
                df = pd.read_excel(file_path)
            elif file_extension == '.csv':
                # 自动检测编码格式
                encodings = ['utf-8', 'gbk', 'gb2312', 'latin1']
                df = None
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    raise Exception("无法确定文件编码")
            else:
                print(f"跳过不支持的文件格式: {file_path}")
                continue
            
            # 添加文件名作为一列，便于追踪数据来源
            df['source_file'] = os.path.basename(file_path)
            
            # 添加到列表
            dfs.append(df)
            
        except Exception as e:
            print(f"读取文件 {file_path} 时出错: {str(e)}")
            continue
    
    # 合并所有DataFrame
    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        print(f"合并完成！总共合并了 {len(file_list)} 个文件，总计 {len(merged_df)} 行数据")
        return merged_df
    else:
        print("没有成功读取任何文件")
        return pd.DataFrame()

def san_dy_self_live_data(df):
    df["直播扣退成交"] = df.apply(lambda row: row["直播间成交金额"] - row["直播间退款金额"],axis=1)
    df["日期"] = df.apply(lambda x:auto_format_date(x))
    df['日期'] = pd.to_datetime(df['日期'])
    df["周"] = df.apply(lambda x:ensure_max_5_weeks(x))
    df['月'] = df['日期'].dt.month
    return df
def convert_units(series):
    series = series.astype(str)
    mask = series.str.contains('万')
    series = series.str.replace('万', '', regex=False)
    series = pd.to_numeric(series, errors='coerce')
    series[mask] = series[mask] * 10000
    return series.fillna(0)
def calculate_all_metrics(df, date_col='直播开始时间', channel_col='渠道名称', 
                        sales_col='成交金额', viewers_col='观看次数', 
                        overall_label='整体'):
    """
    计算所有指标：占比和千次观看成交金额
    
    参数:
    df: 原始DataFrame
    date_col: 日期列名
    channel_col: 渠道列名
    sales_col: 成交金额列名
    viewers_col: 观看人数列名
    overall_label: 整体标识
    
    返回:
    包含所有计算指标的DataFrame
    """
    # 创建副本
    result_df = df.copy()
    
    # 确保数值列是数值类型
    result_df[sales_col] = convert_units(result_df[sales_col]).fillna(0)
    result_df[viewers_col] = convert_units(result_df[viewers_col]).fillna(0)
    
    # 获取整体值
    overall_df = result_df[result_df[channel_col] == overall_label]
    overall_sales_map = overall_df.set_index(date_col)[sales_col].to_dict()
    overall_viewers_map = overall_df.set_index(date_col)[viewers_col].to_dict()
    
    # 计算成交金额占比
    result_df[f'{sales_col}_占比'] = result_df.apply(
        lambda row: 1 if row[channel_col] == overall_label else 
                (row[sales_col] / overall_sales_map.get(row[date_col], 1)
                    if overall_sales_map.get(row[date_col], 0) > 0 else 0),
        axis=1
    )
    
    # 计算观看人数占比
    result_df[f'{viewers_col}_占比'] = result_df.apply(
        lambda row: 1 if row[channel_col] == overall_label else 
                (row[viewers_col] / overall_viewers_map.get(row[date_col], 1)
                    if overall_viewers_map.get(row[date_col], 0) > 0 else 0),
        axis=1
    )
    # 计算千次观看成交金额
    result_df['千次观看成交金额'] = result_df.apply(
        lambda row: row[sales_col] / row[viewers_col] * 1000 if row[viewers_col] > 0 else 0,
        axis=1
    )
    
    return result_df


# 周计算逻辑
def calculate_week_excel_style(date_obj):
    """
    按照Excel WEEKNUM函数逻辑计算周数
    ="第"&WEEKNUM(B22,15)-WEEKNUM(B22-DAY(B22)+1,15)+1&"周"
    参数15表示：周一为一周的第一天，从包含1月1日的周开始
    """
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    
    # 获取当月第一天
    first_day_of_month = pd.Timestamp(year=year, month=month, day=1)
    
    # 计算当月第一天是周几 (0=周一, 6=周日)
    first_day_weekday = first_day_of_month.weekday()
    
    # 计算当月第一天所在的周的第一天（周一）
    first_monday_of_month = first_day_of_month - timedelta(days=first_day_weekday)
    
    # 计算当前日期所在的周的第一天（周一）
    current_monday = date_obj - timedelta(days=date_obj.weekday())
    
    # 计算是第几周
    week_of_month = ((current_monday - first_monday_of_month).days // 7) + 1
    
    # 确保周数在1-5之间
    week_of_month = max(1, min(week_of_month, 5))
    
    return f"第{week_of_month}周"
def calculate_week_excel_style_series(date_series):
    """
    专门为Series设计的向量化版本
    """
    date_series = pd.to_datetime(date_series)
    
    # 获取当月第一天
    first_day_of_month = pd.to_datetime(date_series.dt.to_period('M').dt.start_time)
    
    # 计算当月第一天所在的周一
    first_monday_of_month = first_day_of_month - pd.to_timedelta(first_day_of_month.dt.weekday, unit='D')
    
    # 计算当前日期所在的周一
    current_monday = date_series - pd.to_timedelta(date_series.dt.weekday, unit='D')
    
    # 计算周数
    week_of_month = ((current_monday - first_monday_of_month).dt.days // 7) + 1
    week_of_month = week_of_month.clip(1, 5)
    
    # 处理缺失值并转换为整数
    week_of_month = week_of_month.fillna(1).astype(int)
    
    return "第" + week_of_month.astype(str) + "周"
import os
import re
from datetime import datetime

def clean_duplicate_livestream_files(directory_path):
    """
    清理直播间数据文件的重复版本，保留每个直播间最新的下载文件
    
    Args:
        directory_path (str): 包含Excel文件的目录路径
    """
    # 匹配文件名的正则表达式
    pattern = r'直播间详情页_整场数据下载_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\((.*?_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\)'
    
    file_groups = {}
    
    # 遍历目录中的所有文件
    for filename in os.listdir(directory_path):
        match = re.match(pattern, filename)
        if match:
            download_time_str = match.group(1)  # 下载时间
            store_info = match.group(2)  # 店铺信息和时间
            
            # 将下载时间字符串转换为datetime对象
            try:
                download_time = datetime.strptime(download_time_str, '%Y-%m-%d_%H-%M-%S')
            except ValueError:
                print(f"跳过文件 {filename} - 时间格式不正确")
                continue
            
            # 使用店铺信息作为分组键
            if store_info not in file_groups:
                file_groups[store_info] = []
            
            file_groups[store_info].append({
                'filename': filename,
                'download_time': download_time,
                'full_path': os.path.join(directory_path, filename)
            })
    
    # 处理每个分组，保留最新的文件
    deleted_count = 0
    kept_count = 0
    
    for store_info, files in file_groups.items():
        if len(files) > 1:
            # 按下载时间排序，最新的在前面
            files.sort(key=lambda x: x['download_time'], reverse=True)
            
            # 保留第一个（最新的）文件，删除其他的
            kept_file = files[0]
            print(f"\n店铺 '{store_info}' 的文件组:")
            kept_count += 1
            
            for file_to_delete in files[1:]:
                try:
                    os.remove(file_to_delete['full_path'])
                    print(f"  删除: {file_to_delete['filename']} (下载时间: {file_to_delete['download_time'].strftime('%Y-%m-%d %H:%M:%S')})")
                    deleted_count += 1
                except OSError as e:
                    print(f"  删除失败 {file_to_delete['filename']}: {e}")
        else:
            # 只有一个文件，直接保留
            print(f"\n店铺 '{store_info}' 只有一个文件:")
            kept_count += 1
    
    print(f"\n处理完成!")
    print(f"总共处理了 {len(file_groups)} 个不同的直播间")
    print(f"保留了 {kept_count} 个文件")
    print(f"删除了 {deleted_count} 个重复文件") 

def get_latest_file(live_room):
    # 定义文件模式
    path = r"D:\python project\合并结果"
    file_pattern = f"合并后的直播数据_{live_room}_*.xlsx"  # 如果是其他格式可以修改
    file_pattern = os.path.join(path,file_pattern)
    # 获取所有匹配的文件
    files = glob.glob(file_pattern)
    
    if not files:
        raise FileNotFoundError("没有找到匹配的文件")
    
    # 提取文件名中的时间戳并排序
    file_times = []
    for file in files:
        # 从文件名中提取时间戳部分
        match = re.search(r'_(\d{8}_\d{6})', file)
        if match:
            timestamp_str = match.group(1)
            try:
                # 将时间戳字符串转换为datetime对象
                file_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                file_times.append((file_time, file))
            except ValueError:
                continue
    
    if not file_times:
        raise ValueError("无法从文件名中解析时间戳")
    
    # 按时间戳排序，选择最新的
    latest_file = max(file_times, key=lambda x: x[0])[1]
    return latest_file