# -*- coding: utf-8 -*-

import pandas as pd
import os
import glob
import re  
import numpy as np
from openpyxl import load_workbook
from datetime import datetime,timedelta
from typing import Tuple, List
from data_functions import merge_excel_files
now = datetime.now()
today = now.strftime("%Y-%m-%d")
end_time = (now - timedelta(days=1)).strftime("%Y-%m-%d")
start_time = (now - timedelta(days=14)).strftime("%Y-%m-%d")
all_data_time_start = (now - timedelta(days=30)).strftime("%Y-%m-%d")



def save_to_excel(dataframes, filename, **kwargs):
    """
    简化版的Excel写入函数
    
    参数:
    dataframes: dict, 格式为 {'sheet_name': dataframe} 或 list of (sheet_name, dataframe) tuples
    filename: str, 输出的Excel文件名
    **kwargs: 其他传递给ExcelWriter的参数
    """
    # 如果传入的是列表，转换为字典
    if isinstance(dataframes, list):
        dataframes = dict(dataframes)
    
    try:
        # 使用with语句确保文件正确关闭
        with pd.ExcelWriter(filename, engine='openpyxl', **kwargs) as writer:
            for sheet_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"成功将 {len(dataframes)} 个sheet写入文件: {filename}")
        
    except Exception as e:
        print(f"保存到Excel失败: {e}")
        raise

def safe_divide(numerator, denominator):
    """
    安全的除法运算，处理分母为0或空值的情况
    
    参数:
    numerator -- 分子
    denominator -- 分母
    
    返回:
    float -- 除法结果，分母为0或空值时返回0
    """
    # 检查分母是否为0、空值或NaN
    if pd.isna(denominator) or denominator == 0:
        return 0.0
    else:
        return numerator / denominator

def calculate_roi_safely(df, numerator_col, denominator_col, result_col="时段roi"):
    """
    安全地计算ROI，处理分母为0和空值的情况
    
    参数:
    df -- 输入的DataFrame
    numerator_col -- 分子列名
    denominator_col -- 分母列名
    result_col -- 结果列名，默认为"时段roi"
    
    返回:
    DataFrame -- 包含计算结果的新DataFrame
    """
    result_df = df.copy()
    
    # 使用安全的除法运算
    result_df[result_col] = result_df.apply(
        lambda row: safe_divide(row[numerator_col], row[denominator_col]), 
        axis=1
    )
    
    return result_df

def convert_to_percentage(df):
    """
    将DataFrame中列名包含"率"或"占比"的列转换为百分数格式
    
    参数:
    df -- 输入的DataFrame
    
    返回:
    DataFrame -- 转换后的DataFrame
    """
    # 创建DataFrame的副本，避免修改原始数据
    result_df = df.copy()
    numeric_cols = result_df.select_dtypes(include=[np.number]).columns

    for col in numeric_cols:
        result_df[col] = result_df[col].fillna(0).round(4)
    # 找出所有列名中包含"率"或"占比"的列
    target_columns = [col for col in result_df.columns if '率' in col or '占比' in col or '转化' in col or '环比' in col]
    
    print(f"找到需要转换的列: {target_columns}")
    
    for col in target_columns:
        # 检查列是否为数值类型
        if result_df[col].dtype in ['float64', 'int64', 'float32', 'int32']:
            # 将空值填充为0，然后转换为百分数字符串
            result_df[col] = result_df[col].fillna(0).apply(lambda x: f"{x:.2%}")
        else:
            print(f"警告: 列 '{col}' 不是数值类型，无法转换为百分数格式")

    return result_df

def filter_time_data(df,start_time,end_time,column="日期"):
    df = df[(df[column] >= start_time) & (df[column] <= end_time)]
    return df

def swatch_case_time_tran(df,column):
    # 定义班次映射函数
    def get_shift(hour_range):
        # 提取开始小时
        start_hour = int(hour_range.split(':')[0])
        
        # 根据班次定义映射
        if 8 <= start_hour < 12:
            return '早班'
        elif 12 <= start_hour < 16:
            return '次早班'
        elif 16 <= start_hour < 20:
            return '次晚班'
        elif 20 <= start_hour <= 23:
            return '晚班'
        else:
            return '其他'  # 处理不在班次时间范围内的情况
    # 应用函数创建班次列
    df['班次'] = df[column].apply(get_shift)
    return df

def concat_file_list_data(file_list):
    all_data = pd.DataFrame()

    for file in file_list:
        try:
            # 读取Excel文件
            df = pd.read_excel(file)
            
            # 添加文件名作为标识列（可选）
            # df['来源文件'] = os.path.basename(file)
            
            # 方法2：更简洁的写法
            all_data = pd.concat([all_data, df]).drop_duplicates()
        except Exception as e:
            print(f"读取文件 {file} 时出错: {e}")
    all_data = all_data.drop_duplicates()
    if '整体消耗' in all_data.columns:
        all_data['整体消耗'] = all_data['整体消耗'].apply(lambda x: float(x.replace(',', '')) if isinstance(x, str) else float(str(x).replace(',', '')))
        all_data['整体消耗'] = pd.to_numeric(all_data['整体消耗'], errors='coerce').fillna(0)
        all_data = (all_data.sort_values('整体消耗', ascending=False)
                  .drop_duplicates('时间-小时')
                  .sort_values('时间-小时'))
    #     all_data = all_data.loc[all_data.groupby('时间-小时')['整体消耗'].idxmax()].reset_index(drop=True)
    return all_data

#处理字符串类型
def process_columns(df):
    df_processed = df.copy()
    
    for col in df_processed.columns:
        if '率' in col:
            # 处理百分比列
            df_processed.loc[:, col] = (
                df_processed[col].astype(str)
                .str.replace('%', '')
                .replace(['nan', ''], '0')
                .astype(float) / 100
            )
            
        elif '金额' in col:
            # 处理金额列
            df_processed.loc[:, col] = (
                df_processed[col].astype(str)
                .str.replace(r'[¥,]', '', regex=True)
                .replace(['nan', ''], '0')
                .astype(float)
            )
    
    return df_processed

###将耗时转化成秒
def convert_time_to_seconds(time_str):
    """
    稳健的时间字符串转秒数函数
    """
    if pd.isna(time_str) or time_str == '' or not isinstance(time_str, str):
        return 0
    
    total_seconds = 0
    
    try:
        # 使用更全面的正则表达式
        patterns = [
            (r'(\d+)\s*[小]?时', 3600),      # 小时
            (r'(\d+)\s*分钟?', 60),          # 分钟
            (r'(\d+)\s*秒', 1)               # 秒
        ]
        
        for pattern, multiplier in patterns:
            match = re.search(pattern, time_str)
            if match:
                value = int(match.group(1))
                total_seconds += value * multiplier
        
        return total_seconds
    except Exception as e:
        print(f"转换错误: {time_str}, 错误: {e}")
        return 0

##计算
def process_shift_data(shift_time_col='班次时间', shift_name_col='班次', anchor_col='主播'):
    """
    处理班次数据：提取开始结束时间 + 计算上播时长
    
    参数:
    df: 包含班次时间信息的DataFrame
    shift_time_col: 班次时间列的列名
    shift_name_col: 班次名称列的列名
    anchor_col: 主播名称列的列名
    
    返回:
    添加了开始时间、结束时间和上播时长列的新DataFrame
    """
    # 复制原始DataFrame以避免修改原数据
    df = pd.read_excel("banci.xlsx")
    result_df = df.copy()
    
    # 提取开始时间和结束时间
    result_df['开始时间'] = result_df[shift_time_col].str.split('-').str[0]
    result_df['结束时间'] = result_df[shift_time_col].str.split('-').str[1]
    
    # 转换为时间格式（可选）
    try:
        result_df['开始时间'] = pd.to_datetime(result_df['开始时间'], format='%H:%M').dt.time
        result_df['结束时间'] = pd.to_datetime(result_df['结束时间'], format='%H:%M').dt.time
    except:
        # 如果转换失败，保持字符串格式
        pass
    
    # 计算上播时长（小时）
    def calculate_hours(shift_time):
        """计算单个班次的时长"""
        try:
            start_time, end_time = shift_time.split('-')
            
            # 提取小时和分钟
            start_h, start_m = map(int, start_time.split(':'))
            end_h, end_m = map(int, end_time.split(':'))
            
            # 计算总分钟数
            start_total_minutes = start_h * 60 + start_m
            end_total_minutes = end_h * 60 + end_m
            
            # 处理跨天的情况（如23:00-01:00）
            if end_total_minutes < start_total_minutes:
                end_total_minutes += 24 * 60  # 加上一天的分钟数
            
            # 计算时长（小时）
            duration_hours = (end_total_minutes - start_total_minutes) / 60.0
            return round(duration_hours, 2)
            
        except:
            return 0  # 如果格式错误返回0
    
    result_df['上播时长(小时)'] = result_df[shift_time_col].apply(calculate_hours)
    
    # 重新排列列的顺序
    columns_order = [shift_time_col, shift_name_col, anchor_col, '开始时间', '结束时间', '上播时长(小时)']
    # 只保留存在的列
    columns_order = [col for col in columns_order if col in result_df.columns]
    result_df = result_df[columns_order]
    
    return result_df

##标杆数据周期处理
def get_data_period(input_date):
    """
    根据输入日期确定数据周期
    规则：从2025-08-27开始，每14天为一个周期，下一周期使用上一周期的数据
    
    参数:
        input_date: 字符串格式的日期 'YYYY-MM-DD' 或 datetime对象
        
    返回:
        tuple: (数据周期开始日期, 数据周期结束日期) 格式为 'YYYY-MM-DD'
    """
    
    # 基准日期
    base_date = datetime(2025, 8, 27)
    
    # 处理输入日期
    if isinstance(input_date, str):
        input_date = datetime.strptime(input_date, '%Y-%m-%d')
    
    # 计算输入日期与基准日期的天数差
    days_diff = (input_date - base_date).days
    
    # 计算当前周期编号
    period_num = days_diff // 14
    
    # 计算数据周期（使用上一周期的数据）
    if period_num > 0:
        data_period_start = base_date + timedelta(days=(period_num - 1) * 14)
    else:
        # 如果是第一个周期，没有上一周期数据，使用当前周期
        data_period_start = base_date
    
    data_period_end = data_period_start + timedelta(days=13)
    
    return (data_period_start.strftime('%Y-%m-%d'), 
            data_period_end.strftime('%Y-%m-%d'))