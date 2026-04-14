import pandas as pd
import os
import re
from datetime import datetime
import glob
import pymysql
from qianchuan_juliang_live import swatch_case_time_tran,concat_file_list_data,convert_to_percentage,today
from data_functions import calculate_all_metrics,expand_hours,clean_duplicate_livestream_files,calculate_all_metrics

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'qianchuan_sucai',
    'charset': 'utf8mb4'
}

def get_qianchuan_from_db(live_room):
    """从数据库获取千川数据"""
    live_dict = {
        "鱼子酱":"弹动官方旗舰店",
        "椰子":"弹动个人护理旗舰店"
    }
    connection = pymysql.connect(**DB_CONFIG)
    try:
        query = "SELECT * FROM qianchuan_douyin where `账号名称`='{}'".format(live_dict[live_room])
        df = pd.read_sql(query, connection)
        # 列名映射：数据库列名 -> 代码需要的列名
        column_mapping = {
            '小时区间': '时间-小时',
            '智能优惠券金额': '整体成交智能优惠券金额'
        }
        # 应用列名映射
        df = df.rename(columns=column_mapping)
        return df
    finally:
        connection.close()


def day_of_week_chinese(df):
    # 生成星期列
    # 创建星期映射字典
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

def merge_baiyin_qianchuan_data(live_room):
    # 从数据库获取千川数据
    print("正在从数据库读取千川数据...")
    qianchuan_df_modify = get_qianchuan_from_db(live_room)
    if qianchuan_df_modify.empty:
        print("数据库中没有千川数据")
        return pd.DataFrame(), pd.DataFrame()
    print(f"从数据库获取到 {len(qianchuan_df_modify)} 条千川数据")

    # 处理小时区间格式，生成时间-小时列
    if '时间-小时' in qianchuan_df_modify.columns:
        # 去除时间-小时中的空格
        qianchuan_df_modify['时间-小时'] = qianchuan_df_modify['时间-小时'].str.replace(' ', '', regex=False)
        # 提取小时部分
        qianchuan_df_modify['小时'] = qianchuan_df_modify['时间-小时'].str.split('-').str[0]
        # 将日期转换为字符串格式
        qianchuan_df_modify['日期'] = qianchuan_df_modify['日期'].astype(str)
        # 重新组合日期和小时为时间-小时格式
        qianchuan_df_modify['时间-小时'] = qianchuan_df_modify['日期'] + ' ' + qianchuan_df_modify['小时'] + ':00'

    qianchuan_df_modify.to_excel("qianchuan_df_modify1.xlsx",index=False)
    print("qianchuan_df_modify.columns:",qianchuan_df_modify.columns.tolist())
    print("qianchuan_df_modify 日期-小时 示例:", qianchuan_df_modify['日期-小时'].head().tolist() if '日期-小时' in qianchuan_df_modify.columns else "无日期-小时列")

    # 百应数据仍从本地文件读取
    data_path = os.path.join(r"D:\python project\python project", live_room)
    #筛选日期为2025-08-27到2025-09-07的数据  ------ 非必要重复
    # qianchuan_df_modify = filter_time_data(qianchuan_df_modify,'2025-08-31',end_time)
    # qianchuan_df_modify = swatch_case_time_tran(qianchuan_df_modify,"小时区间")
    # qianchuan_df_modify['小时'] = qianchuan_df_modify["小时区间"].apply(lambda x:x[:2])
    datetime_series = pd.to_datetime(qianchuan_df_modify['时间-小时'])
    qianchuan_df_modify['小时'] = datetime_series.dt.hour
    qianchuan_df_modify['日期'] = datetime_series.dt.strftime('%Y-%m-%d')
    qianchuan_df_modify[['整体消耗','整体支付ROI','整体成交金额','整体成交订单数','整体成交订单成本','净成交金额',
                        '整体成交智能优惠券金额','1小时内退款金额','1小时内退款订单数']] \
    = qianchuan_df_modify[['整体消耗','整体支付ROI','整体成交金额','整体成交订单数','整体成交订单成本','净成交金额',
                        '整体成交智能优惠券金额','1小时内退款金额','1小时内退款订单数']]\
    .applymap(lambda x: float(str(x).replace(',', '')) if pd.notna(x) else x)

    # 定义百应文件名的开头模式
    file_pattern = "*流量综合趋势分析下载_数据更新日期*"
    file_list = glob.glob(os.path.join(data_path, f"{file_pattern}.xlsx"))
    print(file_list)

    baiyin_df = concat_file_list_data(file_list)
    # baiyin_df["小时"] = baiyin_df["时间"].apply(lambda x:x[11:13])
    # baiyin_df["时间"] = pd.to_datetime(baiyin_df['时间'])
    # baiyin_df["时间"] = baiyin_df["时间"].apply(lambda x:x[:10]).apply(lambda x:x.replace("/","-"))

    # 转换为datetime格式
    baiyin_df["datetime"] = pd.to_datetime(baiyin_df["时间"], format="%Y/%m/%d %H:%M")
    baiyin_df["datetime"] = baiyin_df["datetime"].dt.floor('H')  # 向下取整到小时

    # 提取小时（保持两位数格式）
    baiyin_df["小时"] = baiyin_df["datetime"].dt.strftime("%H")

    # 格式化日期
    baiyin_df["时间"] = baiyin_df["datetime"].dt.strftime("%Y-%m-%d")
    baiyin_df["datetime"] = baiyin_df["datetime"].dt.strftime('%Y-%m-%d %H:00')

    baiyin_df_modify = swatch_case_time_tran(baiyin_df,"小时")
    baiyin_df_modify = baiyin_df_modify.drop_duplicates()
    baiyin_df_modify = baiyin_df_modify[['时间','datetime','小时','评论次数','新加直播团人数','新增粉丝数']]
    baiyin_df_modify.rename(columns={'时间': '日期',"datetime":"日期-小时"}, inplace=True)
    
    qianchuan_df_modify = day_of_week_chinese(qianchuan_df_modify)
    baiyin_df_modify = day_of_week_chinese(baiyin_df_modify)
    
    return baiyin_df_modify,qianchuan_df_modify

###直播间概览数据
def  process_live_time(data_dict):
    """
    处理直播时间数据，提取直播开始时间并匹配到其他sheet
    
    Args:
        data_dict: 字典，key为sheet名称，value为对应的DataFrame
        
    Returns:
        处理后的字典，包含新增的直播开始时间和日期字段
    """
    # 获取基本信息sheet
    basic_info_df = data_dict.get('基本信息')
    if basic_info_df is None:
        raise ValueError("字典中未找到'基本信息'sheet")
    
    # 确保存在直播时间字段
    if '直播时间' not in basic_info_df.columns:
        raise ValueError("基本信息sheet中未找到'直播时间'字段")
    
    # 提取直播开始时间
    def extract_start_time(time_str):
        if pd.isna(time_str) or time_str == '':
            return None
        # 使用正则表达式提取开始时间部分
        match = re.search(r'(\d{4}-\d{2}-\d{2}[_ ]\d{2}-\d{2}-\d{2})~', str(time_str))
        if match: 
            return match.group(1).replace(' ', '_')  # 统一转换为下划线格式
        return None
    
    # 提取直播结束时间
    def extract_end_time(time_str):
        if pd.isna(time_str) or time_str == '':
            return None
        # 使用正则表达式提取结束时间部分
        match = re.search(r'~(\d{4}-\d{2}-\d{2}[_ ]\d{2}-\d{2}-\d{2})', str(time_str))
        if match:
            return match.group(1).replace(' ', '_')  # 统一转换为下划线格式
        return None
    
    # 提取日期部分（年月日）
    def extract_date(start_time):
        if pd.isna(start_time) or start_time == '':
            return None
        # 提取日期部分（前10个字符）
        return start_time[:10] if len(start_time) >= 10 else None
    
    # 标准化时间格式（将空格转换为下划线）
    def standardize_time_format(time_str):
        if pd.isna(time_str) or time_str == '':
            return None
        return str(time_str).replace(' ', '_')
    
    # 在基本信息sheet中提取开始时间和结束时间
    basic_info_df['直播开始时间'] = basic_info_df['直播时间'].apply(extract_start_time)
    basic_info_df['直播结束时间'] = basic_info_df['直播时间'].apply(extract_end_time)
    basic_info_df['日期'] = basic_info_df['直播开始时间'].apply(extract_date)
    
    # 创建直播结束时间到直播开始时间的映射字典
    end_time_to_start_time = {}
    end_time_to_date = {}
    
    for _, row in basic_info_df.iterrows():
        if pd.notna(row['直播结束时间']):
            end_time = row['直播结束时间']
            end_time_to_start_time[end_time] = row['直播开始时间']
            end_time_to_date[end_time] = row['日期']
    
    print("映射关系创建完成:")
    for end_time, start_time in end_time_to_start_time.items():
        print(f"结束时间: {end_time} -> 开始时间: {start_time}")
    
    # 遍历所有sheet（除了基本信息）
    for sheet_name, df in data_dict.items():
        if sheet_name != '基本信息':
            # 检查是否有直播结束时间字段
            if '直播结束时间' in df.columns:
                print(f"处理sheet: {sheet_name}")
                print(f"原始数据中的结束时间: {df['直播结束时间'].tolist()[:5]}")  # 只显示前5个
                
                # 标准化其他sheet中的结束时间格式
                df['标准化结束时间'] = df['直播结束时间'].apply(standardize_time_format)
                
                # 为每个sheet添加直播开始时间和日期字段
                df['直播开始时间'] = df['标准化结束时间'].map(end_time_to_start_time)
                df['日期'] = df['标准化结束时间'].map(end_time_to_date)
                
                # 移除临时列
                df.drop('标准化结束时间', axis=1, inplace=True)
                
                print(f"匹配后的开始时间: {df['直播开始时间'].tolist()[:5]}")
                print(f"匹配后的日期: {df['日期'].tolist()[:5]}")
                print(f"成功匹配数量: {df['直播开始时间'].notna().sum()}/{len(df)}")

    # 更新字典中的各个sheet
    for sheet_name in data_dict.keys():
        if sheet_name != '基本信息':
            data_dict[sheet_name] = data_dict[sheet_name]
    
    return data_dict

def parse_filename(filename):
    """
    解析文件名，提取直播间名称、直播结束时间和数据下载时间
    格式：直播间详情页_整场数据下载_2025-09-09_15-40-44(弹动个人护理旗舰店_2025-09-08_00-00-46).xlsx
    """
    pattern = r'直播间详情页_整场数据下载_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\((.+)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\)\.xlsx'
    match = re.match(pattern, filename)
    
    if match:
        download_time = match.group(1).replace('_', ' ')
        live_name = match.group(2)
        end_time = match.group(3).replace('_', ' ')
        return {
            'download_time': download_time,
            'live_name': live_name,
            'end_time': end_time,
            'filename': filename
        }
    return None

def split_complete_dataframe(df): 
    """
    将完整的直播数据DataFrame拆分为三个子表：
    1. 直播间总体数据（固定第一行）
    2. 商品数据（数量不固定）
    3. SKU数据（数量不固定，与商品关联）
    """
    # 初始化三个表
    live_room_df = pd.DataFrame()
    product_df = pd.DataFrame()
    sku_df = pd.DataFrame()
    
    # 定义各表的列名
    live_room_columns = ['商品点击人数', '直播间商品曝光-点击率(人数)', '商品点击-成交转化率(人数)', '客单价']
    product_columns = ["商品名称", "商品图片", "商品编号", "成交金额", "成交件数", "成交人数", 
                    "成交订单数", "预售订单数", "预售创建金额", "商品曝光人数", "商品点击人数", 
                    "商品曝光-点击率(人数)", "商品点击-成交转化率(人数)", "退款人数", "退款金额", "退款订单数"]
    sku_columns = ["商品编号", "商品名称", "sku名称", "sku图片", "成交金额", "成交件数", "成交人数", 
                "成交订单数", "预售订单数", "预售创建金额", "退款人数", "退款金额", "退款订单数", "商品支付GPM"]
    
    # 1. 提取直播间总体数据（固定第一行）
    if len(df) > 0:
        first_row = df.iloc[0]
        live_data = {
            '商品点击人数': first_row.iloc[0] if len(first_row) > 0 else None,
            '直播间商品曝光-点击率(人数)': first_row.iloc[1] if len(first_row) > 1 else None,
            '商品点击-成交转化率(人数)': first_row.iloc[2] if len(first_row) > 2 else None,
            '客单价': first_row.iloc[3] if len(first_row) > 3 else None
        }
        live_room_df = pd.DataFrame([live_data])
    
    # 2. 遍历剩余行，提取商品和SKU数据
    current_product_id = None
    current_product_name = None
    current_product_data = None
    in_sku_section = False
    
    for index, row in df.iloc[1:].iterrows():  # 从第二行开始
        # 跳过空行或全为空的行
        if row.isna().all():
            continue
            
        # 检查是否是商品行（有具体的商品名称和编号）
        if (pd.notna(row.iloc[0]) and 
            str(row.iloc[0]).strip() not in ['', '-', 'sku名称', '商品名称'] and
            pd.notna(row.iloc[2]) and  # 商品编号
            str(row.iloc[2]).strip() not in ['', '商品编号']):
            
            # 如果有上一个商品数据，先保存
            if current_product_data is not None:
                product_df = pd.concat([product_df, pd.DataFrame([current_product_data])], ignore_index=True)
            
            # 开始新的商品记录
            current_product_data = {
                "商品名称": row.iloc[0],
                "商品图片": row.iloc[1] if len(row) > 1 else None,
                "商品编号": row.iloc[2] if len(row) > 2 else None,
                "成交金额": row.iloc[3] if len(row) > 3 else None,
                "成交件数": row.iloc[4] if len(row) > 4 else None,
                "成交人数": row.iloc[5] if len(row) > 5 else None,
                "成交订单数": row.iloc[6] if len(row) > 6 else None,
                "预售订单数": row.iloc[7] if len(row) > 7 else None,
                "预售创建金额": row.iloc[8] if len(row) > 8 else None,
                "商品曝光人数": row.iloc[9] if len(row) > 9 else None,
                "商品点击人数": row.iloc[10] if len(row) > 10 else None,
                "商品曝光-点击率(人数)": row.iloc[11] if len(row) > 11 else None,
                "商品点击-成交转化率(人数)": row.iloc[12] if len(row) > 12 else None,
                "退款人数": row.iloc[13] if len(row) > 13 else None,
                "退款金额": row.iloc[14] if len(row) > 14 else None,
                "退款订单数": row.iloc[15] if len(row) > 15 else None
            }
            
            current_product_id = row.iloc[2] if len(row) > 2 else None
            current_product_name = row.iloc[0]
            in_sku_section = False
            continue
        
        # 检查是否是SKU标题行（通常是"-"行，且第二列是"sku名称"）
        if (pd.notna(row.iloc[0]) and 
            str(row.iloc[0]).strip() == '-' and
            pd.notna(row.iloc[1]) and 
            str(row.iloc[1]).strip() == 'sku名称'):
            in_sku_section = True
            continue
        
        # 检查是否是SKU数据行（在SKU区域内）
        if in_sku_section:
            # SKU数据行的特征：第一列通常是空或"-"，第二列有具体的SKU名称
            if (pd.isna(row.iloc[0]) or str(row.iloc[0]).strip() in ['', '-']) and \
            pd.notna(row.iloc[1]) and str(row.iloc[1]).strip() not in ['', 'sku名称']:
                
                sku_data = {
                    "商品编号": current_product_id,
                    "商品名称": current_product_name,
                    "sku名称": row.iloc[1],
                    "sku图片": row.iloc[2] if len(row) > 2 else None,
                    "成交金额": row.iloc[3] if len(row) > 3 else None,
                    "成交件数": row.iloc[4] if len(row) > 4 else None,
                    "成交人数": row.iloc[5] if len(row) > 5 else None,
                    "成交订单数": row.iloc[6] if len(row) > 6 else None,
                    "预售订单数": row.iloc[7] if len(row) > 7 else None,
                    "预售创建金额": row.iloc[8] if len(row) > 8 else None,
                    "退款人数": row.iloc[9] if len(row) > 9 else None,
                    "退款金额": row.iloc[10] if len(row) > 10 else None,
                    "退款订单数": row.iloc[11] if len(row) > 11 else None,
                    "商品支付GPM": row.iloc[12] if len(row) > 12 else None
                }
                
                sku_df = pd.concat([sku_df, pd.DataFrame([sku_data])], ignore_index=True)
    
    # 保存最后一个商品数据
    if current_product_data is not None:
        product_df = pd.concat([product_df, pd.DataFrame([current_product_data])], ignore_index=True)
    
    # 确保列顺序正确
    if not live_room_df.empty:
        live_room_df = live_room_df[live_room_columns]
    if not product_df.empty:
        product_df = product_df[product_columns]
    if not sku_df.empty:
        sku_df = sku_df[sku_columns]
    
    return live_room_df, product_df, sku_df

def process_excel_files(folder_path):
    """
    处理文件夹中的所有Excel文件，按照sheet名称合并数据
    对商品sheet进行拆分处理
    从基本信息sheet中提取直播开始时间并添加到所有sheet中
    """
    # 获取所有xlsx文件
    excel_files = glob.glob(os.path.join(folder_path, '直播间详情页_整场数据下载*.xlsx'))
    
    # 解析文件名并过滤符合格式的文件
    file_info_list = []
    valid_files = []
    
    for file_path in excel_files:
        filename = os.path.basename(file_path)
        file_info = parse_filename(filename)  
        
        if file_info:
            file_info['file_path'] = file_path
            file_info_list.append(file_info)
            valid_files.append(file_path)
    
    print(f"找到 {len(valid_files)} 个符合格式的Excel文件")
    
    if not valid_files:
        return {}
    
    # 获取所有文件的sheet名称（假设所有文件有相同的sheet结构）
    first_file = pd.ExcelFile(valid_files[0])
    all_sheet_names = first_file.sheet_names
    print(f"发现 {len(all_sheet_names)} 个sheet: {all_sheet_names}")

    # 为每个sheet创建一个空的DataFrame列表
    merged_data = {sheet_name: [] for sheet_name in all_sheet_names}
    # 添加拆分后的sheet
    merged_data['直播间总体数据'] = []
    merged_data['商品数据'] = []
    merged_data['SKU数据'] = []
    
    # 遍历所有文件，读取每个sheet的数据
    for file_info in file_info_list:
        file_path = file_info['file_path']
        
        try:
            # 读取Excel文件的所有sheet
            excel_data = pd.read_excel(file_path, sheet_name=None)
            
            # 从基本信息sheet中提取直播开始时间
            live_start_date = None
            if '基本信息' in excel_data:
                basic_info_df = excel_data['基本信息']
                # 查找包含直播时间的列
                for col in basic_info_df.columns:
                    if '直播时间' in col or '直播时段' in col:
                        # 提取直播时间字符串
                        time_str = basic_info_df[col].iloc[0] if not basic_info_df.empty else None
                        if time_str and isinstance(time_str, str) and '~' in time_str:
                            # 提取开始时间部分并转换为日期格式
                            start_time_str = time_str.split('~')[0].strip()
                            # 提取年月日部分 (格式: yyyy-mm-dd)
                            try:
                                # 分割日期和时间部分
                                date_part = start_time_str.split('_')[0]
                                # 确保格式正确
                                live_start_date = pd.to_datetime(date_part).strftime('%Y-%m-%d')
                                break
                            except:
                                print(f"无法解析直播开始时间: {start_time_str}")
            
            # 处理每个sheet
            for sheet_name, df in excel_data.items():
                if sheet_name == '商品':  # 特殊处理商品sheet
                    # 拆分商品sheet
                    live_room_df, product_df, sku_df = split_complete_dataframe(df)
                    
                    # 为拆分后的数据添加文件信息和直播开始时间
                    for split_df, split_sheet_name in [
                        (live_room_df, '直播间总体数据'),
                        (product_df, '商品数据'), 
                        (sku_df, 'SKU数据')
                    ]:
                        if not split_df.empty:
                            split_df = split_df.copy()
                            split_df['来源文件名'] = file_info['filename']
                            split_df['直播间名称'] = file_info['live_name']
                            split_df['直播结束时间'] = file_info['end_time']
                            split_df['数据下载时间'] = file_info['download_time']
                            # 添加直播开始时间
                            if live_start_date:
                                split_df['直播开始日期'] = live_start_date
                            merged_data[split_sheet_name].append(split_df)
                
                else:  # 其他sheet正常处理
                    if sheet_name in merged_data:
                        df = df.copy()
                        df['来源文件名'] = file_info['filename']
                        df['直播间名称'] = file_info['live_name']
                        df['直播结束时间'] = file_info['end_time']
                        df['数据下载时间'] = file_info['download_time']
                        # 添加直播开始时间
                        if live_start_date:
                            df['直播开始日期'] = live_start_date
                        merged_data[sheet_name].append(df)
                        
        except Exception as e:
            print(f"处理文件 {file_info['filename']} 时出错: {e}")
            import traceback
            traceback.print_exc()
    
    # 合并每个sheet的数据
    final_merged_data = {}
    for sheet_name, df_list in merged_data.items():
        if df_list:
            try:
                final_merged_data[sheet_name] = pd.concat(df_list, ignore_index=True)
                print(f"已合并sheet '{sheet_name}'，共 {len(final_merged_data[sheet_name])} 行数据")
            except Exception as e:
                print(f"合并sheet '{sheet_name}' 时出错: {e}")
                final_merged_data[sheet_name] = pd.DataFrame()
        else:
            final_merged_data[sheet_name] = pd.DataFrame()
            print(f"sheet '{sheet_name}' 没有数据")
    
    return final_merged_data

def save_merged_data(merged_data, output_folder):
    """
    将合并后的数据保存到Excel文件，每个sheet保存为一个单独的工作表
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_folder, f"合并后的直播数据_{live_room}_{timestamp}.xlsx")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in merged_data.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"已保存sheet: {sheet_name} ({len(df)} 行)")
    
    print(f"\n所有数据已保存到: {output_file}")
    return output_file


def merge_data_to_df(merged_data):
    basic_infomation = merged_data["基本信息"]
    traffic_conversion_funnel = merged_data["流量&转化-转化漏斗"]
    traffic_analysis_channel = merged_data["流量分析-渠道分析"]
    traffic_conversion_short_video = merged_data["流量&转化-短视频引流"]
    Interaction_afterSale = merged_data["互动&人群&售后"]
    live_room_data = merged_data["直播间总体数据"]
    product_data = merged_data["商品数据"]
    sku_data = merged_data["SKU数据"]
    return basic_infomation,traffic_conversion_funnel,traffic_analysis_channel,traffic_conversion_short_video,Interaction_afterSale,live_room_data,product_data,sku_data

def live_room_okr_data(merged_data):
    df = pd.DataFrame()
    df = merged_data["基本信息"][["日期","直播结束时间",'直播开始时间','千次观看成交金额']]
    df["直播结束时间"] = df["直播结束时间"].apply(lambda x:x.replace("_"," "))
    df["直播场次计数"] = 1
    
    
    
    df = day_of_week_chinese(df)
    
    df = df.merge(merged_data["流量分析-渠道分析"][merged_data["流量分析-渠道分析"]["渠道名称"]=="整体"][["直播结束时间",'直播开始时间',"千川消耗","人均观看时长","观看次数"]],on=["直播结束时间",'直播开始时间'])
    df["人均观看时长"] = df["人均观看时长"].replace(['nan',''],0).astype(float)

    df['观看次数'] = df['观看次数'].apply(lambda x: float(x.replace('万', '')) * 10000 if '万' in str(x) else float(x))
    df["千川消耗"] = df["千川消耗"].astype(float)
    
    df = df.merge(merged_data["流量&转化-转化漏斗"][["直播结束时间",'直播开始时间',"自然流量观看人数","付费流量观看人数",
                                            "平均在线人数","直播间曝光人数","直播间观看人数","直播间曝光次数","商品曝光人数","商品点击人数","成交人数"]],on=["直播结束时间",'直播开始时间'])
    df[["自然流量观看人数","付费流量观看人数"]] = df[["自然流量观看人数","付费流量观看人数"]].replace(["-",''],0)
    df[["自然流量观看人数","付费流量观看人数"]] = df[["自然流量观看人数","付费流量观看人数"]].astype("float")
    df["观看总时长"] = df.apply(lambda row: 
                    row["直播间观看人数"] * row["人均观看时长"], 
                    axis=1)
    df = df.merge(merged_data["互动&人群&售后"][["直播结束时间",'直播开始时间',"退款人数","新增粉丝数"]],on=["直播结束时间",'直播开始时间'])    

    target_columns = ["平均在线人数","直播间观看人数","直播间曝光人数","商品曝光人数","自然流量观看人数","付费流量观看人数","观看次数",
                            "直播间曝光次数","商品点击人数","成交人数","千次观看成交金额",
                            "退款人数","直播场次计数","观看总时长","新增粉丝数"]

    df_day = df.groupby(['日期','直播开始时间','直播结束时间','星期'])[target_columns].sum(numeric_only=True).reset_index()
    print(df_day.columns)

    
    df_day['观看-成交转化率'] = df_day.apply(lambda row: 
                    row["成交人数"] / row["直播间观看人数"] 
                    if row["直播间观看人数"] != 0 else 0, 
                    axis=1)
    
    df_day['点击-成交转化率'] = df_day.apply(lambda row: 
                    row["成交人数"] / row["商品点击人数"] 
                    if row["商品点击人数"] != 0 else 0, 
                    axis=1)    
    
    df_day['曝光-成交转化率'] = df_day.apply(lambda row: 
                    row["成交人数"] / row["直播间曝光人数"] 
                    if row["直播间曝光人数"] != 0 else 0, 
                    axis=1)
    df_day['曝光-观看率'] = df_day.apply(lambda row: 
                    row["直播间观看人数"] / row["直播间曝光人数"] 
                    if row["直播间曝光人数"] != 0 else 0, 
                    axis=1)
    df_product =  merged_data["基本信息"][["日期",'直播开始时间',"直播结束时间"]]
    df_product["直播结束时间"] = df_product["直播结束时间"].apply(lambda x:x.replace("_"," "))
    df_product = df_product.merge(merged_data["商品数据"][["直播结束时间",'直播开始时间',"成交订单数"]],on=["直播结束时间","直播开始时间"])
    df_product["成交订单数"] = df_product["成交订单数"].replace(['nan',''],0).astype("float")
    df_product = df_product.groupby(["日期",'直播开始时间',"直播结束时间"])["成交订单数"].sum(numeric_only=True).reset_index()

    df_day['日期'] = df_day['日期'].dt.strftime('%Y-%m-%d')
    df_product['日期'] = df_product['日期'].astype(str)
    df_day = df_day.merge(df_product,on=['日期','直播开始时间',"直播结束时间"])
    

    df_day["人均观看时长"] = df_day.apply(lambda row: 
                        row["观看总时长"] / row["直播间观看人数"]
                        if row["直播间观看人数"] != 0 else 0, 
                        axis=1)
    df_day["商品曝光-点击率"] = df_day.apply(lambda row: 
                        row["商品点击人数"] / row["商品曝光人数"]
                        if row["商品曝光人数"] != 0 else 0, 
                        axis=1)
    
    df_day["商品点击-成交转化率"] = df_day.apply(lambda row: 
                        row["成交人数"] / row["商品点击人数"]
                        if row["商品点击人数"] != 0 else 0, 
                        axis=1)
    
    df_day["商品曝光-观看率"] = df_day.apply(lambda row: 
                        row["商品曝光人数"] / row["直播间观看人数"]
                        if row["直播间观看人数"] != 0 else 0, 
                        axis=1)
    return df_day   

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

def process_columns(merged_data):
    merged_data_new = {}
    
    for sheet_name,df in merged_data.items():
        df_processed = df.copy()
        df_processed = df_processed.replace("-","")
        for col in df_processed.columns:      
            if '率' in col:
                # 处理百分比列 
                df_processed.loc[:, col] = (
                    df_processed[col].astype(str)
                    .apply(lambda x: str(float(x.replace('%', '')) / 100) if '%' in x else x)
                    .replace(['nan', ''], '0')
                    .astype(float)
                )
                
            elif '金额' in col or '笔单价' in col or '消耗' in col:
                # 处理金额列
                df_processed.loc[:, col] = (
                    df_processed[col].astype(str)
                    .str.replace(r'[¥,]', '', regex=True)
                    .replace(['nan', ''], '0')
                    .astype(float)
                )
            elif '人均观看时长' in col:
                # 调用转换函数
                df_processed.loc[:, col] = df_processed[col].apply(time_to_seconds)
        merged_data_new[sheet_name] = df_processed
    return merged_data_new

def qianchuan_baiyin_group_merge(qianchuan,baiyin,live_time_day_time):
    print("调试: qianchuan 日期-小时 示例:", qianchuan['日期-小时'].head().tolist() if '日期-小时' in qianchuan.columns else "无")
    print("调试: live_time_day_time 日期-小时 示例:", live_time_day_time['日期-小时'].head().tolist() if '日期-小时' in live_time_day_time.columns else "无")

    # 统一日期格式：将日期-小时列转换为统一的格式 "YYYY-MM-DD HH:00"
    if '日期-小时' in qianchuan.columns:
        qianchuan['日期-小时'] = pd.to_datetime(qianchuan['日期-小时']).dt.strftime('%Y-%m-%d %H:00')
    if '日期-小时' in live_time_day_time.columns:
        live_time_day_time['日期-小时'] = pd.to_datetime(live_time_day_time['日期-小时']).dt.strftime('%Y-%m-%d %H:00')

    print("调试: 格式化后 qianchuan 日期-小时 示例:", qianchuan['日期-小时'].head().tolist() if '日期-小时' in qianchuan.columns else "无")
    print("调试: 格式化后 live_time_day_time 日期-小时 示例:", live_time_day_time['日期-小时'].head().tolist() if '日期-小时' in live_time_day_time.columns else "无")

    qianchuan = qianchuan.merge(live_time_day_time,on=['日期-小时'])
    baiyin = baiyin.merge(live_time_day_time,on=['日期-小时'])
    qianchuan = qianchuan.groupby(['直播开始时间'])[['整体消耗','整体支付ROI','整体成交金额','整体成交订单数','整体成交订单成本','净成交金额',
                        '整体成交智能优惠券金额','1小时内退款金额','1小时内退款订单数']].sum().reset_index()
    
    qianchuan['整体支付ROI'] = qianchuan.apply(lambda row: 
                    row["整体成交金额"] / row["整体消耗"] 
                    if row["整体消耗"] != 0 else 0, 
                    axis=1)
    qianchuan['净成交ROI'] = qianchuan.apply(lambda row: 
                    row["净成交金额"] / row["整体消耗"] 
                    if row["整体消耗"] != 0 else 0, 
                    axis=1)
    qianchuan['成交订单成本'] = qianchuan.apply(lambda row:
                    row["整体消耗"] / row["整体成交订单数"] 
                    if row["整体成交订单数"] != 0 else 0, 
                    axis=1)
    baiyin = baiyin.groupby(['直播开始时间'])[['评论次数','新加直播团人数','新增粉丝数']].sum().reset_index()
    baiyin["日期"] = baiyin["直播开始时间"].apply(lambda x:x[:10])
    qianchuan["日期"] = qianchuan["直播开始时间"].apply(lambda x:x[:10])
    baiyin["日期"] = pd.to_datetime(baiyin["日期"])
    qianchuan["日期"] = pd.to_datetime(qianchuan["日期"])
    return baiyin,qianchuan



def main(live_room):
    
    # 设置文件夹路径
    folder_path = os.path.join(r"D:\python project\python project", live_room)
    
    if not os.path.exists(folder_path):
        print("文件夹路径不存在！")
        return
    
    # 处理文件
    print("开始处理文件...")
    merged_data = process_excel_files(folder_path)
    baiyin_df_modify,qianchuan_df_modify = merge_baiyin_qianchuan_data(live_room)
    # qianchuan_group(qianchuan_df_modify)
    if not merged_data:
        print("没有找到符合格式的文件或处理失败")
        return

    # 显示汇总信息
    print("\n=== 合并完成 ===")
    print("各sheet数据量统计:")
    for sheet_name, df in merged_data.items():
        if not df.empty:
            print(f"  {sheet_name}: {len(df)} 行")
    qianchuan_df_modify = qianchuan_df_modify.rename(columns={"时间-小时":"日期-小时"})
    return merged_data,baiyin_df_modify,qianchuan_df_modify

# 为DataFrame添加日环比字段的函数
def add_daily_ratio(df, date_column='日期'):
    """
    为DataFrame的数值列添加日环比字段
    
    参数:
    df: 要处理的DataFrame
    date_column: 日期列的列名
    """
    # 确保日期列是datetime类型并按日期排序
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    df = df.sort_values(date_column)
    
    # 排除日期和星期列
    exclude_columns = [date_column, '星期']
    numeric_columns = [col for col in df.columns 
                       if col not in exclude_columns and pd.api.types.is_numeric_dtype(df[col])]
    
    # 为每个数值列计算日环比
    for col in numeric_columns:
        new_col_name = f"{col}_日环比"
        df[new_col_name] = df[col].pct_change()  # 计算环比
        
    return df

def calculate_traffic_period_metrics(traffic_df, period='W'):
    """
    计算流量结构的周期指标（周/月），基于原始数据重新计算
    
    参数:
    traffic_df: 原始流量结构DataFrame
    period: 周期类型，'W'表示周，'M'表示月
    """
    # 确保日期列是datetime类型
    traffic_df = traffic_df.copy()
    traffic_df['日期'] = pd.to_datetime(traffic_df['日期'])
    
    # 添加周期标识
    if period == 'W':
        period_col = '周'
        ratio_suffix = '周环比'
        traffic_df[period_col] = traffic_df['日期'].dt.to_period('W')
    elif period == 'M':
        period_col = '月'
        ratio_suffix = '月环比'
        traffic_df[period_col] = traffic_df['日期'].dt.to_period('M')
    else:
        raise ValueError("period参数必须是'W'（周）或'M'（月）")
    
    # 首先按周期和渠道分组求和（对于原始数据）
    period_sum_df = traffic_df.groupby([period_col, '渠道名称']).agg({
        '观看人数': 'sum',
        '成交金额': 'sum',
    }).reset_index()
    
    # 计算每个周期的整体数据
    period_total_df = period_sum_df.groupby([period_col]).agg({
        '观看人数': 'sum',
        '成交金额': 'sum',
    }).reset_index()
    period_total_df['渠道名称'] = '整体'
    
    # 合并整体数据和渠道数据
    combined_df = pd.concat([period_sum_df, period_total_df], ignore_index=True)
    
    # 计算占比和千次观看成交金额
    merged_df = combined_df.merge(period_total_df[[period_col, '观看人数', '成交金额']], 
                                 on=period_col, 
                                 suffixes=('', '_整体'))
    
    # 计算各项指标
    merged_df['观看人数_占比'] = merged_df.apply(
        lambda row: (row['观看人数'] / row['观看人数_整体'] * 100) if row['观看人数_整体'] > 0 else 0,
        axis=1
    )
    
    merged_df['成交金额_占比'] = merged_df.apply(
        lambda row: (row['成交金额'] / row['成交金额_整体'] * 100) if row['成交金额_整体'] > 0 else 0,
        axis=1
    )
    
    merged_df['千次观看成交金额'] = merged_df.apply(
        lambda row: (row['成交金额'] / row['观看人数'] * 1000) if row['观看人数'] > 0 else 0,
        axis=1
    )
    
    # 选择需要的列
    result_df = merged_df[[period_col, '渠道名称', '观看人数_占比', '成交金额_占比', '千次观看成交金额']]
    
    # 创建透视表
    pivot_period_df = result_df.pivot_table(
        index=[period_col],
        columns=['渠道名称'],
        values=['观看人数_占比', '成交金额_占比', '千次观看成交金额']
    )
    
    # 处理多级列索引
    pivot_period_df.columns = ['_'.join(filter(None, col)).strip('_') for col in pivot_period_df.columns]
    pivot_period_df = pivot_period_df.reset_index()
    pivot_period_df[period_col] = pivot_period_df[period_col].astype(str)
    
    # 添加周期环比
    pivot_period_df = add_period_ratio_traffic(pivot_period_df, period_col, ratio_suffix)
    
    return pivot_period_df

def add_period_ratio_traffic(df, period_column, ratio_suffix):
    """
    为流量结构周期数据添加环比
    
    参数:
    df: 周期数据DataFrame
    period_column: 周期标识列名
    ratio_suffix: 环比列后缀（如'周环比'、'月环比'）
    """
    df = df.copy()
    
    # 按周期排序（处理周和月的特殊格式）
    if period_column == '周':
        # 将周格式转换为可排序的格式
        df['sort_key'] = df[period_column].apply(lambda x: pd.to_datetime(x.split('/')[0]))
        df = df.sort_values('sort_key')
        df = df.drop('sort_key', axis=1)
    elif period_column == '月':
        df['sort_key'] = df[period_column].apply(lambda x: pd.to_datetime(x))
        df = df.sort_values('sort_key')
        df = df.drop('sort_key', axis=1)
    else:
        df = df.sort_values(period_column)
    
    # 排除周期标识列
    exclude_columns = [period_column]
    numeric_columns = [col for col in df.columns 
                        if col not in exclude_columns and pd.api.types.is_numeric_dtype(df[col])]
    
    # 为每个数值列计算周期环比
    for col in numeric_columns:
        new_col_name = f"{col}_{ratio_suffix}"
        df[new_col_name] = df[col].pct_change()
    
    return df

# 处理流量结构表的简化版本
def process_traffic_structure_df(df):
    """
    处理流量结构表的简化版本 - 所有列保持字符串格式
    """
    # 复制DataFrame以避免修改原始数据
    df = df.copy()
    
    # 将多级列索引转换为字符串表示
    df.columns = ['_'.join(filter(None, col)).strip('_') for col in df.columns]
    
    # 重置索引，将日期作为列
    df = df.reset_index()
    
    # 确保日期列正确
    date_col = df.columns[0]  # 第一列应该是日期
    df[date_col] = pd.to_datetime(df[date_col])
    
    # 按日期排序
    df = df.sort_values(date_col)
    
    # 为数值列计算日环比
    numeric_columns = []
    for col in df.columns:
        # 跳过日期列和非数值列
        if col != date_col and pd.api.types.is_numeric_dtype(df[col]):
            numeric_columns.append(col)
    
    # 为每个数值列计算日环比
    for col in numeric_columns:
        new_col_name = f"{col}_日环比"
        df[new_col_name] = df[col].pct_change()
    
    # 将日期列重新设置为索引
    df = df.set_index(date_col)
    
    return df

def reorder_columns_with_ratio_all_periods(df, period_type='日'):
    """
    重新排序列，确保环比指标紧跟在对应原始指标右边
    支持日、周、月不同周期的环比后缀
    
    参数:
    df: 要处理的DataFrame
    period_type: 周期类型，'日'、'周'或'月'
    """
    # 获取所有列名
    all_columns = df.columns.tolist()
    
    # 根据周期类型确定环比后缀
    if period_type == '日':
        ratio_suffix = '_日环比'
    elif period_type == '周':
        ratio_suffix = '_周环比'
    elif period_type == '月':
        ratio_suffix = '_月环比'
    else:
        ratio_suffix = '_环比'
    
    # 分离原始指标和环比指标
    original_cols = [col for col in all_columns if not col.endswith(ratio_suffix)]
    ratio_cols = [col for col in all_columns if col.endswith(ratio_suffix)]
    
    # 创建新的列顺序
    new_order = []
    for col in original_cols:
        new_order.append(col)
        # 查找对应的环比列
        ratio_col = f"{col}{ratio_suffix}"
        if ratio_col in ratio_cols:
            new_order.append(ratio_col)
            ratio_cols.remove(ratio_col)
    
    # 添加剩余的环比列（如果有）
    new_order.extend(ratio_cols)
    
    return df[new_order]

def calculate_period_metrics(daily_df, period='W'):
    """
    计算周期指标（周/月），基于周期汇总数据计算比率指标
    
    参数:
    daily_df: 日度数据DataFrame
    period: 周期类型，'W'表示周，'M'表示月
    """
    # 确保日期列是datetime类型
    daily_df = daily_df.copy()
    daily_df['日期'] = pd.to_datetime(daily_df['日期'])
    
    # 添加周期标识
    if period == 'W':
        period_col = '周'
        ratio_suffix = '周环比'
        daily_df[period_col] = daily_df['日期'].dt.to_period('W')
    elif period == 'M':
        period_col = '月'
        ratio_suffix = '月环比'
        daily_df[period_col] = daily_df['日期'].dt.to_period('M')
    else:
        raise ValueError("period参数必须是'W'（周）或'M'（月）")
    
    # 定义需要求和的列（数值列）
    sum_cols = [
        '直播间观看人数', '整体消耗', '平均在线人数',
        '成交人数', '成交订单数', '1小时内退款金额',
        '1小时内退款订单数', '退款人数', '自然流量观看人数', '付费流量观看人数',
        '商品点击人数', '商品曝光人数', '整体成交金额','直播间曝光人数',
        '整体成交订单数', '整体成交智能优惠券金额', '直播间曝光次数',
        '观看次数', '评论次数', '新加直播团人数', '新增粉丝数','净成交金额','净成交ROI',
        '观看总时长', '直播场次计数','整体成交订单成本'
    ]
    
    # 只保留需要的列
    period_df = daily_df[[period_col, '星期'] + sum_cols].copy()

    missing_cols = [col for col in sum_cols if col not in period_df.columns]
    existing_cols = [col for col in sum_cols if col in period_df.columns]
    print("缺失的列:", missing_cols)  # 应该是空列表
    print("存在的列:", existing_cols)  # 应该包含所有sum_cols
    print("period_df的列:", period_df.columns.tolist())
    print("period_df的形状:", period_df.shape)

    # 检查数据类型
    print("\n数据类型:")
    for col in sum_cols:
        print(f"{col}: {period_df[col].dtype}")

    # 构建聚合字典
    agg_dict = {col: 'sum' for col in existing_cols}
    agg_dict['星期'] = 'first'
    print("聚合字典:", agg_dict)

    try:
        # 尝试分组聚合
        period_df = period_df.groupby(period_col).agg(agg_dict).reset_index()
        print("聚合成功!")
    except Exception as e:
        print(f"聚合失败，错误详情: {e}")
        print(f"错误类型: {type(e).__name__}")
        
        # 进一步检查是否有空值或其他问题
        print("\n检查数据详情:")
        for col in sum_cols:
            null_count = period_df[col].isnull().sum()
            print(f"{col}: 空值数量 = {null_count}")
    # 按周期分组求和
    period_df = period_df.groupby(period_col).agg({
        **{col: 'sum' for col in sum_cols},
        '星期': 'first'  # 保留第一个星期值（可以是任意值，这里只是为了保持结构）
    }).reset_index()

    # 计算周期比率指标（基于周期汇总数据）
    period_df['整体支付ROI'] = period_df.apply(
        lambda row: row["整体成交金额"] / row["整体消耗"] if row["整体消耗"] != 0 else 0, 
        axis=1
    )
    period_df['千次观看成交金额'] = period_df.apply(
        lambda row: (row['整体成交金额'] / row['观看次数'] * 1000) if row['直播间观看人数'] > 0 else 0,
        axis=1
    )
    
    period_df['观看-成交转化率'] = period_df.apply(
        lambda row: row["成交人数"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['点击-成交转化率'] = period_df.apply(
        lambda row: row["成交人数"] / row["商品点击人数"] if row["商品点击人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['客单价'] = period_df.apply(
        lambda row: row["整体成交金额"] / row["成交订单数"] if row["成交订单数"] != 0 else 0, 
        axis=1
    )
    
    period_df['退款率'] = period_df.apply(
        lambda row: row["1小时内退款金额"] / row["整体成交金额"] if row["整体成交金额"] != 0 else 0, 
        axis=1
    )
    
    period_df['退款后成交金额'] = period_df.apply(
        lambda row: row["整体成交金额"] - row["1小时内退款金额"], 
        axis=1
    )
    
    period_df['退款后roi'] = period_df.apply(
        lambda row: row["退款后成交金额"] / row["整体消耗"] if row["整体消耗"] != 0 else 0, 
        axis=1
    )
    
    period_df['曝光-成交转化率'] = period_df.apply(
        lambda row: row["成交人数"] / row["直播间曝光次数"] if row["直播间曝光次数"] != 0 else 0, 
        axis=1
    )
    
    period_df['曝光-观看率'] = period_df.apply(
        lambda row: row["直播间观看人数"] / row["直播间曝光次数"] if row["直播间曝光次数"] != 0 else 0, 
        axis=1
    )

    period_df['商品曝光-观看率'] = period_df.apply(
        lambda row: row["直播间观看人数"] / row["直播间曝光次数"] if row["直播间曝光次数"] != 0 else 0, 
        axis=1
    )   
    
    period_df['uv价值'] = period_df.apply(
        lambda row: row["整体成交金额"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['人均观看时长'] = period_df.apply(
        lambda row: row["观看总时长"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['商品曝光-点击率'] = period_df.apply(
        lambda row: row["商品曝光人数"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['商品点击-成交转化率'] = period_df.apply(
        lambda row: row["成交人数"] / row["商品点击人数"] if row["商品点击人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['互动率'] = period_df.apply(
        lambda row: row["评论次数"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['增粉率'] = period_df.apply(
        lambda row: row["新增粉丝数"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['加团率'] = period_df.apply(
        lambda row: row["新加直播团人数"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['自然流量观看占比'] = period_df.apply(
        lambda row: row["自然流量观看人数"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    
    period_df['付费流量观看占比'] = period_df.apply(
        lambda row: row["付费流量观看人数"] / row["直播间观看人数"] if row["直播间观看人数"] != 0 else 0, 
        axis=1
    )
    period_df['成交订单成本'] = period_df.apply(
        lambda row: row["整体消耗"] / row["整体成交订单数"] if row["整体成交订单数"] != 0 else 0, 
        axis=1)
    # 添加周期环比
    period_df = add_period_ratio(period_df, period_col, ratio_suffix)
    
    # 格式化周期列为字符串
    period_df[period_col] = period_df[period_col].astype(str)
    
    return period_df

def add_period_ratio(df, period_column, ratio_suffix):
    """
    为周期数据添加环比
    
    参数:
    df: 周期数据DataFrame
    period_column: 周期标识列名
    ratio_suffix: 环比列后缀（如'周环比'、'月环比'）
    """
    df = df.copy()
    df = df.sort_values(period_column)
    
    # 排除周期标识列
    exclude_columns = [period_column, '星期']
    numeric_columns = [col for col in df.columns 
                       if col not in exclude_columns and pd.api.types.is_numeric_dtype(df[col])]
    
    # 为每个数值列计算周期环比
    for col in numeric_columns:
        new_col_name = f"{col}_{ratio_suffix}"
        df[new_col_name] = df[col].pct_change()
    
    return df

# 修改主函数中的相关部分
if __name__ == "__main__":
    live_room = "鱼子酱"
    clean_duplicate_livestream_files(os.path.join(r"D:\python project\python project",live_room))
    merged_data,baiyin_df_modify,qianchuan_df_modify = main(live_room)
    merged_data = process_live_time(merged_data)
    merged_data = process_columns(merged_data)
    baiyin_df_modify.to_excel("baiyin_df_modify.xlsx",index=False)
    qianchuan_df_modify.to_excel("qianchuan_df_modify.xlsx",index=False)
    
    # 保存结果
    output_folder = os.path.join("./", "合并结果")
    output_file = save_merged_data(merged_data, output_folder)
    live_room_okr_df = live_room_okr_data(merged_data)


    live_room_okr_df["日期"] = pd.to_datetime(live_room_okr_df["日期"])
    live_room_okr_df.drop(columns="新增粉丝数",inplace=True)
    
    live_time_day_time = live_room_okr_df[["日期","直播开始时间","直播结束时间"]]
    live_time_day_time_new = pd.DataFrame([record for _, row in live_time_day_time.iterrows() for record in expand_hours(row)])
    live_time_day_time_new.to_excel("live_time_day_time_new.xlsx",index=False)

    baiyin,qianchuan = qianchuan_baiyin_group_merge(qianchuan_df_modify,baiyin_df_modify,live_time_day_time_new)
    print("调试: 合并后 qianchuan 行数:", len(qianchuan))
    print("调试: 合并后 baiyin 行数:", len(baiyin))
    baiyin.to_excel("mergebaiyin.xlsx",index=False)
    qianchuan.to_excel("mergeqianchuan.xlsx",index=False)


    all_live_room_okr_df = live_room_okr_df.merge(baiyin,on=['直播开始时间','日期'],how="left").merge(qianchuan,on=["直播开始时间",'日期'])
    
    # 计算互动相关指标
    all_live_room_okr_df['客单价'] = all_live_room_okr_df.apply(lambda row: 
                    row["整体成交金额"] / row["成交订单数"] 
                    if row["成交订单数"] != 0 else 0, 
                    axis=1)  
    # 然后再计算退款率
    all_live_room_okr_df['退款率'] = all_live_room_okr_df.apply(lambda row: 
                        row["1小时内退款金额"] / row["整体成交金额"] 
                        if row["整体成交金额"] != 0 else 0, 
                        axis=1)
    all_live_room_okr_df['退款后成交金额'] = all_live_room_okr_df.apply(lambda row:
                        row["整体成交金额"] - row["整体成交金额"]*row['退款率'],
                        axis=1)
    all_live_room_okr_df['退款后roi'] = all_live_room_okr_df.apply(lambda row: 
                        row["退款后成交金额"] / row["整体消耗"] 
                        if row["整体消耗"] != 0 else 0, 
                        axis=1)
    all_live_room_okr_df["uv价值"] = all_live_room_okr_df.apply(lambda row: 
                        row["整体成交金额"] / row["直播间观看人数"] 
                        if row["直播间观看人数"] != 0 else 0, 
                        axis=1)
    all_live_room_okr_df["互动率"] = all_live_room_okr_df.apply(lambda row: 
                            row["评论次数"] / row["直播间观看人数"]
                            if row["直播间观看人数"] != 0 else 0, 
                            axis=1)
    all_live_room_okr_df["增粉率"] = all_live_room_okr_df.apply(lambda row: 
                            row["新增粉丝数"] / row["直播间观看人数"]
                            if row["直播间观看人数"] != 0 else 0, 
                            axis=1)
    all_live_room_okr_df["加团率"] = all_live_room_okr_df.apply(lambda row: 
                            row["新加直播团人数"] / row["直播间观看人数"]
                            if row["直播间观看人数"] != 0 else 0, 
                            axis=1)
    all_live_room_okr_df["自然流量观看占比"] = all_live_room_okr_df.apply(lambda row: 
                            row["自然流量观看人数"] / row["直播间观看人数"]
                            if row["直播间观看人数"] != 0 else 0, 
                            axis=1)
    all_live_room_okr_df["付费流量观看占比"] = all_live_room_okr_df.apply(lambda row: 
                            row["付费流量观看人数"] / row["直播间观看人数"]
                            if row["直播间观看人数"] != 0 else 0, 
                            axis=1)
    print("all_live_room_okr_df:",all_live_room_okr_df.columns)
    no_radio_all_live_room_okr_df = all_live_room_okr_df.copy()
    # 添加日环比
    all_live_room_okr_df = add_daily_ratio(all_live_room_okr_df)
    
    # 重新排序列，确保日环比指标在对应日汇总指标右边
    all_live_room_okr_df = reorder_columns_with_ratio_all_periods(all_live_room_okr_df, '日')
    
    # 计算周度指标
    weekly_df = calculate_period_metrics(all_live_room_okr_df, period='W')
    weekly_df = reorder_columns_with_ratio_all_periods(weekly_df, '周')
    
    # 计算月度指标
    monthly_df = calculate_period_metrics(all_live_room_okr_df, period='M')
    monthly_df = reorder_columns_with_ratio_all_periods(monthly_df, '月')
    
    # 首先确保我们有正确的流量分析数据
    traffic_analysis_df = merged_data["流量分析-渠道分析"].copy()
    
    # 确保数值列是数值类型
    numeric_cols = ['观看人数', '成交金额']
    for col in numeric_cols:
        traffic_analysis_df[col] = pd.to_numeric(traffic_analysis_df[col], errors='coerce').fillna(0)
    
    # 计算流量结构日度数据
    daily_traffic_df = calculate_all_metrics(traffic_analysis_df)
    daily_traffic_pivot = daily_traffic_df.pivot_table(
        index=['日期'],
        columns=['渠道名称'],
        values=["观看次数_占比", "成交金额_占比", "千次观看成交金额"]
    )
    daily_traffic_pivot = process_traffic_structure_df(daily_traffic_pivot)
    
    # 为流量结构日度数据添加日环比并重新排序
    daily_traffic_reset = daily_traffic_pivot.reset_index()
    daily_traffic_reset = daily_traffic_reset.rename(columns={'index': '日期'})
    daily_traffic_reset['日期'] = pd.to_datetime(daily_traffic_reset['日期'])
    daily_traffic_reset = add_daily_ratio(daily_traffic_reset, '日期')
    daily_traffic_reset = reorder_columns_with_ratio_all_periods(daily_traffic_reset, '日')
    daily_traffic_final = daily_traffic_reset.set_index('日期')
    
    # 计算流量结构周度数据（使用重新计算的函数）
    weekly_traffic_df = calculate_traffic_period_metrics(daily_traffic_df, period='W')
    weekly_traffic_df = reorder_columns_with_ratio_all_periods(weekly_traffic_df, '周')
    
    # 计算流量结构月度数据
    monthly_traffic_df = calculate_traffic_period_metrics(daily_traffic_df, period='M')
    monthly_traffic_df = reorder_columns_with_ratio_all_periods(monthly_traffic_df, '月')
    
    
    # 转换格式
    all_live_room_okr_df = convert_to_percentage(all_live_room_okr_df)
    weekly_df = convert_to_percentage(weekly_df)
    monthly_df = convert_to_percentage(monthly_df)  
    daily_traffic_final = convert_to_percentage(daily_traffic_final)
    weekly_traffic_df = convert_to_percentage(weekly_traffic_df)
    monthly_traffic_df = convert_to_percentage(monthly_traffic_df)
    no_radio_all_live_room_okr_df = convert_to_percentage(no_radio_all_live_room_okr_df)
    print("no_radio_all_live_room_okr_df:",no_radio_all_live_room_okr_df.columns)
    # 保存到Excel
    columns_sort = ['日期','星期','直播间观看人数',"整体消耗",'整体成交金额',"整体支付ROI","净成交金额","净成交ROI","观看-成交转化率",'曝光-成交转化率',"点击-成交转化率",
                    '千次观看成交金额','客单价','退款率','退款后成交金额','退款后roi','平均在线人数','曝光-观看率',
                    '直播间观看人数','直播间曝光次数','直播间观看人数','整体成交金额','成交人数','成交订单数','千次观看成交金额',
                    'uv价值','人均观看时长','互动率','新增粉丝数','增粉率','新加直播团人数','加团率',
                    '1小时内退款金额','1小时内退款订单数','退款人数','退款率','商品点击人数','商品曝光-点击率','商品点击-成交转化率','客单价',
                    '直播间曝光人数','直播间观看人数','商品曝光人数','商品点击人数','成交人数','曝光-观看率','曝光-成交转化率',
                    '商品曝光-观看率','商品曝光-点击率','商品点击-成交转化率']
    
    no_radio_all_live_room_okr_df_sort = no_radio_all_live_room_okr_df[columns_sort]
    with pd.ExcelWriter(f'直播间核心数据{today}_{live_room}.xlsx') as writer:
        # # 日汇总
        # all_live_room_okr_df_with_room = all_live_room_okr_df.copy()
        # all_live_room_okr_df_with_room.insert(0, '直播间', live_room)
        # all_live_room_okr_df_with_room.to_excel(writer, sheet_name='日汇总', index=False)
        
        # # 周汇总
        # weekly_df_with_room = weekly_df.copy()
        # weekly_df_with_room.insert(0, '直播间', live_room)
        # weekly_df_with_room.to_excel(writer, sheet_name='周汇总', index=False)
        
        # # 月汇总
        # monthly_df_with_room = monthly_df.copy()
        # monthly_df_with_room.insert(0, '直播间', live_room)
        # monthly_df_with_room.to_excel(writer, sheet_name='月汇总', index=False)
        
        # # 流量结构_日
        # daily_traffic_final_with_room = daily_traffic_final.copy()
        # daily_traffic_final_with_room.insert(0, '直播间', live_room)
        # daily_traffic_final_with_room.to_excel(writer, sheet_name='流量结构_日', index=False)
        
        # # 流量结构_周
        # weekly_traffic_df_with_room = weekly_traffic_df.copy()
        # weekly_traffic_df_with_room.insert(0, '直播间', live_room)
        # weekly_traffic_df_with_room.to_excel(writer, sheet_name='流量结构_周', index=False)
        
        # # 流量结构_月
        # monthly_traffic_df_with_room = monthly_traffic_df.copy()
        # monthly_traffic_df_with_room.insert(0, '直播间', live_room)
        # monthly_traffic_df_with_room.to_excel(writer, sheet_name='流量结构_月', index=False)
        
        # 日登记数据
        no_radio_all_live_room_okr_df_with_room = no_radio_all_live_room_okr_df_sort.copy()
        no_radio_all_live_room_okr_df_with_room.insert(0, '直播间', live_room)
        no_radio_all_live_room_okr_df_with_room.to_excel(writer, sheet_name='日登记数据', index=False)

        # 周登记数据
        no_radio_weekly_df = weekly_df.copy()
        columns_sort_week = columns_sort.copy()
        columns_sort_week.remove("星期")
        columns_sort_week[0]="周"
        no_radio_weekly_df = no_radio_weekly_df[columns_sort_week]
        no_radio_weekly_df.insert(0, '直播间', live_room)
        no_radio_weekly_df.to_excel(writer, sheet_name='周登记数据', index=False)

        # 月登记数据
        no_radio_monthly_df = monthly_df.copy()
        columns_sort_month = columns_sort.copy()
        columns_sort_month.remove("星期")
        columns_sort_month[0]="月"
        no_radio_monthly_df = no_radio_monthly_df[columns_sort_month]
        no_radio_monthly_df.insert(0, '直播间', live_room)
        no_radio_monthly_df.to_excel(writer, sheet_name='月登记数据', index=False)

        # 投放日登记数据
        toufang_sort_columns = ['日期','整体成交金额','整体消耗','直播间观看人数','千次观看成交金额','客单价','退款率','1小时内退款金额','净成交金额','净成交ROI','整体消耗','整体支付ROI','整体成交金额','成交订单数'
                                ,'成交订单成本','直播间曝光次数','观看次数','直播间曝光人数','直播间观看人数','商品曝光人数','商品点击人数','成交人数','曝光-观看率','商品曝光-观看率'
                                ,'商品曝光-点击率','商品点击-成交转化率']
        toufang_radio_all_live_room_okr_df = no_radio_all_live_room_okr_df.copy()
        toufang_radio_all_live_room_okr_df = toufang_radio_all_live_room_okr_df[toufang_sort_columns]
        toufang_radio_all_live_room_okr_df.insert(0, '直播间', live_room)
        toufang_radio_all_live_room_okr_df.to_excel(writer, sheet_name='投放-日登记数据', index=False)

        print("数据汇总完成！")
        print(f"日汇总数据: {len(all_live_room_okr_df)} 行")
        print(f"周汇总数据: {len(weekly_df)} 行") 
        print(f"月汇总数据: {len(monthly_df)} 行")
        print(f"流量结构日度数据: {len(daily_traffic_final)} 行")
        print(f"流量结构周度数据: {len(weekly_traffic_df)} 行")
        print(f"流量结构月度数据: {len(monthly_traffic_df)} 行")