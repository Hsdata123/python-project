import pandas as pd
import os
import re
from datetime import datetime
import glob
from qianchuan_juliang_live import swatch_case_time_tran,analyze_live_detail_files,filter_time_data,concat_file_list_data,convert_to_percentage,end_time,today
from data_functions import calculate_all_metrics,expand_hours,clean_duplicate_livestream_files
data_path = r"C:\Users\Administrator\Downloads"
file_list = glob.glob(os.path.join(data_path, "*.xlsx"))

# 更严格的正则表达式，确保只匹配时间戳格式的文件
pattern = r'^\d{4}-\d{2}-\d{2} \d{2}_\d{2}_\d{2}_\d{19}\.xlsx$'
matched_files = [f for f in file_list if re.match(pattern, os.path.basename(f))]
print("matched_files:",matched_files)
qianchuan_df_modify = concat_file_list_data(matched_files)

qianchuan_df_modify["时间-小时"] = pd.to_datetime(qianchuan_df_modify["时间-小时"])
qianchuan_df_modify["小时"] = qianchuan_df_modify["时间-小时"].dt.hour

def no_live_hour(x):
    if x <=5 and x >=2:
        return "未播时段"
    else:
        return "已播时段"
qianchuan_df_modify["时段类型"] = qianchuan_df_modify["小时"].apply(lambda x:no_live_hour(x))
qianchuan_df_modify["日期"] = qianchuan_df_modify["时间-小时"].dt.date
qianchuan_df_modify[['整体消耗','整体成交金额']] \
= qianchuan_df_modify[['整体消耗','整体成交金额']]\
.applymap(lambda x: float(str(x).replace(',', '')) if pd.notna(x) else x)

qianchuan_df_modify["日汇总成交金额"] = qianchuan_df_modify.groupby(["日期"])['整体成交金额'].transform('sum')
qianchuan_df_modify["日汇总消耗"] = qianchuan_df_modify.groupby(["日期"])['整体消耗'].transform('sum')

qianchuan_df_modify["时段消耗占比"] = qianchuan_df_modify["整体消耗"]/qianchuan_df_modify["日汇总消耗"]
qianchuan_df_modify["时段成交占比"] = qianchuan_df_modify["整体成交金额"]/qianchuan_df_modify["日汇总成交金额"]
qianchuan_df_modify.to_excel("时段分析.xlsx",index=False)