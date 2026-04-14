import pandas as pd
import re
from pathlib import Path
import os
import glob
import sys
import numpy as np
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import shutil
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_functions import calculate_week_excel_style,process_columns,clean_duplicate_livestream_files,get_latest_file,calculate_all_metrics,calculate_week_excel_style_series
from qianchuan_juliang_live import swatch_case_time_tran,concat_file_list_data
import swifter

def merge_baiyin_qianchuan_data(live_room):
    #遍历文件夹下的千川文数据文件
    data_path = os.path.join(r"D:\python project\python project", live_room)
    file_list = glob.glob(os.path.join(data_path, "*.xlsx"))
    
    # 更严格的正则表达式，确保只匹配时间戳格式的文件
    pattern = r'^\d{4}-\d{2}-\d{2} \d{2}_\d{2}_\d{2}_\d{19}\.xlsx$'
    matched_files = [f for f in file_list if re.match(pattern, os.path.basename(f))]
    if matched_files == []:
        return pd.DataFrame(),pd.DataFrame()
    else:
        print("matched_files:",matched_files)
        qianchuan_df_modify = concat_file_list_data(matched_files)
        qianchuan_df_modify.to_excel("qianchuan_df_modify1.xlsx",index=False)
        print("qianchuan_df_modify.columns:",qianchuan_df_modify)
        #筛选日期为2025-08-27到2025-09-07的数据  ------ 非必要重复
        # qianchuan_df_modify = filter_time_data(qianchuan_df_modify,'2025-08-31',end_time)
        # qianchuan_df_modify = swatch_case_time_tran(qianchuan_df_modify,"小时区间")
        # qianchuan_df_modify['小时'] = qianchuan_df_modify["小时区间"].apply(lambda x:x[:2])
        datetime_series = pd.to_datetime(qianchuan_df_modify['时间-小时'])
        qianchuan_df_modify['小时'] = datetime_series.dt.hour
        qianchuan_df_modify['日期'] = datetime_series.dt.strftime('%Y-%m-%d')
        qianchuan_df_modify[['整体消耗','整体支付ROI','整体成交金额','整体成交订单数','整体成交订单成本',
                            '整体成交智能优惠券金额','1小时内退款金额','1小时内退款订单数']] \
        = qianchuan_df_modify[['整体消耗','整体支付ROI','整体成交金额','整体成交订单数','整体成交订单成本',
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
        # qianchuan_df_modify = day_of_week_chinese(qianchuan_df_modify)
        # baiyin_df_modify = day_of_week_chinese(baiyin_df_modify)
        return baiyin_df_modify,qianchuan_df_modify

def return_column_rate(df,df_rate,df_son,df_prents):
    """
    安全计算占比
    """
    df[df_rate] = df.swifter.apply(lambda row: 
                row[df_son] / row[df_prents] 
                if row[df_prents] != 0 else 0, 
                axis=1)
    return df


def daliy_data_bash(df,live_room=['日期', '周', '月']):
    """
    店铺GMV未扣退（已加平台券）	"扣退店铺成交
    (已加平台券)"	"扣退店铺成交
    (未加平台券)"	退款金额占比	"自播
    销售占比"	"达人
    销售占比"	小店自卖-销售占比	"总单量
    （已扣退）"	"总客单
    （已扣退）"	商品卡销售占比	短视频销售占比
    """
    df['日期'] = pd.to_datetime(df['订单提交时间']).dt.date
    if "直播间" not in live_room:
        use_col = ['日期',"订单提交时间","支付完成时间","周","月","主订单编号","是否自播","是否达播","是否退款"
                ,"流量来源","流量体裁","平台实际承担优惠金额","商家实际承担优惠金额","达人实际承担优惠金额","退款金额","订单应付金额"]
    else:
        use_col = ['日期',"直播间","订单提交时间","支付完成时间","周","月","主订单编号","是否自播","是否达播","是否退款"
                ,"流量来源","流量体裁","平台实际承担优惠金额","商家实际承担优惠金额","达人实际承担优惠金额","退款金额","订单应付金额"]       
    daily_df = df[use_col].copy()
    
    daily_df['平台实际承担优惠金额'] = pd.to_numeric(daily_df['平台实际承担优惠金额'], errors='coerce').fillna(0)
    daily_df['商家实际承担优惠金额'] = pd.to_numeric(daily_df['商家实际承担优惠金额'], errors='coerce').fillna(0)
    daily_df['达人实际承担优惠金额'] = pd.to_numeric(daily_df['达人实际承担优惠金额'], errors='coerce').fillna(0)
    daily_df['退款金额'] = pd.to_numeric(daily_df['退款金额'], errors='coerce').fillna(0)
    daily_df['订单应付金额'] = pd.to_numeric(daily_df['订单应付金额'], errors='coerce').fillna(0)

    daily_df["达播销售金额"] = np.where(
    daily_df["是否达播"] == "达播",
    daily_df['平台实际承担优惠金额'] + daily_df['订单应付金额']  + daily_df['达人实际承担优惠金额'],
    0,
)
    
    daily_df["自播销售金额"] = np.where(
    daily_df["是否自播"] == "自播",
    daily_df['平台实际承担优惠金额'] + daily_df['订单应付金额']  + daily_df['达人实际承担优惠金额'],
    0,
)
    
    daily_df["小店自卖销售金额"] = np.where(
    daily_df["流量来源"] == "小店自卖",
    daily_df['平台实际承担优惠金额'] + daily_df['订单应付金额']  + daily_df['达人实际承担优惠金额'],
    0,
)      
    
    daily_df["商品卡销售金额"] = np.where(
    daily_df["流量体裁"] == "商品卡",
    daily_df['平台实际承担优惠金额'] + daily_df['订单应付金额']  + daily_df['达人实际承担优惠金额'],
    0,
)     
    
    daily_df["短视频销售金额"] = np.where(
    daily_df["流量体裁"] == "短视频",
    daily_df['平台实际承担优惠金额'] + daily_df['订单应付金额']  + daily_df['达人实际承担优惠金额'],
    0,
)
    daily_df["其他销售金额"] = np.where(
    daily_df["流量体裁"] == "其他",
    daily_df['平台实际承担优惠金额'] + daily_df['订单应付金额']  + daily_df['达人实际承担优惠金额'],
    0,
)       
    # 然后进行分组聚合
    daily_df = daily_df.groupby(live_room).agg({
        '主订单编号':'nunique',
        '平台实际承担优惠金额': 'sum',
        '商家实际承担优惠金额': 'sum',
        '达人实际承担优惠金额': 'sum',
        '自播销售金额': 'sum',
        '达播销售金额': 'sum',
        '小店自卖销售金额': 'sum',
        '商品卡销售金额': 'sum',
        '短视频销售金额': 'sum',
        '退款金额': 'sum',
        '订单应付金额': 'sum'
    }).reset_index()
    daily_df = daily_df.rename(columns={"主订单编号":"总订单数"})
    # 退款订单部分
    print("daily_df:",daily_df.columns)
    group_list = live_room.copy()
    group_list.extend(["是否退款"])
    daily_df_return = df[use_col].groupby(group_list).agg({
        '主订单编号':'nunique'
    }).reset_index()
    daily_df_return = daily_df_return[daily_df_return["是否退款"]==1]
    daily_df_return = daily_df_return.rename(columns={"主订单编号":"退款订单数"})
    # 计算指标
    split_col = ['平台实际承担优惠金额','退款金额','订单应付金额','商家实际承担优惠金额','达人实际承担优惠金额'
                               ,'自播销售金额','达播销售金额','小店自卖销售金额','商品卡销售金额','短视频销售金额','总订单数']
    split_col.extend(live_room)
    print(split_col)
    daily_df_split = daily_df[split_col].copy()
    daily_df_split['店铺GMV(已加平台券)未扣退'] = daily_df_split['订单应付金额'] + daily_df_split["平台实际承担优惠金额"] + daily_df['达人实际承担优惠金额']
    daily_df_split['扣退店铺成交(已加平台券)'] = daily_df_split['店铺GMV(已加平台券)未扣退'] - daily_df_split['退款金额']
    daily_df_split['扣退店铺成交(未加平台券)'] = daily_df_split['扣退店铺成交(已加平台券)'] - daily_df_split['平台实际承担优惠金额']
    
    ##退款占比
    daily_df_split = return_column_rate(daily_df_split,'退款金额占比','退款金额','店铺GMV(已加平台券)未扣退')
    ##自播销售占比
    daily_df_split = return_column_rate(daily_df_split,'自播销售占比','自播销售金额','店铺GMV(已加平台券)未扣退')
    ##达播销售占比
    daily_df_split = return_column_rate(daily_df_split,'达播销售占比','达播销售金额','店铺GMV(已加平台券)未扣退')
    ##小店自卖销售占比
    daily_df_split = return_column_rate(daily_df_split,'小店自卖销售占比','小店自卖销售金额','店铺GMV(已加平台券)未扣退')
    ##商品卡销售占比
    daily_df_split = return_column_rate(daily_df_split,'商品卡销售占比','商品卡销售金额','店铺GMV(已加平台券)未扣退')
    ##短视频销售占比
    daily_df_split = return_column_rate(daily_df_split,'短视频销售占比','短视频销售金额','店铺GMV(已加平台券)未扣退')
    ##其他销售占比
    daily_df_split = return_column_rate(daily_df_split,'其他销售占比','其他销售金额','店铺GMV(已加平台券)未扣退')
    ##合并退款订单数据
    daily_df_merge = daily_df_split.merge(daily_df_return,on=live_room)
    ##总单数(已扣退)
    daily_df_merge['总单数(已扣退)'] = daily_df_merge['总订单数']-daily_df_merge['退款订单数']
    ##客单价(已扣退)
    daily_df_merge = return_column_rate(daily_df_merge,'客单价(已扣退)','扣退店铺成交(未加平台券)','总单数(已扣退)')
    if "直播间" not in live_room:
        use_col = ['日期','周','月','店铺GMV(已加平台券)未扣退','扣退店铺成交(已加平台券)','扣退店铺成交(未加平台券)','退款金额占比','自播销售占比','达播销售占比','小店自卖销售占比','总单数(已扣退)','客单价(已扣退)','商品卡销售占比','短视频销售占比']
    else:
        use_col = ['日期','周','月','直播间','店铺GMV(已加平台券)未扣退','扣退店铺成交(已加平台券)','扣退店铺成交(未加平台券)','总单数(已扣退)','客单价(已扣退)','退款金额占比','自播销售金额','自播销售占比','达播销售金额','达播销售占比','小店自卖销售金额','小店自卖销售占比','商品卡销售金额','商品卡销售占比','短视频销售金额','短视频销售占比','其他销售金额','其他销售占比']
    daily_df_merge = daily_df_merge[use_col]
    return daily_df_merge

def daily_self_data_bash(df,group_level=['日期','周','月']):
    """
    "自播成交
    （未扣退）"	"自播成交
    （扣退）"	"销售数
    （件）"	退款金额占比	总单量
    """
    df['日期'] = pd.to_datetime(df['订单提交时间']).dt.date
    if "直播间" not in group_level:
        use_col = ['日期',"订单提交时间","支付完成时间","商品数量","周","月","主订单编号","是否自播","是否达播","是否退款"
                ,"流量来源","流量体裁","平台实际承担优惠金额","商家实际承担优惠金额","达人实际承担优惠金额","退款金额","订单应付金额"]
        sort_col = ["周","日期","自播成交(未扣退)","自播成交(扣退)","销售数","退款金额占比","总单量"]
    else:
        use_col = ['日期',"直播间","订单提交时间","支付完成时间","商品数量","周","月","主订单编号","是否自播","是否达播","是否退款"
                ,"流量来源","流量体裁","平台实际承担优惠金额","商家实际承担优惠金额","达人实际承担优惠金额","退款金额","订单应付金额"]
        sort_col = ["周","直播间","日期","自播成交(未扣退)","自播成交(扣退)","销售数","退款金额占比","总单量"]
    daily_self_df = df[use_col][df["是否自播"]=="自播"].copy()
    
    daily_self_df['平台实际承担优惠金额'] = pd.to_numeric(daily_self_df['平台实际承担优惠金额'], errors='coerce').fillna(0)
    daily_self_df['商家实际承担优惠金额'] = pd.to_numeric(daily_self_df['商家实际承担优惠金额'], errors='coerce').fillna(0)
    daily_self_df['达人实际承担优惠金额'] = pd.to_numeric(daily_self_df['达人实际承担优惠金额'], errors='coerce').fillna(0)
    daily_self_df['退款金额'] = pd.to_numeric(daily_self_df['退款金额'], errors='coerce').fillna(0)
    daily_self_df['订单应付金额'] = pd.to_numeric(daily_self_df['订单应付金额'], errors='coerce').fillna(0)
    daily_self_df['自播销售金额'] = daily_self_df['订单应付金额'] + daily_self_df["平台实际承担优惠金额"]

    daily_self_df = daily_self_df.groupby(group_level).agg({
        '主订单编号':'nunique',
        '商品数量': 'sum',
        '自播销售金额': 'sum',
        '退款金额': 'sum'
    }).reset_index()
    daily_self_df = daily_self_df.rename(columns={"主订单编号":"总单量","商品数量":"销售数","自播销售金额":"自播成交(未扣退)"})

    daily_self_df["自播成交(扣退)"] = daily_self_df["自播成交(未扣退)"] - daily_self_df["退款金额"]
    daily_self_df = return_column_rate(daily_self_df,'退款金额占比',"退款金额","自播成交(未扣退)")
    

    return daily_self_df[sort_col]

def liuliang_data(quanyu_analysis_data):
    df = quanyu_analysis_data.copy()
    df_pivot = df.pivot_table(columns=['渠道名称'],index=['直播间','直播开始日期','直播开始时间'],values=['成交金额_占比','观看次数_占比'])
    return df_pivot

if __name__ == "__main__":
    ###订单数据
    file_path = r"D:\python project\python project\订单"
    clean_floder = r"D:\python project\python project"
    baiyin_data = pd.DataFrame()
    qianchuan_data = pd.DataFrame()
    data = pd.DataFrame()
    quanyu_analysis_data = pd.DataFrame()
    name_list = [{"name":"弹动官方旗舰店","data":"1762996440_3a4c72bd5ea481e72b0110c7cc4ef0d0cSqUBCXQ.csv","live_room":"鱼子酱"}
                 ,{"name":"弹动人参直播间","data":"人参.csv","live_room":"人参"}]

    for name_dict in name_list:
        file_name = os.path.join(file_path,name_dict["data"])
        df = pd.read_csv(file_name)
        df["直播间"] = name_dict["name"]
        data = pd.concat([df,data])
        clean_path = os.path.join(clean_floder,name_dict["live_room"])
        clean_duplicate_livestream_files(clean_path)

        ###千川消耗&百应流量
        baiyin,qianchuan = merge_baiyin_qianchuan_data(name_dict["live_room"])
        if baiyin.shape[0]:

            baiyin["直播间"] = name_dict["name"]
            baiyin_data = pd.concat([baiyin_data,baiyin])

            qianchuan["直播间"] = name_dict["name"]
            qianchuan_data = pd.concat([qianchuan_data,qianchuan])

            ###全域流量
            latest_file = get_latest_file(name_dict["live_room"])
            quanyu = pd.read_excel(latest_file,sheet_name="流量分析-渠道分析")
            quanyu_tran_data = pd.read_excel(latest_file,sheet_name="流量&转化-转化漏斗")[['直播开始时间','日期','直播间曝光-成交转化率(人数)','直播间曝光-观看率(人数)','直播间观看-商品曝光率(人数)','直播间商品曝光-点击率(人数)','直播间商品点击-成交转化率(人数)','直播间观看-成交转化率(人数)']]
            quanyu_basic_data = pd.read_excel(latest_file,sheet_name="基本信息")[['直播开始时间','日期','千次观看成交金额']]
            quanyu_live_data = quanyu_tran_data.merge(quanyu_basic_data,on=['直播开始时间','日期'])
            quanyu_live_data["直播间"] = name_dict["name"]
            quanyu_analysis = calculate_all_metrics(quanyu)
            quanyu_analysis["直播间"] = name_dict["name"]
            quanyu_analysis_data = pd.concat([quanyu_analysis_data,quanyu_analysis])
        else:
            continue


    for col in data.columns:
        data[col] = data[col].swifter.apply(lambda x:x.replace("\t","") if isinstance(x, str) else x)
    data[['平台实际承担优惠金额','订单应付金额','商家实际承担优惠金额','达人实际承担优惠金额']] \
    = data[['平台实际承担优惠金额','订单应付金额','商家实际承担优惠金额','达人实际承担优惠金额']]\
    .applymap(lambda x: float(str(x).replace(',', '')) if pd.notna(x) else x)
    print(data.columns)
    data["周"] = calculate_week_excel_style_series(data["订单提交时间"])
    data["月"] = data["订单提交时间"].swifter.apply(lambda x:pd.to_datetime(x).month)
    data["是否自播"] = np.where(
        ((data["达人昵称"] == "弹动官方旗舰店") | (data["达人昵称"] == "弹动个人护理旗舰店") | (data["达人昵称"] == "弹动官方旗舰店直播间")) & (data["流量体裁"] == "直播"),
        "自播",
        "非自播"
    )
    data["是否达播"] = np.where(
        (data["达人昵称"] != "弹动官方旗舰店") & (data["达人昵称"] != "弹动个人护理旗舰店") & (data["达人昵称"] != "弹动官方旗舰店直播间") & (data["流量体裁"] == "直播"),
        "达播",
        "非达播"
    )
    data["是否退款"] = np.where(
        (data["售后状态"] == "退款成功") | (data["售后状态"] == "已全额退款"),
        1,
        0
    )

    data["退款金额"] = np.where(
        (data["售后状态"] == "退款成功") | (data["售后状态"] == "已全额退款"),
        data["平台实际承担优惠金额"] + data["订单应付金额"] + data['达人实际承担优惠金额'],
        0
    )
    data = process_columns(data)
    print(data.head())
    print(data.info())
    data[['主订单编号','子订单编号','商品ID']] = data[['主订单编号','子订单编号','商品ID']].astype("str")


    data_daliy = daliy_data_bash(data)
    data_daily_self = daliy_data_bash(data,['日期', '周', '月', '直播间'])
    data_daily_group_live_room_self = daily_self_data_bash(data, ['直播间' ,'日期', '周', '月'])

    quanyu_analysis_pivot = liuliang_data(quanyu_analysis_data)

    with pd.ExcelWriter('订单数据_弹动.xlsx') as writer:
        data_daliy.to_excel(writer, sheet_name='日报', index=False)
        data_daily_self.to_excel(writer, sheet_name="分直播间日报",index=False)
        data_daily_group_live_room_self[data_daily_group_live_room_self["直播间"]=="弹动官方旗舰店"].to_excel(writer, sheet_name='弹动官方旗舰店', index=False)

        data_daily_group_live_room_self[data_daily_group_live_room_self["直播间"]=="弹动个人护理旗舰店"].to_excel(writer, sheet_name='弹动个人护理旗舰店', index=False)

        data_daily_group_live_room_self[data_daily_group_live_room_self["直播间"]=="弹动人参直播间"].to_excel(writer, sheet_name='弹动人参直播间', index=False)

        # data.to_excel(writer, sheet_name='明细数据', index=False)

        quanyu_analysis_data.to_excel(writer, sheet_name="quanyu_analysis_data", index=False)
        qianchuan_data.to_excel(writer, sheet_name="qianchuan_data", index=False)
        quanyu_analysis_pivot.to_excel(writer, sheet_name="quanyu_analysis_pivot")
        quanyu_live_data.to_excel(writer, sheet_name="quanyu_live_data",index=False)