import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from data_functions import calculate_week_excel_style

def calculate_metrics(time_period, period_type, order_df, overview_df, compass_df):
    """
    统一计算指标函数
    time_period: 时间周期（日期或周）
    period_type: 'date' 或 'week'
    """
    
    print(f"\n计算{period_type}周期: {time_period}")
    
    # 根据周期类型过滤数据
    if period_type == 'date':
        period_order_df = order_df[order_df['日期'] == time_period]
        period_overview = overview_df[overview_df['日期'] == time_period]
        period_compass = compass_df[compass_df['日期'] == time_period]
        period_key = '日期'
    else:  # week
        period_order_df = order_df[order_df['周'] == time_period]
        period_overview = overview_df[overview_df['周'] == time_period]
        period_compass = compass_df[compass_df['周'] == time_period]
        period_key = '周'
    
    print(f"订单数据行数: {len(period_order_df)}")
    print(f"概览数据行数: {len(period_overview)}")
    print(f"罗盘数据行数: {len(period_compass)}")
    
    if period_order_df.empty:
        print("订单数据为空")
        return None
    if period_overview.empty:
        print("概览数据为空")
        return None
    if period_compass.empty:
        print("罗盘数据为空")
        return None
    
    # 基础指标计算
    shop_gmv = period_order_df['商家收入金额'].sum()
    print(f"店铺GMV: {shop_gmv}")
    
    # 处理售后状态可能为NaN的情况
    refund_orders = period_order_df[period_order_df['售后状态'].notna() & period_order_df['售后状态'].str.contains('退款成功', na=False)]
    refund_amount = refund_orders['商家收入金额'].sum()
    print(f"退款金额: {refund_amount}")
    
    gmv_after_refund_with_coupon = shop_gmv - refund_amount
    
    # 处理概览数据中的千川智能优惠券金额
    if '退款后千川智能优惠券金额(支付时间)' in period_overview.columns:
        qc_coupon_refund = period_overview['退款后千川智能优惠券金额(支付时间)'].sum()
    else:
        qc_coupon_refund = 0
        print("警告: 未找到千川智能优惠券金额列")
    
    gmv_after_refund_without_coupon = gmv_after_refund_with_coupon - qc_coupon_refund
    
    # 占比类指标计算
    total_user_payment = period_compass['用户支付金额'].sum()
    print(f"总用户支付金额: {total_user_payment}")
    
    # 自播销售
    self_live_mask = (period_compass['自营/带货'] == '自营') & (period_compass['载体类型'] == '直播')
    self_live_amount = period_compass[self_live_mask]['用户支付金额'].sum()
    
    # 达人销售
    talent_live_mask = (period_compass['自营/带货'] == '带货') & (period_compass['载体类型'] == '直播')
    talent_live_amount = period_compass[talent_live_mask]['用户支付金额'].sum()
    
    # 小店自卖
    small_shop_mask = period_order_df['流量来源'].notna() & period_order_df['流量来源'].str.contains('小店自卖', na=False)
    small_shop_amount = period_order_df[small_shop_mask]['商家收入金额'].sum()
    
    # 商品卡
    product_card_mask = period_compass['载体类型'] == '商品卡'
    product_card_amount = period_compass[product_card_mask]['用户支付金额'].sum()
    
    # 短视频
    short_video_mask = period_compass['载体类型'] == '短视频'
    short_video_amount = period_compass[short_video_mask]['用户支付金额'].sum()
    
    # 订单相关
    total_orders = len(period_order_df) - len(refund_orders)
    
    # 计算结果
    result = {
        period_key: time_period,
        '店铺GMV未扣退（已加平台券）': shop_gmv,
        '扣退店铺成交(已加平台券)': gmv_after_refund_with_coupon,
        '扣退店铺成交(未加平台券)': gmv_after_refund_without_coupon,
        '退款金额占比': (shop_gmv - gmv_after_refund_with_coupon) / shop_gmv if shop_gmv > 0 else 0,
        '自播销售占比': self_live_amount / total_user_payment if total_user_payment > 0 else 0,
        '达人销售占比': talent_live_amount / total_user_payment if total_user_payment > 0 else 0,
        '小店自卖-销售占比': small_shop_amount / shop_gmv if shop_gmv > 0 else 0,
        '总单量（已扣退）': total_orders,
        '总客单（已扣退）': gmv_after_refund_without_coupon / total_orders if total_orders > 0 else 0,
        '商品卡销售占比': product_card_amount / total_user_payment if total_user_payment > 0 else 0,
        '短视频销售占比': short_video_amount / total_user_payment if total_user_payment > 0 else 0
    }
    
    print(f"计算完成: {result}")
    return result

def main():
    """主函数"""
    try:
        # 读取订单数据
        order_df = pd.read_csv('1759117076_405926dfe7624ded7f30b787ca20040acSqUBCXQ.csv')

        # 读取罗盘数据
        compass_df = pd.read_excel('抖音电商罗盘-成交分析-20250918-20250928.xlsx', sheet_name='载体构成')
        overview_df = pd.read_excel('抖音电商罗盘-成交分析-20250918-20250928.xlsx', sheet_name='成交概览')
        
        # 打印数据基本信息用于调试
        print("订单数据形状:", order_df.shape)
        print("罗盘数据形状:", compass_df.shape)
        print("概览数据形状:", overview_df.shape)
        
        print("\n订单数据列:", order_df.columns.tolist())
        print("罗盘数据列:", compass_df.columns.tolist())
        print("概览数据列:", overview_df.columns.tolist())
        
        # 数据清洗和处理
        # 订单数据处理
        order_df['订单提交时间'] = pd.to_datetime(order_df['订单提交时间'])
        order_df['日期'] = order_df['订单提交时间'].dt.date

        # 转换商家收入金额为数值类型
        print("\n转换商家收入金额为数值类型...")
        order_df['商家收入金额'] = pd.to_numeric(order_df['商家收入金额'], errors='coerce')
          
        # 罗盘数据处理 - 转换日期格式从yyyymmdd到yyyy-mm-dd
        print("\n转换罗盘数据日期格式...")
        compass_df['日期'] = pd.to_datetime(compass_df['日期'].astype(str), format='%Y%m%d').dt.date
        
        # 概览数据处理 - 转换日期格式从yyyymmdd到yyyy-mm-dd
        print("转换概览数据日期格式...")
        overview_df['日期'] = pd.to_datetime(overview_df['日期'].astype(str), format='%Y%m%d').dt.date
        
        print("\n订单数据日期范围:", order_df['日期'].min(), "到", order_df['日期'].max())
        print("概览数据日期范围:", overview_df['日期'].min(), "到", overview_df['日期'].max())
        print("罗盘数据日期范围:", compass_df['日期'].min(), "到", compass_df['日期'].max())
        
        # 为所有数据添加周字段
        order_df['周'] = order_df['日期'].apply(calculate_week_excel_style)
        overview_df['周'] = overview_df['日期'].apply(calculate_week_excel_style)
        compass_df['周'] = compass_df['日期'].apply(calculate_week_excel_style)
        
        print("\n周字段添加完成")
        print("订单数据周分布:", order_df['周'].value_counts().to_dict())
        print("概览数据周分布:", overview_df['周'].value_counts().to_dict())
        
        # 计算日维度指标
        print("\n开始计算日维度指标...")
        daily_results = []
        unique_dates = sorted(order_df['日期'].unique())
        
        for date in unique_dates:
            print(f"\n处理日期: {date}")
            result = calculate_metrics(date, 'date', order_df, overview_df, compass_df)
            if result:
                daily_results.append(result)
        
        daily_df = pd.DataFrame(daily_results)
        
        # 计算周维度指标
        print("\n开始计算周维度指标...")
        weekly_results = []
        unique_weeks = sorted(order_df['周'].unique())
        
        for week in unique_weeks:
            print(f"\n处理周: {week}")
            result = calculate_metrics(week, 'week', order_df, overview_df, compass_df)
            if result:
                weekly_results.append(result)
        
        weekly_df = pd.DataFrame(weekly_results)
        
        # 输出结果
        print("\n日维度指标:")
        if not daily_df.empty:
            print(daily_df.round(4).to_string(index=False))
        else:
            print("无日维度数据")
        
        print("\n周维度指标:")
        if not weekly_df.empty:
            print(weekly_df.round(4).to_string(index=False))
        else:
            print("无周维度数据")
        
        # 保存到Excel
        if not daily_df.empty or not weekly_df.empty:
            with pd.ExcelWriter('电商指标分析结果.xlsx') as writer:
                if not daily_df.empty:
                    daily_df.to_excel(writer, sheet_name='日维度指标', index=False)
                if not weekly_df.empty:
                    weekly_df.to_excel(writer, sheet_name='周维度指标', index=False)
            print("\n结果已保存到 '电商指标分析结果.xlsx'")
        else:
            print("\n无数据可保存")
            
    except FileNotFoundError as e:
        print(f"文件未找到错误: {e}")
        print("请检查文件路径和文件名是否正确")
    except Exception as e:
        print(f"发生错误: {e}")
        print("请检查数据格式和结构")

if __name__ == "__main__":
    main()


