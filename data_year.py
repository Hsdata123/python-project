import pandas as pd

# 读取数据
data = pd.read_csv(r"D:\python project\python project\订单\2025年订单.csv", encoding="utf-8")
print(data.count())
# 通过索引位置获取关键列（避免重复列名问题）
col_shopping = data.columns[2]   # 选购商品
col_shopping_count = data.columns[4] #商品数量
col_amount = data.columns[8]     # 商品应收
col_pay_time = data.columns[32]  # 支付完成时间
col_platform = data.columns[56]  # 平台实际承担优惠金额

# 1. 剔除支付完成时间为空的数据
data = data[data[col_pay_time]!=""]
data = data[~data[col_pay_time].isna()]
print(f"剔除空值后的数据量: {len(data)}")

# 2. 清洗商品应收金额和平台优惠金额（去除制表符和非数字字符）
data[col_amount] = pd.to_numeric(data[col_amount].astype(str).str.replace('\t', '').str.replace(',', ''))
data[col_platform] = pd.to_numeric(data[col_platform].astype(str).str.replace('\t', '').str.replace(',', ''))
data[col_shopping_count] = pd.to_numeric(data[col_shopping_count].astype(str).str.replace('\t', '').str.replace(',', ''))
# 3. 计算收入 = 商品应收 + 平台实际承担优惠金额
data['收入'] = data[col_amount] + data[col_platform]
data['销量'] = data[col_shopping_count]
# 4. 从支付完成时间提取月份
data['月份'] = pd.to_datetime(data[col_pay_time]).dt.month

# 5. 计算每个选购商品每月的订单应付金额、平台优惠和收入
monthly_summary = data.groupby([col_shopping, '月份']).agg({
    col_amount: 'sum',
    col_platform: 'sum',
    '收入': 'sum',
    '销量': 'sum'
}).reset_index()
monthly_summary.columns = ['选购商品', '月份', '月订单应付金额', '月平台优惠', '月收入', '销量']

# 6. 汇总一年的订单应付金额、平台优惠和收入
yearly_summary = data.groupby(col_shopping).agg({
    col_amount: 'sum',
    col_platform: 'sum',
    '收入': 'sum',
    '销量': 'sum'
}).reset_index()
yearly_summary.columns = ['选购商品', '年订单应付金额', '年平台优惠', '年收入', '年销量']

print("\n=== 每件商品每月数据汇总 ===")
print(monthly_summary.to_string(index=False))

print("\n=== 每件商品年度汇总 ===")
print(yearly_summary.to_string(index=False))

print(f"\n=== 年度总订单应付金额: {yearly_summary['年订单应付金额'].sum():.2f} ===")
print(f"=== 年度总平台优惠金额: {yearly_summary['年平台优惠'].sum():.2f} ===")
print(f"=== 年度总收入金额: {yearly_summary['年收入'].sum():.2f} ===")

# 保存结果
monthly_summary.to_csv('每月商品汇总.csv', index=False, encoding='utf-8-sig')
yearly_summary.to_csv('年度商品汇总.csv', index=False, encoding='utf-8-sig')