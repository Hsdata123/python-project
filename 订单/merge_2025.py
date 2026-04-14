import pandas as pd
import os

# 订单目录路径
order_dir = os.path.dirname(os.path.abspath(__file__))
months = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']

all_data = []

for month in months:
    file_path = os.path.join(order_dir, f'{month}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        all_data.append(df)
        print(f'已读取: {month}.csv ({len(df)} 行)')

# 合并所有数据
combined_df = pd.concat(all_data, ignore_index=True)

# 保存为2025年订单
output_path = os.path.join(order_dir, '2025年订单.csv')
combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f'\n合并完成!')
print(f'总计: {len(combined_df)} 行')
print(f'保存至: {output_path}')
