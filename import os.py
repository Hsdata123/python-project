import pandas as pd
import numpy as np
import time
from openpyxl import Workbook
import os

def test_excel_performance():
    """测试Excel文件处理性能"""
    results = {}
    
    # 1. 创建测试数据
    print("创建测试数据...")
    sample_sizes = [1000, 10000, 50000, 100000, 200000]
    
    for size in sample_sizes:
        print(f"\n测试 {size} 行数据:")
        
        # 创建测试DataFrame
        df = pd.DataFrame({
            'id': range(size),
            'name': [f'user_{i}' for i in range(size)],
            'value1': np.random.rand(size),
            'value2': np.random.randint(1, 100, size),
            'category': np.random.choice(['A', 'B', 'C', 'D'], size)
        })
        
        filename = f'test_{size}.xlsx'
        
        # 测试写入性能
        start_time = time.time()
        df.to_excel(filename, index=False, engine='openpyxl')
        write_time = time.time() - start_time
        
        # 测试读取性能
        start_time = time.time()
        df_read = pd.read_excel(filename, engine='openpyxl')
        read_time = time.time() - start_time
        
        # 测试数据处理性能
        start_time = time.time()
        # 常见操作
        grouped = df_read.groupby('category').agg({'value1': 'mean', 'value2': 'sum'})
        filter_result = df_read[df_read['value2'] > 50]
        sorted_df = df_read.sort_values('value1', ascending=False)
        process_time = time.time() - start_time
        
        # 清理文件
        if os.path.exists(filename):
            os.remove(filename)
        
        results[size] = {
            'write_time': write_time,
            'read_time': read_time,
            'process_time': process_time,
            'total_time': write_time + read_time + process_time
        }
        
        print(f"  写入: {write_time:.2f}s, 读取: {read_time:.2f}s, 处理: {process_time:.2f}s")
    
    return results

# 运行测试
performance_results = test_excel_performance()