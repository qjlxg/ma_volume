# volume_bottom_scanner.py (最终优化版本：修复列名，收紧参数)

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import glob
import time

# --- 1. 筛选条件配置 (已收紧参数) ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 8.0          # 【调整】最新收盘价不低于 8.0 元 (原 5.0)
VOLUME_PERIOD = 120      # 【调整】计算天量时的周期 N (原 60)，使用 120 天天量
PRICE_LOW_PERIOD = 40    # 【调整】价格低位确认周期 M (原 20)，使用 40 天低位
VOLUME_SHRINK_RATIO = 0.10  # 【调整】缩量比例 10% (原 0.20/20%)
PRICE_LOW_RANGE_RATIO = 0.10 # 【新增】要求最新价在 PRICE_LOW_PERIOD 周期内最低价的 10% 范围内

# --- 2. 数据列名映射 (适配您的中文格式) ---
DATE_COL = '日期'
CLOSE_COL = '收盘'
VOLUME_COL = '成交量'
# 注意：如果您的CSV是用 TAB 或空格分隔的，请在 read_csv 中调整 sep 参数

def load_stock_names():
    """修复：加载股票代码和名称的映射表，假设 stock_names.csv 无标题行。"""
    print(f"尝试加载股票名称文件: {STOCK_NAMES_FILE}")
    try:
        # 假设文件没有标题行 (header=None)，并手动指定列名
        names_df = pd.read_csv(
            STOCK_NAMES_FILE, 
            header=None, 
            names=['Code', 'Name'], 
            dtype={'Code': str}
        )
        names_df['Code'] = names_df['Code'].astype(str).str.strip().str.zfill(6) 
        print(f"成功加载 {len(names_df)} 条股票名称记录。")
        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def analyze_stock_file(file_path):
    """
    分析单个股票的CSV文件，应用筛选条件。
    已适配使用中文列名: '日期', '收盘', '成交量'
    """
    try:
        df = pd.read_csv(file_path)
        
        # 确保数据按日期升序排列
        df = df.sort_values(by=DATE_COL).reset_index(drop=True)
        
        # 确保有足够的历史数据
        if len(df) < max(VOLUME_PERIOD, PRICE_LOW_PERIOD):
            return None

        latest_data = df.iloc[-1]
        code = os.path.basename(file_path).split('.')[0].zfill(6)
        
        # 使用中文列名获取数据
        latest_close = latest_data[CLOSE_COL]
        latest_volume = latest_data[VOLUME_COL]
        
        # 1. 价格筛选: 最新收盘价不能低于 PRICE_MIN
        if latest_close < PRICE_MIN:
            return None

        # --- 缩量见底核心逻辑 ---
        history_df = df.iloc[-max(VOLUME_PERIOD, PRICE_LOW_PERIOD):]
        
        # 2. 缩量条件: 最新成交量 <= 120 天天量的 10%
        max_volume = history_df[VOLUME_COL].iloc[-VOLUME_PERIOD:].max()
        
        if latest_volume > max_volume * VOLUME_SHRINK_RATIO:
            return None
        
        # 3. 价格低位确认: 最新价处于过去 40 天的最低 10% 范围内
        price_history = history_df[CLOSE_COL].iloc[-PRICE_LOW_PERIOD:]
        low_price = price_history.min()
        high_price = price_history.max()
        price_range = high_price - low_price
        
        # 计算价格低位阈值：低点 + 10% * 价格范围
        low_threshold = low_price + PRICE_LOW_RANGE_RATIO * price_range
        
        if latest_close > low_threshold:
            # 价格不在近期底部区域 (不在最低 10% 范围内)
            return None

        # 所有条件满足
        return {
            'Code': code,
            'Name': '', 
            'Latest_Close': latest_close,
            'Latest_Volume': latest_volume,
            'Max_Volume_120d': max_volume,
            'Low_Price_40d_Threshold': low_threshold
        }

    except KeyError as e:
        print(f"Error: File {file_path} is missing expected column: {e}. Check your data format.")
        return None
    except Exception as e:
        # 可以根据需要启用这行来调试其他类型的错误
        # print(f"Error processing file {file_path}: {e}")
        return None

def main():
    """主函数，管理并行处理和结果输出。"""
    print(f"--- 启动缩量见底扫描 (价格 >= {PRICE_MIN}，缩量 <= {VOLUME_SHRINK_RATIO*100}%，低位 <= {PRICE_LOW_RANGE_RATIO*100}%) ---")
    
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Directory '{STOCK_DATA_DIR}' not found.")
        return

    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"Error: No CSV files found in {STOCK_DATA_DIR}")
        return

    stock_names = load_stock_names()
    results = []
    
    workers = os.cpu_count() * 2 if os.cpu_count() else 4
    print(f"使用 {workers} 个工作线程并行扫描 {len(all_files)} 个文件...")
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_file = {executor.submit(analyze_stock_file, file_path): file_path for file_path in all_files}
        
        for future in as_completed(future_to_file):
            result = future.result()
            if result:
                results.append(result)
            
    
    # 格式化输出文件路径
    current_time = datetime.now()
    output_dir = current_time.strftime('output/%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = current_time.strftime('%Y%m%d_%H%M%S')
    final_output_path = os.path.join(output_dir, f'volume_bottom_scan_results_{timestamp}.csv')

    if not results:
        print("\n扫描完成：没有股票满足筛选条件。")
        # 创建一个空文件
        pd.DataFrame(columns=['Code', 'Name', 'Latest_Close', 'Latest_Volume', 'Max_Volume_120d', 'Low_Price_40d_Threshold']).to_csv(final_output_path, index=False)
        print(f"已创建空结果文件: {final_output_path}")
        return

    # 将结果转换为 DataFrame 并匹配名称
    results_df = pd.DataFrame(results)
    results_df['Name'] = results_df['Code'].map(stock_names).fillna('未知名称')

    # 排序和保存结果
    results_df = results_df[['Code', 'Name', 'Latest_Close', 'Latest_Volume', 'Max_Volume_120d', 'Low_Price_40d_Threshold']]
    results_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')

    print("\n--- 筛选结果 ---")
    print(results_df.to_string(index=False))
    print(f"\n扫描完成，共找到 {len(results_df)} 只满足条件的股票。")
    print(f"结果已保存到: {final_output_path}")

if __name__ == '__main__':
    main()
