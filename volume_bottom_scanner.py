# volume_bottom_scanner.py (兼容中文列名版本)

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import glob
import time

# --- 配置参数 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 5.0  # 最新收盘价不低于 5.0 元
VOLUME_PERIOD = 60  # 计算天量时的周期 N
PRICE_LOW_PERIOD = 20  # 价格低位确认周期 M
VOLUME_SHRINK_RATIO = 0.20  # 缩量比例 20%

# --- 数据列名映射（使用您的中文列名） ---
DATE_COL = '日期'
CLOSE_COL = '收盘'
VOLUME_COL = '成交量'

def load_stock_names():
    """修复：加载股票代码和名称的映射表，假设 stock_names.csv 无标题行。"""
    print(f"尝试加载股票名称文件: {STOCK_NAMES_FILE}")
    try:
        # 假设文件没有标题行 (header=None)，并手动指定列名
        names_df = pd.read_csv(
            STOCK_NAMES_FILE, 
            header=None, 
            names=['Code', 'Name'], 
            # 注意：如果文件是 tab 分隔或空格分隔，需要添加 sep='\t' 或 sep='\s+'
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
        # 注意：如果您的CSV是用 TAB 或空格分隔的，这里可能需要调整 read_csv 的参数，
        # 默认是逗号分隔 (sep=',')。
        df = pd.read_csv(file_path)
        
        # 确保数据按日期升序排列
        df = df.sort_values(by=DATE_COL).reset_index(drop=True)
        
        if len(df) < max(VOLUME_PERIOD, PRICE_LOW_PERIOD):
            return None

        latest_data = df.iloc[-1]
        
        # 股票代码从文件名获取
        code = os.path.basename(file_path).split('.')[0].zfill(6)
        
        # 使用中文列名获取数据
        latest_close = latest_data[CLOSE_COL]
        latest_volume = latest_data[VOLUME_COL]
        
        # 1. 最新收盘价不能低于 5.0 元
        if latest_close < PRICE_MIN:
            return None

        history_df = df.iloc[-VOLUME_PERIOD:]
        
        # 2. 缩量见底条件
        max_volume = history_df[VOLUME_COL].max()
        
        if latest_volume > max_volume * VOLUME_SHRINK_RATIO:
            # 最新成交量超过天量的 20%
            return None
        
        # 3. 价格低位确认 (最新价处于过去 PRICE_LOW_PERIOD 天的底部 25% 范围内)
        price_history = df.iloc[-PRICE_LOW_PERIOD:][CLOSE_COL]
        low_price = price_history.min()
        high_price = price_history.max()
        price_range = high_price - low_price
        
        # 计算价格低位阈值：低点 + 25% * 价格范围
        low_threshold = low_price + 0.25 * price_range
        
        if latest_close > low_threshold:
            # 价格不在近期底部区域 (不在最低 25% 范围内)
            return None

        # 所有条件满足
        return {
            'Code': code,
            'Name': '', 
            'Latest_Close': latest_close,
            'Latest_Volume': latest_volume,
            'Max_Volume_60d': max_volume,
            'Low_Price_20d_Threshold': low_threshold
        }

    except KeyError as e:
        # 如果数据文件缺少您的中文列名，抛出警告
        print(f"Error: File {file_path} is missing expected column: {e}. Check your data format.")
        return None
    except Exception as e:
        # print(f"Error processing file {file_path}: {e}")
        return None

def main():
    """主函数，管理并行处理和结果输出。"""
    print(f"--- 启动缩量见底扫描 (价格 >= {PRICE_MIN}，缩量 <= {VOLUME_SHRINK_RATIO*100}%) ---")
    
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
        pd.DataFrame(columns=['Code', 'Name', 'Latest_Close', 'Latest_Volume', 'Max_Volume_60d', 'Low_Price_20d_Threshold']).to_csv(final_output_path, index=False)
        print(f"已创建空结果文件: {final_output_path}")
        return

    # 将结果转换为 DataFrame 并匹配名称
    results_df = pd.DataFrame(results)
    results_df['Name'] = results_df['Code'].map(stock_names).fillna('未知名称')

    # 排序和保存结果
    results_df = results_df[['Code', 'Name', 'Latest_Close', 'Latest_Volume', 'Max_Volume_60d', 'Low_Price_20d_Threshold']]
    results_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')

    print("\n--- 筛选结果 ---")
    print(results_df.to_string(index=False))
    print(f"\n扫描完成，共找到 {len(results_df)} 只满足条件的股票。")
    print(f"结果已保存到: {final_output_path}")

if __name__ == '__main__':
    main()
