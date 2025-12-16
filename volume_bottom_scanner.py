# volume_bottom_scanner.py (最终稳定版本：包含价格上下限和ST/创业板排除)

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import glob
import time

# --- 1. 筛选条件配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 5.0          # 【修改】股价筛选：最新收盘价不低于 5.0 元 
PRICE_MAX = 15.0         # 【新增】股价筛选：最新收盘价不高于 15.0 元
VOLUME_PERIOD = 120      # 缩量周期：计算天量时的历史周期 N
PRICE_LOW_PERIOD = 40    # 低位周期：价格低位确认周期 M
VOLUME_SHRINK_RATIO = 0.03  # 【沿用】缩量比例：最新成交量 <= 天量的 3% 
PRICE_LOW_RANGE_RATIO = 0.03 # 【沿用】低位范围：要求最新价在低位周期最低价的 3% 范围内

# --- 2. 数据列名映射 ---
DATE_COL = '日期'
CLOSE_COL = '收盘'
VOLUME_COL = '成交量'

# --- 3. 股票名称字典 (在主函数中加载) ---
STOCK_NAMES_DICT = {}

def load_stock_names():
    """加载股票代码和名称的映射表，适配 'code,name' 标题格式。"""
    global STOCK_NAMES_DICT
    print(f"尝试加载股票名称文件: {STOCK_NAMES_FILE}")
    try:
        names_df = pd.read_csv(
            STOCK_NAMES_FILE, 
            dtype={'code': str} 
        )
        names_df.columns = ['Code', 'Name'] 
        names_df['Code'] = names_df['Code'].astype(str).str.strip().str.zfill(6) 
        STOCK_NAMES_DICT = names_df.set_index('Code')['Name'].to_dict()
        print(f"成功加载 {len(STOCK_NAMES_DICT)} 条股票名称记录。")
        return STOCK_NAMES_DICT
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def analyze_stock_file(file_path):
    """分析单个股票的CSV文件，应用所有筛选条件。"""
    
    code = os.path.basename(file_path).split('.')[0].zfill(6)
    name = STOCK_NAMES_DICT.get(code, '未知名称')
    
    # --- A. 基本面/交易规则排除 ---
    # 1. 排除创业板 (30开头)
    if code.startswith('30'):
        # print(f"排除 {code} ({name}): 创业板股票 (30开头)")
        return None
        
    # 2. 排除 ST/PT 股
    if 'ST' in name or 'PT' in name or '*' in name:
        # print(f"排除 {code} ({name}): ST/PT 或带 * 股票")
        return None
    
    # 3. 排除深沪A股以外的代码 (主要排除科创板/创业板以外的 B/H/CDR 等，但前面已排除了创业板)
    # 此处假设您的 stock_data 文件夹中只包含 A 股代码，主要排除逻辑放在代码前缀和名称上。
    # 补充：只保留 60, 00, 30 开头，由于 30 已排除，此处主要检查 60/00/688
    if not (code.startswith('60') or code.startswith('00')):
        # print(f"排除 {code} ({name}): 非深沪A股主要代码")
        return None


    # --- B. 数据加载和技术筛选 ---
    try:
        df = pd.read_csv(file_path)
        df = df.sort_values(by=DATE_COL).reset_index(drop=True)
        
        if len(df) < max(VOLUME_PERIOD, PRICE_LOW_PERIOD):
            return None

        latest_data = df.iloc[-1]
        latest_close = latest_data[CLOSE_COL]
        latest_volume = latest_data[VOLUME_COL]
        
        # 4. 价格上下限筛选
        if not (PRICE_MIN <= latest_close <= PRICE_MAX):
            # print(f"排除 {code} ({name}): 价格 {latest_close} 不在 [{PRICE_MIN}, {PRICE_MAX}] 范围内")
            return None

        # 5. 缩量条件: 最新成交量 <= 120 天天量的 5%
        history_df = df.iloc[-max(VOLUME_PERIOD, PRICE_LOW_PERIOD):]
        max_volume = history_df[VOLUME_COL].iloc[-VOLUME_PERIOD:].max()
        
        if latest_volume > max_volume * VOLUME_SHRINK_RATIO:
            return None
        
        # 6. 价格低位确认: 最新价处于过去 40 天的最低 5% 范围内
        price_history = history_df[CLOSE_COL].iloc[-PRICE_LOW_PERIOD:]
        low_price = price_history.min()
        high_price = price_history.max()
        price_range = high_price - low_price
        
        low_threshold = low_price + PRICE_LOW_RANGE_RATIO * price_range
        
        if latest_close > low_threshold:
            return None

        # 所有条件满足
        return {
            'Code': code,
            'Name': name, 
            'Latest_Close': latest_close,
            'Latest_Volume': latest_volume,
            'Max_Volume_120d': max_volume,
            'Low_Price_40d_Threshold': low_threshold
        }

    except KeyError as e:
        print(f"Error: File {file_path} is missing expected column: {e}. Check your data format.")
        return None
    except Exception as e:
        # print(f"Error processing file {file_path}: {e}")
        return None

def main():
    """主函数，管理并行处理和结果输出。"""
    print(f"--- 启动缩量见底扫描 (价格 [{PRICE_MIN}, {PRICE_MAX}]，缩量 <= {VOLUME_SHRINK_RATIO*100}%，低位 <= {PRICE_LOW_RANGE_RATIO*100}%) ---")
    
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Directory '{STOCK_DATA_DIR}' not found.")
        return

    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"Error: No CSV files found in {STOCK_DATA_DIR}")
        return

    # 预加载股票名称字典
    load_stock_names()
    results = []
    
    workers = os.cpu_count() * 2 if os.cpu_count() else 4
    print(f"使用 {workers} 个工作线程并行扫描 {len(all_files)} 个文件...")
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # 确保只将沪深A股代码文件放入线程池（基于文件名）
        # 这一步是为了避免对非A股/非标代码进行耗时的数据读取和分析
        filtered_files = [
            f for f in all_files 
            if os.path.basename(f).split('.')[0].zfill(6).startswith(('60', '00'))
        ]
        
        future_to_file = {executor.submit(analyze_stock_file, file_path): file_path for file_path in filtered_files}
        
        for future in as_completed(future_to_file):
            result = future.result()
            if result:
                results.append(result)
            
    
    current_time = datetime.now()
    output_dir = current_time.strftime('output/%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = current_time.strftime('%Y%m%d_%H%M%S')
    final_output_path = os.path.join(output_dir, f'volume_bottom_scan_results_{timestamp}.csv')

    output_columns = ['Code', 'Name', 'Latest_Close', 'Latest_Volume', 'Max_Volume_120d', 'Low_Price_40d_Threshold']

    if not results:
        print("\n扫描完成：没有股票满足筛选条件。")
        pd.DataFrame(columns=output_columns).to_csv(final_output_path, index=False)
        print(f"已创建空结果文件: {final_output_path}")
        return

    results_df = pd.DataFrame(results)
    
    # 结果的名称匹配和排序
    results_df = results_df[output_columns]
    results_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')

    print("\n--- 筛选结果 ---")
    print(results_df.to_string(index=False))
    print(f"\n扫描完成，共找到 {len(results_df)} 只满足条件的股票。")
    print(f"结果已保存到: {final_output_path}")

if __name__ == '__main__':
    # 增加全局字典声明
    STOCK_NAMES_DICT = {}
    main()
