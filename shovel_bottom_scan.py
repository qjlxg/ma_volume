import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count
import pytz

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
RESULTS_BASE_DIR = 'results'
MIN_CLOSING_PRICE = 5.0
# 使用上海时区（与北京时间一致）
TIMEZONE = pytz.timezone('Asia/Shanghai')

def load_stock_names(file_path):
    """加载股票代码和名称的映射表"""
    try:
        # 假设 stock_names.csv 格式为 Code, Name
        names_df = pd.read_csv(file_path, dtype={'Code': str})
        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def check_shovel_bottom(df: pd.DataFrame) -> bool:
    """
    检查“铲底形态”筛选条件。
    
    形态：通常是一个看涨反转形态，一般考察最近三到四天的K线。
    这里我们将按照图片中的四根K线结构进行建模 (C1, C2, C3, C4)。
    
    假设数据按日期降序排列（最新数据在最前面）。
    需要至少有 4 条数据。
    
    C4 (最老)  C3 (老)  C2 (新)  C1 (最新)
    --------------------------------------
    图片解析的形态特征 (近似于晨星或启明星):
    1. C4 (第一根): 大阴线 (Red/Close < Open)，实体较大。
    2. C3 (第二根): 小K线（可阴可阳，通常是小阴线/小阳线，或十字星），体现止跌。
    3. C2 (第三根): 大阳线 (Green/Close > Open)，实体较大，收盘价明显高于 C3。
    4. C1 (第四根): 小实体 K 线，实体较小，表明整理。
    5. **关键**：C4, C3, C2 的低点应接近，形成底部。
    
    注意：由于常见的股票数据中，阳线是红/绿取决于市场惯例（A股通常红涨绿跌），
    但为了简化和通用性，我们只判断开盘价和收盘价。
    
    K线数据列假设: Date, Open, High, Low, Close, Volume...
    
    """
    if len(df) < 4:
        return False
    
    # 确保日期是降序（最新数据在前）
    # C1=最新, C2=次新, C3=第三新, C4=第四新
    c1, c2, c3, c4 = df.iloc[0], df.iloc[1], df.iloc[2], df.iloc[3]
    
    # --- 条件 1: 价格下限检查 ---
    if c1['Close'] < MIN_CLOSING_PRICE:
        return False

    # --- 条件 2: 铲底形态判断 ---
    
    # 1. C4（最老）：大阴线 (Close < Open)，实体较大
    is_c4_bearish = c4['Close'] < c4['Open']
    c4_body_ratio = abs(c4['Close'] - c4['Open']) / (c4['High'] - c4['Low'] + 1e-6)
    is_c4_large_body = c4_body_ratio > 0.5 and abs(c4['Close'] - c4['Open']) > (c4['Open'] * 0.01) # 实体大于1%
    
    # 2. C3（次老）：小实体 K 线，通常低点更低或持平
    c3_body_ratio = abs(c3['Close'] - c3['Open']) / (c3['High'] - c3['Low'] + 1e-6)
    is_c3_small_body = c3_body_ratio < 0.4
    
    # 3. C2（第三新）：大阳线 (Close > Open)，实体较大，收盘价高于 C3 的高点
    is_c2_bullish = c2['Close'] > c2['Open']
    c2_body_ratio = abs(c2['Close'] - c2['Open']) / (c2['High'] - c2['Low'] + 1e-6)
    is_c2_large_body = c2_body_ratio > 0.5 and abs(c2['Close'] - c2['Open']) > (c2['Open'] * 0.015) # 实体大于1.5%
    is_c2_higher_than_c3 = c2['Close'] > c3['High']
    
    # 4. C1 (最新): 整理/回调，收盘价高于 C2 的开盘价（维持强势）
    is_c1_stable = c1['Close'] > c2['Open'] 
    
    # 5. 底部确认：C4, C3, C2 的低点在相似水平，形成底部区域
    lows = [c4['Low'], c3['Low'], c2['Low']]
    low_range = max(lows) - min(lows)
    is_bottom_area = low_range < (c4['Close'] * 0.02) # 低点波动小于 2%
    
    # 综合判断
    if (is_c4_bearish and is_c4_large_body and 
        is_c3_small_body and 
        is_c2_bullish and is_c2_large_body and is_c2_higher_than_c3 and
        is_c1_stable and
        is_bottom_area):
        return True
        
    return False

def process_file(file_path):
    """
    处理单个 CSV 文件，检查形态条件。
    返回 (代码, True) 或 (代码, False)。
    """
    stock_code = os.path.basename(file_path).replace('.csv', '')
    try:
        # 假设 CSV 包含 'Date', 'Open', 'High', 'Low', 'Close', 'Volume' 列
        df = pd.read_csv(file_path, parse_dates=['Date'])
        # 确保数据按日期降序排列 (最新数据在前面)
        df = df.sort_values(by='Date', ascending=False).reset_index(drop=True)
        
        if check_shovel_bottom(df):
            latest_date = df.iloc[0]['Date'].strftime('%Y-%m-%d')
            latest_close = df.iloc[0]['Close']
            return {
                'Code': stock_code, 
                'Date': latest_date, 
                'Close': latest_close
            }
        
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        
    return None

def main():
    start_time = datetime.now(TIMEZONE)
    print(f"Starting scan at {start_time.strftime('%Y-%m-%d %H:%M:%S')} ({TIMEZONE.tzname(start_time)})")
    
    # 1. 加载股票名称
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    
    # 2. 扫描所有数据文件
    file_paths = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not file_paths:
        print(f"No CSV files found in directory: {STOCK_DATA_DIR}")
        return

    print(f"Found {len(file_paths)} stock data files. Using {cpu_count()} cores for parallelism.")

    # 3. 使用多进程并行处理
    # 使用 Pool 运行 process_file 函数
    with Pool(cpu_count()) as pool:
        results = pool.map(process_file, file_paths)
    
    # 4. 过滤有效结果
    matched_stocks = [r for r in results if r is not None]
    
    if not matched_stocks:
        print("No stocks matched the Shovel Bottom pattern and price filter.")
        return

    # 5. 构建结果 DataFrame 并添加股票名称
    results_df = pd.DataFrame(matched_stocks)
    results_df['Name'] = results_df['Code'].apply(lambda x: stock_names.get(x, 'N/A'))
    
    # 调整列顺序
    results_df = results_df[['Code', 'Name', 'Close', 'Date']]
    
    # 6. 保存结果
    timestamp_str = start_time.strftime('%Y%m%d_%H%M%S')
    current_year = start_time.strftime('%Y')
    current_month = start_time.strftime('%m')
    
    # 创建结果目录 (results/YYYY/MM/)
    save_dir = os.path.join(RESULTS_BASE_DIR, current_year, current_month)
    os.makedirs(save_dir, exist_ok=True)
    
    # 结果文件名
    output_filename = f"{timestamp_str}_shovel_bottom.csv"
    output_path = os.path.join(save_dir, output_filename)
    
    # 保存为 CSV
    results_df.to_csv(output_path, index=False, encoding='utf-8')
    
    end_time = datetime.now(TIMEZONE)
    print("--- Scan Summary ---")
    print(f"Matched stocks: {len(results_df)}")
    print(f"Results saved to: {output_path}")
    print(f"Time taken: {end_time - start_time}")

if __name__ == '__main__':
    main()
