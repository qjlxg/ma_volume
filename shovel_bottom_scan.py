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
MAX_CLOSING_PRICE = 20.0  # 新增：收盘价上限
# 使用上海时区（与北京时间一致）
TIMEZONE = pytz.timezone('Asia/Shanghai')

def load_stock_names(file_path):
    """加载股票代码和名称的映射表"""
    try:
        # 假设 stock_names.csv 格式为 Code, Name
        names_df = pd.read_csv(file_path, dtype={'Code': str})
        # 确保列名是 'Code' 和 'Name'
        if 'Code' not in names_df.columns or 'Name' not in names_df.columns:
             # 尝试自动修正列名，假设前两列是 Code 和 Name
            print("Warning: stock_names.csv columns might not be 'Code', 'Name'. Assuming first two columns.")
            names_df.columns = ['Code', 'Name'] + list(names_df.columns[2:])

        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}
        
def check_stock_filters(code: str, name: str, close_price: float) -> bool:
    """
    检查股票代码、名称和价格是否符合筛选要求。
    
    1. 价格筛选: 5.0 <= Close <= 20.0
    2. 排除 ST 股: 名称中不含 "ST" 或 "*ST"
    3. 排除创业板: 代码不以 "30" 开头
    4. 仅深沪 A 股: (默认通过代码规则实现，如 60/00开头，非 30/80/40 开头等)
    """
    
    # --- 1. 价格筛选 ---
    if not (MIN_CLOSING_PRICE <= close_price <= MAX_CLOSING_PRICE):
        # print(f"Filter fail (Price): {code} - {close_price}")
        return False

    # --- 2. 排除 ST 股 ---
    if isinstance(name, str) and ("ST" in name.upper() or "*ST" in name.upper()):
        # print(f"Filter fail (ST): {code} - {name}")
        return False
        
    # --- 3. 排除创业板 (30开头) ---
    if code.startswith('30'):
        # print(f"Filter fail (GEM): {code}")
        return False
        
    # --- 4. 仅深沪 A 股 (排除科创板 688, 北交所 8/4 开头等)
    # 此处假设数据源本身主要为深沪 A 股，但我们强化排除 688（科创板），尽管科创板也是沪市。
    # 如果您明确要排除科创板，可以加上：
    # if code.startswith('688'):
    #     return False
        
    return True


def check_shovel_bottom(df: pd.DataFrame) -> bool:
    """
    检查“铲底形态”筛选条件 (形态逻辑不变)。
    """
    if len(df) < 4:
        return False
    
    # 确保日期是降序（最新数据在前）
    # C1=最新, C2=次新, C3=第三新, C4=第四新
    c1, c2, c3, c4 = df.iloc[0], df.iloc[1], df.iloc[2], df.iloc[3]
    
    # --- 形态判断逻辑（与之前版本保持一致）---
    
    # 1. C4（最老）：大阴线 (Close < Open)，实体较大
    is_c4_bearish = c4['Close'] < c4['Open']
    c4_body_ratio = abs(c4['Close'] - c4['Open']) / (c4['High'] - c4['Low'] + 1e-6)
    is_c4_large_body = c4_body_ratio > 0.5 and abs(c4['Close'] - c4['Open']) > (c4['Open'] * 0.01)
    
    # 2. C3（次老）：小实体 K 线，通常低点更低或持平
    c3_body_ratio = abs(c3['Close'] - c3['Open']) / (c3['High'] - c3['Low'] + 1e-6)
    is_c3_small_body = c3_body_ratio < 0.4
    
    # 3. C2（第三新）：大阳线 (Close > Open)，实体较大，收盘价高于 C3 的高点
    is_c2_bullish = c2['Close'] > c2['Open']
    c2_body_ratio = abs(c2['Close'] - c2['Open']) / (c2['High'] - c2['Low'] + 1e-6)
    is_c2_large_body = c2_body_ratio > 0.5 and abs(c2['Close'] - c2['Open']) > (c2['Open'] * 0.015)
    is_c2_higher_than_c3 = c2['Close'] > c3['High']
    
    # 4. C1 (最新): 整理/回调，收盘价高于 C2 的开盘价（维持强势）
    is_c1_stable = c1['Close'] > c2['Open'] 
    
    # 5. 底部确认：C4, C3, C2 的低点在相似水平，形成底部区域
    lows = [c4['Low'], c3['Low'], c2['Low']]
    low_range = max(lows) - min(lows)
    is_bottom_area = low_range < (c4['Close'] * 0.02)
    
    # 综合判断
    if (is_c4_bearish and is_c4_large_body and 
        is_c3_small_body and 
        is_c2_bullish and is_c2_large_body and is_c2_higher_than_c3 and
        is_c1_stable and
        is_bottom_area):
        return True
        
    return False

def process_file(file_path, stock_names):
    """
    处理单个 CSV 文件，检查形态条件和股票筛选条件。
    """
    stock_code = os.path.basename(file_path).replace('.csv', '')
    stock_name = stock_names.get(stock_code, 'N/A')
    
    try:
        # 假设 CSV 包含 'Date', 'Open', 'High', 'Low', 'Close', 'Volume' 列
        df = pd.read_csv(file_path, parse_dates=['Date'])
        # 确保数据按日期降序排列 (最新数据在前面)
        df = df.sort_values(by='Date', ascending=False).reset_index(drop=True)
        
        if df.empty:
            return None

        latest_close = df.iloc[0]['Close']
        latest_date = df.iloc[0]['Date'].strftime('%Y-%m-%d')
        
        # --- 1. 首先进行股票基础筛选 ---
        if not check_stock_filters(stock_code, stock_name, latest_close):
            return None
        
        # --- 2. 然后进行技术形态筛选 ---
        if check_shovel_bottom(df):
            return {
                'Code': stock_code, 
                'Name': stock_name,
                'Date': latest_date, 
                'Close': latest_close
            }
        
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        
    return None

def main():
    start_time = datetime.now(TIMEZONE)
    print(f"Starting scan at {start_time.strftime('%Y-%m-%d %H:%M:%S')} ({start_time.tzname()})")
    
    # 1. 加载股票名称
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    
    # 2. 扫描所有数据文件
    file_paths = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not file_paths:
        print(f"No CSV files found in directory: {STOCK_DATA_DIR}")
        return

    print(f"Found {len(file_paths)} stock data files. Using {cpu_count()} cores for parallelism.")

    # 3. 使用多进程并行处理
    # 传递 stock_names 到 process_file
    def run_process_file(file_path):
        return process_file(file_path, stock_names)

    with Pool(cpu_count()) as pool:
        results = pool.map(run_process_file, file_paths)
    
    # 4. 过滤有效结果
    matched_stocks = [r for r in results if r is not None]
    
    if not matched_stocks:
        print("No stocks matched the updated filters.")
        return

    # 5. 构建结果 DataFrame
    results_df = pd.DataFrame(matched_stocks)
    
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
