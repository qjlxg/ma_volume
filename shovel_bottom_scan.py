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
MAX_CLOSING_PRICE = 20.0
# 使用上海时区（与北京时间一致）
TIMEZONE = pytz.timezone('Asia/Shanghai')

# 定义一个全局变量来存储股票名称映射，供子进程使用
GLOBAL_STOCK_NAMES = None 

def load_stock_names(file_path):
    """加载股票代码和名称的映射表"""
    try:
        # 假设 stock_names.csv 格式为 Code, Name
        names_df = pd.read_csv(file_path, dtype={'Code': str})
        
        # 兼容性检查
        if 'Code' not in names_df.columns or 'Name' not in names_df.columns:
            print("Warning: stock_names.csv columns might not be 'Code', 'Name'. Assuming first two columns.")
            # 假设第一个是 Code，第二个是 Name
            names_df.columns = ['Code', 'Name'] + list(names_df.columns[2:])

        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        # 在主进程中打印错误，不影响子进程
        print(f"Error loading stock names: {e}") 
        return {}
        
def initializer(stock_names_dict):
    """
    Pool 初始化函数，在每个子进程启动时调用。
    将股票名称字典加载到每个子进程的全局变量 GLOBAL_STOCK_NAMES 中。
    """
    global GLOBAL_STOCK_NAMES
    GLOBAL_STOCK_NAMES = stock_names_dict

def check_stock_filters(code: str, name: str, close_price: float) -> bool:
    """
    检查股票代码、名称和价格是否符合筛选要求。
    """
    
    # --- 1. 价格筛选 ---
    if not (MIN_CLOSING_PRICE <= close_price <= MAX_CLOSING_PRICE):
        return False

    # --- 2. 排除 ST 股 ---
    if isinstance(name, str) and ("ST" in name.upper() or "*ST" in name.upper()):
        return False
        
    # --- 3. 排除创业板 (30开头) ---
    if code.startswith('30'):
        return False
        
    return True


def check_shovel_bottom(df: pd.DataFrame) -> bool:
    """
    检查“铲底形态”筛选条件 (基于图片中的四根K线结构)。
    """
    # 需要至少有 4 条数据来形成形态
    if len(df) < 4:
        return False
    
    # C1=最新, C2=次新, C3=第三新, C4=第四新
    c1, c2, c3, c4 = df.iloc[0], df.iloc[1], df.iloc[2], df.iloc[3]
    
    # 1. C4（最老）：大阴线 (Close < Open)，实体较大
    is_c4_bearish = c4['Close'] < c4['Open']
    c4_body_ratio = abs(c4['Close'] - c4['Open']) / (c4['High'] - c4['Low'] + 1e-6)
    is_c4_large_body = c4_body_ratio > 0.5 and abs(c4['Close'] - c4['Open']) > (c4['Open'] * 0.01)
    
    # 2. C3（次老）：小实体 K 线，体现止跌
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

def process_file(file_path):
    """
    处理单个 CSV 文件，检查形态条件和股票筛选条件。
    从全局变量 GLOBAL_STOCK_NAMES 中获取名称。
    """
    stock_code = os.path.basename(file_path).replace('.csv', '')
    
    # 从子进程的全局变量中获取名称
    stock_name = GLOBAL_STOCK_NAMES.get(stock_code, 'N/A')
    
    try:
        # 假设 CSV 包含 'Date', 'Open', 'High', 'Low', 'Close', 'Volume' 列
        df = pd.read_csv(file_path, parse_dates=['Date'])
        # 确保数据按日期降序排列 (最新数据在前面)
        df = df.sort_values(by='Date', ascending=False).reset_index(drop=True)
        
        if df.empty:
            return None

        latest_close = df.iloc[0]['Close']
        latest_date = df.iloc[0]['Date'].strftime('%Y-%m-%d')
        
        # --- 1. 首先进行股票基础筛选 (价格、ST、创业板) ---
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
        # 在子进程中打印错误，便于调试
        print(f"Error processing file {file_path}: {e}")
        
    return None

def main():
    start_time = datetime.now(TIMEZONE)
    # 修正：使用 strftime('%Z') 安全获取时区名称
    print(f"Starting scan at {start_time.strftime('%Y-%m-%d %H:%M:%S')} ({start_time.strftime('%Z')})")
    
    # 1. 加载股票名称 (仅在主进程中执行一次)
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    
    # 2. 扫描所有数据文件
    file_paths = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not file_paths:
        print(f"No CSV files found in directory: {STOCK_DATA_DIR}")
        return

    print(f"Found {len(file_paths)} stock data files. Using {cpu_count()} cores for parallelism.")

    # 3. 使用多进程并行处理
    # 使用 initializer 和 initargs 将 stock_names 字典传递给子进程
    # 解决了 PicklingError
    with Pool(initializer=initializer, initargs=(stock_names,)) as pool:
        results = pool.map(process_file, file_paths)
    
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
