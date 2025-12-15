import pandas as pd
import os
import glob
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# 配置日志
# 级别设置为 WARNING，减少正常运行时 INFO 日志的输出，让 Action 运行日志更简洁
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_DIR = 'output'
MAX_WORKERS = 4  # 并行进程数

# --- 筛选条件 ---
MIN_PRICE = 5.0
MAX_PRICE = 20.0

# 定义CSV文件中的关键列名
COL_DATE = '日期'
COL_CLOSE = '收盘'
COL_LOW = '最低'
COL_VOLUME = '成交量'

# --- 核心筛选函数 ---

def meets_tech_criteria(df: pd.DataFrame) -> bool:
    """
    实现图中的技术分析筛选条件 (基于量价和回踩确认)。
    """
    # 需要至少25天数据来计算MA20和检查回踩
    if df.empty or len(df) < 25: 
        return False

    # 1. 计算20日均线 (MA20)
    df['MA20'] = df[COL_CLOSE].rolling(window=20).mean()

    # 取最新的数据点
    latest = df.iloc[-1]
    
    # 最近三天的最低价 (模拟“三天不破”)
    recent_lows = df[COL_LOW].iloc[-3:].min()

    # --- 条件量化 ---
    
    # C1: 最新收盘价高于20日均线 (上升趋势确认)
    C1_Trend = latest[COL_CLOSE] > latest['MA20']
    
    # C2: 模拟“回踩三天不破买入”：当前价格高于最近的支撑确认价
    # 假设支撑位在MA20附近，且当前收盘价高于最近三天的最低价
    C2_Retracement_Check = (latest[COL_CLOSE] > recent_lows) and (recent_lows > latest['MA20'].shift(3) * 0.99)
    
    # C3: 模拟“放量突破”：今天成交量高于前5日平均成交量 (1.5倍)
    latest_vol = latest[COL_VOLUME]
    avg_vol_5 = df[COL_VOLUME].iloc[-6:-1].mean()
    C3_Volume = latest_vol > avg_vol_5 * 1.5
    
    # 综合判断
    return C1_Trend and C2_Retracement_Check and C3_Volume

def meets_basic_criteria(df: pd.DataFrame, stock_code: str) -> bool:
    """
    实现基本面/价格筛选条件。
    """
    if df.empty:
        return False

    latest_close = df.iloc[-1][COL_CLOSE]
    
    # C4: 价格范围筛选 (5.0 元 <= 收盘价 <= 20.0 元)
    C4_Price_Range = (latest_close >= MIN_PRICE) and (latest_close <= MAX_PRICE)
    
    # C5: 排除条件：ST, 30开头 (创业板)。只要深沪A股 (00, 60开头)。
    # ST 股排除依赖于数据源，这里主要通过代码前缀排除 30 开头，只保留 00 和 60 开头。
    C5_Exchange_Exclude = stock_code.startswith('60') or stock_code.startswith('00')
    
    return C4_Price_Range and C5_Exchange_Exclude

def process_file(file_path: str) -> dict or None:
    """
    处理单个CSV文件并应用所有筛选条件。
    """
    # 文件名格式：代码.csv
    stock_code = os.path.basename(file_path).split('.')[0]
    
    try:
        df = pd.read_csv(file_path)
        df.sort_values(COL_DATE, inplace=True)
        df.dropna(inplace=True)

        if not meets_basic_criteria(df, stock_code):
            return None
        
        if not meets_tech_criteria(df):
            return None

        latest_close = df.iloc[-1][COL_CLOSE]
        # logging.info(f"✅ Found qualifying stock: {stock_code} @ {latest_close}") # 仅在调试时使用
        return {'Code': stock_code, 'Close': latest_close}
    
    except Exception as e:
        # 记录处理单个文件时的错误
        logging.error(f"Error processing file {file_path}: {e}")
        return None

def main():
    start_time = datetime.now()
    logging.warning("--- Starting Stock Screener Advanced ---")

    # 1. 准备数据文件列表
    file_paths = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    
    if not file_paths:
        logging.error(f"FATAL: No CSV files found in {STOCK_DATA_DIR}. Please check the directory structure.")
        return

    # 2. 并行处理文件
    results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        processed_results = executor.map(process_file, file_paths)
        results = [res for res in processed_results if res is not None]

    if not results:
        logging.warning("❌ No stocks matched all criteria.")
        return

    # 3. 匹配股票名称 (使用 code 和 name)
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
        names_df.rename(columns={'code': 'Code', 'name': 'StockName'}, inplace=True)
    except Exception as e:
        logging.error(f"FATAL: Could not load stock names file {STOCK_NAMES_FILE} or column mismatch: {e}")
        return

    results_df = pd.DataFrame(results)
    
    final_df = pd.merge(results_df, names_df, on='Code', how='left')
    final_df = final_df[['Code', 'StockName', 'Close']]

    # 4. 保存结果
    current_time_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    current_year_month = datetime.now().strftime('%Y-%m')
    
    output_subdir = os.path.join(OUTPUT_DIR, current_year_month)
    os.makedirs(output_subdir, exist_ok=True)
    
    output_filename = f"screener_{current_time_str}.csv"
    output_path = os.path.join(output_subdir, output_filename)
    
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logging.warning(f"✅ Screening complete. {len(final_df)} stocks found. Results saved to: {output_path}")
    logging.warning(f"Total runtime: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()
