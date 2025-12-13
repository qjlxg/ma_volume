import os
import re
import pandas as pd
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# --- 常量定义 ---
STOCK_DATA_DIR = 'stock_data'  # 确保您的CSV文件都在这个目录下
MAX_WORKERS = 8       # 并行处理的最大线程数
HOLD_DAYS = 30        # 持有天数
BACKTEST_START_DATE = '2020-01-01'  # 回测起始日期
BACKTEST_END_DATE = '2025-12-13'    # 回测结束日期
BACKTEST_STEP_DAYS = 30             # 每隔N天运行一次筛选

# --- 筛选逻辑函数 (已修复 SettingWithCopyWarning) ---
def calculate_indicators(data):
    """计算 MA5 和 MA20，并避免 SettingWithCopyWarning。"""
    if len(data) < 30:
        return pd.DataFrame()
    df = data.copy() # 使用 .copy() 明确操作一个副本
    
    df.loc[:, 'Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df.loc[:, 'Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
    
    df.loc[:, 'MA5'] = df['Close'].rolling(window=5).mean()
    df.loc[:, 'MA20'] = df['Close'].rolling(window=20).mean()
    
    return df.dropna()

# ... (check_c1_golden_cross, check_c4_trend_control, select_stock_logic 保持不变)
def check_c1_golden_cross(data):
    if len(data) < 2: return False
    d0 = data.iloc[-1]
    d1 = data.iloc[-2]
    golden_cross = (d0['MA5'] > d0['MA20']) and (d1['MA5'] <= d1['MA20'])
    entry_point = d0['Close'] > d0['MA20']
    return golden_cross and entry_point

def check_c4_trend_control(data, max_drawdown=0.15, max_days=30):
    if len(data) < 30: return False
    ma20_slope = data['MA20'].iloc[-1] - data['MA20'].iloc[-5]
    is_ma20_up = ma20_slope > 0
    recent_high = data['Close'].iloc[-max_days:].max()
    current_price = data['Close'].iloc[-1]
    if recent_high == 0: return False
    drawdown = (recent_high - current_price) / recent_high
    is_drawdown_controlled = drawdown <= max_drawdown
    return is_ma20_up and is_drawdown_controlled

def select_stock_logic(data):
    data = calculate_indicators(data)
    if data.empty: return False
    data = data.sort_values(by='Date').reset_index(drop=True) 
    condition_final = check_c1_golden_cross(data) and check_c4_trend_control(data)
    return condition_final

# ... (get_data_up_to_date, calculate_return 保持不变)
def get_data_up_to_date(data, target_date):
    data = data[data['Date'] <= target_date]
    return data

def calculate_return(data, buy_date, hold_days):
    buy_date_naive = buy_date.replace(tzinfo=None)
    buy_data = data[data['Date'] == buy_date_naive]
    if buy_data.empty:
        next_days = data[data['Date'] > buy_date_naive].sort_values(by='Date')
        if next_days.empty: return None
        buy_idx = next_days.index[0]
    else:
        buy_idx = buy_data.index[0]
        
    buy_price = data.at[buy_idx, 'Close']
    buy_date_actual = data.at[buy_idx, 'Date']

    sell_date_target = buy_date_actual + timedelta(days=hold_days)
    future_data = data[(data['Date'] >= buy_date_actual) & (data['Date'] <= sell_date_target)]
    
    if future_data.empty or len(future_data) < 2: 
        return None 
    
    sell_price = future_data['Close'].iloc[-1]

    return (sell_price - buy_price) / buy_price


def backtest_single_stock(file_path, test_dates):
    """回测单个股票，精确匹配日期格式并尝试多种编码。"""
    try:
        match = re.search(r'(\d{6})\.csv$', file_path)
        if not match:
            return None
        stock_code = match.group(1)
        
        column_names = ['Date', 'Code', 'Open', 'Close', 'High', 'Low', 'Volume', 'Amount', 'Amplitude', 'ChangePct', 'ChangeAmt', 'Turnover']
        
        # --- 核心修复 1：修复文件编码问题，尝试最兼容的中文编码 ---
        for encoding_type in ['utf-8', 'gb18030', 'gbk']:
            try:
                data = pd.read_csv(
                    file_path,
                    header=0,
                    names=column_names,
                    encoding=encoding_type
                )
                break 
            except UnicodeDecodeError:
                continue
        else:
            raise UnicodeDecodeError(f"Failed to decode file {file_path} with utf-8, gb18030, or gbk. Please check file integrity.")
        
        # --- 核心修复 2：精确指定日期格式，以提高性能和准确性 ---
        # 严格使用您提供的格式 'YYYY-MM-DD'。
        data.loc[:, 'Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d', errors='coerce').dt.tz_localize(None)
        data = data.dropna(subset=['Date'])
        # ----------------------------------------------------
        
        data = data.sort_values(by='Date').reset_index(drop=True)
        
        results = []
        for test_date in test_dates:
            hist_data = get_data_up_to_date(data, test_date)
            
            is_trade_day = not hist_data[hist_data['Date'] == test_date].empty
            if not is_trade_day:
                continue

            if select_stock_logic(hist_data):
                ret = calculate_return(data, test_date, HOLD_DAYS)
                if ret is not None:
                    results.append({'code': stock_code, 'buy_date': test_date, 'return': ret})
        return results if results else None
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None
    except Exception as e:
        print(f"Error backtesting {file_path}: {e}")
        return None

def main_backtester():
    """主回测函数。"""
    start_time = time.time()
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(shanghai_tz)
    
    start_date_tz = datetime.strptime(BACKTEST_START_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    end_date_tz = datetime.strptime(BACKTEST_END_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    test_dates = []
    current_date = start_date_tz
    while current_date <= end_date_tz:
        test_dates.append(current_date.replace(tzinfo=None))
        current_date += timedelta(days=BACKTEST_STEP_DAYS)
    
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Stock data directory '{STOCK_DATA_DIR}' not found. Please create it and place CSV files inside.")
        return

    all_files = [os.path.join(STOCK_DATA_DIR, f) for f in os.listdir(STOCK_DATA_DIR) if f.endswith('.csv') and re.match(r'\d{6}\.csv$', f)]
    if not all_files:
        print(f"No stock data CSV files found in '{STOCK_DATA_DIR}'.")
        return

    print(f"Found {len(all_files)} files. Starting parallel backtesting with {MAX_WORKERS} workers...")
    print(f"Testing {len(test_dates)} dates from {BACKTEST_START_DATE} to {BACKTEST_END_DATE}.")

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(backtest_single_stock, file, test_dates): file for file in all_files}
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
            except Exception as exc:
                print(f'{file_path} generated an unexpected exception: {exc}')

    if not all_results:
        print("\nNo backtest signals found leading to trades.")
        return

    results_df = pd.DataFrame(all_results)
    
    total_trades = len(results_df)
    avg_return = results_df['return'].mean()
    win_rate = (results_df['return'] > 0).sum() / total_trades if total_trades > 0 else 0
    
    end_time = time.time()
    run_time = end_time - start_time
    
    output_dir = now.strftime('%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f"backtest_results_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    results_df.to_csv(output_path, index=False, encoding='utf-8')

    print("\n" + "="*50)
    print("📈 回测完成")
    print(f"回测期间: {BACKTEST_START_DATE} to {BACKTEST_END_DATE}")
    print(f"持有天数: {HOLD_DAYS} 天")
    print(f"总交易次数: {total_trades}")
    print(f"平均回报率: {avg_return:.2%}")
    print(f"胜率 (回报率 > 0): {win_rate:.2%}")
    print(f"总运行时间: {run_time:.2f} 秒")
    print(f"结果已保存至: {output_path}")
    print("="*50)

if __name__ == '__main__':
    main_backtester()
