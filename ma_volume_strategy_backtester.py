import os
import re
import pandas as pd
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 常量定义 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MAX_WORKERS = 8  # 并行处理的最大线程数
HOLD_DAYS = 30   # 持有天数（回测中假设买入后持有N天）
BACKTEST_START_DATE = '2020-01-01'  # 回测起始日期（可调整）
BACKTEST_END_DATE = '2025-12-13'    # 回测结束日期（当前日期）
BACKTEST_STEP_DAYS = 30             # 每隔N天运行一次筛选（模拟月度回测）

# 从原脚本中复制筛选逻辑函数（为了独立性，避免import原脚本）
def calculate_indicators(data):
    if len(data) < 30:
        return pd.DataFrame()
    data['Close'] = pd.to_numeric(data['Close'], errors='coerce')
    data['Volume'] = pd.to_numeric(data['Volume'], errors='coerce')
    data['MA5'] = data['Close'].rolling(window=5).mean()
    data['MA20'] = data['Close'].rolling(window=20).mean()
    return data.dropna()

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
    condition_final = check_c1_golden_cross(data) and check_c4_trend_control(data)
    return condition_final

# --- 回测函数 ---

def get_data_up_to_date(data, target_date):
    """截取数据到指定日期（模拟历史回测）。"""
    data = data[data['Date'] <= target_date]
    return data

def calculate_return(data, buy_date, hold_days):
    """计算持有期回报。"""
    buy_idx = data[data['Date'] == buy_date].index[0]
    sell_date = buy_date + timedelta(days=hold_days)
    future_data = data[(data['Date'] > buy_date) & (data['Date'] <= sell_date)]
    if len(future_data) < hold_days // 2:  # 数据不足，跳过
        return None
    buy_price = data.at[buy_idx, 'Close']
    sell_price = future_data['Close'].iloc[-1] if not future_data.empty else buy_price
    return (sell_price - buy_price) / buy_price

def backtest_single_stock(file_path, test_dates):
    """回测单个股票。"""
    try:
        match = re.search(r'(\d{6})\.csv$', file_path)
        if not match:
            return None
        stock_code = match.group(1)
        data = pd.read_csv(
            file_path,
            header=None,
            names=['Date', 'Code', 'Open', 'Close', 'High', 'Low', 'Volume', 'Amount', 'Amplitude', 'ChangePct', 'ChangeAmt', 'Turnover'],
            parse_dates=['Date'],
            date_format='%Y-%m-%d'
        )
        data = data.sort_values(by='Date').reset_index(drop=True)
        
        results = []
        for test_date in test_dates:
            hist_data = get_data_up_to_date(data, test_date)
            if select_stock_logic(hist_data):
                ret = calculate_return(data, test_date, HOLD_DAYS)
                if ret is not None:
                    results.append({'code': stock_code, 'buy_date': test_date, 'return': ret})
        return results if results else None
    except Exception as e:
        print(f"Error backtesting {file_path}: {e}")
        return None

def main_backtester():
    """主回测函数。"""
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(shanghai_tz)
    
    # 生成测试日期列表（从起始到结束，每隔STEP天）
    start_date = datetime.strptime(BACKTEST_START_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    end_date = datetime.strptime(BACKTEST_END_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    test_dates = []
    current_date = start_date
    while current_date <= end_date:
        test_dates.append(current_date)
        current_date += timedelta(days=BACKTEST_STEP_DAYS)
    
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Stock data directory '{STOCK_DATA_DIR}' not found.")
        return

    all_files = [os.path.join(STOCK_DATA_DIR, f) for f in os.listdir(STOCK_DATA_DIR) if f.endswith('.csv') and re.match(r'\d{6}\.csv$', f)]
    if not all_files:
        print("No stock data CSV files found.")
        return

    print(f"Found {len(all_files)} files. Starting parallel backtesting...")

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(backtest_single_stock, file, test_dates): file for file in all_files}
        for future in as_completed(future_to_file):
            results = future.result()
            if results:
                all_results.extend(results)

    if not all_results:
        print("No backtest results found.")
        return

    results_df = pd.DataFrame(all_results)
    
    # 计算汇总指标
    avg_return = results_df['return'].mean()
    win_rate = (results_df['return'] > 0).mean()
    print(f"Average Return: {avg_return:.2%}")
    print(f"Win Rate: {win_rate:.2%}")

    # 保存结果
    output_dir = now.strftime('%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f"backtest_results_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    results_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Backtest results saved to: {output_path}")

if __name__ == '__main__':
    main_backtester()
