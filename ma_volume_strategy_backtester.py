import os
import re
import pandas as pd
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# --- 常量定义 ---
STOCK_DATA_DIR = 'stock_data'
MAX_WORKERS = 8       
HOLD_DAYS = 30        
BACKTEST_START_DATE = '2020-01-01'
BACKTEST_END_DATE = '2025-12-13'    
BACKTEST_STEP_DAYS = 30             

# --- 筛选逻辑函数 (保持优化状态) ---
def calculate_indicators(data):
    if len(data) < 30:
        return pd.DataFrame()
    df = data.copy()
    df.loc[:, 'Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df.loc[:, 'Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
    df.loc[:, 'MA5'] = df['Close'].rolling(window=5).mean()
    df.loc[:, 'MA20'] = df['Close'].rolling(window=20).mean()
    return df.dropna()

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

# --- 回测辅助函数 (保持优化状态) ---
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
        
        # 尝试多种编码
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
        
        # 精确指定日期格式
        data.loc[:, 'Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d', errors='coerce').dt.tz_localize(None)
        data = data.dropna(subset=['Date'])
        
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
    """主回测函数 (增加日志输出)。"""
    start_time = time.time()
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    
    # 1. 初始化和生成测试日期
    print("--- 步骤 1: 初始化和生成测试日期列表 ---")
    start_date_tz = datetime.strptime(BACKTEST_START_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    end_date_tz = datetime.strptime(BACKTEST_END_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    test_dates = []
    current_date = start_date_tz
    while current_date <= end_date_tz:
        test_dates.append(current_date.replace(tzinfo=None))
        current_date += timedelta(days=BACKTEST_STEP_DAYS)
    print(f"✅ 完成。共生成 {len(test_dates)} 个回测点。")
    
    # 2. 检查数据目录和文件
    print("--- 步骤 2: 查找数据文件 ---")
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Stock data directory '{STOCK_DATA_DIR}' not found. Please create it and place CSV files inside.")
        return

    all_files = [os.path.join(STOCK_DATA_DIR, f) for f in os.listdir(STOCK_DATA_DIR) if f.endswith('.csv') and re.match(r'\d{6}\.csv$', f)]
    if not all_files:
        print(f"Error: No stock data CSV files found in '{STOCK_DATA_DIR}'.")
        return

    print(f"✅ 完成。找到 {len(all_files)} 个股票文件。")
    
    # 3. 执行并行回测
    print(f"--- 步骤 3: 启动并行回测 (使用 {MAX_WORKERS} 个线程) ---")
    print("🚀 预计耗时较长，请等待第一个结果或异常输出...")
    all_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(backtest_single_stock, file, test_dates): file for file in all_files}
        
        # 使用进度计数器
        processed_count = 0
        total_files = len(all_files)
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            processed_count += 1
            
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
                    print(f"🎉 成功回测 {file_path} 并发现 {len(results)} 个信号。({processed_count}/{total_files})")
                # else:
                #     print(f"✅ 完成回测 {file_path}，未发现信号。({processed_count}/{total_files})")
            except Exception as exc:
                print(f'❌ 错误: {file_path} 产生异常: {exc} ({processed_count}/{total_files})')

    # 4. 汇总和输出结果
    print("\n--- 步骤 4: 汇总结果 ---")
    if not all_results:
        print("未发现任何符合策略的交易信号。")
        return

    # ... (结果处理和打印，保持不变)
    results_df = pd.DataFrame(all_results)
    
    total_trades = len(results_df)
    avg_return = results_df['return'].mean()
    win_rate = (results_df['return'] > 0).sum() / total_trades if total_trades > 0 else 0
    
    end_time = time.time()
    run_time = end_time - start_time
    
    now = datetime.now(shanghai_tz)
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
