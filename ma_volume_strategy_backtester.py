import os
import re
import pandas as pd
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys

# --- 常量定义：针对测试效率和精确度优化 ---
STOCK_DATA_DIR = 'stock_data'
MAX_STOCK_COUNT = 50     # 限制回测的股票文件数量
MAX_WORKERS = 4           # 保持 4 个线程，适应 GitHub CI/CD 环境
HOLD_DAYS = 30            # 持有天数
BACKTEST_START_DATE = '2020-01-01'
BACKTEST_END_DATE = '2025-12-13'    
BACKTEST_STEP_DAYS = 1    # 每日回测，确保回测精确性

# --- 筛选逻辑函数 (保持不变，已修复 Pandas 警告) ---
def calculate_indicators(data):
    """计算所需的均线（MA）和成交量指标。"""
    if len(data) < 30: return pd.DataFrame()
    df = data.copy()
    df.loc[:, 'Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df.loc[:, 'Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
    df.loc[:, 'MA5'] = df['Close'].rolling(window=5).mean()
    df.loc[:, 'MA20'] = df['Close'].rolling(window=20).mean()
    return df.dropna()

def check_c1_golden_cross(data):
    """检查5日均线金叉20日均线及入场点。"""
    if len(data) < 2: return False
    d0 = data.iloc[-1]
    d1 = data.iloc[-2]
    golden_cross = (d0['MA5'] > d0['MA20']) and (d1['MA5'] <= d1['MA20'])
    entry_point = d0['Close'] > d0['MA20']
    return golden_cross and entry_point

def check_c4_trend_control(data, max_drawdown=0.15, max_days=30):
    """检查趋势向上和回撤控制。"""
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
    """组合策略逻辑。"""
    data = calculate_indicators(data)
    if data.empty: return False
    data = data.sort_values(by='Date').reset_index(drop=True) 
    condition_final = check_c1_golden_cross(data) and check_c4_trend_control(data)
    return condition_final

# --- 回测及止损逻辑 (已包含 MA20 止损) ---
def get_data_up_to_date(data, target_date):
    """获取截止到目标日期的数据。"""
    data = data[data['Date'] <= target_date]
    return data

def calculate_return(data, buy_date, hold_days, stop_loss_ma=20):
    """计算回报率，并使用 MA20 作为动态止损线。"""
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
    
    # 获取买入日到目标卖出日期间的完整数据，用于计算 MA20
    full_data_for_ma = data[data['Date'] <= sell_date_target].sort_values(by='Date')
    
    if len(full_data_for_ma) < stop_loss_ma:
        return None 

    # 计算 MA20 止损线
    full_data_for_ma.loc[:, 'MA20_SL'] = full_data_for_ma['Close'].rolling(window=stop_loss_ma).mean()
    future_data_with_ma = full_data_for_ma[full_data_for_ma['Date'] > buy_date_actual].reset_index(drop=True)
    
    if future_data_with_ma.empty: 
        # 如果买入日后没有更多数据，但满足条件，则视为信号无效
        return None
    
    # 检查是否有止损点：收盘价低于 MA20
    stop_loss_trigger = future_data_with_ma[future_data_with_ma['Close'] < future_data_with_ma['MA20_SL']]
    
    if not stop_loss_trigger.empty:
        # 发生止损
        stop_loss_day = stop_loss_trigger.iloc[0]
        sell_price = stop_loss_day['Close']
        sell_date = stop_loss_day['Date']
        return (sell_price - buy_price) / buy_price, sell_date
    
    # 如果未触发止损，则在持有期结束时卖出
    sell_price = future_data_with_ma['Close'].iloc[-1]
    return (sell_price - buy_price) / buy_price, sell_date_target


def backtest_single_stock(file_path, test_dates):
    """回测单个股票。"""
    try:
        match = re.search(r'(\d{6})\.csv$', file_path)
        if not match: return None
        stock_code = match.group(1)
        
        column_names = ['Date', 'Code', 'Open', 'Close', 'High', 'Low', 'Volume', 'Amount', 'Amplitude', 'ChangePct', 'ChangeAmt', 'Turnover']
        
        # 尝试多种编码
        for encoding_type in ['utf-8', 'gb18030', 'gbk']:
            try:
                data = pd.read_csv(file_path, header=0, names=column_names, encoding=encoding_type)
                break 
            except UnicodeDecodeError:
                continue
        else:
            return None
        
        data.loc[:, 'Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d', errors='coerce').dt.tz_localize(None)
        data = data.dropna(subset=['Date'])
        
        data = data.sort_values(by='Date').reset_index(drop=True)
        
        results = []
        for test_date in test_dates:
            hist_data = get_data_up_to_date(data, test_date)
            
            is_trade_day = not hist_data[hist_data['Date'] == test_date].empty
            if not is_trade_day: continue

            if select_stock_logic(hist_data):
                ret_tuple = calculate_return(data, test_date, HOLD_DAYS)
                if ret_tuple is not None:
                    ret, sell_date = ret_tuple
                    results.append({'code': stock_code, 'buy_date': test_date, 'sell_date': sell_date, 'return': ret})
        return results if results else None
    except Exception as e:
        print(f'❌ 内部错误: {file_path} 回测失败: {e}')
        return None

def main_backtester():
    """主回测函数。(包含 I/O 优化和更早的日志输出)"""
    start_time = time.time()
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    
    # 强制在初始化后立即打印，便于发现问题
    print(f"--- 启动回测程序 (当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")
    
    # 1. 初始化和生成测试日期
    print("--- 步骤 1: 初始化和生成测试日期列表 ---")
    start_date_tz = datetime.strptime(BACKTEST_START_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    end_date_tz = datetime.strptime(BACKTEST_END_DATE, '%Y-%m-%d').replace(tzinfo=shanghai_tz)
    test_dates = []
    current_date = start_date_tz
    while current_date <= end_date_tz:
        test_dates.append(current_date.replace(tzinfo=None))
        current_date += timedelta(days=BACKTEST_STEP_DAYS)
    print(f"✅ 完成。步长 {BACKTEST_STEP_DAYS} 天 (每日回测)，共生成 {len(test_dates)} 个回测点。")
    
    # 2. 检查数据目录和文件 
    print("--- 步骤 2: 查找数据文件 ---")
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Stock data directory '{STOCK_DATA_DIR}' not found.")
        return

    # I/O 优化点：使用 os.scandir 
    all_files_full = []
    try:
        for entry in os.scandir(STOCK_DATA_DIR):
            if entry.name.endswith('.csv') and re.match(r'\d{6}\.csv$', entry.name):
                all_files_full.append(os.path.join(STOCK_DATA_DIR, entry.name))
    except Exception as e:
        print(f"Warning: os.scandir failed ({e}), falling back to os.listdir.")
        all_files_full = [os.path.join(STOCK_DATA_DIR, f) for f in os.listdir(STOCK_DATA_DIR) if f.endswith('.csv') and re.match(r'\d{6}\.csv$', f)]

    if not all_files_full:
        print(f"Error: No stock data CSV files found in '{STOCK_DATA_DIR}'.")
        return

    # 限制股票数量
    all_files = all_files_full[:MAX_STOCK_COUNT]
    
    print(f"✅ 完成。找到 {len(all_files_full)} 个股票文件。本次仅回测前 {len(all_files)} 个文件。")
    
    # 3. 执行并行回测
    print(f"--- 步骤 3: 启动并行回测 (股票数: {len(all_files)} / 线程数: {MAX_WORKERS}) ---")
    all_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(backtest_single_stock, file, test_dates): file for file in all_files}
        
        processed_count = 0
        total_files = len(all_files)
        
        for future in as_completed(future_to_file):
            processed_count += 1
            
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
            except Exception as exc:
                file_path = future_to_file[future]
                print(f'❌ 线程错误: {file_path} 产生异常: {exc} ({processed_count}/{total_files})')
            
            # 每处理 20 个文件打印一次进度
            if processed_count % 20 == 0:
                print(f"⏳ 进度: 已处理 {processed_count}/{total_files} 个文件...")
        
        if total_files % 20 != 0 and processed_count == total_files:
             print(f"⏳ 进度: 已处理 {processed_count}/{total_files} 个文件...")


    # 4. 汇总和输出结果
    print("\n--- 步骤 4: 汇总结果 ---")
    if not all_results:
        print("未发现任何符合策略的交易信号。")
        return

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
    output_filename = f"backtest_results_100_daily_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    results_df[['code', 'buy_date', 'sell_date', 'return']].to_csv(output_path, index=False, encoding='utf-8')

    print("\n" + "="*50)
    print("📈 回测完成")
    print(f"回测范围: **前 {MAX_STOCK_COUNT} 只股票**")
    print(f"回测类型: 每日精确回测 (步长 {BACKTEST_STEP_DAYS} 天)")
    print(f"总交易次数 (信号数量): {total_trades}")
    print(f"平均回报率: {avg_return:.2%}")
    print(f"胜率 (回报率 > 0): {win_rate:.2%}")
    print(f"总运行时间: {run_time:.2f} 秒")
    print(f"结果已保存至: {output_path}")
    print("="*50)

if __name__ == '__main__':
    # 强制刷新 stdout 缓冲区，解决 CI/CD 日志延迟问题
    sys.stdout.reconfigure(line_buffering=True)
    main_backtester()
