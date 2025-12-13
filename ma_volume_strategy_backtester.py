import os
import re
import pandas as pd

# 设置 Pandas 选项，消除 FutureWarning
pd.set_option('future.no_silent_downcasting', True)

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import numpy as np

# --- 常量定义 ---
STOCK_DATA_DIR = 'stock_data'
# 【关键修正 1】：将最大并行工作线程数降低到安全范围，防止 I/O 阻塞
MAX_WORKERS = 4 

# --- 策略函数：指标计算与信号生成 (保持不变) ---

def calculate_indicators(data):
    """计算所需的均线（MA）和成交量指标。"""
    data = data.copy() 
    if len(data) < 30: 
        return pd.DataFrame() 
    data['Close'] = pd.to_numeric(data['Close'], errors='coerce')
    data['Volume'] = pd.to_numeric(data['Volume'], errors='coerce')
    data['MA5'] = data['Close'].rolling(window=5).mean()
    data['MA20'] = data['Close'].rolling(window=20).mean()
    return data.dropna()

def check_c1_golden_cross(data):
    """检查5日均线金叉20日均线。"""
    if len(data) < 2: return False
    d0 = data.iloc[-1]; d1 = data.iloc[-2]
    golden_cross = (d0['MA5'] > d0['MA20']) and (d1['MA5'] <= d1['MA20'])
    entry_point = d0['Close'] > d0['MA20']
    return golden_cross and entry_point

def check_c4_trend_control(data, max_drawdown=0.15, max_days=30):
    """检查趋势与风险控制。"""
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
    """综合所有条件，执行选股逻辑。"""
    data = calculate_indicators(data)
    if data.empty: return False
    condition_final = check_c1_golden_cross(data) and check_c4_trend_control(data)
    return condition_final

# --- 辅助函数：回测指标计算 (保持不变) ---

def calculate_returns(equity_curve):
    """计算年化收益率、最大回撤和夏普比率。"""
    if equity_curve.empty or equity_curve.iloc[-1] == 1.0:
        return 0.0, 0.0, 0.0

    returns = equity_curve.pct_change().dropna()
    annual_return = (1 + returns.mean()) ** 252 - 1

    cumulative_max = equity_curve.cummax()
    drawdown = (cumulative_max - equity_curve) / cumulative_max
    max_drawdown = drawdown.max()
    
    annual_volatility = returns.std() * np.sqrt(252)
    sharpe_ratio = annual_return / annual_volatility if annual_volatility != 0 else 0.0

    return annual_return, max_drawdown, sharpe_ratio

# --- 回测主流程函数 (修正后) ---

def process_file_for_backtest(file_path):
    """读取单个股票文件，并为回测处理数据，生成每日信号。"""
    try:
        match = re.search(r'(\d{6})\.csv$', file_path)
        if not match: return None

        stock_code = match.group(1)
        
        data = pd.read_csv(
            file_path, 
            header=None, 
            names=['Date', 'Code', 'Open', 'Close', 'High', 'Low', 'Volume', 'Amount', 'Amplitude', 'ChangePct', 'ChangeAmt', 'Turnover'],
            parse_dates=['Date'],
            date_format='%Y-%m-%d'
        )
        
        data = data.sort_values(by='Date').reset_index(drop=True).copy()
        
        data = calculate_indicators(data)
        if data.empty: return None

        data['Signal'] = data.apply(
            lambda row: select_stock_logic(data.loc[:row.name]), axis=1
        )
        data['Signal'] = data['Signal'].shift(1).fillna(False).astype(bool) 
        
        return stock_code, data[['Date', 'Close', 'Signal']]

    except Exception as e:
        print(f"Error processing {file_path} for backtest: {e}") 
        return None

def run_backtest(start_date, end_date):
    """主回测函数：模拟投资组合表现。"""
    
    # 1. 数据准备
    # 【关键修正 2】：在扫描文件列表前先打印，确认脚本是否卡在更早的阶段
    print(f"Checking directory: {STOCK_DATA_DIR}")
    
    try:
        all_files = [os.path.join(STOCK_DATA_DIR, f) 
                    for f in os.listdir(STOCK_DATA_DIR) 
                    if f.endswith('.csv') and re.match(r'\d{6}\.csv$', f)]
    except FileNotFoundError:
        print(f"Error: Stock data directory '{STOCK_DATA_DIR}' not found. Please ensure data is present.")
        return
        
    if not all_files:
        print("No stock data CSV files found for backtest.")
        return

    total_files = len(all_files)
    print(f"Found {total_files} files. Starting parallel data processing with {MAX_WORKERS} workers...")

    all_data = []
    processed_count = 0
    
    # 使用 ThreadPoolExecutor 并行处理股票文件
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(process_file_for_backtest, file): file for file in all_files}
        
        for future in as_completed(future_to_file):
            result = future.result()
            processed_count += 1
            
            if result:
                all_data.append(result)
            
            # 打印进度日志
            if processed_count % 100 == 0 or processed_count == total_files:
                print(f"Processing Progress: {processed_count}/{total_files} files processed ({processed_count/total_files:.1%})")


    if not all_data:
        print("No valid stock data processed.")
        return

    # 2. 准备日期范围和净值曲线
    print(f"\n--- Data Preparation Complete. Valid stocks: {len(all_data)} ---")

    all_dates = sorted(pd.concat([data[1]['Date'] for data in all_data]).unique())
    dates_df = pd.DataFrame({'Date': all_dates})
    dates_df['Date'] = pd.to_datetime(dates_df['Date'])
    
    dates_df = dates_df[
        (dates_df['Date'] >= pd.to_datetime(start_date)) & 
        (dates_df['Date'] <= pd.to_datetime(end_date))
    ].reset_index(drop=True)

    if dates_df.empty:
        print("No trading days found in the specified range.")
        return

    daily_returns = pd.Series(0.0, index=dates_df['Date'])
    stock_data_map = {code: df.set_index('Date') for code, df in all_data}
    
    print(f"\nStarting Backtest simulation from {start_date} to {end_date}...")

    # 3. 模拟交易 (串行执行)
    for i in range(1, len(dates_df)):
        current_date = dates_df.iloc[i]['Date']
        prev_date = dates_df.iloc[i-1]['Date']
        
        total_daily_return = 0.0
        signal_count = 0
        
        for code, df in stock_data_map.items():
            
            if prev_date in df.index and current_date in df.index:
                
                if df.loc[prev_date, 'Signal']:
                    
                    try:
                        return_pct = df.loc[current_date, 'Close'] / df.loc[prev_date, 'Close'] - 1
                        total_daily_return += return_pct
                        signal_count += 1
                    except:
                         continue
        
        if signal_count > 0:
            daily_returns[current_date] = total_daily_return / signal_count

    # 4. 计算净值曲线和指标
    equity_curve = (1 + daily_returns).cumprod().fillna(1.0)
    annual_return, max_drawdown, sharpe_ratio = calculate_returns(equity_curve)

    # 5. 输出结果
    print("\n" + "="*50)
    print("📈 **Backtest Results (MA/Volume Strategy)** 📊")
    print(f"  Start Date (开始日期): {start_date}")
    print(f"  End Date (结束日期):   {end_date}")
    print("="*50)
    print(f"  Annualized Return (年化收益): {annual_return:.2%}")
    print(f"  Max Drawdown (最大回撤):       {max_drawdown:.2%}")
    print(f"  Sharpe Ratio (夏普比率):      {sharpe_ratio:.2f}")
    print("="*50)
    
    # 6. 保存净值曲线
    output_df = pd.DataFrame({
        'Date': equity_curve.index, 
        'Equity': equity_curve.values
    })
    
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(shanghai_tz)
    output_filename = f"backtest_equity_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = os.path.join('backtest_results', output_filename)
    os.makedirs('backtest_results', exist_ok=True)
    output_df.to_csv(output_path, index=False)
    print(f"Equity curve saved to: {output_path}")

if __name__ == '__main__':
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    end_date = datetime.now(shanghai_tz).strftime('%Y-%m-%d')
    start_date = '2020-01-01'
    
    run_backtest(start_date, end_date)
