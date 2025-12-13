import os
import re
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

# --- 常量定义 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MAX_WORKERS = 8  # 并行处理的最大线程数

# --- 筛选逻辑函数 ---

def calculate_indicators(data):
    """计算所需的均线（MA）和成交量指标。"""
    # 【已修正】如果数据不足30条，返回空的DataFrame，让上层函数跳过
    if len(data) < 30: 
        return pd.DataFrame() 
        
    # 确保'Close'和'Volume'列是数值类型
    data['Close'] = pd.to_numeric(data['Close'], errors='coerce')
    data['Volume'] = pd.to_numeric(data['Volume'], errors='coerce')
    
    data['MA5'] = data['Close'].rolling(window=5).mean()
    data['MA20'] = data['Close'].rolling(window=20).mean()
    
    return data.dropna()

def check_c1_golden_cross(data):
    """检查5日均线金叉20日均线 (图1, 图6)。"""
    if len(data) < 2: return False
    
    d0 = data.iloc[-1]
    d1 = data.iloc[-2]
    
    # 当日 MA5 上穿 MA20
    golden_cross = (d0['MA5'] > d0['MA20']) and (d1['MA5'] <= d1['MA20'])
    
    # 且当日收盘价在MA20之上（确保形态有效）
    entry_point = d0['Close'] > d0['MA20']
    
    return golden_cross and entry_point

def check_c2_low_volume(data, volume_ratio=0.2):
    """检查地量缩量确认 (图2)。"""
    if len(data) < 60: return False
    
    # 查找前60天（不含当日）的成交量
    recent_volume = data['Volume'].iloc[-60:-1]
    
    if recent_volume.empty: return False
    
    max_volume = recent_volume.max()
    current_volume = data['Volume'].iloc[-1]
    
    if max_volume == 0: return False
    
    # 判断是否为地量（不超过顶部天量的20%）
    is_low_volume = current_volume <= (max_volume * volume_ratio)
    
    return is_low_volume

def check_c3_long_lower_shadow(data):
    """检查止跌信号 - 长下影线 (图3)。"""
    if data.empty: return False
    
    d0 = data.iloc[-1]
    close = d0['Close']
    low = d0['Low']
    open_p = d0['Open']
    high = d0['High']
    
    body_length = abs(close - open_p)
    lower_shadow_length = min(open_p, close) - low
    total_length = high - low
    
    if total_length == 0 or total_length < close * 0.005:
        return False
    
    shadow_ratio = lower_shadow_length / total_length
    
    # 经验值：下影线长度占K线总长度的比例大于 40%，且下影线比实体长
    is_long_shadow = (shadow_ratio > 0.4) and (lower_shadow_length > body_length)
    
    return is_long_shadow

def check_c4_trend_control(data, max_drawdown=0.15, max_days=30):
    """检查趋势与风险控制 (图4, 图5)。"""
    if len(data) < 30: return False
    
    # 1. 20日均线必须持续上行 (图5)
    ma20_slope = data['MA20'].iloc[-1] - data['MA20'].iloc[-5] 
    is_ma20_up = ma20_slope > 0 
    
    # 2. 回调幅度不超过 15%，时间不超过 30 天 (图4)
    recent_high = data['Close'].iloc[-max_days:].max()
    current_price = data['Close'].iloc[-1]
    
    if recent_high == 0: return False

    drawdown = (recent_high - current_price) / recent_high
    
    is_drawdown_controlled = drawdown <= max_drawdown
    
    return is_ma20_up and is_drawdown_controlled

def select_stock_logic(data):
    """
    综合所有条件，执行选股逻辑。
    策略组合：C1（金叉启动） + C4（趋势控制）
    """
    
    data = calculate_indicators(data)
    
    # 【关键修正】：如果 data 是空，说明数据不足以计算指标，直接返回 False
    if data.empty: return False
    
    condition_final = check_c1_golden_cross(data) and check_c4_trend_control(data)
                       
    return condition_final

# --- 并行处理主函数 ---

def screen_single_stock(file_path):
    """处理单个股票数据文件，如果满足条件则返回股票代码。"""
    try:
        # 从文件名中提取股票代码
        match = re.search(r'(\d{6})\.csv$', file_path)
        if not match:
            print(f"Skipping file: {file_path}. Cannot extract stock code.")
            return None
            
        stock_code = match.group(1)
        
        # 读取CSV数据
        data = pd.read_csv(
            file_path, 
            header=None, 
            names=['Date', 'Code', 'Open', 'Close', 'High', 'Low', 'Volume', 'Amount', 'Amplitude', 'ChangePct', 'ChangeAmt', 'Turnover'],
            parse_dates=['Date'],
            date_format='%Y-%m-%d'
        )
        
        # 按照日期排序，确保最新的数据在最后
        data = data.sort_values(by='Date').reset_index(drop=True)
        
        # 运行选股逻辑
        if select_stock_logic(data):
            return stock_code
            
    except Exception as e:
        # 错误捕获会精确到文件名，但由于上面修正了 'MA5' 错误，这里应该不会再报错。
        # 如果出现其他错误，可以定位问题。
        print(f"Error processing {file_path}: {e}")
        
    return None

def main_screener():
    """主函数：并行扫描所有股票并输出结果。"""
    
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Stock data directory '{STOCK_DATA_DIR}' not found.")
        return

    # 1. 扫描所有股票数据文件
    all_files = [os.path.join(STOCK_DATA_DIR, f) 
                 for f in os.listdir(STOCK_DATA_DIR) 
                 if f.endswith('.csv') and re.match(r'\d{6}\.csv$', f)]
                 
    if not all_files:
        print("No stock data CSV files found in 'stock_data' directory.")
        return

    print(f"Found {len(all_files)} files. Starting parallel screening...")

    matched_codes = []
    
    # 2. 使用 ThreadPoolExecutor 进行并行处理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(screen_single_stock, file): file for file in all_files}
        
        for future in as_completed(future_to_file):
            code = future.result()
            if code:
                matched_codes.append(code)

    print(f"\nScreening completed. Found {len(matched_codes)} matched stocks.")

    # 3. 加载股票名称
    try:
        stock_names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
        
        if 'code' not in stock_names_df.columns:
            print(f"Error: '{STOCK_NAMES_FILE}' must contain a column named 'code'.")
            return
            
        name_map = stock_names_df.set_index('code')['name'].to_dict()
        
    except FileNotFoundError:
        print(f"Error: Stock names file '{STOCK_NAMES_FILE}' not found. Cannot output names.")
        name_map = {}
    
    # 4. 准备最终输出
    results = []
    for code in matched_codes:
        name = name_map.get(code, 'N/A')
        results.append({'code': code, 'name': name})

    results_df = pd.DataFrame(results)

    # 5. 生成带时间戳的文件名和目录结构 (上海时区)
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(shanghai_tz)
    
    output_dir = now.strftime('%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f"screener_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # 6. 保存结果
    results_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Results saved to: {output_path}")

if __name__ == '__main__':
    main_screener()
