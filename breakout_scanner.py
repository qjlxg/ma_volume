import pandas as pd
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE_DIR = 'scanned_results'

# 技术指标参数
N_DAYS_LOW = 60  # 用于判断近期低点的时间范围 (60天内低点)
MA_SHORT = 5     # 短期均线
MA_LONG = 20     # 长期均线
VOLUME_AVG_DAYS = 5 # 成交量平均天数

# 最低收盘价限制
MIN_CLOSE_PRICE = 5.0
# --- 结束配置 ---

def load_stock_names(file_path):
    """加载股票代码和名称的映射表"""
    try:
        # 假设 stock_names.csv 格式为 Code, Name
        names_df = pd.read_csv(file_path, dtype={'Code': str}, encoding='utf-8')
        names_df['Code'] = names_df['Code'].astype(str).str.zfill(6) # 确保代码是6位字符串
        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def check_breakout_pattern(file_path):
    """
    检查单个股票CSV文件是否符合技术突破形态
    返回 (股票代码, True/False, 最新价)
    """
    try:
        # 从文件名解析股票代码
        filename = os.path.basename(file_path)
        stock_code = filename.replace('.csv', '')
        
        # 读取数据
        df = pd.read_csv(file_path, parse_dates=['Date'])
        # 确保数据按日期排序
        df = df.sort_values(by='Date').reset_index(drop=True)

        if len(df) < max(N_DAYS_LOW, MA_LONG, VOLUME_AVG_DAYS) + 1:
            return (stock_code, False, None) # 数据不足

        # 1. 计算均线
        df['MA_Short'] = df['Close'].rolling(window=MA_SHORT).mean()
        df['MA_Long'] = df['Close'].rolling(window=MA_LONG).mean()
        
        # 2. 计算成交量均值
        df['Volume_Avg'] = df['Volume'].rolling(window=VOLUME_AVG_DAYS).mean()
        
        # 3. 计算近期最低价
        # 注意: 这里的近期低点是用于确认底部/回调的，我们看的是当前价格是否从低点反弹并突破。
        # 简化处理：确保最新收盘价高于N日内的最低价，且高于均线。
        df['N_Day_Low'] = df['Low'].rolling(window=N_DAYS_LOW).min()
        
        # 获取最新数据
        latest = df.iloc[-1]
        
        # 4. 最低价限制
        if latest['Close'] < MIN_CLOSE_PRICE:
            return (stock_code, False, latest['Close'])

        # 5. 形态筛选逻辑
        
        # a) 最新收盘价高于所有均线 (突破均线)
        price_breakout = (latest['Close'] > latest['MA_Short']) and (latest['Close'] > latest['MA_Long'])
        
        # b) 最新成交量放大 (突破确认)
        volume_confirm = latest['Volume'] > 1.2 * latest['Volume_Avg'] # 放大1.2倍作为确认

        # c) 最新价高于近期低点 (从底部反弹)
        # 这个条件通常在数据中是自然成立的，但加上它来确认是从回调中走出的。
        bounced_from_low = latest['Close'] > latest['N_Day_Low'] 
        
        
        is_breakout = price_breakout and volume_confirm and bounced_from_low

        return (stock_code, is_breakout, latest['Close'])

    except Exception as e:
        # print(f"Error processing {file_path}: {e}") # 调试时开启
        return (stock_code, False, None)

def main():
    # 1. 加载股票名称
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    if not stock_names:
        print("Warning: stock_names.csv is empty or failed to load. Only stock codes will be output.")

    # 2. 收集所有数据文件
    data_files = [os.path.join(STOCK_DATA_DIR, f) 
                  for f in os.listdir(STOCK_DATA_DIR) 
                  if f.endswith('.csv')]
    
    if not data_files:
        print(f"Error: No CSV files found in {STOCK_DATA_DIR}. Exiting.")
        return

    print(f"Found {len(data_files)} stock data files. Starting parallel scan...")

    # 3. 并行处理文件
    # 使用所有可用的CPU核心
    with Pool(cpu_count()) as pool:
        results = pool.map(check_breakout_pattern, data_files)

    # 4. 收集符合条件的股票
    successful_scans = []
    for code, is_breakout, close_price in results:
        if is_breakout:
            name = stock_names.get(code, 'N/A')
            successful_scans.append({
                'Code': code,
                'Name': name,
                'Latest Close': f'{close_price:.2f}' if close_price else 'N/A'
            })

    # 5. 输出结果
    if not successful_scans:
        print("No stocks matched the breakout criteria.")
        # 仍然创建一个空文件以保持流程一致
        output_df = pd.DataFrame(columns=['Code', 'Name', 'Latest Close'])
    else:
        output_df = pd.DataFrame(successful_scans)
        output_df = output_df.sort_values(by='Latest Close', ascending=False)
        print(f"Successfully found {len(successful_scans)} stocks matching the criteria.")

    # 6. 保存结果到指定目录 (年月/文件名_时间戳.csv)
    
    # 结果推送到仓库中的年月目录
    now = datetime.now() 
    # 使用上海时区的时间戳，但在GitHub Actions中，我们统一使用UTC，然后在脚本中通过环境变量调整
    # 由于Actions的Runner时间是UTC，我们在这里统一使用UTC时间，避免在Python层面处理时区复杂性。
    # 文件名和目录将使用脚本运行时的系统时间。
    
    year_month_dir = now.strftime('%Y-%m')
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f'breakout_stocks_{timestamp_str}.csv'
    
    output_dir = os.path.join(OUTPUT_BASE_DIR, year_month_dir)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_filename)
    
    output_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Results saved to: {output_path}")

if __name__ == '__main__':
    # 模拟创建stock_data目录和stock_names.csv，如果它们不存在
    # 实际运行中，您需要确保这些文件已经上传到您的仓库
    os.makedirs(STOCK_DATA_DIR, exist_ok=True)
    if not os.path.exists(STOCK_NAMES_FILE):
        print(f"Creating a placeholder {STOCK_NAMES_FILE}...")
        pd.DataFrame({'Code': ['603456', '603458', '603693'], 'Name': ['Placeholder A', 'Placeholder B', 'Placeholder C']}).to_csv(STOCK_NAMES_FILE, index=False)

    main()
