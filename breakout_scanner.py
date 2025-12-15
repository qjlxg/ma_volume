import pandas as pd
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE_DIR = 'scanned_results'

# 技术指标参数
N_DAYS_LOW = 60  # 用于判断近期低点的时间范围
MA_SHORT = 5     # 短期均线
MA_LONG = 20     # 长期均线
VOLUME_AVG_DAYS = 5 # 成交量平均天数
MIN_CLOSE_PRICE = 5.0 # 最低收盘价限制

# 列名映射字典 (将您的中文列名映射到脚本使用的英文列名)
COLUMN_MAP = {
    '日期': 'Date',
    '开盘': 'Open',
    '收盘': 'Close',
    '最高': 'High',
    '最低': 'Low',
    '成交量': 'Volume'
    # 脚本只使用了这五个关键数据列
}
# --- 结束配置 ---

def load_stock_names(file_path):
    """加载股票代码和名称的映射表，修正为小写列名 'code' 和 'name'"""
    try:
        # 尝试读取文件，使用 utf-8 编码
        names_df = pd.read_csv(file_path, encoding='utf-8')
        
        # 核心修复点：将小写的 'code' 和 'name' 映射为脚本内部使用的大写 'Code' 和 'Name'
        # 同时也处理了可能的编码问题导致的空格
        names_df.columns = names_df.columns.str.lower().str.strip()
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'}) 
        
        if 'Code' not in names_df.columns or 'Name' not in names_df.columns:
             # 如果仍然找不到，抛出错误提示用户检查文件内容
             raise ValueError("The 'stock_names.csv' must contain columns named 'code' and 'name'.")

        names_df['Code'] = names_df['Code'].astype(str).str.zfill(6) # 确保代码是6位字符串
        # 打印加载成功信息（用于调试）
        print(f"Successfully loaded {len(names_df)} stock names.")
        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        # 这里会捕获您运行时的 'Error loading stock names: 'Code'' 错误
        print(f"Error loading stock names: {e}")
        return {}

def check_breakout_pattern(file_path):
    """
    检查单个股票CSV文件是否符合技术突破形态
    返回 (股票代码, True/False, 最新价)
    """
    stock_code = os.path.basename(file_path).replace('.csv', '')
    try:
        # 尝试读取文件
        df = pd.read_csv(file_path)
        
        # 核心修复点：将中文列名映射为英文
        df = df.rename(columns=COLUMN_MAP)
        
        # 确保关键列存在且数据有效
        required_cols = ['Date', 'Close', 'High', 'Low', 'Volume']
        if not all(col in df.columns and df[col].notna().any() for col in required_cols):
             # 仅在调试时打印，避免日志过于冗长
             # print(f"[{stock_code}] Data missing required columns or is empty after mapping.")
             return (stock_code, False, None)

        # 数据清洗和准备
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce') # errors='coerce' 避免日期格式错误中断
        df = df.dropna(subset=['Date', 'Close', 'Volume']) # 丢弃关键数据缺失的行
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        # 确保有足够数据进行计算
        min_rows = max(N_DAYS_LOW, MA_LONG, VOLUME_AVG_DAYS) + 1
        if len(df) < min_rows:
            return (stock_code, False, None) 

        # 1. 计算均线 (MA5和MA20)
        df['MA_Short'] = df['Close'].rolling(window=MA_SHORT).mean()
        df['MA_Long'] = df['Close'].rolling(window=MA_LONG).mean()
        
        # 2. 计算成交量均值
        df['Volume_Avg'] = df['Volume'].rolling(window=VOLUME_AVG_DAYS).mean()
        
        # 3. 计算近期最低价 (用于确认底部/回调)
        df['N_Day_Low'] = df['Low'].rolling(window=N_DAYS_LOW).min()
        
        # 获取最新数据
        latest = df.iloc[-1]
        
        # 4. 最低价限制：最新收盘价不能低于 5.0 元
        if latest['Close'] < MIN_CLOSE_PRICE:
            return (stock_code, False, latest['Close'])

        # 5. 形态筛选逻辑 (模仿图中的“底部突破上车”形态)
        # 
        # a) 价格突破：最新收盘价高于所有均线 (MA5和MA20)，确认趋势强劲
        price_breakout = (latest['Close'] > latest['MA_Short']) and (latest['Close'] > latest['MA_Long'])
        
        # b) 量能确认：最新成交量高于过去5天平均成交量的1.2倍，确认突破的有效性
        volume_confirm = latest['Volume'] > 1.2 * latest['Volume_Avg'] 

        # c) 从底部反弹：最新价高于N日内的最低价，确保不是在高位震荡
        bounced_from_low = latest['Close'] > latest['N_Day_Low'] 
        
        # 筛选逻辑组合
        is_breakout = price_breakout and volume_confirm and bounced_from_low

        return (stock_code, is_breakout, latest['Close'])

    except Exception as e:
        # 捕获其他处理错误，确保并行任务不中断
        # print(f"Error processing {file_path}: {e}") 
        return (stock_code, False, None)

def main():
    # 1. 加载股票名称
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    if not stock_names:
        print("Warning: stock_names.csv failed to load completely. Results will only contain stock codes.")

    # 2. 收集所有数据文件
    data_files = [os.path.join(STOCK_DATA_DIR, f) 
                  for f in os.listdir(STOCK_DATA_DIR) 
                  if f.endswith('.csv')]
    
    if not data_files:
        print(f"Error: No CSV files found in {STOCK_DATA_DIR}. Exiting.")
        return

    print(f"Found {len(data_files)} stock data files. Starting parallel scan...")
    print("--- Note: The core filtering logic attempts to match the 'bottom breakout' pattern. ---")
    
    # 3. 并行处理文件
    # 使用所有可用的CPU核心，加速运行
    try:
        with Pool(cpu_count()) as pool:
            results = pool.map(check_breakout_pattern, data_files)
    except Exception as e:
        print(f"Critical Error during parallel processing: {e}")
        return

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
        print("\n--- Filtering complete ---")
        print("No stocks matched the breakout criteria.")
        output_df = pd.DataFrame(columns=['Code', 'Name', 'Latest Close'])
    else:
        output_df = pd.DataFrame(successful_scans)
        output_df = output_df.sort_values(by='Latest Close', ascending=False)
        print(f"\n--- Filtering complete ---")
        print(f"Successfully found {len(successful_scans)} stocks matching the criteria.")

    # 6. 保存结果到指定目录 (年月/文件名_时间戳.csv)
    now = datetime.now() 
    year_month_dir = now.strftime('%Y-%m')
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f'breakout_stocks_{timestamp_str}.csv'
    
    output_dir = os.path.join(OUTPUT_BASE_DIR, year_month_dir)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_filename)
    
    output_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Results saved to: {output_path}")

if __name__ == '__main__':
    main()
