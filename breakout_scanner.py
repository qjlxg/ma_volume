import pandas as pd
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count
import re

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE_DIR = 'scanned_results'

# 技术指标参数
N_DAYS_LOW = 60  # 用于判断近期低点的时间范围
MA_SHORT = 5     # 短期均线
MA_LONG = 20     # 长期均线
VOLUME_AVG_DAYS = 5 # 成交量平均天数

# 价格限制
MIN_CLOSE_PRICE = 5.0 
MAX_CLOSE_PRICE = 15.0 # 新增：最新收盘价不高于 15.0 元

# 列名映射字典 (将您的中文列名映射到脚本使用的英文列名)
COLUMN_MAP = {
    '日期': 'Date',
    '开盘': 'Open',
    '收盘': 'Close',
    '最高': 'High',
    '最低': 'Low',
    '成交量': 'Volume'
}

# 排除规则
# 30开头 (创业板) 排除
EXCLUDE_CODE_PREFIX = ['30'] 
# 深沪A股的典型代码范围 (只允许 60xxxx, 00xxxx)
# 但这里我们只做排除，如果您的数据源是干净的A股，可以只排除创业板和ST。
# 排除所有非A股代码，通常不需要，因为数据文件已经在 stock_data 目录中。
# 我们将依赖名称排除ST和代码排除30开头的。
# --- 结束配置 ---

def load_stock_names(file_path):
    """加载股票代码和名称的映射表，修正为小写列名 'code' 和 'name'"""
    try:
        names_df = pd.read_csv(file_path, encoding='utf-8')
        
        # 核心修复点：将小写的 'code' 和 'name' 映射为脚本内部使用的大写 'Code' 和 'Name'
        names_df.columns = names_df.columns.str.lower().str.strip()
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'}) 
        
        if 'Code' not in names_df.columns or 'Name' not in names_df.columns:
             raise ValueError("The 'stock_names.csv' must contain columns named 'code' and 'name'.")

        names_df['Code'] = names_df['Code'].astype(str).str.zfill(6) 
        print(f"Successfully loaded {len(names_df)} stock names.")
        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def check_breakout_pattern(file_path):
    """
    检查单个股票CSV文件是否符合技术突破形态和新增的排除规则
    返回 (股票代码, True/False, 最新价)
    """
    stock_code = os.path.basename(file_path).replace('.csv', '')
    
    # 1. 快速代码排除 (30开头)
    if any(stock_code.startswith(prefix) for prefix in EXCLUDE_CODE_PREFIX):
        return (stock_code, False, None)
        
    try:
        df = pd.read_csv(file_path)
        
        # 核心修复点：将中文列名映射为英文
        df = df.rename(columns=COLUMN_MAP)
        
        required_cols = ['Date', 'Close', 'High', 'Low', 'Volume']
        if not all(col in df.columns and df[col].notna().any() for col in required_cols):
             return (stock_code, False, None)

        # 数据清洗和准备
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce') 
        df = df.dropna(subset=['Date', 'Close', 'Volume']) 
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        min_rows = max(N_DAYS_LOW, MA_LONG, VOLUME_AVG_DAYS) + 1
        if len(df) < min_rows:
            return (stock_code, False, None) 

        # 获取最新数据
        latest = df.iloc[-1]
        
        # 2. 价格区间限制：[5.0, 20.0]
        if not (MIN_CLOSE_PRICE <= latest['Close'] <= MAX_CLOSE_PRICE):
            return (stock_code, False, latest['Close'])

        # 3. 计算技术指标...
        df['MA_Short'] = df['Close'].rolling(window=MA_SHORT).mean()
        df['MA_Long'] = df['Close'].rolling(window=MA_LONG).mean()
        df['Volume_Avg'] = df['Volume'].rolling(window=VOLUME_AVG_DAYS).mean()
        df['N_Day_Low'] = df['Low'].rolling(window=N_DAYS_LOW).min()
        
        
        # 4. 形态筛选逻辑
        
        # a) 价格突破：最新收盘价高于所有均线 (MA5和MA20)
        price_breakout = (latest['Close'] > latest['MA_Short']) and (latest['Close'] > latest['MA_Long'])
        
        # b) 量能确认：最新成交量高于过去5天平均成交量的1.2倍
        volume_confirm = latest['Volume'] > 1.2 * latest['Volume_Avg'] 

        # c) 从底部反弹：最新价高于N日内的最低价
        bounced_from_low = latest['Close'] > latest['N_Day_Low'] 
        
        
        is_breakout = price_breakout and volume_confirm and bounced_from_low

        return (stock_code, is_breakout, latest['Close'])

    except Exception as e:
        # 捕获其他处理错误
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
    print("--- Note: Applying price filter [5.0, 20.0] and excluding 30-start codes. ---")
    
    # 3. 并行处理文件
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
            
            # 5. 额外排除：名称包含 ST
            if re.search(r'ST', name, re.IGNORECASE):
                 # print(f"Excluded {code} due to 'ST' in name.") # 调试用
                 continue 
                 
            successful_scans.append({
                'Code': code,
                'Name': name,
                'Latest Close': f'{close_price:.2f}' if close_price else 'N/A'
            })

    # 6. 输出结果
    if not successful_scans:
        print("\n--- Filtering complete ---")
        print("No stocks matched the breakout criteria.")
        output_df = pd.DataFrame(columns=['Code', 'Name', 'Latest Close'])
    else:
        output_df = pd.DataFrame(successful_scans)
        output_df = output_df.sort_values(by='Latest Close', ascending=False)
        print(f"\n--- Filtering complete ---")
        print(f"Successfully found {len(successful_scans)} stocks matching the criteria.")

    # 7. 保存结果到指定目录
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
