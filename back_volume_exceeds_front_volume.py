import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0   # 最新收盘价最低限制
MAX_CLOSE_PRICE = 20.0  # 最新收盘价最高限制
RESULTS_BASE_DIR = '筛选结果'
MAX_WORKERS = os.cpu_count() * 2 if os.cpu_count() else 4

# --- 辅助函数：检查股票代码和名称是否符合要求 ---
def is_valid_stock(stock_code, stock_name):
    """
    检查股票是否为深沪A股，并排除ST股和创业板、科创板等。
    """
    # 1. 排除ST股
    if isinstance(stock_name, str) and ('ST' in stock_name.upper()):
        # print(f"Excluded: {stock_code} {stock_name} (ST Stock)")
        return False
        
    # 2. 排除创业板 (30开头) 和科创板 (688开头) 及北交所 (4, 8开头)
    # 只保留标准的沪市A股 (60开头) 和深市A股 (00开头)
    if stock_code.startswith(('30', '688', '4', '8')):
        # print(f"Excluded: {stock_code} (Non-Main A-share: 30/688/4/8)")
        return False
        
    # 如果不是60或00开头，但又不在排除列表中，可能需要更严格的检查，
    # 但根据中国A股的惯例，主要A股就是60/00开头，以上排除已基本覆盖要求。
    if not (stock_code.startswith('60') or stock_code.startswith('00')):
        # 如果不是标准的沪深A股代码开头，但又不是明确的被排除代码，也先排除。
        return False

    return True


# --- 筛选逻辑函数：基于图形分析的“后量过前量”模式 ---
def analyze_stock(file_path, stock_names_map):
    """
    分析单个股票的CSV数据，检查是否符合量价形态、价格和类型要求。
    """
    stock_code = os.path.basename(file_path).split('.')[0]
    stock_name = stock_names_map.get(stock_code, "未知")

    # 0. 股票类型和名称筛选
    if not is_valid_stock(stock_code, stock_name):
        return None
    
    try:
        # 1. 读取数据并排序
        df = pd.read_csv(file_path)
        # 确保数据按日期升序排列，并处理列名
        if '日期' in df.columns:
            df.rename(columns={'日期': 'Date', '收盘价': 'Close', '成交量': 'Volume'}, inplace=True)
        elif 'trade_date' in df.columns:
             df.rename(columns={'trade_date': 'Date', 'close': 'Close', 'vol': 'Volume'}, inplace=True)
        else:
             return None

        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(by='Date', inplace=True)
        
        if len(df) < 20:
            return None 

        # 2. 选取最近一个交易日的数据
        latest_day = df.iloc[-1]
        latest_close = latest_day['Close']
        
        # 3. 价格区间筛选: 最新收盘价在 5.0 到 20.0 之间
        if latest_close < MIN_CLOSE_PRICE or latest_close > MAX_CLOSE_PRICE:
            # print(f"Excluded: {stock_code} (Price {latest_close} out of range)")
            return None
            
        # 4. 量价形态筛选: “后量过前量”的放量突破形态
        # 检查前 10 个交易日的量价情况
        period = 10 
        pre_period_df = df.iloc[-period-1:-1]
        
        if pre_period_df.empty:
            return None

        # 关键量价指标
        latest_volume = latest_day['Volume']
        max_volume_pre_period = pre_period_df['Volume'].max()
        max_close_pre_period = pre_period_df['Close'].max()
        
        # a) 后量过前量（当前量显著大于前 period 日的最高量，这里定义为大于1.5倍）
        volume_condition = (latest_volume > max_volume_pre_period * 1.5)
        
        # b) 向上突破（当前收盘价高于前 period 日的最高收盘价）
        price_condition = (latest_close > max_close_pre_period)
        
        if volume_condition and price_condition:
            # 返回符合条件的股票代码、名称和最新收盘价
            return {'code': stock_code, 'name': stock_name, 'close': latest_close}
            
    except Exception as e:
        # 忽略处理单个文件时可能出现的错误
        # print(f"Error processing file {file_path}: {e}")
        return None
        
    return None

# --- 主执行函数 ---
def main():
    print(f"--- 股票筛选脚本启动 @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    # 1. 加载股票名称并创建映射 (用于类型筛选)
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'代码': str})
        names_df.rename(columns={'代码': 'code', '名称': 'name'}, inplace=True)
        names_df['code'] = names_df['code'].astype(str)
        stock_names_map = names_df.set_index('code')['name'].to_dict()
    except Exception as e:
        print(f"Error loading stock names: {e}. Cannot perform name-based filtering.")
        stock_names_map = {}

    # 2. 扫描所有数据文件
    data_files = [os.path.join(STOCK_DATA_DIR, f) 
                  for f in os.listdir(STOCK_DATA_DIR) 
                  if f.endswith('.csv')]
                  
    if not data_files:
        print(f"Error: No CSV files found in directory '{STOCK_DATA_DIR}'. Exiting.")
        return

    print(f"Found {len(data_files)} data files. Starting parallel processing on {MAX_WORKERS} cores...")
    
    # 3. 并行处理文件
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 将 stock_names_map 传入 analyze_stock 函数
        futures = [executor.submit(analyze_stock, file, stock_names_map) for file in data_files]
        for future in futures:
            result = future.result()
            if result:
                results.append(result)

    print(f"Parallel processing finished. Found {len(results)} stocks matching the criteria.")

    if not results:
        print("No stocks matched the updated criteria.")
        return

    # 4. 生成最终结果
    final_df = pd.DataFrame(results)
    final_df = final_df[['code', 'name', 'close']]
    final_df.rename(columns={'code': '股票代码', 'name': '股票名称', 'close': '最新收盘价'}, inplace=True)

    # 5. 保存结果到指定路径
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    current_year = datetime.now().strftime('%Y')
    current_month = datetime.now().strftime('%m')
    
    output_dir = os.path.join(RESULTS_BASE_DIR, current_year, current_month)
    os.makedirs(output_dir, exist_ok=True)
    
    # 文件名格式：脚本名_时间戳.csv
    output_filename = f"{os.path.basename(__file__).replace('.py', '')}_{current_time}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"Successfully saved {len(final_df)} results to: {output_path}")

if __name__ == '__main__':
    main()
