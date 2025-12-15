import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0  # 最新收盘价不能低于 5.0 元
RESULTS_BASE_DIR = '筛选结果'
MAX_WORKERS = os.cpu_count() * 2 if os.cpu_count() else 4 # 并行处理的核心数

# --- 筛选逻辑函数：基于图形分析的“后量过前量”模式 ---
def analyze_stock(file_path):
    """
    分析单个股票的CSV数据，检查是否符合“后量过前量”且突破的形态。
    
    形态定义：
    1. 最近一个交易日的收盘价 >= 5.0 元。
    2. 今天的成交量 (Volume) 相比前 N 日的最高量有显著放大 (后量过前量/倍量)。
    3. 今天的股价 (Close) 相比前 N 日的高点有向上突破。
    
    这里我们使用一个稍微宽松且实用的版本：
    - 检查最近一个交易日是否放量突破了前期的震荡区间。
    """
    try:
        # 1. 读取数据并排序
        df = pd.read_csv(file_path)
        # 确保数据按日期升序排列，并处理列名
        if '日期' in df.columns:
            df.rename(columns={'日期': 'Date', '收盘价': 'Close', '成交量': 'Volume'}, inplace=True)
        elif 'trade_date' in df.columns:
             df.rename(columns={'trade_date': 'Date', 'close': 'Close', 'vol': 'Volume'}, inplace=True)
        else:
             print(f"Warning: Columns not found in {file_path}")
             return None

        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(by='Date', inplace=True)
        
        # 确保数据长度足够进行分析
        if len(df) < 20:
            return None 

        # 2. 选取最近一个交易日的数据
        latest_day = df.iloc[-1]
        
        # 3. 筛选条件 1: 最新收盘价不能低于 5.0 元
        if latest_day['Close'] < MIN_CLOSE_PRICE:
            return None
            
        # 4. 筛选条件 2: “后量过前量”的放量突破形态
        # 检查前 10 个交易日的量价情况
        period = 10 
        
        # 前 period 日的数据 (不包含最新一天)
        pre_period_df = df.iloc[-period-1:-1]
        
        if pre_period_df.empty:
            return None

        # 关键量价指标
        latest_volume = latest_day['Volume']
        max_volume_pre_period = pre_period_df['Volume'].max()
        max_close_pre_period = pre_period_df['Close'].max()
        
        # 判断条件：
        # a) 后量过前量（当前量显著大于前 period 日的最高量，例如大于1.5倍）
        #    * 注: “倍量”是强力信号，这里放宽到显著放量。
        volume_condition = (latest_volume > max_volume_pre_period * 1.5)
        
        # b) 向上突破（当前收盘价高于前 period 日的最高收盘价）
        price_condition = (latest_day['Close'] > max_close_pre_period)
        
        if volume_condition and price_condition:
            # 提取股票代码 (从文件名中获取)
            stock_code = os.path.basename(file_path).split('.')[0]
            # 返回符合条件的股票代码和最新收盘价
            return {'code': stock_code, 'close': latest_day['Close']}
            
    except Exception as e:
        # print(f"Error processing file {file_path}: {e}")
        return None
        
    return None

# --- 主执行函数 ---
def main():
    print(f"--- 股票筛选脚本启动 @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    # 1. 扫描所有数据文件
    data_files = [os.path.join(STOCK_DATA_DIR, f) 
                  for f in os.listdir(STOCK_DATA_DIR) 
                  if f.endswith('.csv')]
                  
    if not data_files:
        print(f"Error: No CSV files found in directory '{STOCK_DATA_DIR}'. Exiting.")
        return

    print(f"Found {len(data_files)} data files. Starting parallel processing...")
    
    # 2. 并行处理文件
    results = []
    # 使用 ThreadPoolExecutor 进行并行处理以加快速度
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(analyze_stock, file) for file in data_files]
        for future in futures:
            result = future.result()
            if result:
                results.append(result)

    print(f"Parallel processing finished. Found {len(results)} stocks matching the criteria.")

    if not results:
        print("No stocks matched the '后量过前量' and price > 5.0 criteria.")
        return

    # 3. 加载股票名称
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
        names_df.rename(columns={'代码': 'code', '名称': 'name'}, inplace=True)
        # 确保 code 列是字符串类型
        names_df['code'] = names_df['code'].astype(str)
    except FileNotFoundError:
        print(f"Warning: Stock names file '{STOCK_NAMES_FILE}' not found. Output will only contain codes.")
        names_df = pd.DataFrame({'code': [], 'name': []})
    except Exception as e:
        print(f"Error loading stock names: {e}")
        names_df = pd.DataFrame({'code': [], 'name': []})

    # 4. 匹配名称并生成最终结果
    results_df = pd.DataFrame(results)
    
    # 确保 code 列是字符串类型
    results_df['code'] = results_df['code'].astype(str)
    
    # 合并 (左连接，保持筛选结果完整性)
    final_df = pd.merge(results_df, names_df[['code', 'name']], on='code', how='left')
    
    # 整理输出列
    final_df = final_df[['code', 'name', 'close']]
    final_df.rename(columns={'code': '股票代码', 'name': '股票名称', 'close': '最新收盘价'}, inplace=True)

    # 5. 保存结果到指定路径
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    current_year = datetime.now().strftime('%Y')
    current_month = datetime.now().strftime('%m')
    
    output_dir = os.path.join(RESULTS_BASE_DIR, current_year, current_month)
    os.makedirs(output_dir, exist_ok=True)
    
    # 文件名与脚本名一致并加上时间戳
    output_filename = f"{os.path.basename(__file__).replace('.py', '')}_{current_time}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"Successfully saved {len(final_df)} results to: {output_path}")

if __name__ == '__main__':
    main()
