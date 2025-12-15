import pandas as pd
import os
import glob
from datetime import datetime
from joblib import Parallel, delayed, cpu_count

# --- 配置 ---
DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_DIR = 'screener_results'
MIN_CLOSE_PRICE = 5.0
MAX_CLOSE_PRICE = 20.0 # 新增价格上限
MA_PERIODS = [5, 20] 
VOL_MA_PERIODS = [5, 20] 

# --- 关键：使用您的文件中的实际列名进行映射 ---
HISTORICAL_COLS_MAP = {
    '日期': 'Date',          
    '收盘': 'Close',        
    '成交量': 'Volume',      
    '开盘': 'Open',
    '最高': 'High',
    '最低': 'Low'
}

NAMES_COLS_MAP = {
    'code': 'StockCode',     
    'name': 'StockName'      
}

def check_stock_code_and_name(stock_code, stock_name_df):
    """根据股票代码和名称排除非A股、ST和创业板"""
    
    # 1. 排除创业板 (30开头) 和其他非沪深A股
    if stock_code.startswith('30'):
        return False # 排除创业板
    # 沪深A股代码特征 (简化判断，主要排除北交所/其他，并确保是60/00开头)
    # A股主板和中小板：600/601/603/605 (沪市), 000/001/002 (深市)
    if not (stock_code.startswith('60') or stock_code.startswith('00')):
        return False # 排除所有其他非A股主板/中小板代码
        
    # 2. 排除 ST 股票 (需要匹配股票名称)
    try:
        # 在股票名称DataFrame中查找当前代码的名称
        name_row = stock_name_df[stock_name_df[NAMES_COLS_MAP['code']] == stock_code]
        if not name_row.empty:
            name = name_row.iloc[0][NAMES_COLS_MAP['name']]
            if 'ST' in name or '*ST' in name:
                return False # 排除ST股
    except:
        # 如果名称匹配失败，为安全起见，不排除，让其继续，除非是关键ST股
        pass 

    return True # 通过所有检查

def calculate_indicators(df):
    """计算所需的均线和量能指标"""
    close_col = HISTORICAL_COLS_MAP['收盘']
    volume_col = HISTORICAL_COLS_MAP['成交量']
    date_col = HISTORICAL_COLS_MAP['日期']
    
    df = df.sort_values(by=date_col).reset_index(drop=True)
    
    # 计算均线和量均线
    for p in MA_PERIODS:
        df[f'MA{p}'] = df[close_col].rolling(window=p).mean()
    for p in VOL_MA_PERIODS:
        df[f'Vol_MA{p}'] = df[volume_col].rolling(window=p).mean()
        
    # 低位反转检查
    df['Low_Reversal_Check'] = df[close_col].rolling(window=30).apply(
        lambda x: (x[:-1] <= df.loc[x.index[:-1], 'MA20']).any(), 
        raw=False
    )
    return df

def apply_screener_logic(df, stock_code):
    """应用筛选条件"""
    close_col = HISTORICAL_COLS_MAP['收盘']
    
    if df.empty or len(df) < max(MA_PERIODS):
        return None
    
    latest = df.iloc[-1]
    
    # 1. 价格区间检查
    if not (MIN_CLOSE_PRICE <= latest[close_col] <= MAX_CLOSE_PRICE):
        return None # 排除低于5.0或高于20.0的
        
    # 2. 短期趋势反转 (MA5 > MA20 且 Close > MA5)
    if not (latest['MA5'] > latest['MA20'] and latest[close_col] > latest['MA5']):
        return None
        
    # 3. 低位反转信号
    if not latest['Low_Reversal_Check']:
        return None
        
    # 4. 量能配合 (5日量均线 > 20日量均线)
    if not (latest['Vol_MA5'] > latest['Vol_MA20']):
        return None
        
    # 匹配成功
    return {
        NAMES_COLS_MAP['code']: stock_code,
        'Latest_Close': latest[close_col],
        'MA5': latest['MA5'],
        'MA20': latest['MA20']
    }

def process_single_file(file_path, stock_name_df):
    """并行处理单个CSV文件 (增加了名称DF作为参数)"""
    stock_code = os.path.basename(file_path).split('.')[0]
    
    # --- A. 市场/ST/创业板 快速检查 ---
    if not check_stock_code_and_name(stock_code, stock_name_df):
        print(f"Skipping {stock_code}: Excluded by code/name rule (ST/30-Start/Non-A-Share).")
        return None
        
    try:
        df = pd.read_csv(file_path)
        
        required_original_cols = list(HISTORICAL_COLS_MAP.keys())
        missing_cols = [col for col in required_original_cols if col not in df.columns]
        
        if missing_cols:
            print(f"Skipping {stock_code}: Missing required column(s) {missing_cols}.")
            return None
            
        # 重命名并过滤列
        df.rename(columns=HISTORICAL_COLS_MAP, inplace=True)
        df = df[list(HISTORICAL_COLS_MAP.values())] 
        
        if len(df) < max(MA_PERIODS):
            return None
        
        df_indicators = calculate_indicators(df)
        result = apply_screener_logic(df_indicators, stock_code)
        
        return result
        
    except Exception as e:
        print(f"Error processing {stock_code}: {e}")
        return None

def main():
    """主程序"""
    if not os.path.exists(DATA_DIR):
        print(f"Error: Data directory '{DATA_DIR}' not found.")
        return

    # 1. 加载股票名称文件 (用于ST股和市场排除)
    try:
        stock_name_df = pd.read_csv(STOCK_NAMES_FILE) 
        stock_name_df.rename(columns={
            'code': NAMES_COLS_MAP['code'], 
            'name': NAMES_COLS_MAP['name']
        }, inplace=True)
        stock_name_df[NAMES_COLS_MAP['code']] = stock_name_df[NAMES_COLS_MAP['code']].astype(str)
    except Exception as e:
        print(f"Error reading stock names file {STOCK_NAMES_FILE}: {e}")
        return

    # 2. 扫描所有数据文件
    all_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    
    # 3. 并行处理文件
    print(f"Found {len(all_files)} files. Starting parallel processing...")
    num_cores = cpu_count()
    
    # 将 stock_name_df 传递给并行函数
    results = Parallel(n_jobs=num_cores)(
        delayed(process_single_file)(file, stock_name_df) for file in all_files
    )

    # 4. 收集并清洗筛选结果
    successful_results = [r for r in results if r is not None]
    if not successful_results:
        print("No stocks matched the complex screening criteria.")
        return

    screened_df = pd.DataFrame(successful_results)
    
    # 5. 匹配股票名称 (使用已加载的DF)
    final_df = pd.merge(screened_df, stock_name_df, on=NAMES_COLS_MAP['code'], how='left')

    # 6. 保存结果
    now_shanghai = datetime.now()
    output_month_dir = now_shanghai.strftime('%Y-%m')
    timestamp_str = now_shanghai.strftime('%Y%m%d_%H%MM%S')
    output_filename = f"screener_{timestamp_str}.csv"
    
    final_output_path = os.path.join(OUTPUT_DIR, output_month_dir)
    os.makedirs(final_output_path, exist_ok=True)
    
    full_path = os.path.join(final_output_path, output_filename)
    
    final_df.to_csv(full_path, index=False, encoding='utf-8')
    print(f"\n✅ Screening complete! Results saved to: {full_path}")
    print("Please commit the new files to the repository.")

if __name__ == "__main__":
    main()
