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
MA_PERIODS = [5, 20] 
VOL_MA_PERIODS = [5, 20] 

# --- 关键：使用您的文件中的实际列名进行映射 ---
# 历史数据CSV文件的原始列名(键)和脚本内部使用的列名(值)的映射
# 根据您的文件片段：日期,股票代码,开盘,收盘,最高,最低,成交量...
HISTORICAL_COLS_MAP = {
    '日期': 'Date',          # 您的CSV文件中的列名 '日期'
    '收盘': 'Close',        # 您的CSV文件中的列名 '收盘'
    '成交量': 'Volume',      # 您的CSV文件中的列名 '成交量'
    # 尽管不需要其他列，但如果需要可以添加:
    '开盘': 'Open',
    '最高': 'High',
    '最低': 'Low'
}

# 股票名称文件 (stock_names.csv) 的列名映射
# 根据您的文件片段：code,name
NAMES_COLS_MAP = {
    'code': 'StockCode',     # 您的stock_names.csv中的列名 'code'
    'name': 'StockName'      # 您的stock_names.csv中的列名 'name'
}

def calculate_indicators(df):
    """计算所需的均线和量能指标"""
    # 使用内部统一的列名
    close_col = HISTORICAL_COLS_MAP['收盘']
    volume_col = HISTORICAL_COLS_MAP['成交量']
    date_col = HISTORICAL_COLS_MAP['日期']
    
    df = df.sort_values(by=date_col).reset_index(drop=True)
    
    # 计算价格均线和量均线
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
    
    # 1. 强制条件：最新收盘价不能低于 5.0 元
    if latest[close_col] < MIN_CLOSE_PRICE:
        return None
        
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

def process_single_file(file_path):
    """并行处理单个CSV文件"""
    stock_code = os.path.basename(file_path).split('.')[0]
    
    try:
        df = pd.read_csv(file_path)
        
        # --- 关键调试与匹配 ---
        required_original_cols = list(HISTORICAL_COLS_MAP.keys())
        missing_cols = [col for col in required_original_cols if col not in df.columns]
        
        if missing_cols:
            # 报告未找到的列，并跳过
            print(f"Skipping {stock_code}: Missing required column(s) {missing_cols}. Please ensure CSV headers match the required Chinese names.")
            return None
            
        # 严格重命名并过滤列
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

    # 1. 扫描所有数据文件
    all_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
    
    # 2. 并行处理文件
    print(f"Found {len(all_files)} files. Starting parallel processing...")
    num_cores = cpu_count()
    results = Parallel(n_jobs=num_cores)(
        delayed(process_single_file)(file) for file in all_files
    )

    # 3. 收集并清洗筛选结果
    successful_results = [r for r in results if r is not None]
    if not successful_results:
        print("No stocks matched the screening criteria.")
        return

    screened_df = pd.DataFrame(successful_results)
    
    # 4. 匹配股票名称 (stock_names.csv 带有 'code,name' 头部)
    try:
        # 读取 stock_names.csv，因为它有头部，我们不使用 header=None
        names_df = pd.read_csv(STOCK_NAMES_FILE) 
        
        # 重命名列以匹配内部标准
        names_df.rename(columns={
            'code': NAMES_COLS_MAP['code'], 
            'name': NAMES_COLS_MAP['name']
        }, inplace=True)
        
        names_df[NAMES_COLS_MAP['code']] = names_df[NAMES_COLS_MAP['code']].astype(str)
        
        # 合并
        final_df = pd.merge(screened_df, names_df, on=NAMES_COLS_MAP['code'], how='left')
        
    except Exception as e:
        print(f"Warning: Error reading or matching stock names: {e}. Skipping name matching.")
        final_df = screened_df.copy()
        final_df[NAMES_COLS_MAP['name']] = 'N/A' 

    # 5. 保存结果
    now_shanghai = datetime.now()
    output_month_dir = now_shanghai.strftime('%Y-%m')
    timestamp_str = now_shanghai.strftime('%Y%m%d_%H%M%S')
    output_filename = f"screener_{timestamp_str}.csv"
    
    final_output_path = os.path.join(OUTPUT_DIR, output_month_dir)
    os.makedirs(final_output_path, exist_ok=True)
    
    full_path = os.path.join(final_output_path, output_filename)
    
    final_df.to_csv(full_path, index=False, encoding='utf-8')
    print(f"\n✅ Screening complete! Results saved to: {full_path}")
    print("Please commit the new files to the repository.")

if __name__ == "__main__":
    main()
