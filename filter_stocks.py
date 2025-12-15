import pandas as pd
import glob
import os
import time
from datetime import datetime
import multiprocessing as mp
import pytz

# --- 筛选逻辑参数：已收紧条件 ---
DAYS_LOOKBACK = 15     # 寻找低点和拉升的周期 (略微缩短，确保拉升更近)
MIN_GAIN_PERCENT = 50.0  # N天内最低价到最高价的最小涨幅百分比 (提高到 50%)
DROP_LOOKBACK = 5      # 寻找短期高点的周期
MIN_DROP_PERCENT = 15.0  # 从M天高点到最新收盘价的最小回落跌幅百分比 (提高到 15%)

# --- 附加过滤参数 (用于减少结果数量) ---
LATEST_CLOSE_MIN = 5.0      # 最新收盘价不能低于 5.0 元
AVG_AMOUNT_MIN = 10000000.0 # 最近 N 天平均成交额不能低于 1000 万
# --------------------------------------------------------

# 定义处理单个CSV文件的函数
def process_file(file_path):
    """
    处理单个CSV文件，筛选符合快速拉升后回落条件的股票。
    """
    try:
        # 根据实际文件片段定义 12 列名称，跳过标题行
        column_names = [
            'date', 'code_file', 'open', 'close', 'high', 'low', 
            'volume', 'amount', 'amplitude', 'pct_chg', 'chg', 'turnover'
        ]
        
        # 1. 准确读取数据：使用 date_format 加速解析并消除警告
        df = pd.read_csv(
            file_path, 
            header=None,
            skiprows=1,  # 跳过实际的标题行
            names=column_names,
            dtype={'code_file': str}, 
            parse_dates=['date'], 
            date_format='%Y-%m-%d' # 明确指定日期格式，优化性能
        )
        
        # 确保数据按日期降序排列
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        
        if len(df) < DAYS_LOOKBACK:
            return None # 数据不足

        recent_data = df.head(DAYS_LOOKBACK)
        stock_code = os.path.basename(file_path).split('.')[0]
        
        # 2. 【附加过滤】排除低流动性和低价股
        latest_close = recent_data.iloc[0]['close']
        
        if latest_close < LATEST_CLOSE_MIN:
             return None
             
        avg_amount = recent_data['amount'].mean()
        if avg_amount < AVG_AMOUNT_MIN:
             return None
        
        # 3. 检查快速拉升条件 (N天内最低价到最高价的涨幅)
        low_price_n = recent_data['low'].min()
        high_price_n = recent_data['high'].max()
        
        if low_price_n <= 0: return None
            
        gain_percent = (high_price_n - low_price_n) / low_price_n * 100
        
        # 使用收紧后的 MIN_GAIN_PERCENT
        if gain_percent < MIN_GAIN_PERCENT:
            return None 

        # 4. 检查短期见顶/回落条件 (M天内高点到最新收盘价的跌幅)
        drop_data = recent_data.head(min(DROP_LOOKBACK, len(recent_data)))
        high_price_m = drop_data['high'].max()
        
        if high_price_m <= 0: return None
            
        drop_percent = (high_price_m - latest_close) / high_price_m * 100
        
        # 使用收紧后的 MIN_DROP_PERCENT
        if drop_percent >= MIN_DROP_PERCENT:
            return (stock_code, gain_percent, high_price_m, latest_close, drop_percent)
            
        return None

    except Exception as e:
        # 为了调试，保留错误输出
        # print(f"Error processing file {file_path}: {e}")
        return None

def main():
    # --- 目录和文件设置 (不变) ---
    data_dir = 'stock_data'
    stock_list_file = 'stock_names.csv' 
    
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now_shanghai = datetime.now(shanghai_tz)
    
    output_dir = now_shanghai.strftime('results/%Y-%m')
    output_filename = now_shanghai.strftime('%Y%m%d_%H%M%S_filtered.csv')
    output_path = os.path.join(output_dir, output_filename)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. 扫描所有数据文件
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    
    if not csv_files:
        print(f"Error: No CSV files found in {data_dir}. Exiting.")
        return

    print(f"Scanning {len(csv_files)} stock data files in {data_dir} using {mp.cpu_count()} processes...")
    
    # 2. 使用多进程并行处理
    pool = mp.Pool(mp.cpu_count())
    results = pool.map(process_file, csv_files)
    pool.close()
    pool.join()
    
    # 3. 过滤出有效结果
    filtered_results = [res for res in results if res is not None]
    
    if not filtered_results:
        print("No stocks matched the filtering conditions.")
        empty_df = pd.DataFrame(columns=['Code', 'Name', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct'])
        empty_df.to_csv(output_path, index=False, encoding='utf-8')
        return

    # 4. 转换为DataFrame
    columns = ['Code', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct']
    filtered_df = pd.DataFrame(filtered_results, columns=columns)
    
    # 5. 读取股票名称对照表 (stock_names.csv)
    try:
        names_df = pd.read_csv(stock_list_file, dtype={'code': str})
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'})
        
        # 6. 合并数据，匹配名称
        filtered_df['Code'] = filtered_df['Code'].astype(str)
        final_df = pd.merge(filtered_df, names_df[['Code', 'Name']], on='Code', how='left')
        
        final_df = final_df[['Code', 'Name', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct']]
        final_df['Name'] = final_df['Name'].fillna('N/A (Name Not Found)')

    except FileNotFoundError:
        print(f"Warning: Stock names file '{stock_list_file}' not found. Output will contain N/A for names.")
        final_df = filtered_df.copy()
        final_df.insert(1, 'Name', 'N/A (Names File Missing)')
    
    # 7. 保存结果
    final_df.to_csv(output_path, index=False, float_format='%.2f', encoding='utf-8')
    print(f"Successfully filtered {len(final_df)} stocks. Results saved to {output_path}")

if __name__ == '__main__':
    main()
