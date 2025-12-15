import pandas as pd
import glob
import os
import time
from datetime import datetime
import multiprocessing as mp
import pytz # 必须导入 pytz 来处理时区

# --- 筛选逻辑参数 (模拟快速拉升后短期见顶回落的形态) ---
DAYS_LOOKBACK = 20  # 寻找低点和拉升的周期 (例如：20个交易日)
MIN_GAIN_PERCENT = 40.0  # N天内最低价到最高价的最小涨幅百分比 (例如：40%)
DROP_LOOKBACK = 5  # 寻找短期高点的周期 (例如：5个交易日)
MIN_DROP_PERCENT = 10.0  # 从M天高点到最新收盘价的最小回落跌幅百分比 (例如：10%)
# --------------------------------------------------------

# 定义处理单个CSV文件的函数
def process_file(file_path):
    """
    处理单个CSV文件，筛选符合快速拉升后回落条件的股票。
    """
    try:
        # CSV格式假设：日期,开盘价,最高价,最低价,收盘价,成交量,成交额
        df = pd.read_csv(file_path, header=None, names=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        
        # 确保数据按日期降序排列（最新数据在最前面）
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        
        if len(df) < DAYS_LOOKBACK:
            return None # 数据不足

        # 1. 提取最近 N 天数据 (包含最新的一天)
        recent_data = df.head(DAYS_LOOKBACK)
        stock_code = os.path.basename(file_path).split('.')[0]
        
        # 2. 检查快速拉升条件 (N天内最低价到最高价的涨幅)
        low_price_n = recent_data['low'].min()
        high_price_n = recent_data['high'].max()
        
        if low_price_n <= 0: return None
            
        gain_percent = (high_price_n - low_price_n) / low_price_n * 100
        
        if gain_percent < MIN_GAIN_PERCENT:
            return None 

        # 3. 检查短期见顶/回落条件 (M天内高点到最新收盘价的跌幅)
        latest_close = recent_data.iloc[0]['close']
        
        # 获取 M 天内最高价 (确保不超过数据长度)
        drop_data = recent_data.head(min(DROP_LOOKBACK, len(recent_data)))
        high_price_m = drop_data['high'].max()
        
        if high_price_m <= 0: return None
            
        # 计算回落幅度
        drop_percent = (high_price_m - latest_close) / high_price_m * 100
        
        if drop_percent >= MIN_DROP_PERCENT:
            # 成功筛选，返回包含所有必要信息的元组
            return (stock_code, gain_percent, high_price_m, latest_close, drop_percent)
            
        return None

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

def main():
    # --- 目录和文件设置 ---
    data_dir = 'stock_data'
    stock_list_file = 'stock_names.csv' 
    
    # 确定输出目录和文件名，使用上海时区 (Asia/Shanghai)
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    # 使用 pytz 获取当前上海时间
    now_shanghai = datetime.now(shanghai_tz)
    
    output_dir = now_shanghai.strftime('results/%Y-%m')
    output_filename = now_shanghai.strftime('%Y%m%d_%H%M%S_filtered.csv')
    output_path = os.path.join(output_dir, output_filename)
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. 扫描所有数据文件
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    
    if not csv_files:
        print(f"Error: No CSV files found in {data_dir}. Exiting.")
        return

    print(f"Scanning {len(csv_files)} stock data files in {data_dir} using {mp.cpu_count()} processes...")
    
    # 2. 使用多进程并行处理 (加快运行速度)
    pool = mp.Pool(mp.cpu_count())
    results = pool.map(process_file, csv_files)
    pool.close()
    pool.join()
    
    # 3. 过滤出有效结果
    filtered_results = [res for res in results if res is not None]
    
    if not filtered_results:
        print("No stocks matched the filtering conditions.")
        # 创建一个带有列名的空CSV文件，以确保文件被提交
        empty_df = pd.DataFrame(columns=['Code', 'Name', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct'])
        empty_df.to_csv(output_path, index=False, encoding='utf-8')
        return

    # 4. 转换为DataFrame
    columns = ['Code', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct']
    filtered_df = pd.DataFrame(filtered_results, columns=columns)
    
    # 5. 读取股票名称对照表 (stock_names.csv)
    try:
        # 假设 stock_names.csv 是两列，无标题行
        names_df = pd.read_csv(stock_list_file, header=None, names=['Code', 'Name'], dtype={'Code': str})
        names_df['Code'] = names_df['Code'].astype(str).str.split('.').str[0] 
        
        # 6. 合并数据，匹配名称
        filtered_df['Code'] = filtered_df['Code'].astype(str)
        final_df = pd.merge(filtered_df, names_df, on='Code', how='left')
        
        # 调整列顺序并处理缺失名称
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
