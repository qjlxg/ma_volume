import pandas as pd
import glob
import os
import time
from datetime import datetime
import multiprocessing as mp
import pytz

# --- 筛选逻辑参数 ---
DAYS_LOOKBACK = 20  # 寻找低点和拉升的周期
MIN_GAIN_PERCENT = 40.0  # N天内最低价到最高价的最小涨幅百分比
DROP_LOOKBACK = 5  # 寻找短期高点的周期
MIN_DROP_PERCENT = 10.0  # 从M天高点到最新收盘价的最小回落跌幅百分比
# --------------------------------------------------------

# 定义处理单个CSV文件的函数
def process_file(file_path):
    """
    处理单个CSV文件，筛选符合条件的股票。
    """
    try:
        # 根据实际文件片段定义 12 列名称，确保顺序和数量准确
        column_names = [
            'date', 'code_file', 'open', 'close', 'high', 'low', 
            'volume', 'amount', 'amplitude', 'pct_chg', 'chg', 'turnover'
        ]
        
        # 1. 准确读取数据
        # skipinitialspace=True 处理可能存在的空格
        df = pd.read_csv(
            file_path, 
            header=None, # 文件片段中第一行是标题，但我们使用 header=None，然后跳过第一行，以确保数据始终从第二行开始
            skiprows=1,  # 跳过实际的标题行
            names=column_names,
            dtype={'code_file': str}, # 确保代码是字符串
            # 修正：明确指定日期格式，消除 UserWarning，加速解析
            parse_dates=['date'], 
            date_format='%Y-%m-%d' 
        )
        
        # 确保 df['date'] 成功转换
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            # 如果 parse_dates 失败 (例如，您的 CSV 文件中日期列有不规范的数据)，
            # 也可以手动强制转换，但此时警告会再次出现。
            # 这里依赖 date_format 正常工作
            pass 

        # 确保数据按日期降序排列（最新数据在最前面）
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        
        if len(df) < DAYS_LOOKBACK:
            return None # 数据不足

        # 2. 提取股票代码 (使用文件名作为代码，因为文件名是纯代码)
        stock_code = os.path.basename(file_path).split('.')[0]
        
        # 3. 筛选逻辑
        recent_data = df.head(DAYS_LOOKBACK)
        
        # 检查快速拉升条件 (N天内最低价到最高价的涨幅)
        low_price_n = recent_data['low'].min()
        high_price_n = recent_data['high'].max()
        
        if low_price_n <= 0: return None
            
        gain_percent = (high_price_n - low_price_n) / low_price_n * 100
        
        if gain_percent < MIN_GAIN_PERCENT:
            return None 

        # 检查短期见顶/回落条件 (M天内高点到最新收盘价的跌幅)
        latest_close = recent_data.iloc[0]['close']
        
        drop_data = recent_data.head(min(DROP_LOOKBACK, len(recent_data)))
        high_price_m = drop_data['high'].max()
        
        if high_price_m <= 0: return None
            
        drop_percent = (high_price_m - latest_close) / high_price_m * 100
        
        if drop_percent >= MIN_DROP_PERCENT:
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
    now_shanghai = datetime.now(shanghai_tz)
    
    output_dir = now_shanghai.strftime('results/%Y-%m')
    output_filename = now_shanghai.strftime('%Y%m%d_%H%M%S_filtered.csv')
    output_path = os.path.join(output_dir, output_filename)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. 扫描所有数据文件 (省略扫描逻辑...)

    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    # ... (多进程处理和结果过滤逻辑不变) ...
    
    print(f"Scanning {len(csv_files)} stock data files in {data_dir} using {mp.cpu_count()} processes...")
    
    pool = mp.Pool(mp.cpu_count())
    results = pool.map(process_file, csv_files)
    pool.close()
    pool.join()
    
    filtered_results = [res for res in results if res is not None]
    
    if not filtered_results:
        print("No stocks matched the filtering conditions.")
        empty_df = pd.DataFrame(columns=['Code', 'Name', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct'])
        empty_df.to_csv(output_path, index=False, encoding='utf-8')
        return

    columns = ['Code', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct']
    filtered_df = pd.DataFrame(filtered_results, columns=columns)
    
    # 5. 读取股票名称对照表 (stock_names.csv)
    try:
        # 修正：根据实际片段 (code,name)，Pandas 默认读取 header=0
        names_df = pd.read_csv(stock_list_file, dtype={'code': str})
        
        # 统一列名为 'Code' 和 'Name' 以便合并
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'})
        
        # 6. 合并数据，匹配名称
        filtered_df['Code'] = filtered_df['Code'].astype(str)
        
        # 仅选择名称DF中的 'Code' 和 'Name' 进行合并，避免其他列干扰
        final_df = pd.merge(filtered_df, names_df[['Code', 'Name']], on='Code', how='left')
        
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
