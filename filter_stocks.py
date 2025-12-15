import pandas as pd
import glob
import os
import time
from datetime import datetime
import multiprocessing as mp

# --- 筛选逻辑参数 ---
DAYS_LOOKBACK = 20  # 寻找低点和拉升的周期
MIN_GAIN_PERCENT = 40.0  # N天内最低价到最高价的最小涨幅百分比
DROP_LOOKBACK = 5  # 寻找短期高点的周期
MIN_DROP_PERCENT = 10.0  # 从M天高点回落的最小跌幅百分比
# --------------------

# 定义处理单个CSV文件的函数
def process_file(file_path):
    """
    处理单个CSV文件，筛选符合条件的股票。
    :param file_path: CSV文件路径。
    :return: 股票代码（str）或 None。
    """
    try:
        # CSV格式：日期,开盘价,最高价,最低价,收盘价,成交量,成交额
        df = pd.read_csv(file_path, header=None, names=['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        
        # 确保数据降序（最新数据在最前面，如果不是，需要反转）
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        
        if len(df) < DAYS_LOOKBACK:
            return None # 数据不足

        # 1. 提取最近 N 天数据
        recent_data = df.head(DAYS_LOOKBACK)
        
        stock_code = os.path.basename(file_path).split('.')[0]
        
        # 2. 检查快速拉升条件 (从最低点到最高点的涨幅)
        low_price_n = recent_data['low'].min()
        high_price_n = recent_data['high'].max()
        
        # 防止除以零
        if low_price_n == 0:
            return None
            
        gain_percent = (high_price_n - low_price_n) / low_price_n * 100
        
        if gain_percent < MIN_GAIN_PERCENT:
            return None # 不符合最小涨幅要求

        # 3. 检查短期见顶/回落条件 (从 M 天内高点到最新收盘价的跌幅)
        # 获取最新的收盘价
        latest_close = recent_data.iloc[0]['close']
        
        # 获取 M 天内最高价 (确保不超过数据长度)
        drop_data = recent_data.head(min(DROP_LOOKBACK, len(recent_data)))
        high_price_m = drop_data['high'].max()
        
        # 防止除以零
        if high_price_m == 0:
            return None
            
        # 计算回落幅度
        drop_percent = (high_price_m - latest_close) / high_price_m * 100
        
        if drop_percent >= MIN_DROP_PERCENT:
            # 成功筛选
            # 返回一个包含所有必要信息的元组
            return (stock_code, gain_percent, high_price_m, latest_close, drop_percent)
            
        return None

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

def main():
    # --- 目录和文件设置 ---
    data_dir = 'stock_data'
    stock_list_file = 'stock_names.csv' # 股票代码和名称对照表
    
    # 确定输出目录和文件名
    # 使用上海时区（Asia/Shanghai）
    shanghai_tz = 'Asia/Shanghai'
    now_shanghai = datetime.now(pytz.timezone(shanghai_tz))
    
    output_dir = now_shanghai.strftime('results/%Y-%m')
    output_filename = now_shanghai.strftime('%Y%m%d_%H%M%S_filtered.csv')
    output_path = os.path.join(output_dir, output_filename)
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. 扫描所有数据文件
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    
    print(f"Scanning {len(csv_files)} stock data files in {data_dir}...")
    
    # 2. 使用多进程并行处理
    pool = mp.Pool(mp.cpu_count())
    # results 存储 (stock_code, gain_percent, high_price_m, latest_close, drop_percent) 或 None
    results = pool.map(process_file, csv_files)
    pool.close()
    pool.join()
    
    # 3. 过滤出有效结果
    filtered_results = [res for res in results if res is not None]
    
    if not filtered_results:
        print("No stocks matched the filtering conditions.")
        # 创建一个空的CSV文件以保留记录
        pd.DataFrame(columns=['Code', 'Name', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct']).to_csv(output_path, index=False)
        return

    # 4. 转换为DataFrame
    columns = ['Code', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct']
    filtered_df = pd.DataFrame(filtered_results, columns=columns)
    
    # 5. 读取股票名称对照表
    try:
        names_df = pd.read_csv(stock_list_file, header=None, names=['Code', 'Name'], dtype={'Code': str})
        names_df['Code'] = names_df['Code'].str.split('.').str[0] # 假设代码在 stock_names.csv 中可能带后缀，先清除
        
        # 6. 合并数据，匹配名称
        # 确保 filtered_df['Code'] 是字符串类型
        filtered_df['Code'] = filtered_df['Code'].astype(str)
        final_df = pd.merge(filtered_df, names_df, on='Code', how='left')
        
        # 调整列顺序
        final_df = final_df[['Code', 'Name', 'Gain_20D_Pct', 'High_Price', 'Latest_Close', 'Drop_Pct']]
        final_df['Name'] = final_df['Name'].fillna('N/A') # 找不到名称的填N/A

    except FileNotFoundError:
        print(f"Warning: Stock names file '{stock_list_file}' not found. Output will only contain codes.")
        final_df = filtered_df.copy()
        final_df.insert(1, 'Name', 'N/A')
    
    # 7. 保存结果
    final_df.to_csv(output_path, index=False, float_format='%.2f', encoding='utf-8')
    print(f"Successfully filtered {len(final_df)} stocks. Results saved to {output_path}")

if __name__ == '__main__':
    # 为了在GitHub Actions环境中运行，需要安装 pytz
    try:
        import pytz
    except ImportError:
        print("Required 'pytz' library is not installed. Please install it with 'pip install pytz'.")
        exit(1)
        
    main()
