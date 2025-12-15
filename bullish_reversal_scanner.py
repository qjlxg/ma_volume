# bullish_reversal_scanner.py

import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0

def check_reversal_pattern(df):
    """
    检查 dataframe 最后三根K线是否符合“长阴短柱 + 次日反包”形态。
    
    形态定义（从倒数第三天开始）：
    1. K3（长阴）：实体较大。
    2. K2（短柱）：实体较小，且 K2 的最高价低于 K3 的收盘价，K2 开盘价低于 K3 开盘价（跳空低开）。
    3. K1（反包）：K1 收盘价高于 K2 开盘价，最好能吞没 K3 的大部分实体。
    
    为简化和适应自动化筛选，我们采用以下更严格和量化的条件：
    - K1: 今天的K线 (df.iloc[-1])
    - K2: 昨天的K线 (df.iloc[-2])
    - K3: 前天的K线 (df.iloc[-3])
    """
    
    if len(df) < 3:
        return False
        
    k1 = df.iloc[-1] # 反包阳线
    k2 = df.iloc[-2] # 短柱/底部犹豫
    k3 = df.iloc[-3] # 长阴/大跌

    # 1. K3 必须是阴线（收盘低于开盘）
    k3_is_bear = k3['Close'] < k3['Open']
    k3_entity_size = abs(k3['Close'] - k3['Open'])
    
    # 2. K1 必须是阳线（收盘高于开盘）
    k1_is_bull = k1['Close'] > k1['Open']
    
    # 3. K3 必须是“长”阴：实体大小至少是前三日平均K线实体大小的两倍
    avg_entity = (abs(df['Close'].diff()) + abs(df['Open'].diff())).dropna().tail(3).mean()
    k3_is_long = k3_entity_size > (avg_entity * 2) if not pd.isna(avg_entity) else k3_entity_size > (k3['High'] - k3['Low']) * 0.4
    
    # 4. K2 是“短”柱：实体大小小于 K3 实体大小的一半
    k2_entity_size = abs(k2['Close'] - k2['Open'])
    k2_is_short = k2_entity_size < k3_entity_size * 0.5
    
    # 5. K2 的实体位于 K3 的下部或低于 K3 实体（跳空低开/低位整理）
    # K2 开盘低于 K3 收盘
    k2_gap_down = k2['Open'] < k3['Close']
    
    # 6. K1 成功“反包”：K1 收盘价高于 K2 的开盘价，且最好能收复 K3 的一半跌幅（吞没 K3 实体一半以上）
    k1_engulfs_k2 = k1['Close'] > k2['Open']
    k1_recovers_k3 = k1['Close'] > k3['Open'] - (k3_entity_size / 2)

    # 7. 附加条件：最新收盘价不能低于 5.0 元
    price_check = k1['Close'] >= MIN_CLOSE_PRICE

    # 综合判断
    if (k3_is_bear and k1_is_bull and k3_is_long and k2_is_short and 
        k2_gap_down and k1_engulfs_k2 and k1_recovers_k3 and price_check):
        return True
        
    return False

def process_file(file_path):
    """处理单个 CSV 文件，检查形态并返回结果（股票代码和最新价格）"""
    try:
        # 假设 CSV 文件包含 'Date', 'Open', 'High', 'Low', 'Close', 'Volume' 列
        df = pd.read_csv(file_path, parse_dates=['Date']).sort_values(by='Date').dropna()
        
        # 提取股票代码
        basename = os.path.basename(file_path)
        stock_code = basename.replace('.csv', '')
        
        if check_reversal_pattern(df):
            latest_close = df.iloc[-1]['Close']
            latest_date = df.iloc[-1]['Date'].strftime('%Y-%m-%d')
            return {'Code': stock_code, 'Close': latest_close, 'Date': latest_date}
            
    except Exception as e:
        # print(f"Error processing {file_path}: {e}")
        pass
        
    return None

def main():
    # 1. 获取所有 CSV 文件路径
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    
    if not all_files:
        print(f"Error: No CSV files found in {STOCK_DATA_DIR}. Exiting.")
        return

    # 2. 并行处理文件
    print(f"Scanning {len(all_files)} files using {cpu_count()} cores...")
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_file, all_files)

    # 3. 过滤有效结果
    matches = [res for res in results if res is not None]
    
    if not matches:
        print("No matching bullish reversal stocks found.")
        return

    # 4. 匹配股票名称
    matched_df = pd.DataFrame(matches)
    
    try:
        stock_names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        
        # 确保 stock_names.csv 中的 'Code' 和 'Name' 列存在
        if 'Code' not in stock_names_df.columns or 'Name' not in stock_names_df.columns:
             print(f"Error: {STOCK_NAMES_FILE} must contain 'Code' and 'Name' columns.")
             final_output_df = matched_df[['Code', 'Close', 'Date']]
        else:
            final_output_df = pd.merge(
                matched_df, 
                stock_names_df[['Code', 'Name']], 
                on='Code', 
                how='left'
            )
            # 调整列顺序
            final_output_df = final_output_df[['Code', 'Name', 'Date', 'Close']]
            
    except FileNotFoundError:
        print(f"Warning: {STOCK_NAMES_FILE} not found. Outputting code and price only.")
        final_output_df = matched_df[['Code', 'Close', 'Date']]

    # 5. 结果保存
    current_time = datetime.now(tz=None) # GitHub Actions 默认使用 UTC，但文件名使用其时间戳
    output_ts = current_time.strftime('%Y%m%d%H%M%S')
    output_date_path = current_time.strftime('%Y/%m')
    output_filename = f'{output_ts}_bullish_reversal_stocks.csv'
    
    # 创建输出目录
    os.makedirs(output_date_path, exist_ok=True)
    
    # 完整输出路径
    output_path = os.path.join(output_date_path, output_filename)
    
    # 导出到 CSV
    final_output_df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Successfully screened {len(final_output_df)} stocks.")
    print(f"Results saved to: {output_path}")

if __name__ == '__main__':
    # 确保 stock_data 目录存在，如果不存在，在本地测试时会失败，但在 GitHub Actions 中需要提前上传
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Creating stock data directory: {STOCK_DATA_DIR}")
        os.makedirs(STOCK_DATA_DIR, exist_ok=True)
        # 注意：在实际运行中，您需要将数据文件放入此目录
        
    main()
