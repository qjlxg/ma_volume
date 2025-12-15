import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSING_PRICE = 5.0
MAX_PULLBACK_PERCENT = 0.15  # 15%
MAX_PULLBACK_DAYS = 30
# --- 配置结束 ---

def analyze_stock(file_path):
    """
    对单个股票的CSV数据进行回调筛选分析。
    CSV文件格式假设包含 'Date' 和 'Close' 列。
    """
    try:
        df = pd.read_csv(file_path)
        
        # 确保数据按日期升序排列
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        # 至少需要30个交易日的数据
        if len(df) < MAX_PULLBACK_DAYS:
            return None

        # 获取股票代码
        stock_code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 1. 检查最新收盘价
        latest_close = df['Close'].iloc[-1]
        if latest_close < MIN_CLOSING_PRICE:
            return None

        # 2. 核心回调逻辑
        
        # 寻找最近一个高点 (不包括最新收盘价)
        # 从倒数第二天开始往前找，因为最后一天是潜在的底部或恢复
        for i in range(2, len(df)):
            current_close = df['Close'].iloc[-i]
            
            # 如果当前价格高于前一个窗口（比如10天）内的所有价格，可以认为是阶段性高点
            # 这里简化为寻找全局历史高点，或在过去30天内的高点
            
            # 简化逻辑：从当前日期往前看MAX_PULLBACK_DAYS天的最高价
            recent_high = df['Close'].iloc[max(0, len(df) - i - MAX_PULLBACK_DAYS + 1):len(df) - i + 1].max()
            
            if current_close == recent_high:
                high_price = current_close
                high_date = df['Date'].iloc[-i]
                
                # 检查回调幅度
                pullback_percent = (high_price - latest_close) / high_price
                
                # 检查回调时间 (从高点到最新收盘日)
                pullback_days = (df['Date'].iloc[-1] - high_date).days
                
                # 筛选条件：
                # 1. 回调幅度在 (0, 15%] 之间 (必须是回调，所以幅度 > 0)
                # 2. 回调时间 <= 30天
                if 0 < pullback_percent <= MAX_PULLBACK_PERCENT and pullback_days <= MAX_PULLBACK_DAYS:
                    return {
                        'Code': stock_code,
                        'Latest_Close': latest_close,
                        'High_Price': high_price,
                        'Pullback_Percent': f"{pullback_percent * 100:.2f}%",
                        'Pullback_Days': pullback_days,
                        'Latest_Date': df['Date'].iloc[-1].strftime('%Y-%m-%d')
                    }
                
                # 找到一个高点后就退出，只关注最近一次从高点开始的回调
                break
        
        return None

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

def main():
    # 1. 获取所有股票数据文件
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    print(f"Found {len(all_files)} stock data files.")

    if not all_files:
        print("No stock data files found. Exiting.")
        return

    # 2. 使用并行处理进行筛选
    results = []
    # 使用CPU核心数进行并行
    with Pool(cpu_count()) as pool:
        results = pool.map(analyze_stock, all_files)

    # 过滤掉 None 的结果
    screened_stocks = [res for res in results if res is not None]
    
    if not screened_stocks:
        print("No stocks matched the screening criteria.")
        return

    result_df = pd.DataFrame(screened_stocks)

    # 3. 匹配股票名称
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        # 确保 stock_names.csv 中 'Code' 列是字符串类型，且匹配格式
        names_df['Code'] = names_df['Code'].astype(str)
        # 假设 stock_names.csv 包含 'Code' 和 'Name' 列
        
        final_df = pd.merge(result_df, names_df, on='Code', how='left')
        final_df['Name'] = final_df['Name'].fillna('名称缺失')
    except FileNotFoundError:
        print(f"Warning: {STOCK_NAMES_FILE} not found. Skipping name matching.")
        final_df = result_df
        final_df['Name'] = '名称缺失'
    
    # 调整列顺序
    final_df = final_df[['Code', 'Name', 'Latest_Date', 'Latest_Close', 'High_Price', 'Pullback_Percent', 'Pullback_Days']]

    # 4. 保存结果
    # 路径格式：/年月/文件名_时间戳.csv
    # 使用上海时区（北京时间）
    shanghai_now = datetime.now()
    output_dir = shanghai_now.strftime('%Y%m')
    timestamp = shanghai_now.strftime('%Y%m%d_%H%M%S')
    
    os.makedirs(output_dir, exist_ok=True)
    output_filename = os.path.join(output_dir, f'healthy_pullback_results_{timestamp}.csv')

    final_df.to_csv(output_filename, index=False, encoding='utf-8')
    print(f"\nSuccessfully screened {len(final_df)} stocks.")
    print(f"Results saved to: {output_filename}")

if __name__ == '__main__':
    main()
