# bullish_reversal_scanner.py (Modified)

import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0
MAX_CLOSE_PRICE = 20.0 # 新增最大收盘价限制

def is_valid_a_share(stock_code, stock_name):
    """
    检查股票是否是符合条件的深沪 A 股：
    1. 排除 ST 股
    2. 排除创业板 (30开头)
    3. 只保留深沪 A 股 (60/00开头)
    """
    
    # 1. 排除 ST 股 (检查名称中是否包含 'ST' 或 '*'，不区分大小写)
    if pd.notna(stock_name) and ('ST' in stock_name.upper() or '*' in stock_name):
        return False
        
    # 2. 检查股票代码是否为深沪 A 股
    # 上交所 A 股 (60 开头)
    is_shanghai_a = stock_code.startswith('60')
    # 深交所 A 股 (00 开头)
    is_shenzhen_a = stock_code.startswith('00')
    
    # 3. 排除创业板 (30 开头)
    is_gem = stock_code.startswith('30')
    
    # 综合判断：必须是 60 或 00 开头，且不能是 30 开头
    if (is_shanghai_a or is_shenzhen_a) and not is_gem:
        return True
        
    return False

def check_reversal_pattern(df):
    """
    检查 dataframe 最后三根K线是否符合“长阴短柱 + 次日反包”形态。
    并加入了价格限制。
    """
    
    if len(df) < 3:
        return False
        
    k1 = df.iloc[-1] # 反包阳线
    k2 = df.iloc[-2] # 短柱/底部犹豫
    k3 = df.iloc[-3] # 长阴/大跌

    # --- 1. 价格和基本市场限制 ---
    # 最新收盘价不能低于 5.0 元，不高于 20.0 元
    price_check = (k1['Close'] >= MIN_CLOSE_PRICE) and (k1['Close'] <= MAX_CLOSE_PRICE)
    
    if not price_check:
        return False
        
    # --- 2. 蜡烛图形态检查（与原脚本逻辑一致）---
    
    # K3 必须是阴线（收盘低于开盘）
    k3_is_bear = k3['Close'] < k3['Open']
    k3_entity_size = abs(k3['Close'] - k3['Open'])
    
    # K1 必须是阳线（收盘高于开盘）
    k1_is_bull = k1['Close'] > k1['Open']
    
    # K3 必须是“长”阴：实体大小至少是前三日平均K线实体大小的两倍
    avg_entity = (abs(df['Close'].diff()) + abs(df['Open'].diff())).dropna().tail(3).mean()
    k3_is_long = k3_entity_size > (avg_entity * 2) if not pd.isna(avg_entity) else k3_entity_size > (k3['High'] - k3['Low']) * 0.4
    
    # K2 是“短”柱：实体大小小于 K3 实体大小的一半
    k2_entity_size = abs(k2['Close'] - k2['Open'])
    k2_is_short = k2_entity_size < k3_entity_size * 0.5
    
    # K2 的实体位于 K3 的下部或低于 K3 实体（跳空低开/低位整理）
    k2_gap_down = k2['Open'] < k3['Close']
    
    # K1 成功“反包”：K1 收盘价高于 K2 的开盘价，且最好能收复 K3 的一半跌幅
    k1_engulfs_k2 = k1['Close'] > k2['Open']
    k1_recovers_k3 = k1['Close'] > k3['Open'] - (k3_entity_size / 2)

    # 综合判断
    if (k3_is_bear and k1_is_bull and k3_is_long and k2_is_short and 
        k2_gap_down and k1_engulfs_k2 and k1_recovers_k3 and price_check):
        return True
        
    return False

def process_file(file_path, stock_names_map):
    """处理单个 CSV 文件，检查形态并返回结果"""
    try:
        # 提取股票代码
        basename = os.path.basename(file_path)
        stock_code = basename.replace('.csv', '')
        stock_name = stock_names_map.get(stock_code, 'N/A')
        
        # 0. 市场和代码筛选（新增步骤）
        if not is_valid_a_share(stock_code, stock_name):
            return None # 排除非 A 股、创业板和 ST 股

        # 1. 读取数据
        df = pd.read_csv(file_path, parse_dates=['Date']).sort_values(by='Date').dropna()
        
        # 2. 检查蜡烛图形态
        if check_reversal_pattern(df):
            latest_close = df.iloc[-1]['Close']
            latest_date = df.iloc[-1]['Date'].strftime('%Y-%m-%d')
            return {'Code': stock_code, 'Close': latest_close, 'Date': latest_date}
            
    except Exception as e:
        # print(f"Error processing {file_path}: {e}")
        pass
        
    return None

def main():
    # 1. 预加载股票代码和名称映射
    try:
        stock_names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        stock_names_map = stock_names_df.set_index('Code')['Name'].to_dict()
    except FileNotFoundError:
        print(f"Warning: {STOCK_NAMES_FILE} not found. Proceeding without name matching.")
        stock_names_map = {}
    except Exception as e:
        print(f"Error loading {STOCK_NAMES_FILE}: {e}")
        return

    # 2. 获取所有 CSV 文件路径
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    
    if not all_files:
        print(f"Error: No CSV files found in {STOCK_DATA_DIR}. Exiting.")
        return

    # 3. 并行处理文件（使用 lambda 传递额外的参数 stock_names_map）
    print(f"Scanning {len(all_files)} files using {cpu_count()} cores...")
    
    # 为了在 Pool.map 中传递额外参数，我们使用列表推导式构造参数列表
    tasks = [(file, stock_names_map) for file in all_files]
    
    def process_wrapper(args):
        return process_file(*args)
        
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_wrapper, tasks)

    # 4. 过滤有效结果
    matches = [res for res in results if res is not None]
    
    if not matches:
        print("No matching bullish reversal stocks found.")
        return

    # 5. 格式化和保存结果
    matched_df = pd.DataFrame(matches)
    
    # 匹配名称 (因为在 process_file 中已经筛选过，这里只需合并)
    final_output_df = pd.merge(
        matched_df, 
        stock_names_df[['Code', 'Name']], 
        on='Code', 
        how='left'
    )
    # 调整列顺序
    final_output_df = final_output_df[['Code', 'Name', 'Date', 'Close']]
            
    # 6. 结果保存
    current_time = datetime.now(tz=None)
    output_ts = current_time.strftime('%Y%m%d%H%M%S')
    output_date_path = current_time.strftime('%Y/%m')
    output_filename = f'{output_ts}_bullish_reversal_stocks.csv'
    
    os.makedirs(output_date_path, exist_ok=True)
    output_path = os.path.join(output_date_path, output_filename)
    
    final_output_df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Successfully screened {len(final_output_df)} stocks.")
    print(f"Results saved to: {output_path}")

if __name__ == '__main__':
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Creating stock data directory: {STOCK_DATA_DIR}")
        os.makedirs(STOCK_DATA_DIR, exist_ok=True)
        
    main()
