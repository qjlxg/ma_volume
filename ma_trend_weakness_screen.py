# ma_trend_weakness_screen.py

import os
import glob
import pandas as pd
from multiprocessing import Pool, cpu_count
from datetime import datetime
import pytz

# --- 配置 ---
STOCK_DATA_DIR = "stock_data"
STOCK_NAMES_FILE = "stock_names.csv"
MA_PERIOD = 20  # 20日均线
SLOPE_CHECK_DAYS = 5 # 检查均线斜率的周期（例如：最近5天的均线变化）
MIN_CLOSE_PRICE = 5.0 # 最低收盘价要求

def calculate_ma(df, period):
    """计算指定周期的移动平均线 (MA)"""
    return df['Close'].rolling(window=period).mean()

def screen_stock(filepath):
    """
    对单个股票文件进行筛选。
    筛选条件：
    1. 最新收盘价 >= 5.0 元。
    2. 20日均线 (MA20) 处于走平或下降趋势 (即最新的MA20值不高于N天前的MA20值)。
    """
    try:
        # 1. 读取数据
        # 假设CSV文件包含 'Date' 和 'Close' 列，且最新数据在最后
        df = pd.read_csv(filepath)
        df = df.dropna(subset=['Close']).sort_values(by='Date').reset_index(drop=True)
        
        if df.empty or len(df) < MA_PERIOD + SLOPE_CHECK_DAYS:
            # 数据不足以计算均线和斜率
            return None

        # 2. 核心数据准备
        df['MA20'] = calculate_ma(df, MA_PERIOD)
        latest_data = df.iloc[-1]
        
        # 3. 筛选条件 1: 最新收盘价
        latest_close = latest_data['Close']
        if latest_close < MIN_CLOSE_PRICE:
            return None

        # 4. 筛选条件 2: 20日均线趋势 (走平或向下)
        latest_ma20 = latest_data['MA20']
        
        # 获取 N 天前的 MA20 值。注意，如果 SLOPE_CHECK_DAYS 超出数据范围，会引发 IndexError。
        # 这里已经检查了数据长度，所以 - (SLOPE_CHECK_DAYS + 1) 是安全的索引。
        ma20_n_days_ago = df.iloc[-(SLOPE_CHECK_DAYS + 1)]['MA20']
        
        # 核心逻辑：最新的 MA20 不大于前 N 天的 MA20，即斜率为负或零。
        is_ma20_weakening = latest_ma20 <= ma20_n_days_ago
        
        if is_ma20_weakening:
            # 提取股票代码
            stock_code = os.path.basename(filepath).replace(".csv", "")
            return {
                'Code': stock_code,
                'Latest_Close': latest_close,
                'Latest_MA20': latest_ma20,
                'MA20_N_Days_Ago': ma20_n_days_ago
            }

    except Exception as e:
        print(f"处理文件 {filepath} 失败: {e}")
        return None
    
    return None

def main():
    # 1. 查找所有股票数据文件
    file_list = glob.glob(os.path.join(STOCK_DATA_DIR, "*.csv"))
    
    if not file_list:
        print(f"未在目录 {STOCK_DATA_DIR} 中找到任何CSV文件。")
        return

    # 2. 并行处理文件
    print(f"找到 {len(file_list)} 个文件，使用 {cpu_count()} 个核心并行处理...")
    with Pool(cpu_count()) as p:
        results = p.map(screen_stock, file_list)
        
    # 过滤掉 None 值
    successful_results = [r for r in results if r is not None]
    
    if not successful_results:
        print("没有股票符合筛选条件。")
        return

    # 3. 合并结果
    screened_df = pd.DataFrame(successful_results)
    print(f"共筛选出 {len(screened_df)} 支符合条件的股票。")

    # 4. 匹配股票名称
    try:
        # 假设 stock_names.csv 包含 'Code' 和 'Name' 列
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        
        # 确保 Code 列是字符串类型，并去除扩展名
        names_df['Code'] = names_df['Code'].apply(lambda x: str(x).replace(".csv", ""))
        
        final_df = pd.merge(screened_df, names_df[['Code', 'Name']], on='Code', how='left')
        final_df['Name'] = final_df['Name'].fillna('名称未知')

    except Exception as e:
        print(f"读取或匹配股票名称文件 {STOCK_NAMES_FILE} 失败: {e}")
        final_df = screened_df
        final_df['Name'] = '名称未匹配'

    # 5. 设置上海时区并生成文件名
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now_shanghai = datetime.now(shanghai_tz)
    
    # 路径格式: YYYY/MM/filename_timestamp.csv
    output_dir = now_shanghai.strftime("%Y/%m")
    timestamp_str = now_shanghai.strftime("%Y%m%d_%H%M%S")
    output_filename = f"ma_trend_weakness_results_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)

    # 6. 保存结果
    os.makedirs(output_dir, exist_ok=True)
    final_df[['Code', 'Name', 'Latest_Close', 'Latest_MA20', 'MA20_N_Days_Ago']].to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"筛选结果已成功保存到: {output_path}")

if __name__ == "__main__":
    main()
