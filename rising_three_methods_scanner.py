import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 设置上海时区
TZ_SHANGHAI = 'Asia/Shanghai'

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0  # 最新收盘价不能低于 5.0 元
# 结果保存目录将动态生成：output/YYYY/MM/
OUTPUT_BASE_DIR = 'output'
# -----------------

def is_rising_three_methods(df):
    """
    判断 K 线序列是否符合“叠形多方炮” (Rising Three Methods) 形态。
    形态要求：
    1. 必须至少有 5 根 K 线。
    2. 第 1 根是长阳线 (长红实体)。
    3. 中间 3 根是小实体 K 线，其交易范围 (高低价) 都在第 1 根 K 线的实体范围内。
    4. 第 5 根是长阳线，其收盘价高于第 1 根 K 线的最高价 (或至少创新高)。

    注意：为了简化和在历史数据中找到形态，我们只检查最后 5 个交易日。
    """
    if len(df) < 5:
        return False

    # 取最后 5 个交易日的数据
    df_5 = df.iloc[-5:]
    
    # 检查所有 K 线数据是否完整
    if df_5[['Open', 'High', 'Low', 'Close']].isnull().any().any():
        return False

    # K 线颜色判断：收盘价 > 开盘价 为阳线 (上涨)，收盘价 < 开盘价 为阴线 (下跌)
    is_bullish_1 = df_5.iloc[0]['Close'] > df_5.iloc[0]['Open']
    is_bullish_5 = df_5.iloc[4]['Close'] > df_5.iloc[4]['Open']
    
    # 1. 第 1 根是长阳线 (此处用实体长度作为简化判断，实体/高低点范围 > 0.6)
    range_1 = df_5.iloc[0]['High'] - df_5.iloc[0]['Low']
    body_1 = abs(df_5.iloc[0]['Close'] - df_5.iloc[0]['Open'])
    
    if not (is_bullish_1 and range_1 > 0 and body_1 / range_1 > 0.6):
        return False

    # 2. 中间 3 根 (第 2, 3, 4 根) 是小实体 K 线
    middle_3 = df_5.iloc[1:4]
    
    # K 线实体范围
    body_low_1 = min(df_5.iloc[0]['Open'], df_5.iloc[0]['Close'])
    body_high_1 = max(df_5.iloc[0]['Open'], df_5.iloc[0]['Close'])
    
    # 检查中间 3 根是否被第 1 根的实体包含
    is_contained = (middle_3['High'].max() < body_high_1) and \
                   (middle_3['Low'].min() > body_low_1)
    
    if not is_contained:
        return False

    # 3. 第 5 根是长阳线，且收盘价高于第 1 根的最高价
    is_breaking_out = (is_bullish_5) and \
                      (df_5.iloc[4]['Close'] > df_5.iloc[0]['High'])

    if not is_breaking_out:
        return False

    # 4. 最新收盘价不能低于 5.0 元
    if df_5.iloc[4]['Close'] < MIN_CLOSE_PRICE:
        return False

    return True

def process_single_stock(file_path):
    """处理单个股票的 CSV 文件，检查形态并返回结果。"""
    try:
        # 从文件名中提取股票代码
        basename = os.path.basename(file_path)
        stock_code = basename.split('.')[0]
        
        # 假设 CSV 文件包含 'Date', 'Open', 'High', 'Low', 'Close', 'Volume' 列
        df = pd.read_csv(file_path, parse_dates=['Date'])
        # 确保按日期升序排序
        df = df.sort_values(by='Date').reset_index(drop=True)

        if is_rising_three_methods(df):
            # 获取最新的日期，用于结果输出
            latest_date = df.iloc[-1]['Date'].strftime('%Y-%m-%d')
            return {'Code': stock_code, 'Date': latest_date, 'Status': 'Success'}
        
        return None
    except Exception as e:
        # print(f"Error processing {file_path}: {e}")
        return None

def main():
    """主函数：并行扫描所有股票数据并保存结果。"""
    print(f"--- 股票形态扫描开始 (筛选条件：叠形多方炮，最新价 >= {MIN_CLOSE_PRICE} 元) ---")
    
    # 1. 扫描所有数据文件路径
    data_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not data_files:
        print(f"错误：在目录 '{STOCK_DATA_DIR}' 中未找到任何 CSV 文件。请确保数据已上传。")
        return

    # 2. 使用多进程并行处理
    print(f"找到 {len(data_files)} 个股票文件，使用 {cpu_count()} 核心并行处理...")
    
    results = []
    # 使用 Pool 进行并行计算
    with Pool(cpu_count()) as p:
        # map() 会将 process_single_stock 函数应用到 data_files 列表中的每个元素
        results = p.map(process_single_stock, data_files)

    # 过滤掉 None 的结果 (即未满足形态的股票)
    filtered_results = [r for r in results if r is not None]
    
    if not filtered_results:
        print("扫描完成，未找到任何符合 '叠形多方炮' 形态的股票。")
        return

    # 3. 将结果转换为 DataFrame
    df_result = pd.DataFrame(filtered_results)
    print(f"扫描完成，找到 {len(df_result)} 个符合形态的股票。")

    # 4. 读取股票名称文件进行匹配
    try:
        df_names = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        # 假设 stock_names.csv 有两列：'Code' (股票代码) 和 'Name' (股票名称)
        df_names = df_names.rename(columns={'Name': 'StockName'})
        
        # 合并结果和名称
        df_final = pd.merge(df_result, df_names[['Code', 'StockName']], on='Code', how='left')
        df_final['StockName'] = df_final['StockName'].fillna('名称未知')
        
    except FileNotFoundError:
        print(f"警告：未找到股票名称文件 '{STOCK_NAMES_FILE}'，结果将只包含代码。")
        df_final = df_result.copy()
        
    # 5. 准备输出文件路径
    now = datetime.now(pd.to_datetime('now').tz_localize(TZ_SHANGHAI))
    
    # 目录：output/YYYY/MM
    output_dir = os.path.join(OUTPUT_BASE_DIR, now.strftime('%Y'), now.strftime('%m'))
    # 文件名：rising_three_methods_YYYYMMDD_HHMMSS.csv
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f"rising_three_methods_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # 6. 保存结果
    os.makedirs(output_dir, exist_ok=True)
    df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"成功将筛选结果保存到: {output_path}")

if __name__ == "__main__":
    main()
