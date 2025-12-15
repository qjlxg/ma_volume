import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 设置上海时区
TZ_SHANGHAI = 'Asia/Shanghai'

# --- 配置 (更新) ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0    # 最新收盘价不能低于 5.0 元
MAX_CLOSE_PRICE = 20.0   # 最新收盘价不能高于 20.0 元
# 结果保存目录将动态生成：output/YYYY/MM/
OUTPUT_BASE_DIR = 'output'
# -----------------

def check_stock_validity(stock_code, latest_close, stock_name):
    """
    检查股票是否符合筛选范围和价格要求。
    """
    
    # 1. 价格区间检查
    if not (MIN_CLOSE_PRICE <= latest_close <= MAX_CLOSE_PRICE):
        # print(f"排除 {stock_code} ({stock_name}): 最新价 {latest_close} 不在 [{MIN_CLOSE_PRICE}, {MAX_CLOSE_PRICE}] 范围内。")
        return False

    # 2. 排除 ST 股票 (名称中包含 'ST')
    if stock_name and 'ST' in stock_name.upper():
        # print(f"排除 {stock_code} ({stock_name}): 包含 'ST'。")
        return False
        
    # 3. 排除 30 开头的创业板 (深市)
    if stock_code.startswith('30'):
        # print(f"排除 {stock_code} ({stock_name}): 属于创业板 (30开头)。")
        return False

    # 4. 只保留深沪 A 股（即排除其他类型，如B股、科创板(688开头)、北交所(8开头)等，
    #    这里简化为只保留以 6 (沪市A股) 或 0 (深市A股) 开头，且不被 30 开头排除的股票）
    if not (stock_code.startswith('6') or stock_code.startswith('0')):
        # print(f"排除 {stock_code} ({stock_name}): 非标准沪深A股代码开头。")
        return False
        
    return True

def is_rising_three_methods(df):
    """
    判断 K 线序列是否符合“叠形多方炮” (Rising Three Methods) 形态。
    （逻辑保持不变，详见上一回复）
    """
    if len(df) < 5:
        return False

    df_5 = df.iloc[-5:]
    if df_5[['Open', 'High', 'Low', 'Close']].isnull().any().any():
        return False

    is_bullish_1 = df_5.iloc[0]['Close'] > df_5.iloc[0]['Open']
    is_bullish_5 = df_5.iloc[4]['Close'] > df_5.iloc[4]['Open']
    
    # 1. 第 1 根是长阳线
    range_1 = df_5.iloc[0]['High'] - df_5.iloc[0]['Low']
    body_1 = abs(df_5.iloc[0]['Close'] - df_5.iloc[0]['Open'])
    
    if not (is_bullish_1 and range_1 > 0 and body_1 / range_1 > 0.6):
        return False

    # 2. 中间 3 根 (第 2, 3, 4 根) 是小实体 K 线，被第 1 根实体包含
    middle_3 = df_5.iloc[1:4]
    body_low_1 = min(df_5.iloc[0]['Open'], df_5.iloc[0]['Close'])
    body_high_1 = max(df_5.iloc[0]['Open'], df_5.iloc[0]['Close'])
    
    is_contained = (middle_3['High'].max() < body_high_1) and \
                   (middle_3['Low'].min() > body_low_1)
    
    if not is_contained:
        return False

    # 3. 第 5 根是长阳线，且收盘价高于第 1 根的最高价
    is_breaking_out = (is_bullish_5) and \
                      (df_5.iloc[4]['Close'] > df_5.iloc[0]['High'])

    if not is_breaking_out:
        return False

    return True

# 加载股票名称字典 (在主进程中加载一次)
STOCK_NAMES_DICT = {}
def load_stock_names():
    global STOCK_NAMES_DICT
    try:
        df_names = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        # 假设 stock_names.csv 有两列：'Code' 和 'Name'
        STOCK_NAMES_DICT = df_names.set_index('Code')['Name'].to_dict()
    except FileNotFoundError:
        print(f"警告：未找到股票名称文件 '{STOCK_NAMES_FILE}'。")

def process_single_stock(file_path):
    """处理单个股票的 CSV 文件，检查形态和所有筛选条件并返回结果。"""
    try:
        # 从文件名中提取股票代码
        basename = os.path.basename(file_path)
        stock_code = basename.split('.')[0]
        
        # 1. 读取数据并排序
        df = pd.read_csv(file_path, parse_dates=['Date'])
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        if df.empty:
            return None
            
        latest_close = df.iloc[-1]['Close']
        stock_name = STOCK_NAMES_DICT.get(stock_code, '名称未知')

        # 2. 检查股票的有效性 (代码、名称和价格)
        if not check_stock_validity(stock_code, latest_close, stock_name):
            return None

        # 3. 检查 K 线形态
        if is_rising_three_methods(df):
            latest_date = df.iloc[-1]['Date'].strftime('%Y-%m-%d')
            return {'Code': stock_code, 'Date': latest_date, 'Status': 'Success', 'StockName': stock_name}
        
        return None
    except Exception as e:
        # print(f"Error processing {file_path}: {e}")
        return None

def main():
    """主函数：并行扫描所有股票数据并保存结果。"""
    print(f"--- 股票形态扫描开始 (筛选条件：叠形多方炮，最新价 [${MIN_CLOSE_PRICE}, ${MAX_CLOSE_PRICE}]，排除ST/30开头) ---")
    
    # 在主进程中加载股票名称
    load_stock_names()
    
    # 1. 扫描所有数据文件路径
    data_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not data_files:
        print(f"错误：在目录 '{STOCK_DATA_DIR}' 中未找到任何 CSV 文件。")
        return

    # 2. 使用多进程并行处理
    print(f"找到 {len(data_files)} 个股票文件，使用 {cpu_count()} 核心并行处理...")
    
    results = []
    with Pool(cpu_count()) as p:
        # map() 会将 process_single_stock 函数应用到 data_files 列表中的每个元素
        results = p.map(process_single_stock, data_files)

    # 过滤掉 None 的结果
    filtered_results = [r for r in results if r is not None]
    
    if not filtered_results:
        print("扫描完成，未找到任何符合所有条件的股票。")
        return

    # 3. 将结果转换为 DataFrame
    df_result = pd.DataFrame(filtered_results)
    print(f"扫描完成，找到 {len(df_result)} 个符合条件的股票。")
        
    # 4. 准备输出文件路径
    now = datetime.now(pd.to_datetime('now').tz_localize(TZ_SHANGHAI))
    
    # 目录：output/YYYY/MM
    output_dir = os.path.join(OUTPUT_BASE_DIR, now.strftime('%Y'), now.strftime('%m'))
    # 文件名：rising_three_methods_YYYYMMDD_HHMMSS.csv
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f"rising_three_methods_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # 5. 保存结果 (包含代码和名称)
    os.makedirs(output_dir, exist_ok=True)
    df_result[['Code', 'StockName', 'Date', 'Status']].to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"成功将筛选结果保存到: {output_path}")

if __name__ == "__main__":
    main()
