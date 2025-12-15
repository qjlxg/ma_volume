import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 设置上海时区
TZ_SHANGHAI = 'Asia/Shanghai'

# --- 运行配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0    # 最新收盘价不能低于 5.0 元
MAX_CLOSE_PRICE = 20.0   # 最新收盘价不能高于 20.0 元
OUTPUT_BASE_DIR = 'output'
# -----------------

# 全局变量用于存储股票名称，避免在多进程中重复加载
STOCK_NAMES_DICT = {}

def load_stock_names():
    """
    加载股票名称字典，兼容用户提供的 'code' 和 'name' 小写列名。
    """
    global STOCK_NAMES_DICT
    try:
        # 假设 stock_names.csv 是逗号分隔 (如果不是，需要指定 sep='\t' 或其他)
        df_names = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
        
        # 使用用户提供的列名 'code' 和 'name'
        STOCK_NAMES_DICT = df_names.set_index('code')['name'].to_dict()
        print(f"成功加载 {len(STOCK_NAMES_DICT)} 个股票名称。")
    except FileNotFoundError:
        print(f"警告：未找到股票名称文件 '{STOCK_NAMES_FILE}'。")
    except KeyError as e:
        print(f"致命错误：股票名称文件 '{STOCK_NAMES_FILE}' 中的列名不匹配。请确保文件包含 'code' 和 'name' (小写) 两列。原始错误: {e}")
        raise # 抛出错误以确保工作流失败

def check_stock_validity(stock_code, latest_close, stock_name):
    """
    检查股票是否符合所有筛选要求：深沪A股, 价格 [5.0, 20.0], 排除ST/创业板。
    """
    
    # 1. 价格区间检查
    if not (MIN_CLOSE_PRICE <= latest_close <= MAX_CLOSE_PRICE):
        return False

    # 2. 排除 ST 股票 (名称中包含 'ST')
    if stock_name and 'ST' in stock_name.upper():
        return False
        
    # 3. 排除 30 开头的创业板 (深市)
    if stock_code.startswith('30'):
        return False

    # 4. 只保留标准的深沪 A 股 (6开头沪市A股，0开头深市A股)
    if not (stock_code.startswith('6') or stock_code.startswith('0')):
        return False
        
    return True

def is_rising_three_methods(df):
    """
    判断 K 线序列是否符合“叠形多方炮” (Rising Three Methods) 形态。
    

[Image of Rising Three Methods Candlestick Pattern]

    形态判断基于最后 5 根 K 线：
    1. K1: 长阳线。
    2. K2, K3, K4: 小实体 K 线，其高低点范围完全被 K1 的实体包含。
    3. K5: 长阳线，收盘价高于 K1 的最高价 (突破确认)。
    """
    if len(df) < 5:
        return False

    # 取最后 5 个交易日的数据
    df_5 = df.iloc[-5:]
    if df_5[['Open', 'High', 'Low', 'Close']].isnull().any().any():
        return False

    # K 线颜色：收盘 > 开盘 为阳线 (上涨)
    is_bullish_1 = df_5.iloc[0]['Close'] > df_5.iloc[0]['Open']
    is_bullish_5 = df_5.iloc[4]['Close'] > df_5.iloc[4]['Open']
    
    # 1. K1 检查：长阳线 (实体/范围 > 0.6 作为长阳线的简化标准)
    range_1 = df_5.iloc[0]['High'] - df_5.iloc[0]['Low']
    body_1 = abs(df_5.iloc[0]['Close'] - df_5.iloc[0]['Open'])
    
    if not (is_bullish_1 and range_1 > 0 and body_1 / range_1 > 0.6):
        return False

    # 2. K2, K3, K4 检查：整理 K 线，且被 K1 实体包含
    middle_3 = df_5.iloc[1:4]
    body_low_1 = min(df_5.iloc[0]['Open'], df_5.iloc[0]['Close'])
    body_high_1 = max(df_5.iloc[0]['Open'], df_5.iloc[0]['Close'])
    
    # 检查中间 3 根 K 线的最高价和最低价是否完全在 K1 的实体范围内
    is_contained = (middle_3['High'].max() < body_high_1) and \
                   (middle_3['Low'].min() > body_low_1)
    
    if not is_contained:
        return False

    # 3. K5 检查：长阳线突破
    is_breaking_out = (is_bullish_5) and \
                      (df_5.iloc[4]['Close'] > df_5.iloc[0]['High']) # 收盘价突破K1最高价

    if not is_breaking_out:
        return False

    return True

def process_single_stock(file_path):
    """处理单个股票的 CSV 文件，检查形态和所有筛选条件。"""
    try:
        # 从文件名中提取股票代码
        basename = os.path.basename(file_path)
        stock_code = basename.split('.')[0]
        
        # 1. 读取数据并排序
        # 兼容用户提供的中文列名
        df = pd.read_csv(file_path, parse_dates=['日期'])
        df = df.rename(columns={'日期': 'Date', '开盘': 'Open', '收盘': 'Close', 
                                '最高': 'High', '最低': 'Low'})
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        if df.empty:
            return None
            
        latest_close = df.iloc[-1]['Close']
        # 从字典中获取名称
        stock_name = STOCK_NAMES_DICT.get(stock_code, '名称未知')

        # 2. 检查股票的有效性 (代码、名称和价格)
        if not check_stock_validity(stock_code, latest_close, stock_name):
            return None

        # 3. 检查 K 线形态
        if is_rising_three_methods(df):
            latest_date = df.iloc[-1]['Date'].strftime('%Y-%m-%d')
            # 返回结果字典
            return {'Code': stock_code, 'StockName': stock_name, 'Date': latest_date, 'LatestClose': latest_close}
        
        return None
    except Exception as e:
        # print(f"Error processing {file_path}: {e}")
        return None

def main():
    """主函数：加载名称，并行扫描数据，整理并保存结果。"""
    print(f"--- 股票形态扫描开始 (筛选条件：叠形多方炮，最新价 [${MIN_CLOSE_PRICE}, ${MAX_CLOSE_PRICE}]，排除ST/30开头) ---")
    
    # 1. 在主进程中加载股票名称 (已修复兼容性)
    load_stock_names()
    
    # 2. 扫描所有数据文件路径
    data_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not data_files:
        print(f"错误：在目录 '{STOCK_DATA_DIR}' 中未找到任何 CSV 文件。")
        return

    # 3. 使用多进程并行处理
    print(f"找到 {len(data_files)} 个股票文件，使用 {cpu_count()} 核心并行处理...")
    
    results = []
    with Pool(cpu_count()) as p:
        results = p.map(process_single_stock, data_files)

    # 过滤掉 None 的结果
    filtered_results = [r for r in results if r is not None]
    
    if not filtered_results:
        print("扫描完成，未找到任何符合所有条件的股票。")
        return

    # 4. 将结果转换为 DataFrame
    df_result = pd.DataFrame(filtered_results)
    print(f"扫描完成，找到 {len(df_result)} 个符合条件的股票。")
        
    # 5. 准备输出文件路径 (年月目录, 文件名加时间戳, 上海时区)
    now = datetime.now(pd.to_datetime('now').tz_localize(TZ_SHANGHAI))
    
    output_dir = os.path.join(OUTPUT_BASE_DIR, now.strftime('%Y'), now.strftime('%m'))
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f"rising_three_methods_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # 6. 保存结果
    os.makedirs(output_dir, exist_ok=True)
    # 按照代码、名称、价格、日期顺序输出
    df_result[['Code', 'StockName', 'LatestClose', 'Date']].to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"成功将筛选结果保存到: {output_path}")

if __name__ == "__main__":
    main()
