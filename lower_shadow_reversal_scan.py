import pandas as pd
import os
import glob
import logging
from multiprocessing import Pool, cpu_count
from datetime import datetime
import pytz

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'

# --- 筛选条件收紧和限定 ---
MIN_CLOSE_PRICE = 5.0
MAX_CLOSE_PRICE = 20.0  # 新增：收盘价不高于 20.0 元
LOWER_SHADOW_RATIO = 0.75
MIN_TURNOVER_RATE = 1.0

# --- 中文列名映射 ---
COLUMNS_MAP = {
    'Open': '开盘',
    'Close': '收盘',
    'High': '最高',
    'Low': '最低',
    'TurnoverRate': '换手率'
}
REQUIRED_COLS = list(COLUMNS_MAP.values())

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def load_stock_names(filepath):
    """加载股票代码和名称的映射表，并返回包含代码、名称的 DataFrame"""
    try:
        # 假设 stock_names.csv 包含 'code' 和 'name' 列
        df = pd.read_csv(filepath, dtype={'code': str})
        df['code'] = df['code'].apply(lambda x: x.zfill(6))
        
        # 将代码和名称作为字典返回，用于后续匹配
        names_dict = df.set_index('code')['name'].to_dict()
        
        # 返回用于排除 ST 股的完整DataFrame
        return df, names_dict 
    except Exception as e:
        logging.error(f"加载股票名称文件失败: {e}")
        return pd.DataFrame(), {}

def check_exclusions(stock_code, stock_name):
    """
    检查股票是否符合排除条件（ST股、创业板、非A股）
    返回 True 表示排除，False 表示保留
    """
    # 1. 排除 ST 股
    if '*ST' in stock_name or 'ST' in stock_name:
        return True, "ST 股"

    # 2. 排除 300 开头（创业板）
    if stock_code.startswith('300'):
        return True, "创业板 (300开头)"
        
    # 3. 只保留深沪A股（主板和科创板，但创业板已排除）
    # A股代码范围通常是：
    # 600, 601, 603, 605, 688 开头 (沪市)
    # 000, 001, 002, 003 开头 (深市)
    if not (stock_code.startswith(('60', '68')) or stock_code.startswith(('00', '30'))):
        # 排除所有非上述开头的代码，如900（B股）、200（B股）等
        return True, "非深沪A股"

    # 3. 排除其他非A股（如B股、新三板等，此条件主要由上面的代码开头检查覆盖，但可以更精细化）
    # 确保是A股代码，避免误选如北交所 8/4 开头等
    if stock_code.startswith(('1', '2', '4', '8', '9')): 
        return True, "非深沪A股"

    return False, "" # 保留

def process_file(file_path, stock_names_dict):
    """
    处理单个 CSV 文件，筛选符合条件的股票。
    """
    try:
        basename = os.path.basename(file_path)
        stock_code = os.path.splitext(basename)[0].zfill(6)
        stock_name = stock_names_dict.get(stock_code, '未知名称')

        # --- 0. 排除股票类型检查 ---
        should_exclude, reason = check_exclusions(stock_code, stock_name)
        if should_exclude:
            # logging.debug(f"Code {stock_code} excluded: {reason}") # 调试时可开启
            return None
        
        df = pd.read_csv(file_path, engine='python')

        if df.empty:
            return None

        # --- 鲁棒性增强：检查必需的中文列 (包含换手率) ---
        if not all(col in df.columns for col in REQUIRED_COLS):
            # 警告在之前运行中已记录，此处跳过以加速
            return None
        
        latest_data = df.iloc[-1]
        
        # 提取关键价格和换手率
        close = latest_data[COLUMNS_MAP['Close']]
        open_price = latest_data[COLUMNS_MAP['Open']]
        high = latest_data[COLUMNS_MAP['High']]
        low = latest_data[COLUMNS_MAP['Low']]
        turnover_rate = latest_data[COLUMNS_MAP['TurnoverRate']]

        # 1. 价格区间限定
        if not (MIN_CLOSE_PRICE <= close <= MAX_CLOSE_PRICE):
            return None
        
        # 2. 换手率必须高于 MIN_TURNOVER_RATE
        if turnover_rate < MIN_TURNOVER_RATE:
            return None
            
        # 3. 筛选带显著下影线的K线（使用更高的比例）
        total_range = high - low
        
        if total_range < 0.01: 
            return None

        # 下影线长度：较小值(开盘价, 收盘价) - 最低价
        lower_shadow = min(open_price, close) - low
        
        # 下影线占总区间比例
        ratio = lower_shadow / total_range

        if ratio >= LOWER_SHADOW_RATIO:
            # 返回符合条件的股票代码
            return stock_code
        
        return None

    except Exception as e:
        # 捕获其他可能的错误
        logging.warning(f"处理文件 {file_path} 时发生错误: {e}")
        return None

def main():
    start_time = datetime.now()
    logging.info("--- 启动股票筛选程序 (最终限定条件) ---")
    
    # 1. 加载股票名称映射 (包含用于排除 ST 股的完整列表)
    stock_names_df, stock_names_dict = load_stock_names(STOCK_NAMES_FILE)
    if stock_names_df.empty:
        logging.error("无法获取股票名称数据，程序终止。")
        return

    # 2. 获取所有数据文件列表
    search_path = os.path.join(STOCK_DATA_DIR, '*.csv')
    all_files = glob.glob(search_path)
    
    if not all_files:
        logging.error(f"在目录 {STOCK_DATA_DIR} 中未找到任何 CSV 文件。")
        return

    logging.info(f"找到 {len(all_files)} 个数据文件，开始并行处理...")

    # 3. 使用多进程并行处理
    # 传递 stock_names_dict 给 process_file 函数
    process_args = [(f, stock_names_dict) for f in all_files]
    
    with Pool(cpu_count()) as pool:
        # pool.starmap 用于传递多个参数
        results = pool.starmap(process_file, process_args)

    # 4. 收集和整理结果
    filtered_codes = [code for code in results if code is not None]

    if not filtered_codes:
        logging.info("未筛选到任何符合条件的股票。")
        result_df = pd.DataFrame(columns=['Code', 'Name'])
    else:
        result_list = []
        for code in filtered_codes:
            name = stock_names_dict.get(code, '未知名称')
            result_list.append({'Code': code, 'Name': name})
        
        result_df = pd.DataFrame(result_list).drop_duplicates(subset=['Code']) # 去重
        logging.info(f"筛选到 {len(result_df)} 个符合最终限定条件的股票。")

    # 5. 生成带时间戳的输出文件名并保存
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now_shanghai = datetime.now(shanghai_tz)
    
    output_dir = now_shanghai.strftime('%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp_str = now_shanghai.strftime('%Y%m%d_%H%M%S')
    output_filename = f"result_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    result_df.to_csv(output_path, index=False, encoding='utf-8')
    logging.info(f"筛选结果已保存至: {output_path}")
    
    end_time = datetime.now()
    logging.info(f"--- 筛选程序运行结束，耗时: {end_time - start_time} ---")

if __name__ == "__main__":
    main()
