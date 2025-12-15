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
MIN_CLOSE_PRICE = 5.0
LOWER_SHADOW_RATIO = 0.6

# --- 中文列名映射 ---
# 确保脚本能识别您的数据格式
COLUMNS_MAP = {
    'Open': '开盘',
    'Close': '收盘',
    'High': '最高',
    'Low': '最低'
}
REQUIRED_COLS = list(COLUMNS_MAP.values())

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_stock_names(filepath):
    """加载股票代码和名称的映射表"""
    try:
        # 假设 stock_names.csv 包含 'code' 和 'name' 列
        df = pd.read_csv(filepath, dtype={'code': str})
        # 确保代码是6位数，如果不是，前面补零
        df['code'] = df['code'].apply(lambda x: x.zfill(6))
        return df.set_index('code')['name'].to_dict()
    except Exception as e:
        logging.error(f"加载股票名称文件失败: {e}")
        return {}

def process_file(file_path):
    """
    处理单个 CSV 文件，筛选符合条件的股票。
    现在假设 CSV 文件表头使用中文: 日期, 股票代码, 开盘, 收盘, 最高, 最低, ...
    """
    try:
        # 从文件名中提取股票代码，假设文件名是 XXXXXX.csv
        basename = os.path.basename(file_path)
        stock_code = os.path.splitext(basename)[0].zfill(6)
        
        # 使用 engine='python' 避免 C engine 无法处理中文字符集的问题
        df = pd.read_csv(file_path, engine='python')

        if df.empty:
            logging.warning(f"文件 {basename} 是空的。跳过。")
            return None

        # --- 鲁棒性增强：检查必需的中文列 ---
        if not all(col in df.columns for col in REQUIRED_COLS):
            missing_cols = [col for col in REQUIRED_COLS if col not in df.columns]
            # 记录警告，明确指出缺少哪些关键列
            logging.warning(f"文件 {basename} (代码: {stock_code}) 缺少必需的中文列: {missing_cols}。跳过。")
            return None
        # --- 鲁棒性增强结束 ---
        
        # 确保数据已按时间排序，取最后一行（最新数据）
        latest_data = df.iloc[-1]
        
        # 提取关键价格，使用中文列名
        close = latest_data[COLUMNS_MAP['Close']]
        open_price = latest_data[COLUMNS_MAP['Open']]
        high = latest_data[COLUMNS_MAP['High']]
        low = latest_data[COLUMNS_MAP['Low']]

        # 1. 最新收盘价不能低于 5.0 元
        if close < MIN_CLOSE_PRICE:
            return None
        
        # 2. 筛选带显著下影线的K线（止跌信号）
        total_range = high - low
        
        # 避免除以接近零的值（即当日价格波动极小）
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
        # 捕获其他可能的错误，例如数据类型转换错误
        logging.warning(f"处理文件 {file_path} 时发生错误: {e}")
        return None

def main():
    start_time = datetime.now()
    logging.info("--- 启动股票筛选程序 ---")
    
    # 1. 加载股票名称映射
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    if not stock_names:
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
    with Pool(cpu_count()) as pool:
        results = pool.map(process_file, all_files)

    # 4. 收集和整理结果
    filtered_codes = [code for code in results if code is not None]

    if not filtered_codes:
        logging.info("未筛选到任何符合条件的股票。")
        result_df = pd.DataFrame(columns=['Code', 'Name'])
    else:
        result_list = []
        for code in filtered_codes:
            name = stock_names.get(code, '未知名称')
            result_list.append({'Code': code, 'Name': name})
        
        result_df = pd.DataFrame(result_list)
        logging.info(f"筛选到 {len(result_df)} 个符合条件的股票。")

    # 5. 生成带时间戳的输出文件名并保存
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now_shanghai = datetime.now(shanghai_tz)
    
    output_dir = now_shanghai.strftime('%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp_str = now_shanghai.strftime('%Y%m%d_%H%M%S')
    output_filename = f"result_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # 保存结果
    result_df.to_csv(output_path, index=False, encoding='utf-8')
    logging.info(f"筛选结果已保存至: {output_path}")
    
    end_time = datetime.now()
    logging.info(f"--- 筛选程序运行结束，耗时: {end_time - start_time} ---")

if __name__ == "__main__":
    main()
