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

# --- 筛选条件收紧 ---
LOWER_SHADOW_RATIO = 0.75  # 增加到 75%，下影线必须占据总价格区间的 75% 以上
MIN_TURNOVER_RATE = 1.0    # 新增条件：要求当日换手率至少为 1.0%

# --- 中文列名映射 ---
COLUMNS_MAP = {
    'Open': '开盘',
    'Close': '收盘',
    'High': '最高',
    'Low': '最低',
    'TurnoverRate': '换手率' # 引入换手率的中文列名
}
REQUIRED_COLS = list(COLUMNS_MAP.values())

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_stock_names(filepath):
    # ... (此函数未更改) ...
    try:
        df = pd.read_csv(filepath, dtype={'code': str})
        df['code'] = df['code'].apply(lambda x: x.zfill(6))
        return df.set_index('code')['name'].to_dict()
    except Exception as e:
        logging.error(f"加载股票名称文件失败: {e}")
        return {}

def process_file(file_path):
    """
    处理单个 CSV 文件，筛选符合条件的股票。
    """
    try:
        basename = os.path.basename(file_path)
        stock_code = os.path.splitext(basename)[0].zfill(6)
        
        # 增加换手率列的检查
        current_required_cols = REQUIRED_COLS
        df = pd.read_csv(file_path, engine='python')

        if df.empty:
            logging.warning(f"文件 {basename} 是空的。跳过。")
            return None

        # --- 鲁棒性增强：检查必需的中文列 (包含换手率) ---
        if not all(col in df.columns for col in current_required_cols):
            missing_cols = [col for col in current_required_cols if col not in df.columns]
            logging.warning(f"文件 {basename} (代码: {stock_code}) 缺少必需的中文列: {missing_cols}。跳过。")
            return None
        # --- 鲁棒性增强结束 ---
        
        latest_data = df.iloc[-1]
        
        # 提取关键价格和换手率
        close = latest_data[COLUMNS_MAP['Close']]
        open_price = latest_data[COLUMNS_MAP['Open']]
        high = latest_data[COLUMNS_MAP['High']]
        low = latest_data[COLUMNS_MAP['Low']]
        turnover_rate = latest_data[COLUMNS_MAP['TurnoverRate']]

        # 1. 最新收盘价不能低于 5.0 元
        if close < MIN_CLOSE_PRICE:
            return None
        
        # 2. 新增条件：换手率必须高于 MIN_TURNOVER_RATE
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
        logging.warning(f"处理文件 {file_path} 时发生错误: {e}")
        return None

def main():
    start_time = datetime.now()
    logging.info("--- 启动股票筛选程序 (已收紧条件) ---")
    
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
