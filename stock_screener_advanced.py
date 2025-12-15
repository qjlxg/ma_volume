import pandas as pd
import os
import glob
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# 配置日志：设置为 WARNING 级别，使 GitHub Actions 运行日志更简洁
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_DIR = 'output'
MAX_WORKERS = 4  # 并行进程数

# --- 筛选条件 ---
MIN_PRICE = 5.0
MAX_PRICE = 20.0

# 定义CSV文件中的关键列名 (根据用户提供格式)
COL_DATE = '日期'
COL_CLOSE = '收盘'
COL_OPEN = '开盘'  # 💥 用于判断是否为阳线/红K线
COL_LOW = '最低'
COL_VOLUME = '成交量'

# --- 核心筛选函数 ---

def meets_tech_criteria(df: pd.DataFrame) -> bool:
    """
    实现图中的技术分析筛选条件 (基于量价和回踩确认)，采用更严格的逻辑。
    - 趋势：股价高于MA20，且MA20必须向上。
    - 回踩：当前价高于最近3天最低价，且最近3天最低价必须严格高于 3 天前的 MA10 支撑。
    - 放量：成交量高于 5 日均量的 2 倍，且必须是阳线。
    """
    # 确保有足够的数据来计算 MA20, MA10 和进行 3 天回踩检查 (至少 25 天)
    if df.empty or len(df) < 25: 
        return False

    # 1. 计算均线
    df['MA20'] = df[COL_CLOSE].rolling(window=20).mean()
    df['MA10'] = df[COL_CLOSE].rolling(window=10).mean()

    # 取最新的数据点
    latest = df.iloc[-1]
    
    # 最近三天的最低价 (模拟“三天不破”的最低点)
    recent_lows = df[COL_LOW].iloc[-3:].min()

    # --- 条件量化 ---
    
    # 尝试获取 3 个交易日前（倒数第 4 行）的 MA10 值作为历史支撑参考
    try:
        ma10_three_days_ago = df['MA10'].iloc[-4]
        ma20_yesterday = df['MA20'].iloc[-2]
    except IndexError:
        # 数据不足，返回 False
        return False
        
    # C1 (修正): 强势上升趋势确认： 
    #     a) 最新收盘价高于MA20 
    #     b) MA20 必须向上倾斜 (今天MA20 > 昨天MA20)
    C1_Trend = (latest[COL_CLOSE] > latest['MA20']) and \
               (latest['MA20'] > ma20_yesterday)
    
    # C2 (修正): 严格回踩三天不破确认： 
    #     a) 当前收盘价高于最近三天的最低价（确保不是在最低点买入）
    #     b) 最近三天的最低价必须严格高于 3 天前的 MA10 支撑位 (无容错，更严格)
    C2_Retracement_Check = (latest[COL_CLOSE] > recent_lows) and \
                           (recent_lows >= ma10_three_days_ago) 
    
    # C3 (修正): 强放量阳线突破：
    #     a) 今天成交量高于前5日平均的 2.0 倍 (💥 提高放量要求)
    #     b) 今天必须是阳线/红K线 (收盘价 > 开盘价)
    latest_vol = latest[COL_VOLUME]
    avg_vol_5 = df[COL_VOLUME].iloc[-6:-1].mean()
    
    C3_Volume = (latest_vol > avg_vol_5 * 2.0) and \
                (latest[COL_CLOSE] > latest[COL_OPEN]) 
    
    # 综合判断
    return C1_Trend and C2_Retracement_Check and C3_Volume

def meets_basic_criteria(df: pd.DataFrame, stock_code: str) -> bool:
    """
    实现基本面/价格筛选条件。
    """
    if df.empty:
        return False

    latest_close = df.iloc[-1][COL_CLOSE]
    
    # C4: 价格范围筛选 (5.0 元 <= 收盘价 <= 20.0 元)
    C4_Price_Range = (latest_close >= MIN_PRICE) and (latest_close <= MAX_PRICE)
    
    # C5: 排除条件：30开头 (创业板) 和 ST。只保留深沪A股 (00, 60开头)。
    C5_Exchange_Exclude = stock_code.startswith('60') or stock_code.startswith('00')
    
    return C4_Price_Range and C5_Exchange_Exclude

def process_file(file_path: str) -> dict or None:
    """
    处理单个CSV文件并应用所有筛选条件。
    """
    stock_code = os.path.basename(file_path).split('.')[0]
    
    try:
        # 1. 读取和清理数据
        df = pd.read_csv(file_path)
        df.sort_values(COL_DATE, inplace=True)
        df.dropna(inplace=True)

        # 2. 应用基本面筛选
        if not meets_basic_criteria(df, stock_code):
            return None
        
        # 3. 应用技术筛选
        if not meets_tech_criteria(df):
            return None

        # 4. 通过筛选，返回结果
        latest_close = df.iloc[-1][COL_CLOSE]
        return {'Code': stock_code, 'Close': latest_close}
    
    except Exception as e:
        # 记录处理单个文件时的错误，不中断其他并行任务
        logging.error(f"Error processing file {file_path}: {e}")
        return None

def main():
    start_time = datetime.now()
    logging.warning("--- Starting Stock Screener Advanced ---")

    # 1. 准备数据文件列表
    file_paths = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    
    if not file_paths:
        logging.error(f"FATAL: No CSV files found in {STOCK_DATA_DIR}. Please check data path.")
        return

    # 2. 并行处理文件
    results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        processed_results = executor.map(process_file, file_paths)
        # 收集非 None 的有效结果
        results = [res for res in processed_results if res is not None]

    if not results:
        logging.warning("❌ No stocks matched all criteria.")
        return

    # 3. 匹配股票名称 (使用 code 和 name)
    try:
        # ⚠️ 根据您的格式：stock_names.csv 是 'code', 'name'
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
        names_df.rename(columns={'code': 'Code', 'name': 'StockName'}, inplace=True)
    except Exception as e:
        logging.error(f"FATAL: Could not load stock names file {STOCK_NAMES_FILE} or column mismatch: {e}")
        return

    results_df = pd.DataFrame(results)
    
    final_df = pd.merge(results_df, names_df, on='Code', how='left')
    final_df = final_df[['Code', 'StockName', 'Close']]

    # 4. 保存结果到指定目录 (年月目录 + 时间戳文件名)
    current_time_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    current_year_month = datetime.now().strftime('%Y-%m')
    
    output_subdir = os.path.join(OUTPUT_DIR, current_year_month)
    os.makedirs(output_subdir, exist_ok=True)
    
    output_filename = f"screener_{current_time_str}.csv"
    output_path = os.path.join(output_subdir, output_filename)
    
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logging.warning(f"✅ Screening complete. {len(final_df)} stocks found. Results saved to: {output_path}")
    logging.warning(f"Total runtime: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()
