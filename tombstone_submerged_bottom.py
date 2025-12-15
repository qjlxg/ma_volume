import os
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0
# K线最高点和最低点对齐的容忍度 (例如: 0.01 = 1%)
ALIGNMENT_TOLERANCE = 0.01

def load_stock_names(file_path):
    """加载股票代码和名称的映射表"""
    try:
        # 假设 stock_names.csv 包含 'code' 和 'name' 两列
        names_df = pd.read_csv(file_path, dtype={'code': str})
        return dict(zip(names_df['code'], names_df['name']))
    except FileNotFoundError:
        print(f"警告: 找不到股票名称文件 {file_path}")
        return {}
    except Exception as e:
        print(f"加载股票名称文件时出错: {e}")
        return {}

def check_tombstone_submerged_bottom(df):
    """
    检查 '巨石沉底' 形态 (T, T-1, T-2 三日数据)。
    df 必须是按日期降序排列的，且至少包含 3 行。
    """
    if len(df) < 3:
        return False

    # 选取最近三天的 K 线数据
    T_2 = df.iloc[2]  # T-2 (最老的一天)
    T_1 = df.iloc[1]  # T-1 (中间一天)
    T = df.iloc[0]    # T (最新一天)

    # 1. 股价要求：最新收盘价不能低于 MIN_CLOSE_PRICE
    if T['Close'] < MIN_CLOSE_PRICE:
        return False

    # 2. K 线实体方向要求
    # 实体大小： abs(Close - Open)
    is_T_2_bearish = T_2['Close'] < T_2['Open'] # T-2 阴线 (Red)
    is_T_1_bullish = T_1['Close'] > T_1['Open'] # T-1 阳线 (Green)
    is_T_bearish   = T['Close'] < T['Open']     # T 阴线 (Red)

    if not (is_T_2_bearish and is_T_1_bullish and is_T_bearish):
        return False

    # 3. 实体大小要求 (定性判断：T-1 小，T-2/T 大)
    # T-1 实体相对较小
    T_1_body_size = abs(T_1['Close'] - T_1['Open'])
    T_2_body_size = abs(T_2['Close'] - T_2['Open'])
    T_body_size = abs(T['Close'] - T['Open'])

    # 设定一个阈值，确保 T-1 实体明显小于 T-2 和 T
    if T_1_body_size * 2 > T_2_body_size or T_1_body_size * 2 > T_body_size:
        # T-1 实体不能太接近 T-2 或 T 的实体
        return False


    # 4. 高低点对齐要求 (巨石沉底的核心)
    # 检查最低价 Low 的对齐
    min_low = min(T['Low'], T_1['Low'], T_2['Low'])
    max_low = max(T['Low'], T_1['Low'], T_2['Low'])
    # 检查最高价 High 的对齐 (图上虚线表示最高收盘价或最高价)
    min_high = min(T['High'], T_1['High'], T_2['High'])
    max_high = max(T['High'], T_1['High'], T_2['High'])
    
    # 容忍度计算：(最大值 - 最小值) / 平均值 <= ALIGNMENT_TOLERANCE
    avg_low = (T['Low'] + T_1['Low'] + T_2['Low']) / 3
    avg_high = (T['High'] + T_1['High'] + T_2['High']) / 3

    low_aligned = (max_low - min_low) / avg_low <= ALIGNMENT_TOLERANCE
    high_aligned = (max_high - min_high) / avg_high <= ALIGNMENT_TOLERANCE

    return low_aligned and high_aligned

def process_file(file_path):
    """单个文件的处理逻辑"""
    stock_code = os.path.splitext(os.path.basename(file_path))[0]
    try:
        # 假设 CSV 包含 Date, Open, High, Low, Close, Volume 等列
        df = pd.read_csv(
            file_path,
            parse_dates=['Date'],
            index_col='Date',
            # 确保列名大写，或者根据您的实际文件调整
            dtype={'Open': float, 'High': float, 'Low': float, 'Close': float}
        )
        
        # 按日期降序排序，确保最新数据在顶部
        df = df.sort_values(by='Date', ascending=False)
        
        if check_tombstone_submerged_bottom(df):
            # 获取最新的日期作为筛选日期
            latest_date = df.iloc[0].name.strftime('%Y-%m-%d')
            latest_close = df.iloc[0]['Close']
            return stock_code, latest_date, latest_close
        
    except Exception as e:
        print(f"处理文件 {stock_code}.csv 时出错: {e}")
    
    return None

def main():
    start_time = datetime.now()
    
    # 1. 查找所有数据文件
    data_files = [
        os.path.join(STOCK_DATA_DIR, f)
        for f in os.listdir(STOCK_DATA_DIR)
        if f.endswith('.csv')
    ]

    if not data_files:
        print("未找到任何 CSV 数据文件，程序退出。")
        return

    # 2. 并行处理文件
    print(f"开始扫描 {len(data_files)} 个文件，使用 {cpu_count()} 核心进行并行处理...")
    
    with Pool(cpu_count()) as pool:
        # 使用 pool.map 并行处理所有文件
        results = pool.map(process_file, data_files)

    # 3. 收集并整理结果
    filtered_stocks = [res for res in results if res is not None]

    if not filtered_stocks:
        print("未找到符合 '巨石沉底' 形态的股票。")
        return
        
    # 4. 加载股票名称
    stock_names = load_stock_names(STOCK_NAMES_FILE)

    results_df = pd.DataFrame(
        filtered_stocks,
        columns=['Code', 'Date', 'Latest_Close']
    )
    
    # 匹配股票名称
    results_df['Name'] = results_df['Code'].apply(lambda c: stock_names.get(c, '未知名称'))
    
    # 重新排序输出列
    results_df = results_df[['Code', 'Name', 'Latest_Close', 'Date']]
    
    # 5. 保存结果到指定路径
    current_time_utc = datetime.utcnow()
    # 转换为上海时区 (UTC+8)
    shanghai_tz = pd.to_datetime(current_time_utc).tz_localize('UTC').tz_convert('Asia/Shanghai')
    
    year_month_dir = shanghai_tz.strftime('%Y%m')
    timestamp_str = shanghai_tz.strftime('%Y%m%d%H%M%S')
    
    output_dir = os.path.join(year_month_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    output_filename = f"tombstone_submerged_bottom_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    results_df.to_csv(output_path, index=False, encoding='utf-8')

    print(f"--- 筛选完成 ---")
    print(f"总共找到 {len(filtered_stocks)} 支符合条件的股票。")
    print(f"结果已保存到: {output_path}")
    print(f"总耗时: {datetime.now() - start_time}")

if __name__ == '__main__':
    # 确保 stock_data 目录存在，如果是在 GitHub Actions 中，需要确保文件已 checkout
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"错误: 找不到数据目录 '{STOCK_DATA_DIR}'，请检查工作流配置。")
    else:
        main()
