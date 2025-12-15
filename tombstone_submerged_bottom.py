import os
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0
MAX_CLOSE_PRICE = 20.0 # 新增：最高收盘价限制
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

def is_valid_stock(code, name):
    """
    检查股票代码和名称是否符合深沪A股和排除条件。
    """
    # 1. 排除 ST/ *ST
    if 'ST' in name.upper() or '*' in name:
        return False
        
    # 2. 排除创业板 (30开头)
    if code.startswith('30'):
        return False

    # 3. 仅保留深沪A股 (60, 00开头的股票，排除30，通常排除20, 90等B股/指数/其他)
    # 简化判断：只要不是 30 开头，且是 6位数字即可，因为数据源通常只包含A股。
    # 更严格的A股判断：
    # 沪市A股: 600xxx, 601xxx, 603xxx, 688xxx (科创板，但可能要保留)
    # 深市A股: 000xxx, 001xxx, 002xxx, 003xxx
    
    # 排除科创板 (688) 和 B股/指数/其他 (20, 90, 8x, 5x, 1x等)
    if code.startswith(('60', '00')):
        return True
    
    # 由于您明确排除了 30 (创业板)，我们默认只保留 60 和 00，除非数据源包含其他A股代码。
    # 稳妥起见，保留上述判断，确保只选取主流A股。
    return False

def check_tombstone_submerged_bottom(df, stock_code, stock_name):
    """
    在 K 线形态检查前，先进行最新的股价和类型检查。
    """
    # 0. 检查是否为有效股票（代码和名称）
    if not is_valid_stock(stock_code, stock_name):
        return False

    if len(df) < 3:
        return False

    T_2 = df.iloc[2]
    T_1 = df.iloc[1]
    T = df.iloc[0]

    # 1. 股价范围要求：最新收盘价 (T['Close'])
    if not (MIN_CLOSE_PRICE <= T['Close'] <= MAX_CLOSE_PRICE):
        return False

    # 2. K 线实体方向要求 (保持不变)
    is_T_2_bearish = T_2['Close'] < T_2['Open']
    is_T_1_bullish = T_1['Close'] > T_1['Open']
    is_T_bearish   = T['Close'] < T['Open']

    if not (is_T_2_bearish and is_T_1_bullish and is_T_bearish):
        return False

    # 3. 实体大小要求 (保持不变)
    T_1_body_size = abs(T_1['Close'] - T_1['Open'])
    T_2_body_size = abs(T_2['Close'] - T_2['Open'])
    T_body_size = abs(T['Close'] - T['Open'])
    
    if T_1_body_size * 2 > T_2_body_size or T_1_body_size * 2 > T_body_size:
        return False

    # 4. 高低点对齐要求 (保持不变)
    min_low = min(T['Low'], T_1['Low'], T_2['Low'])
    max_low = max(T['Low'], T_1['Low'], T_2['Low'])
    min_high = min(T['High'], T_1['High'], T_2['High'])
    max_high = max(T['High'], T_1['High'], T_2['High'])
    
    # 容忍度计算
    avg_low = (T['Low'] + T_1['Low'] + T_2['Low']) / 3
    avg_high = (T['High'] + T_1['High'] + T_2['High']) / 3

    # 避免除以零
    if avg_low == 0 or avg_high == 0:
        return False

    low_aligned = (max_low - min_low) / avg_low <= ALIGNMENT_TOLERANCE
    high_aligned = (max_high - min_high) / avg_high <= ALignment_TOLERANCE

    return low_aligned and high_aligned

def process_file(file_path, stock_names):
    """单个文件的处理逻辑，需要传入股票名称字典"""
    stock_code = os.path.splitext(os.path.basename(file_path))[0]
    stock_name = stock_names.get(stock_code, '未知名称')
    
    # 预先筛选：如果代码或名称不符合基本条件，则不加载数据
    if not is_valid_stock(stock_code, stock_name):
        return None

    try:
        df = pd.read_csv(
            file_path,
            parse_dates=['Date'],
            index_col='Date',
            dtype={'Open': float, 'High': float, 'Low': float, 'Close': float}
        )
        
        df = df.sort_values(by='Date', ascending=False)
        
        # 传入 stock_code 和 stock_name 进行检查
        if check_tombstone_submerged_bottom(df, stock_code, stock_name):
            latest_date = df.iloc[0].name.strftime('%Y-%m-%d')
            latest_close = df.iloc[0]['Close']
            return stock_code, latest_date, latest_close
        
    except Exception as e:
        # 排除那些因为数据格式不正确而导致的错误文件
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

    # 2. 加载股票名称 (并行处理前加载一次)
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    
    # 3. 并行处理文件
    print(f"开始扫描 {len(data_files)} 个文件，使用 {cpu_count()} 核心进行并行处理...")
    
    # 为 Pool.map 准备参数列表 (文件路径 + 名称字典)
    # 由于 Pool.map 只能接受单个迭代器参数，我们使用 lambda/partial 或 tuple 传递，
    # 但在 Pool 的场景下，最好是**修改 process_file 接收两个参数**并在 Pool 外包装。
    # 为了简化，我们使用一个包装函数传递 stock_names
    
    def process_wrapper(file_path):
        return process_file(file_path, stock_names)

    with Pool(cpu_count()) as pool:
        results = pool.map(process_wrapper, data_files)

    # 4. 收集并整理结果
    filtered_stocks = [res for res in results if res is not None]

    if not filtered_stocks:
        print("未找到符合所有条件的股票。")
        return
        
    results_df = pd.DataFrame(
        filtered_stocks,
        columns=['Code', 'Date', 'Latest_Close']
    )
    
    # 匹配股票名称 (在主线程完成)
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
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"错误: 找不到数据目录 '{STOCK_DATA_DIR}'，请检查工作流配置。")
    else:
        main()
