import os
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count
import pytz

# --- 配置常量 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0  # 股价下限
MAX_CLOSE_PRICE = 20.0 # 股价上限
# K线最高点和最低点对齐的容忍度 (例如: 0.01 = 1%)
ALIGNMENT_TOLERANCE = 0.05
SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')

# K线数据列名映射 (匹配您的中文CSV格式)
COL_MAP = {
    '日期': 'Date',
    '股票代码': 'Code',
    '开盘': 'Open',
    '收盘': 'Close',
    '最高': 'High',
    '最低': 'Low'
}

def load_stock_names(file_path):
    """加载股票代码和名称的映射表"""
    try:
        # 修复: 使用您提供的 'code', 'name' 小写列名
        names_df = pd.read_csv(file_path, dtype={'code': str, 'name': str})
        # 将 code 列作为 key，name 列作为 value
        return dict(zip(names_df['code'], names_df['name']))
    except FileNotFoundError:
        print(f"警告: 找不到股票名称文件 {file_path}")
        return {}
    except Exception as e:
        # 打印详细错误信息以供调试
        print(f"加载股票名称文件时出错: {e}")
        return {}

def is_valid_stock(code, name):
    """
    检查股票代码和名称是否符合深沪A股的排除条件。
    排除 ST/*ST，排除 30 开头 (创业板)，保留 00/60 开头的主流 A 股。
    """
    # 1. 排除 ST / *ST
    if 'ST' in name.upper() or '*' in name:
        return False
        
    # 2. 仅保留深沪 A 股 (排除创业板 30)
    # 深市 A 股: 000xxx, 001xxx, 002xxx, 003xxx
    # 沪市 A 股: 600xxx, 601xxx, 603xxx
    if code.startswith(('00', '60')):
        return True
    
    return False

def check_tombstone_submerged_bottom(df, stock_code, stock_name):
    """
    检查 '巨石沉底' 形态 (T, T-1, T-2 三日数据) 和所有筛选条件。
    """
    # 0. 检查是否为有效股票（代码和名称）
    if not is_valid_stock(stock_code, stock_name):
        return False

    if len(df) < 3:
        return False

    # 选取最近三天的 K 线数据 (确保 df 是按日期降序排列的)
    T_2 = df.iloc[2]
    T_1 = df.iloc[1]
    T = df.iloc[0]

    # 1. 股价范围要求：最新收盘价 (T['Close'])
    if not (MIN_CLOSE_PRICE <= T['Close'] <= MAX_CLOSE_PRICE):
        return False

    # 2. K 线实体方向要求
    is_T_2_bearish = T_2['Close'] < T_2['Open'] # T-2 阴线
    is_T_1_bullish = T_1['Close'] > T_1['Open'] # T-1 阳线
    is_T_bearish   = T['Close'] < T['Open']     # T 阴线

    if not (is_T_2_bearish and is_T_1_bullish and is_T_bearish):
        return False

    # 3. 实体大小要求 (T-1 实体相对较小)
    T_1_body_size = abs(T_1['Close'] - T_1['Open'])
    T_2_body_size = abs(T_2['Close'] - T_2['Open'])
    T_body_size = abs(T['Close'] - T['Open'])
    
    # 要求 T-1 实体大小不超过 T-2 和 T 实体大小的 50%
    if T_1_body_size > T_2_body_size * 0.5 or T_1_body_size > T_body_size * 0.5:
        return False

    # 4. 高低点对齐要求 (巨石沉底的核心) 
    # 图中的虚线强调了三根K线的高点和低点都在一个区间内。
    min_low = min(T['Low'], T_1['Low'], T_2['Low'])
    max_low = max(T['Low'], T_1['Low'], T_2['Low'])
    min_high = min(T['High'], T_1['High'], T_2['High'])
    max_high = max(T['High'], T_1['High'], T_2['High'])
    
    avg_low = (T['Low'] + T_1['Low'] + T_2['Low']) / 3
    avg_high = (T['High'] + T_1['High'] + T_2['High']) / 3

    if avg_low < 0.1 or avg_high < 0.1: 
        return False

    # 容忍度计算
    low_aligned = (max_low - min_low) / avg_low <= ALIGNMENT_TOLERANCE
    high_aligned = (max_high - min_high) / avg_high <= ALIGNMENT_TOLERANCE

    return low_aligned and high_aligned

def process_file(file_path_tuple):
    """单个文件的处理逻辑，使用元组传入文件路径和名称字典"""
    file_path, stock_names = file_path_tuple
    stock_code_from_name = os.path.splitext(os.path.basename(file_path))[0]
    stock_name = stock_names.get(stock_code_from_name, '未知名称')
    
    # 预先筛选：如果代码或名称不符合基本条件，则不加载数据
    if not is_valid_stock(stock_code_from_name, stock_name):
        return None

    try:
        # 1. 修复: 读取CSV文件，指定编码
        df = pd.read_csv(
            file_path,
            encoding='gbk', # 尝试使用 gbk 或 utf-8 应对中文乱码
            parse_dates=['日期'],
            dtype={
                '开盘': float, '收盘': float, '最高': float, '最低': float,
                '日期': str, '股票代码': str
            }
        )
        
        # 2. 修复: 重命名列以方便内部处理
        df = df.rename(columns=COL_MAP)
        
        # 确保日期列已被正确解析
        df['Date'] = pd.to_datetime(df['Date'])
        
        # 3. 确保数据有效且按日期降序排序
        if df.empty or len(df) < 3:
            return None

        df = df.sort_values(by='Date', ascending=False).reset_index(drop=True)
        
        # 4. 执行形态检查
        if check_tombstone_submerged_bottom(df, stock_code_from_name, stock_name):
            latest_date = df.iloc[0]['Date'].strftime('%Y-%m-%d')
            latest_close = df.iloc[0]['Close']
            return stock_code_from_name, latest_date, latest_close
        
    except Exception as e:
        # 避免并行输出混乱，但保留代码
        # print(f"处理文件 {stock_code_from_name}.csv 时出错: {e}") 
        pass
    
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

    # 2. 加载股票名称 (修复错误点)
    stock_names = load_stock_names(STOCK_NAMES_FILE)
    
    if not stock_names:
        print("股票名称对照表为空或加载失败，无法进行名称匹配，程序退出。")
        # 修复: 只有找不到名称表时才退出，不因为单个加载错误退出
        # return # 注释掉，如果 stock_names 为空，后面的筛选结果会是 "未知名称"
    
    # 3. 准备并行处理的参数列表
    file_list_with_names = [(f, stock_names) for f in data_files]

    # 4. 并行处理文件
    num_cores = cpu_count()
    print(f"开始扫描 {len(data_files)} 个文件，使用 {num_cores} 核心进行并行处理...")
    
    with Pool(num_cores) as pool:
        results = pool.map(process_file, file_list_with_names)

    # 5. 收集并整理结果
    filtered_stocks = [res for res in results if res is not None]

    if not filtered_stocks:
        print("未找到符合所有条件的股票。")
        return
        
    results_df = pd.DataFrame(
        filtered_stocks,
        columns=['Code', 'Date', 'Latest_Close']
    )
    
    # 匹配股票名称
    results_df['Name'] = results_df['Code'].apply(lambda c: stock_names.get(c, '未知名称'))
    
    # 重新排序输出列
    results_df = results_df[['Code', 'Name', 'Latest_Close', 'Date']]
    
    # 6. 保存结果到指定路径 (上海时区)
    current_time_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    shanghai_tz_dt = current_time_utc.astimezone(SHANGHAI_TZ)
    
    year_month_dir = shanghai_tz_dt.strftime('%Y%m')
    timestamp_str = shanghai_tz_dt.strftime('%Y%m%d%H%M%S')
    
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
        print(f"错误: 找不到数据目录 '{STOCK_DATA_DIR}'，请检查工作流配置和文件结构。")
    else:
        # 确保 pytz 已安装，因为工作流已添加安装命令
        main()
