# volume_bottom_scanner.py

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import glob

# --- 配置参数 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_FILE = 'filtered_stocks.csv'
PRICE_MIN = 5.0  # 最新收盘价不低于 5.0 元
VOLUME_PERIOD = 60  # 计算天量时的周期 N
PRICE_LOW_PERIOD = 20  # 价格低位确认周期 M
VOLUME_SHRINK_RATIO = 0.20  # 缩量比例 20%

def load_stock_names():
    """加载股票代码和名称的映射表。"""
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        names_df['Code'] = names_df['Code'].str.zfill(6) # 确保代码是 6 位
        return names_df.set_index('Code')['Name'].to_dict()
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def analyze_stock_file(file_path):
    """
    分析单个股票的CSV文件，应用筛选条件。
    :param file_path: 股票数据CSV文件的完整路径。
    :return: 满足条件的股票代码和最新收盘价，否则返回 None。
    """
    try:
        df = pd.read_csv(file_path)
        # 确保数据按日期升序排列
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        # 忽略空数据或数据量不足的股票
        if len(df) < max(VOLUME_PERIOD, PRICE_LOW_PERIOD):
            return None

        # 获取最新数据
        latest_data = df.iloc[-1]
        code = os.path.basename(file_path).split('.')[0].zfill(6)
        latest_close = latest_data['Close']
        latest_volume = latest_data['Volume']
        
        # 1. 最新收盘价不能低于 5.0 元
        if latest_close < PRICE_MIN:
            return None

        # 确保有足够的历史数据进行计算
        history_df = df.iloc[-VOLUME_PERIOD:]
        if len(history_df) < VOLUME_PERIOD:
            return None

        # 2. 缩量见底条件
        # a. 找到过去 VOLUME_PERIOD 天的天量
        max_volume = history_df['Volume'].max()
        
        # b. 检查最新成交量是否小于天量的 20%
        if latest_volume > max_volume * VOLUME_SHRINK_RATIO:
            return None
        
        # 3. 价格低位确认 (最新价处于过去 PRICE_LOW_PERIOD 天的底部 25% 范围内)
        price_history = df.iloc[-PRICE_LOW_PERIOD:]['Close']
        low_price = price_history.min()
        high_price = price_history.max()
        price_range = high_price - low_price
        
        # 计算价格低位阈值：低点 + 25% * 价格范围
        low_threshold = low_price + 0.25 * price_range
        
        if latest_close > low_threshold:
            # 价格不在近期底部区域
            return None

        # 所有条件满足
        return {
            'Code': code,
            'Name': '', # 稍后匹配名称
            'Latest_Close': latest_close,
            'Latest_Volume': latest_volume,
            'Max_Volume_60d': max_volume,
            'Low_Price_20d_Threshold': low_threshold
        }

    except Exception as e:
        # print(f"Error processing file {file_path}: {e}")
        return None

def main():
    """主函数，管理并行处理和结果输出。"""
    print(f"--- 启动缩量见底扫描 (价格 >= {PRICE_MIN}，缩量 <= 20%) ---")
    
    # 获取所有股票数据文件路径
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"Error: No CSV files found in {STOCK_DATA_DIR}")
        return

    stock_names = load_stock_names()
    results = []

    # 使用线程池进行并行处理以加速
    with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        future_to_file = {executor.submit(analyze_stock_file, file_path): file_path for file_path in all_files}
        
        # 实时收集结果
        for future in as_completed(future_to_file):
            result = future.result()
            if result:
                results.append(result)
    
    if not results:
        print("扫描完成：没有股票满足筛选条件。")
        
        # 如果没有结果，也创建一个空文件，防止后续 Git 提交失败
        output_dir = datetime.now().strftime('output/%Y/%m')
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_output_path = os.path.join(output_dir, f'volume_bottom_scan_results_{timestamp}.csv')
        
        pd.DataFrame(columns=['Code', 'Name', 'Latest_Close', 'Latest_Volume', 'Max_Volume_60d', 'Low_Price_20d_Threshold']).to_csv(final_output_path, index=False)
        print(f"已创建空结果文件: {final_output_path}")
        return

    # 将结果转换为 DataFrame 并匹配名称
    results_df = pd.DataFrame(results)
    results_df['Name'] = results_df['Code'].map(stock_names).fillna('未知名称')

    # 格式化输出文件路径
    current_time = datetime.now()
    # 结果推送到仓库中年月目录中
    output_dir = current_time.strftime('output/%Y/%m')
    os.makedirs(output_dir, exist_ok=True)
    # 文件名加上时间戳
    timestamp = current_time.strftime('%Y%m%d_%H%M%S')
    final_output_path = os.path.join(output_dir, f'volume_bottom_scan_results_{timestamp}.csv')

    # 排序和保存结果
    results_df = results_df[['Code', 'Name', 'Latest_Close', 'Latest_Volume', 'Max_Volume_60d', 'Low_Price_20d_Threshold']]
    results_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')

    print("\n--- 筛选结果 ---")
    print(results_df.to_string(index=False))
    print(f"\n扫描完成，共找到 {len(results_df)} 只满足条件的股票。")
    print(f"结果已保存到: {final_output_path}")

if __name__ == '__main__':
    main()
