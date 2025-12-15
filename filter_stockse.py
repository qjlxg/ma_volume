import os
import glob
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_DIR = 'filtered_results'
MIN_CLOSE_PRICE = 5.0

# ⚠️ 关键修改点：定义您的CSV文件中实际的列名
DATE_COLUMN = '交易日期' # <--- 请根据您的实际数据修改此处的日期列名！
CLOSE_COLUMN = '收盘价' # <--- 请根据您的实际数据修改此处的收盘价列名！

def process_single_file(file_path):
    """
    处理单个股票历史数据文件，筛选最新数据并检查条件。
    返回 (股票代码, 最新收盘价) 或 None。
    """
    stock_code = os.path.basename(file_path).split('.')[0]
    
    try:
        # ⚠️ 修改点：使用配置的列名
        required_cols = [DATE_COLUMN, CLOSE_COLUMN]

        # 1. 读取数据，只读取需要的列
        df = pd.read_csv(
            file_path, 
            usecols=required_cols,
        )

        # 2. 确保数据不为空
        if df.empty:
            print(f"警告: 文件 {stock_code}.csv 为空。")
            return None

        # 3. 找到最新的收盘价（DataFrame的最后一行）
        latest_close = df[CLOSE_COLUMN].iloc[-1]

        # 4. 筛选条件：最新收盘价不能低于 5.0 元
        if latest_close >= MIN_CLOSE_PRICE:
            return stock_code, latest_close
        
        return None

    except KeyError:
        # 捕获列名缺失错误
        print(f"致命错误: 文件 {stock_code}.csv 缺少所需列：'{DATE_COLUMN}' 或 '{CLOSE_COLUMN}'。请检查列名配置是否正确。")
        return None
    except Exception as e:
        print(f"处理文件 {stock_code}.csv 时发生未预期的错误: {e}")
        return None

# main 函数保持不变，因为改动只在 process_single_file 内部

def main():
    # ... (main函数的其余部分保持不变) ...
    # 为了完整性，我将 main 函数省略，请确保您只修改了顶部的配置和 process_single_file 函数。
    
    # ⚠️ 确保您在 process_single_file 函数中使用了新的列名变量。
    
    # 1. 获取所有股票数据文件路径
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"错误: 在目录 {STOCK_DATA_DIR} 中未找到任何CSV文件。请确保数据已上传。")
        return

    # 2. 并行处理所有文件以加快速度
    print(f"开始处理 {len(all_files)} 个股票文件...")
    
    results = []
    # 使用 ThreadPoolExecutor 进行并行处理
    max_workers = os.cpu_count() * 2 or 8 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        processed_data = executor.map(process_single_file, all_files)
        results = [res for res in processed_data if res is not None]

    if not results:
        print("未筛选出符合条件的股票。")
        return

    # 3. 将筛选结果转换为 DataFrame
    filtered_df = pd.DataFrame(results, columns=['Code', 'Latest_Close'])

    # 4. 读取股票名称匹配文件
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        if 'Code' not in names_df.columns or 'Name' not in names_df.columns:
             print(f"错误: {STOCK_NAMES_FILE} 必须包含 'Code' 和 'Name' 两列。")
             final_output_df = filtered_df
        else:
            final_output_df = pd.merge(
                filtered_df,
                names_df[['Code', 'Name']],
                on='Code',
                how='left'
            )
            final_output_df = final_output_df[['Code', 'Name', 'Latest_Close']]
            final_output_df['Name'] = final_output_df['Name'].fillna('名称缺失')
    except FileNotFoundError:
        print(f"错误: 股票名称文件 {STOCK_NAMES_FILE} 未找到，仅输出代码和价格。")
        final_output_df = filtered_df
    
    print(f"筛选出 {len(final_output_df)} 支符合条件的股票。")

    # 6. 生成带时间戳的文件名和目录
    now = datetime.now()
    output_subdir = now.strftime('%Y/%m')
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f'filtered_stocks_{timestamp_str}.csv'
    
    final_output_path = os.path.join(OUTPUT_DIR, output_subdir, output_filename)
    
    os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

    # 7. 保存结果
    final_output_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')
    print(f"筛选结果已成功保存到: {final_output_path}")

if __name__ == '__main__':
    main()
