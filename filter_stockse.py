import os
import glob
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_DIR = 'filtered_results'
# 筛选条件：最新收盘价不低于 5.0 元
MIN_CLOSE_PRICE = 5.0

def process_single_file(file_path):
    """
    处理单个股票历史数据文件，筛选最新数据并检查条件。
    返回 (股票代码, 最新收盘价) 或 None。
    """
    try:
        # 从文件名中提取股票代码，假设文件名为 XXXXXX.csv
        stock_code = os.path.basename(file_path).split('.')[0]

        # 读取数据，假设CSV文件包含 'Close' (收盘价) 列
        df = pd.read_csv(file_path, parse_dates=['Date'])

        # 确保数据不为空
        if df.empty:
            print(f"警告: 文件 {stock_code}.csv 为空。")
            return None

        # 找到最新的收盘价（通常是DataFrame的最后一行）
        latest_close = df['Close'].iloc[-1]

        # 筛选条件：最新收盘价不能低于 5.0 元
        if latest_close >= MIN_CLOSE_PRICE:
            return stock_code, latest_close
        
        return None

    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {e}")
        return None

def main():
    """主函数，执行文件扫描、并行处理和结果保存。"""
    
    # 1. 获取所有股票数据文件路径
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"错误: 在目录 {STOCK_DATA_DIR} 中未找到任何CSV文件。请确保数据已上传。")
        return

    # 2. 并行处理所有文件以加快速度
    print(f"开始处理 {len(all_files)} 个股票文件...")
    
    # 使用 ThreadPoolExecutor 进行并行处理
    results = []
    # 设定并行工作的线程数，例如CPU核心数
    max_workers = os.cpu_count() or 4 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # map函数按文件顺序返回结果
        processed_data = executor.map(process_single_file, all_files)
        results = [res for res in processed_data if res is not None]

    if not results:
        print("未筛选出符合条件的股票。")
        return

    # 3. 将筛选结果转换为 DataFrame
    filtered_df = pd.DataFrame(results, columns=['Code', 'Latest_Close'])
    print(f"筛选出 {len(filtered_df)} 支符合条件的股票。")

    # 4. 读取股票名称匹配文件
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        # 确保名称文件的 Code 列是字符串格式，避免匹配错误
        if 'Code' not in names_df.columns or 'Name' not in names_df.columns:
             print(f"错误: {STOCK_NAMES_FILE} 必须包含 'Code' 和 'Name' 两列。")
             return
    except FileNotFoundError:
        print(f"错误: 股票名称文件 {STOCK_NAMES_FILE} 未找到。")
        # 如果找不到名称文件，只输出代码和价格
        final_output_df = filtered_df
    else:
        # 5. 合并筛选结果和股票名称
        final_output_df = pd.merge(
            filtered_df,
            names_df[['Code', 'Name']],
            on='Code',
            how='left'
        )
        # 重新排序和选择最终输出的列
        final_output_df = final_output_df[['Code', 'Name', 'Latest_Close']]
        final_output_df['Name'] = final_output_df['Name'].fillna('名称缺失')
    
    # 6. 生成带时间戳的文件名和目录
    now = datetime.now()
    # 结果推送到仓库中的年月目录中
    output_subdir = now.strftime('%Y/%m')
    # 文件名加上时间戳
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f'filtered_stocks_{timestamp_str}.csv'
    
    final_output_path = os.path.join(OUTPUT_DIR, output_subdir, output_filename)
    
    # 确保输出目录存在
    os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

    # 7. 保存结果
    final_output_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')
    print(f"筛选结果已成功保存到: {final_output_path}")

if __name__ == '__main__':
    main()
