import os
import glob
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_DIR = 'filtered_results'

# 筛选条件
MIN_CLOSE_PRICE = 5.0      # 最新收盘价最低限制
MIN_TURNOVER_RATE = 0.5    # 最小换手率（百分比）限制，用于减少结果数量，确保活跃度

# ⚠️ 自动匹配关键词列表：根据您上传的CSV片段 (日期, 收盘, 换手率)
DATE_KEYWORDS = ['日期', 'Date', '交易日期', 'TradeDate', 'TDATE', 'time']
CLOSE_KEYWORDS = ['收盘', 'Close', 'close', 'Adj Close', 'PX_LAST']
TURNOVER_KEYWORDS = ['换手率', 'TurnoverRate', 'Turnover', '换手']


def find_column_name(df_columns, keywords):
    """
    在DataFrame的列名列表中查找与给定关键词匹配的列名。
    返回第一个匹配到的列名，如果找不到则返回 None。
    """
    lower_cols = [col.lower() for col in df_columns]
    for keyword in keywords:
        # 尝试精确匹配
        if keyword in df_columns:
            return keyword
        # 尝试忽略大小写匹配
        if keyword.lower() in lower_cols:
            # 返回原始大小写的列名
            return df_columns[lower_cols.index(keyword.lower())]
    return None


def process_single_file(file_path):
    """
    处理单个股票历史数据文件，筛选最新数据并检查条件。
    返回 (股票代码, 最新收盘价, 最新换手率) 或 None。
    """
    stock_code = os.path.basename(file_path).split('.')[0]
    
    try:
        # 1. 尝试读取整个文件
        df = pd.read_csv(file_path)

        # 2. 自动匹配日期、收盘价和换手率列
        date_col = find_column_name(df.columns, DATE_KEYWORDS)
        close_col = find_column_name(df.columns, CLOSE_KEYWORDS)
        turnover_col = find_column_name(df.columns, TURNOVER_KEYWORDS)
        
        if not date_col or not close_col or not turnover_col:
            missing_part = []
            if not date_col: missing_part.append("日期")
            if not close_col: missing_part.append("收盘价")
            if not turnover_col: missing_part.append("换手率")
                
            print(f"⚠️ 警告: 文件 {stock_code}.csv 缺少所需列: {', '.join(missing_part)}。")
            print(f"    该文件的前几列为: {list(df.columns[:5])}。请检查并更新脚本中的关键词列表。")
            return None

        # 3. 找到最新的收盘价和换手率
        # 确保收盘价和换手率是数值类型，防止因数据清洗不干净而报错
        df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
        df[turnover_col] = pd.to_numeric(df[turnover_col], errors='coerce')
        df.dropna(subset=[close_col, turnover_col], inplace=True) # 移除无法转换为数字的行

        if df.empty:
            return None

        latest_close = df[close_col].iloc[-1]
        latest_turnover = df[turnover_col].iloc[-1]

        # 4. 筛选条件：价格 AND 活跃度 (换手率)
        if latest_close >= MIN_CLOSE_PRICE and latest_turnover >= MIN_TURNOVER_RATE:
            # 返回三个值
            return stock_code, latest_close, latest_turnover
        
        return None

    except Exception as e:
        print(f"处理文件 {stock_code}.csv 时发生未预期的错误: {e}") 
        return None

def main():
    """主函数，执行文件扫描、并行处理和结果保存。"""
    
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"错误: 在目录 {STOCK_DATA_DIR} 中未找到任何CSV文件。请确保数据已上传。")
        return

    print(f"开始处理 {len(all_files)} 个股票文件...")
    
    results = []
    max_workers = os.cpu_count() * 2 or 8 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        processed_data = executor.map(process_single_file, all_files)
        results = [res for res in processed_data if res is not None]

    if not results:
        print("未筛选出符合条件的股票。")
        return

    # 3. 将筛选结果转换为 DataFrame (注意：现在有三列)
    filtered_df = pd.DataFrame(results, columns=['Code', 'Latest_Close', 'Latest_Turnover'])
    
    # 4. 读取股票名称匹配文件 (解决匹配失败问题)
    try:
        # 明确读取 'code' 和 'name' 列
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str}, usecols=['code', 'name'])
        
        # ⚠️ 关键修改：将 names_df 中的 'code' 和 'name' 列重命名为 'Code' 和 'Name' 以便与 filtered_df 匹配
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'})
        
        # 5. 合并筛选结果和股票名称
        final_output_df = pd.merge(
            filtered_df,
            names_df,
            on='Code',
            how='left'
        )
        
        # ⚠️ 输出列顺序：代码、名称、收盘价、换手率
        final_output_df = final_output_df[['Code', 'Name', 'Latest_Close', 'Latest_Turnover']]
        final_output_df['Name'] = final_output_df['Name'].fillna('名称缺失')
        
    except FileNotFoundError:
        print(f"错误: 股票名称文件 {STOCK_NAMES_FILE} 未找到，仅输出代码和价格。")
        final_output_df = filtered_df
    except ValueError:
        print(f"错误: {STOCK_NAMES_FILE} 文件格式或列名不正确，无法匹配。")
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
