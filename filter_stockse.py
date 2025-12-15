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
MIN_TURNOVER_RATE = 0.5    # 最小换手率（百分比）限制 (活跃度辅助条件)

# 均线周期 (定义“眼睛”形态)
MA_SHORT = 5
MA_LONG = 20
LOOKBACK_DAYS = 30 # 检查“眼睛”形态的窗口，例如在最近30个交易日内形成

# 自动匹配关键词列表
DATE_KEYWORDS = ['日期', 'Date', '交易日期', 'TradeDate', 'TDATE', 'time']
CLOSE_KEYWORDS = ['收盘', 'Close', 'close', 'Adj Close', 'PX_LAST']
TURNOVER_KEYWORDS = ['换手率', 'TurnoverRate', 'Turnover', '换手']

# --- 工具函数保持不变 ---
def find_column_name(df_columns, keywords):
    lower_cols = [col.lower() for col in df_columns]
    for keyword in keywords:
        if keyword in df_columns:
            return keyword
        if keyword.lower() in lower_cols:
            return df_columns[lower_cols.index(keyword.lower())]
    return None

def process_single_file(file_path):
    """
    处理单个股票历史数据文件，筛选符合“眼睛”形态的股票。
    """
    stock_code = os.path.basename(file_path).split('.')[0]
    
    try:
        # 1. 尝试读取整个文件
        df = pd.read_csv(file_path)

        # 2. 自动匹配所需的列
        date_col = find_column_name(df.columns, DATE_KEYWORDS)
        close_col = find_column_name(df.columns, CLOSE_KEYWORDS)
        turnover_col = find_column_name(df.columns, TURNOVER_KEYWORDS)
        
        if not date_col or not close_col or not turnover_col:
            # 简化列名缺失处理，不在日志中打印大量警告
            return None

        # 3. 数据清洗和准备
        df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
        df[turnover_col] = pd.to_numeric(df[turnover_col], errors='coerce')
        # 将日期设置为索引，并按时间排序，确保最新数据在最后
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col).sort_index().dropna(subset=[close_col, turnover_col])

        # 确保数据量足够计算 MA20
        if len(df) < MA_LONG:
            return None
        
        # --- 4. 执行筛选条件 ---

        # 4.1. 基本条件检查
        latest_close = df[close_col].iloc[-1]
        latest_turnover = df[turnover_col].iloc[-1]
        
        if latest_close < MIN_CLOSE_PRICE or latest_turnover < MIN_TURNOVER_RATE:
            return None
        
        # 4.2. "眼睛"形态检测 (均线金叉/死叉)
        
        # 计算均线
        df['MA_SHORT'] = df[close_col].rolling(window=MA_SHORT).mean()
        df['MA_LONG'] = df[close_col].rolling(window=MA_LONG).mean()
        
        # 找到 MA_SHORT 和 MA_LONG 的交叉情况
        # 1: MA_SHORT > MA_LONG (金叉或多头排列)
        # 0: MA_SHORT < MA_LONG (死叉或空头排列)
        df['Cross_State'] = (df['MA_SHORT'] > df['MA_LONG']).astype(int)

        # 在最近 LOOKBACK_DAYS 内进行检查
        recent_df = df.iloc[-LOOKBACK_DAYS:].copy()
        
        # 识别交叉点
        # 死叉：状态从 1 变为 0 (MA5下穿MA20)
        # 金叉：状态从 0 变为 1 (MA5上穿MA20)
        recent_df['Dead_Cross'] = (recent_df['Cross_State'].diff() == -1)
        recent_df['Golden_Cross'] = (recent_df['Cross_State'].diff() == 1)
        
        # 找到最近的金叉日期 (GC_Date) 和最近的金叉前的死叉日期 (DC_Date)
        gc_dates = recent_df[recent_df['Golden_Cross']].index
        dc_dates = recent_df[recent_df['Dead_Cross']].index

        if gc_dates.empty or dc_dates.empty:
            return None # 缺乏交叉点，不符合形态

        # 找出最近的那个金叉
        latest_gc_date = gc_dates[-1]
        
        # 找出最近的金叉之前发生的死叉（即“眼睛”的开始）
        # 找到所有发生在 latest_gc_date 之前的死叉
        previous_dc_dates = dc_dates[dc_dates < latest_gc_date]
        
        if previous_dc_dates.empty:
             return None # 没有死叉配合的金叉，不符合“眼睛”形态

        # 最接近金叉的死叉就是形成“眼睛”的死叉
        dc_date = previous_dc_dates[-1]

        # 检查“眼睛”形态的有效性：死叉到金叉的时间间隔要短，体现“短暂下穿后快速回升”
        # 假设“短暂”定义为 1 到 5 个交易日内完成切换
        # "眼睛"的持续时间（交易日数量）
        eye_duration = len(recent_df.loc[dc_date:latest_gc_date]) - 1 

        if 1 <= eye_duration <= 5: # 1 <= 调整周期 <= 5 天 (可根据需要调整)
            # 5. 符合所有条件，返回结果
            return stock_code, latest_close, latest_turnover
            
        return None

    except Exception as e:
        # print(f"处理文件 {stock_code}.csv 时发生未预期的错误: {e}") 
        return None

# --- main 函数 (仅修改了结果 DataFrame 的列名，以及合并逻辑) ---

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
    
    # 4. 读取股票名称匹配文件 (已解决大小写匹配问题)
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str}, usecols=['code', 'name'])
        
        # 关键修改：将 names_df 中的 'code' 和 'name' 列重命名为 'Code' 和 'Name' 以便与 filtered_df 匹配
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'})
        
        # 5. 合并筛选结果和股票名称
        final_output_df = pd.merge(
            filtered_df,
            names_df,
            on='Code',
            how='left'
        )
        
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
