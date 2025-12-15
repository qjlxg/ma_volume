import os
import glob
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_DIR = 'filtered_results'

# 价格筛选条件
MIN_CLOSE_PRICE = 5.0      # 最新收盘价最低限制
MAX_CLOSE_PRICE = 20.0     # 最新收盘价最高限制

# 活跃度条件
MIN_TURNOVER_RATE = 0.5    # 最小换手率（百分比）限制 

# 均线周期 (定义“眼睛”形态)
MA_SHORT = 5
MA_LONG = 20
LOOKBACK_DAYS = 30         # 检查“眼睛”形态的窗口
EYE_DURATION_MAX = 5       # "眼睛"形态持续的最大交易日数量 (短暂下穿后快速金叉)

# ⚠️ 新增量能条件
VOLUME_COLUMN = '成交额'    # 匹配成交额的列名，根据您的 CSV 文件确定
VOLUME_MULTIPLIER = 1.5    # 金叉日成交额必须 >= 前20日成交额均值的 X 倍

# 自动匹配关键词列表
DATE_KEYWORDS = ['日期', 'Date', '交易日期', 'TradeDate', 'TDATE', 'time']
CLOSE_KEYWORDS = ['收盘', 'Close', 'close', 'Adj Close', 'PX_LAST']
TURNOVER_KEYWORDS = ['换手率', 'TurnoverRate', 'Turnover', '换手']
# 匹配成交额关键词
AMOUNT_KEYWORDS = ['成交额', 'Amount', '成交金额', 'TradeAmount']


# --- 工具函数 ---
def find_column_name(df_columns, keywords):
    """
    在DataFrame的列名列表中查找与给定关键词匹配的列名。
    """
    lower_cols = [col.lower() for col in df_columns]
    for keyword in keywords:
        if keyword in df_columns:
            return keyword
        if keyword.lower() in lower_cols:
            return df_columns[lower_cols.index(keyword.lower())]
    return None

def check_code_prefix(code):
    """
    检查股票代码是否属于深沪A股 (00, 60开头)，并排除创业板 (30开头)。
    """
    code = str(code)
    if code.startswith('30'):
        return False
    if code.startswith('00') or code.startswith('60'):
        return True
    return False

def process_single_file(file_path):
    """
    处理单个股票历史数据文件，筛选符合“眼睛”形态和所有条件的股票。
    """
    stock_code = os.path.basename(file_path).split('.')[0]
    
    # 规则 1：排除创业板和其他非A股
    if not check_code_prefix(stock_code):
        return None

    try:
        df = pd.read_csv(file_path)

        # 2. 自动匹配所需的列，新增成交额列
        date_col = find_column_name(df.columns, DATE_KEYWORDS)
        close_col = find_column_name(df.columns, CLOSE_KEYWORDS)
        turnover_col = find_column_name(df.columns, TURNOVER_KEYWORDS)
        amount_col = find_column_name(df.columns, AMOUNT_KEYWORDS) # 新增：成交额
        
        if not date_col or not close_col or not turnover_col or not amount_col:
            # 简化列名缺失处理，直接排除
            return None

        # 3. 数据清洗和准备
        df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
        df[turnover_col] = pd.to_numeric(df[turnover_col], errors='coerce')
        df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce') # 清洗成交额
        
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col).sort_index().dropna(subset=[close_col, turnover_col, amount_col])

        if len(df) < MA_LONG:
            return None
        
        # --- 4. 执行筛选条件 ---

        # 4.1. 价格和活跃度基本条件
        latest_close = df[close_col].iloc[-1]
        latest_turnover = df[turnover_col].iloc[-1]
        
        if not (MIN_CLOSE_PRICE <= latest_close <= MAX_CLOSE_PRICE):
            return None
        
        if latest_turnover < MIN_TURNOVER_RATE:
            return None
        
        # 4.2. "眼睛"形态检测
        df['MA_SHORT'] = df[close_col].rolling(window=MA_SHORT).mean()
        df['MA_LONG'] = df[close_col].rolling(window=MA_LONG).mean()
        df['Cross_State'] = (df['MA_SHORT'] > df['MA_LONG']).astype(int)

        recent_df = df.iloc[-LOOKBACK_DAYS:].copy()
        recent_df['Dead_Cross'] = (recent_df['Cross_State'].diff() == -1)
        recent_df['Golden_Cross'] = (recent_df['Cross_State'].diff() == 1)
        
        gc_dates = recent_df[recent_df['Golden_Cross']].index
        dc_dates = recent_df[recent_df['Dead_Cross']].index

        if gc_dates.empty or dc_dates.empty:
            return None

        latest_gc_date = gc_dates[-1]
        previous_dc_dates = dc_dates[dc_dates < latest_gc_date]
        
        if previous_dc_dates.empty:
             return None 

        dc_date = previous_dc_dates[-1]

        # 检查“眼睛”形态的有效性：持续时间必须短
        eye_duration = len(recent_df.loc[dc_date:latest_gc_date]) - 1 

        if not (1 <= eye_duration <= EYE_DURATION_MAX):
            return None
            
        # ⚠️ 4.3. 新增：形态质量检查 - 量能配合
        
        # 计算金叉日的成交额
        gc_amount = df.loc[latest_gc_date, amount_col]
        
        # 计算金叉日之前 20 日的成交额均值
        # 找到金叉日的前一个索引位置
        gc_index = df.index.get_loc(latest_gc_date)
        if gc_index < MA_LONG - 1: # 确保前面至少有 20 个数据点
            return None
            
        # 取金叉日前 20 个交易日的成交额（不包含金叉日）
        previous_20_amounts = df[amount_col].iloc[gc_index - MA_LONG : gc_index]
        avg_amount = previous_20_amounts.mean()
        
        # 检查量能放大
        if gc_amount < avg_amount * VOLUME_MULTIPLIER:
            return None

        # ⚠️ 4.4. 新增：形态质量检查 - 金叉后无大跌
        
        # 检查从金叉日（包含）到最新交易日（包含）的收盘价
        post_gc_prices = df[close_col].loc[latest_gc_date:].copy()
        gc_close_price = post_gc_prices.iloc[0]

        # 如果金叉后的最低收盘价跌破金叉日的收盘价，则视为形态失败
        if post_gc_prices.min() < gc_close_price:
            return None

        # 5. 符合所有条件，返回结果
        return stock_code, latest_close, latest_turnover

    except Exception as e:
        # 仅在需要调试时取消注释
        # print(f"处理文件 {stock_code}.csv 时发生未预期的错误: {e}") 
        return None

# --- main 函数 (保持不变) ---

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

    filtered_df = pd.DataFrame(results, columns=['Code', 'Latest_Close', 'Latest_Turnover'])
    
    # 4. 读取股票名称匹配文件 (排除 ST 股)
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str}, usecols=['code', 'name'])
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'})
        
        final_output_df = pd.merge(filtered_df, names_df, on='Code', how='left')
        
        # 规则 4：排除名称中包含 'ST' 或 '*ST' 的股票
        st_filter = final_output_df['Name'].str.contains(r'[\*S]T', na=False, regex=True)
        final_output_df = final_output_df[~st_filter]
        
        final_output_df = final_output_df[['Code', 'Name', 'Latest_Close', 'Latest_Turnover']]
        final_output_df['Name'] = final_output_df['Name'].fillna('名称缺失')
        
    except FileNotFoundError:
        print(f"错误: 股票名称文件 {STOCK_NAMES_FILE} 未找到，无法排除 ST 股票和匹配名称。")
        final_output_df = filtered_df
    except ValueError:
        print(f"错误: {STOCK_NAMES_FILE} 文件格式或列名不正确，无法匹配名称。")
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
