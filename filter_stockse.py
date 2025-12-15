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
MAX_CLOSE_PRICE = 20.0     # 最新收盘价最高限制 (新增)

# 活跃度条件
MIN_TURNOVER_RATE = 0.5    # 最小换手率（百分比）限制 (活跃度辅助条件)

# 均线周期 (定义“眼睛”形态)
MA_SHORT = 5
MA_LONG = 20
LOOKBACK_DAYS = 30 # 检查“眼睛”形态的窗口，例如在最近30个交易日内形成
EYE_DURATION_MAX = 5 # "眼睛"形态持续的最大交易日数量 (短暂下穿后快速金叉)

# 自动匹配关键词列表
DATE_KEYWORDS = ['日期', 'Date', '交易日期', 'TradeDate', 'TDATE', 'time']
CLOSE_KEYWORDS = ['收盘', 'Close', 'close', 'Adj Close', 'PX_LAST']
TURNOVER_KEYWORDS = ['换手率', 'TurnoverRate', 'Turnover', '换手']


# --- 工具函数 ---
def find_column_name(df_columns, keywords):
    """
    在DataFrame的列名列表中查找与给定关键词匹配的列名。
    返回第一个匹配到的列名，如果找不到则返回 None。
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
    # 排除创业板 30 开头
    if code.startswith('30'):
        return False
    # 只保留深沪A股 (00, 60 开头)
    if code.startswith('00') or code.startswith('60'):
        return True
    return False

def process_single_file(file_path):
    """
    处理单个股票历史数据文件，筛选符合“眼睛”形态和基本条件的股票。
    """
    stock_code = os.path.basename(file_path).split('.')[0]
    
    # ⚠️ 规则 1：排除创业板 (30开头) 和其他非A股
    if not check_code_prefix(stock_code):
        return None

    try:
        # 1. 尝试读取文件
        df = pd.read_csv(file_path)

        # 2. 自动匹配所需的列
        date_col = find_column_name(df.columns, DATE_KEYWORDS)
        close_col = find_column_name(df.columns, CLOSE_KEYWORDS)
        turnover_col = find_column_name(df.columns, TURNOVER_KEYWORDS)
        
        if not date_col or not close_col or not turnover_col:
            # 列名缺失，直接排除
            return None

        # 3. 数据清洗和准备
        df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
        df[turnover_col] = pd.to_numeric(df[turnover_col], errors='coerce')
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col).sort_index().dropna(subset=[close_col, turnover_col])

        if len(df) < MA_LONG:
            return None
        
        # --- 4. 执行筛选条件 ---

        # 4.1. 价格和活跃度条件检查
        latest_close = df[close_col].iloc[-1]
        latest_turnover = df[turnover_col].iloc[-1]
        
        # ⚠️ 规则 2：最新收盘价不低于 5.0 元且不高于 20.0 元
        if not (MIN_CLOSE_PRICE <= latest_close <= MAX_CLOSE_PRICE):
            return None
        
        # 活跃度检查
        if latest_turnover < MIN_TURNOVER_RATE:
            return None
        
        # 4.2. "眼睛"形态检测 (均线金叉/死叉)
        
        df['MA_SHORT'] = df[close_col].rolling(window=MA_SHORT).mean()
        df['MA_LONG'] = df[close_col].rolling(window=MA_LONG).mean()
        
        # Cross_State: 1 (MA_SHORT > MA_LONG), 0 (MA_SHORT < MA_LONG)
        df['Cross_State'] = (df['MA_SHORT'] > df['MA_LONG']).astype(int)

        recent_df = df.iloc[-LOOKBACK_DAYS:].copy()
        
        # 识别交叉点
        recent_df['Dead_Cross'] = (recent_df['Cross_State'].diff() == -1)
        recent_df['Golden_Cross'] = (recent_df['Cross_State'].diff() == 1)
        
        gc_dates = recent_df[recent_df['Golden_Cross']].index
        dc_dates = recent_df[recent_df['Dead_Cross']].index

        if gc_dates.empty or dc_dates.empty:
            return None

        # 找出最近的那个金叉
        latest_gc_date = gc_dates[-1]
        
        # 找出最近的金叉之前发生的死叉
        previous_dc_dates = dc_dates[dc_dates < latest_gc_date]
        
        if previous_dc_dates.empty:
             return None 

        dc_date = previous_dc_dates[-1]

        # 检查“眼睛”形态的有效性：死叉到金叉的时间间隔要短
        eye_duration = len(recent_df.loc[dc_date:latest_gc_date]) - 1 

        # ⚠️ 规则 3：形态持续时间必须在 1 到 EYE_DURATION_MAX (5) 个交易日内完成
        if 1 <= eye_duration <= EYE_DURATION_MAX:
            # 5. 符合所有条件，返回结果
            return stock_code, latest_close, latest_turnover
            
        return None

    except Exception as e:
        # print(f"处理文件 {stock_code}.csv 时发生未预期的错误: {e}") 
        return None

# --- main 函数 ---

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

    # 3. 将筛选结果转换为 DataFrame
    filtered_df = pd.DataFrame(results, columns=['Code', 'Latest_Close', 'Latest_Turnover'])
    
    # 4. 读取股票名称匹配文件 (用于排除 ST 和 *ST 股票)
    try:
        # 明确读取 'code' 和 'name' 列
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str}, usecols=['code', 'name'])
        
        # 关键修改：重命名并合并
        names_df = names_df.rename(columns={'code': 'Code', 'name': 'Name'})
        
        # 5. 合并筛选结果和股票名称
        final_output_df = pd.merge(
            filtered_df,
            names_df,
            on='Code',
            how='left'
        )
        
        # ⚠️ 规则 4：排除名称中包含 'ST' 或 '*ST' 的股票
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
