# 文件名: bottom_double_bullish_scanner.py - V3.0 (放宽条件版)

import pandas as pd
import glob
import os
from datetime import datetime
import pytz

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE_DIR = '.'
MIN_CLOSE_PRICE = 5.0

# 定义您的CSV文件中的中文列名映射到脚本中使用的英文名称
COLUMN_MAP = {
    '日期': 'Date',
    '开盘': 'Open',
    '收盘': 'Close',
    '最高': 'High',
    '最低': 'Low',
    '成交量': 'Volume',
    '成交额': 'Amount', 
}

# --- 关键筛选参数 ---
# 【调整参数 1】：阳线涨幅要求 (C1 和 C2 信号)
MIN_BULLISH_CHANGE = 0.01  # 调整为 1.0% (原 2.0%)
# 【调整参数 2】：放量倍数要求 (C1 信号)
MIN_VOL_MULTIPLIER = 1.2   # 调整为 1.2 倍于20日均量 (原 1.5 倍)
# 【保持不变】：C2 成交量必须 >= C1 成交量
# 【保持不变】：C2 收盘价必须 > C1 收盘价
# 【保持不变】：底部趋势检查 (T-20 日收盘价)
# 【保持不变】：回调限制 (T-1 日收盘价 >= C1 日收盘价 * 0.95)

# ------------------------------------

def load_stock_names(filepath):
    """加载股票代码和名称的映射"""
    try:
        names_df = pd.read_csv(filepath, encoding='utf-8-sig', dtype={'code': str})
        
        if 'code' not in names_df.columns or 'name' not in names_df.columns:
             print("错误: stock_names.csv 必须包含 'code' 和 'name' 两列。")
             return {}

        names_df['code'] = names_df['code'].astype(str)
        names_map = names_df.set_index('code')['name'].to_dict()
        return names_map
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def check_bottom_double_bullish(df: pd.DataFrame, stock_code: str, names_map: dict) -> list:
    """
    检查单个股票数据是否满足“底部双倍阳”条件
    """
    # 确保数据按日期升序排列
    df = df.sort_values(by='Date').reset_index(drop=True)
    
    # 至少需要 20 个交易日来计算均值和趋势
    if df.empty or len(df) < 20:
        return []

    # 过滤最新收盘价低于 MIN_CLOSE_PRICE 的股票
    if df['Close'].iloc[-1] < MIN_CLOSE_PRICE:
        return []

    # 1. 计算所需的技术指标 (使用矢量化提高速度)
    df['Vol_Avg_20'] = df['Volume'].rolling(window=20).mean()
    df['Close_20_Ago'] = df['Close'].shift(20)
    df['Change_C'] = (df['Close'] - df['Open']) / df['Open'] # 阳线涨幅
    
    # 2. 底部/下跌趋势检查 (T 日收盘价低于 T-20 日收盘价)
    df['Is_Downtrend'] = df['Close'] < df['Close_20_Ago']
    
    # 3. 第一次阳线 (C1) 信号 - T-4 日
    C1_day = df.shift(-4) # 检查 T-4 日的数据
    
    # 【修改点 1】：T-4 日为阳线且涨幅 >= MIN_BULLISH_CHANGE
    C1_is_bullish = C1_day['Change_C'] >= MIN_BULLISH_CHANGE 
    # 【修改点 2】：T-4 日放量 (Volume >= AvgVolume * MIN_VOL_MULTIPLIER)
    C1_is_high_vol = C1_day['Volume'] >= C1_day['Vol_Avg_20'] * MIN_VOL_MULTIPLIER
    
    df['Is_C1'] = C1_is_bullish & C1_is_high_vol
    
    # 4. 震荡/回调检查 - T-1 日
    # T-1 日收盘价不低于 C1 日收盘价的 95%
    df['C1_Close'] = C1_day['Close']
    df['Consolidation_OK'] = df['Close'].shift(-1) >= df['C1_Close'] * 0.95
    
    # 5. 第二次阳线 (C2) 确认 - T 日 (最新日)
    
    # 【修改点 3】：T 日为阳线且涨幅 >= MIN_BULLISH_CHANGE
    C2_is_bullish = df['Change_C'] >= MIN_BULLISH_CHANGE
    # T 日成交量 >= C1 日成交量
    C2_is_higher_vol = df['Volume'] >= C1_day['Volume']
    # T 日收盘价 > C1 日收盘价
    C2_new_high = df['Close'] > C1_day['Close']
    
    df['Is_C2'] = C2_is_bullish & C2_is_higher_vol & C2_new_high
    
    # 6. 综合所有条件
    # 仅关注最后一行数据
    signal_mask = df.index == df.index[-1] 
    
    signal_mask &= df['Is_Downtrend'] & df['Is_C1'] & df['Consolidation_OK'] & df['Is_C2']

    latest_signal = df[signal_mask].iloc[-1] if signal_mask.any() else None

    results = []
    if latest_signal is not None:
        signal_date = latest_signal['Date']
        stock_name = names_map.get(stock_code, '未知名称')
        results.append({
            '代码': stock_code,
            '名称': stock_name,
            '信号日期': signal_date,
            '最新收盘价': latest_signal['Close']
        })
        
    return results

def main():
    """主函数，负责加载数据、并行处理和结果保存"""
    
    # 1. 设置时区并获取当前时间 (上海时区)
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(shanghai_tz)
    
    # 2. 确定输出目录和文件名
    output_dir = now.strftime('%Y-%m')
    output_path = os.path.join(OUTPUT_BASE_DIR, output_dir)
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    output_filename = f"bottom_double_bullish_scanner_{timestamp}.csv"
    output_filepath = os.path.join(output_path, output_filename)
    
    # 3. 加载股票名称
    names_map = load_stock_names(STOCK_NAMES_FILE)
    if not names_map:
        print("警告: 未能加载股票名称文件，结果中将使用 '未知名称'。")

    # 4. 扫描所有 CSV 文件
    csv_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    all_results = []
    
    print(f"找到 {len(csv_files)} 个股票数据文件，开始扫描...")

    for i, file_path in enumerate(csv_files):
        stock_code = os.path.basename(file_path).split('.')[0]
        
        try:
            # 读取数据并重命名列
            df = pd.read_csv(file_path, encoding='utf-8-sig', parse_dates=['日期'])
            df = df.rename(columns=COLUMN_MAP)
            df = df[['Date', 'Open', 'Close', 'High', 'Low', 'Volume']].dropna()
            
            # 运行分析
            results = check_bottom_double_bullish(df, stock_code, names_map)
            all_results.extend(results)
            
        except Exception as e:
            # 打印错误，但继续处理下一个文件
            print(f"处理文件 {file_path} 时发生错误: {e}")
            
    # 5. 保存结果
    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df = results_df[['代码', '名称', '信号日期', '最新收盘价']] 
        results_df.to_csv(output_filepath, index=False, encoding='utf-8-sig')
        print(f"\n筛选完成。找到 {len(all_results)} 个信号，结果已保存至: {output_filepath}")
    else:
        print("\n筛选完成。未找到符合条件的 '底部双倍阳' 信号。")

if __name__ == '__main__':
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"错误: 股票数据目录 '{STOCK_DATA_DIR}' 不存在。")
    elif not os.path.isfile(STOCK_NAMES_FILE):
        print(f"警告: 股票名称文件 '{STOCK_NAMES_FILE}' 不存在。")
    else:
        main()
