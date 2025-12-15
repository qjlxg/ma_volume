# 文件名: bottom_double_bullish_scanner.py

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

# ------------------------------------

def load_stock_names(filepath):
    """加载股票代码和名称的映射"""
    try:
        # 假设 stock_names.csv 包含 'code' 和 'name' 列
        names_df = pd.read_csv(filepath, dtype={'code': str})
        names_df['code'] = names_df['code'].astype(str)
        # 确保代码格式为6位字符串，如果文件中的代码是数字需要转换
        names_map = names_df.set_index('code')['name'].to_dict()
        return names_map
    except Exception as e:
        print(f"Error loading stock names: {e}")
        return {}

def check_bottom_double_bullish(df: pd.DataFrame, stock_code: str, names_map: dict) -> list:
    """
    检查单个股票数据是否满足“底部双倍阳”条件
    :param df: 包含 ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'] 的数据框
    :param stock_code: 股票代码
    :param names_map: 股票代码-名称映射
    :return: 满足条件的信号列表 (Code, Name, Date)
    """
    if df.empty or len(df) < 20:
        return []

    # 确保数据按日期升序排列
    df = df.sort_values(by='Date').reset_index(drop=True)
    
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
    
    # T-4 日为阳线且涨幅 >= 2%
    C1_is_bullish = C1_day['Change_C'] >= 0.02 
    # T-4 日放量 (Volume >= AvgVolume * 1.5)
    C1_is_high_vol = C1_day['Volume'] >= C1_day['Vol_Avg_20'] * 1.5
    
    df['Is_C1'] = C1_is_bullish & C1_is_high_vol
    
    # 4. 震荡/回调检查 - T-1 日
    # T-1 日收盘价不低于 C1 日收盘价的 95%
    df['C1_Close'] = C1_day['Close']
    df['Consolidation_OK'] = df['Close'].shift(-1) >= df['C1_Close'] * 0.95
    
    # 5. 第二次阳线 (C2) 确认 - T 日 (最新日)
    
    # T 日为阳线且涨幅 >= 2%
    C2_is_bullish = df['Change_C'] >= 0.02
    # T 日成交量 >= C1 日成交量
    C2_is_higher_vol = df['Volume'] >= C1_day['Volume']
    # T 日收盘价 > C1 日收盘价
    C2_new_high = df['Close'] > C1_day['Close']
    
    df['Is_C2'] = C2_is_bullish & C2_is_higher_vol & C2_new_high
    
    # 6. 综合所有条件
    # C1 必须发生，且 T-1 日的震荡/回调条件满足，且 C2 发生
    signal_mask = df['Is_Downtrend'] & df['Is_C1'] & df['Consolidation_OK'] & df['Is_C2']
    
    # 找到最新一个满足条件的日期 (由于我们只看最后一条数据，所以只取最后一行)
    latest_signal = df[signal_mask].iloc[-1] if signal_mask.any() else None

    results = []
    if latest_signal is not None:
        # 返回信号当日的日期 (C2 日期)
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
    
    # 由于 I/O 密集型操作在 GitHub Actions 中并行化效果不一定更好，
    # 且为了简化，这里使用顺序处理，但利用 pandas 内部的矢量化提高处理效率。
    print(f"找到 {len(csv_files)} 个股票数据文件，开始扫描...")

    for i, file_path in enumerate(csv_files):
        # 从文件名获取股票代码 (例如: 'stock_data/603693.csv' -> '603693')
        stock_code = os.path.basename(file_path).split('.')[0]
        
        try:
            # 读取数据
            df = pd.read_csv(file_path, parse_dates=['Date'])
            
            # 运行分析
            results = check_bottom_double_bullish(df, stock_code, names_map)
            all_results.extend(results)
            
        except Exception as e:
            print(f"处理文件 {file_path} 时发生错误: {e}")
            
    # 5. 保存结果
    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df = results_df[['代码', '名称', '信号日期', '最新收盘价']] # 调整列顺序
        results_df.to_csv(output_filepath, index=False, encoding='utf-8-sig')
        print(f"\n筛选完成。找到 {len(all_results)} 个信号，结果已保存至: {output_filepath}")
    else:
        print("\n筛选完成。未找到符合条件的 '底部双倍阳' 信号。")
        # 如果没有结果，创建一个空文件，或者不创建。这里选择不创建，但返回信息给用户。

if __name__ == '__main__':
    # 检查所需的目录是否存在
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"错误: 股票数据目录 '{STOCK_DATA_DIR}' 不存在。请确保已创建该目录并将CSV文件放入其中。")
    elif not os.path.isfile(STOCK_NAMES_FILE):
        print(f"警告: 股票名称文件 '{STOCK_NAMES_FILE}' 不存在。")
    else:
        main()
