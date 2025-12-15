# giant_rock_bottom_scanner.py

import os
import pandas as pd
import glob
from pathlib import Path
from datetime import datetime
import concurrent.futures
import pytz

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE_DIR = 'scan_results'

# 价格和板块筛选条件
MIN_CLOSING_PRICE = 5.0
MAX_CLOSING_PRICE = 20.0
EXCLUDED_PREFIXES = ('30',)  # 排除 30 开头的股票代码 (创业板)
INCLUDED_PREFIXES = ('60', '00') # 仅保留 60 (沪市), 00 (深市) 开头

# 设置时区为上海/北京时间
SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')

def load_stock_names():
    """加载股票代码和名称映射表"""
    if not Path(STOCK_NAMES_FILE).exists():
        print(f"警告: 股票名称文件 {STOCK_NAMES_FILE} 不存在。将只输出代码。")
        return {}
    try:
        # 假设 stock_names.csv 格式为 (code, name)
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
        return names_df.set_index('code')['name'].to_dict()
    except Exception as e:
        print(f"读取 {STOCK_NAMES_FILE} 失败: {e}")
        return {}

def check_stock_criteria(stock_code: str, stock_name: str, latest_close: float) -> bool:
    """检查股票是否符合额外的筛选标准：价格、板块和 ST 排除"""

    # 1. 板块排除：排除 30 开头，且只保留 60, 00 开头
    if stock_code.startswith(EXCLUDED_PREFIXES):
        # print(f"排除 {stock_code}: 创业板 (30开头)") # 调试用
        return False
    if not stock_code.startswith(INCLUDED_PREFIXES):
        # print(f"排除 {stock_code}: 非深沪A股 (非60/00开头)") # 调试用
        return False

    # 2. 价格限定：5.0 元 <= 最新收盘价 <= 20.0 元
    if not (MIN_CLOSING_PRICE <= latest_close <= MAX_CLOSING_PRICE):
        # print(f"排除 {stock_code}: 价格 ({latest_close} 不在 5.0-20.0 范围内)") # 调试用
        return False

    # 3. 排除 ST 股
    if 'ST' in stock_name.upper() or '*ST' in stock_name.upper():
        # print(f"排除 {stock_code}: ST股") # 调试用
        return False

    return True


def check_giant_rock_bottom(df: pd.DataFrame, stock_code: str, names_map: dict) -> list:
    """
    检查数据集中是否存在“巨石沉底”形态。
    形态定义（C1, C2, C3为连续三日K线，C3为最新）：
    1. C1 (Day N-2): 阴线 (Close < Open).
    2. C2 (Day N-1): 小实体，且实体完全位于 C1 实体下方 (巨石沉底).
        - max(C2.O, C2.C) < min(C1.O, C1.C)
    3. C3 (Day N): 阳线 (Close > Open)，且收盘价高于 C1 开盘价 (强势反转).
        - C3.Close > C1.Open
    4. 额外条件：符合价格、板块和 ST 排除。
    """
    results = []

    # 确保数据至少有3行
    if len(df) < 3:
        return results

    # 仅保留最近三天的K线数据 (C3, C2, C1)
    recent_data = df.tail(3).reset_index(drop=True)

    # 提取 K 线数据
    C1 = recent_data.iloc[0]  # N-2
    C2 = recent_data.iloc[1]  # N-1
    C3 = recent_data.iloc[2]  # N (最新交易日)
    
    stock_name = names_map.get(stock_code, '未知名称')

    # --- 增加外部条件筛选 ---
    if not check_stock_criteria(stock_code, stock_name, C3['Close']):
        return results

    # --- 巨石沉底形态判断 ---

    # 1. C1: 阴线
    is_c1_bearish = C1['Close'] < C1['Open']

    # 2. C2: 小实体且完全位于 C1 实体下方
    c1_body_low = min(C1['Open'], C1['Close'])
    c2_body_high = max(C2['Open'], C2['Close'])
    is_c2_sunken = c2_body_high < c1_body_low
    # 辅助条件：C2实体相对C1较小 (避免极端情况，增强形态识别的可靠性)
    c1_body_size = abs(C1['Open'] - C1['Close'])
    c2_body_size = abs(C2['Open'] - C2['Close'])
    is_c2_small_body = c2_body_size < 0.5 * c1_body_size

    # 3. C3: 阳线且强势反转 (吞没或强劲拉升)
    is_c3_bullish = C3['Close'] > C3['Open']
    is_c3_strong_reversal = C3['Close'] > C1['Open'] # 收盘价高于 C1 开盘价

    # 综合判断
    if is_c1_bearish and is_c2_sunken and is_c2_small_body and is_c3_bullish and is_c3_strong_reversal:
        latest_date = C3['Date'] if 'Date' in C3 else 'N/A'
        results.append({
            '代码': stock_code,
            '名称': stock_name,
            '最新收盘价': C3['Close'],
            '收盘日期': latest_date,
            '形态': '巨石沉底 (看涨反转)'
        })

    return results

def process_file(file_path: Path, names_map: dict) -> list:
    """处理单个 CSV 文件，筛选形态"""
    stock_code = file_path.stem  # 文件名即为股票代码
    
    # 提前进行简单的代码前缀检查，提高效率
    if not stock_code.startswith(INCLUDED_PREFIXES) or stock_code.startswith(EXCLUDED_PREFIXES):
        return []
        
    try:
        # 假设 CSV 列名包含 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'
        df = pd.read_csv(file_path)
        # 确保数据按日期升序排列
        df = df.sort_values(by='Date').reset_index(drop=True)
        return check_giant_rock_bottom(df, stock_code, names_map)
    except Exception as e:
        # print(f"处理文件 {file_path.name} 失败: {e}")
        return []

def main():
    start_time = datetime.now(SHANGHAI_TZ)
    print(f"--- 巨石沉底形态扫描开始 ({start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}) ---")
    print(f"筛选条件：价格 [{MIN_CLOSING_PRICE:.2f}-{MAX_CLOSING_PRICE:.2f}] 元，排除 ST 和 30 开头。")

    # 1. 加载股票名称
    names_map = load_stock_names()

    # 2. 扫描数据文件
    all_files = list(Path(STOCK_DATA_DIR).rglob('*.csv'))
    if not all_files:
        print(f"错误: 在目录 {STOCK_DATA_DIR} 中未找到任何 CSV 数据文件。")
        return

    # 3. 并行处理文件
    all_results = []
    # 使用线程池加速I/O密集型任务
    max_workers = os.cpu_count() * 2 if os.cpu_count() else 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_file = {executor.submit(process_file, f, names_map): f for f in all_files}
        
        # 收集结果
        for future in concurrent.futures.as_completed(future_to_file):
            results = future.result()
            if results:
                all_results.extend(results)

    # 4. 结果处理和保存
    if not all_results:
        print("未找到符合 '巨石沉底' 形态和筛选条件的股票。")
        return

    results_df = pd.DataFrame(all_results)
    
    # 获取当前上海时区时间戳作为文件名的一部分
    now_shanghai = datetime.now(SHANGHAI_TZ)
    timestamp_str = now_shanghai.strftime('%Y%m%d_%H%M%S')
    
    # 定义输出路径 (年月目录)
    output_dir = Path(OUTPUT_BASE_DIR) / now_shanghai.strftime('%Y') / now_shanghai.strftime('%m')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 定义文件名
    output_filename = f"giant_rock_bottom_scan_{timestamp_str}.csv"
    output_path = output_dir / output_filename

    # 保存结果
    results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n--- 扫描完成 ---")
    print(f"找到 {len(results_df)} 个符合条件的股票。")
    print(f"结果已保存至: {output_path}")

    end_time = datetime.now(SHANGHAI_TZ)
    duration = end_time - start_time
    print(f"总耗时: {duration.total_seconds():.2f} 秒")

if __name__ == '__main__':
    main()
