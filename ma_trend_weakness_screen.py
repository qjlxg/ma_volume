# ma_trend_weakness_screen.py

import os
import glob
import pandas as pd
from multiprocessing import Pool, cpu_count
from datetime import datetime
import pytz

# --- 配置 ---
STOCK_DATA_DIR = "stock_data"
STOCK_NAMES_FILE = "stock_names.csv"
MA_PERIOD = 20  # 20日均线
SLOPE_CHECK_DAYS = 5 # 检查均线斜率的周期
MIN_CLOSE_PRICE = 5.0 # 最新收盘价：最低要求
MAX_CLOSE_PRICE = 20.0 # 最新收盘价：最高限制

def calculate_ma(df, period):
    """计算指定周期的移动平均线 (MA)"""
    return df['Close'].rolling(window=period).mean()

def check_stock_code_and_name(stock_code, stock_name, latest_close):
    """
    检查股票代码、名称和价格是否符合排除规则：
    1. 排除 ST 股票 (名称中包含 ST 或 *ST)。
    2. 排除创业板 (300xxx)。
    3. 排除其他非深沪 A 股（只保留 60xxxx, 00xxxx, 002xxx, 003xxx）。
    4. 排除价格范围外的股票 (收盘价低于 5.0 或高于 20.0)。
    """
    
    # 价格范围检查
    if latest_close < MIN_CLOSE_PRICE or latest_close > MAX_CLOSE_PRICE:
        return False, f"价格 ({latest_close:.2f}) 不在 [{MIN_CLOSE_PRICE}, {MAX_CLOSE_PRICE}] 范围内"
    
    # ST 股票检查
    if stock_name and ("ST" in stock_name.upper() or "*ST" in stock_name.upper()):
        return False, "排除：ST 股票"

    # 板块检查 (假设股票代码是6位数字的字符串)
    if len(stock_code) != 6:
        # 非标准代码，排除
        return False, "排除：非标准6位代码"
        
    # 创业板 (300xxx, 301xxx)
    if stock_code.startswith('30'):
        return False, "排除：创业板 (30开头)"
        
    # 深沪 A 股代码范围检查 (排除科创板 688xxx, 北交所 8xxxx, 4xxxx)
    # 只保留 上交所A股 (60xxxx) 和 深交所A股/中小板 (00xxxx, 002xxx, 003xxx)
    if not (stock_code.startswith('60') or stock_code.startswith('00')):
        return False, "排除：非深沪A股 (非 60, 00 开头)"

    return True, "通过"


def screen_stock(filepath):
    """
    对单个股票文件进行筛选。
    """
    try:
        # 1. 提取股票代码
        stock_code = os.path.basename(filepath).replace(".csv", "")
        
        # 2. 读取数据
        df = pd.read_csv(filepath)
        df = df.dropna(subset=['Close']).sort_values(by='Date').reset_index(drop=True)
        
        if df.empty or len(df) < MA_PERIOD + SLOPE_CHECK_DAYS:
            # 数据不足以计算均线和斜率
            # print(f"警告：{stock_code} 数据不足。")
            return None

        # 3. 核心数据准备
        df['MA20'] = calculate_ma(df, MA_PERIOD)
        latest_data = df.iloc[-1]
        latest_close = latest_data['Close']
        latest_ma20 = latest_data['MA20']

        # 4. 股票基本面和价格排除（这里先预排除价格，其他排除条件在主函数中利用 stock_names_df 进行）
        # 预先进行价格范围检查
        if latest_close < MIN_CLOSE_PRICE or latest_close > MAX_CLOSE_PRICE:
            return None # 价格不符合，直接排除
        
        # 5. 技术面筛选条件: 20日均线趋势 (走平或向下)
        
        # 确保有足够数据计算前 N 天的 MA20
        if len(df) < MA_PERIOD + SLOPE_CHECK_DAYS:
            return None

        # 获取 N 天前的 MA20 值。
        ma20_n_days_ago = df.iloc[-(SLOPE_CHECK_DAYS + 1)]['MA20']
        
        # 核心逻辑：最新的 MA20 不大于前 N 天的 MA20，即斜率为负或零。
        is_ma20_weakening = latest_ma20 <= ma20_n_days_ago
        
        if is_ma20_weakening:
            return {
                'Code': stock_code,
                'Latest_Close': latest_close,
                'Latest_MA20': latest_ma20,
                'MA20_N_Days_Ago': ma20_n_days_ago
            }

    except Exception as e:
        print(f"处理文件 {filepath} 失败: {e}")
        return None
    
    return None

def main():
    # 1. 查找所有股票数据文件
    file_list = glob.glob(os.path.join(STOCK_DATA_DIR, "*.csv"))
    
    if not file_list:
        print(f"未在目录 {STOCK_DATA_DIR} 中找到任何CSV文件。")
        return

    # 2. 并行处理文件
    print(f"找到 {len(file_list)} 个文件，使用 {cpu_count()} 个核心并行处理...")
    with Pool(cpu_count()) as p:
        results = p.map(screen_stock, file_list)
        
    # 过滤掉 None 值
    screened_df = pd.DataFrame([r for r in results if r is not None])
    
    if screened_df.empty:
        print("技术面筛选后，没有股票符合条件。")
        return

    # 3. 匹配股票名称并应用全部排除条件
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        names_df['Code'] = names_df['Code'].apply(lambda x: str(x).replace(".csv", ""))
        
        # 合并以获取股票名称，用于ST排除
        merged_df = pd.merge(screened_df, names_df[['Code', 'Name']], on='Code', how='left')
        merged_df['Name'] = merged_df['Name'].fillna('名称未知')

    except Exception as e:
        print(f"读取或匹配股票名称文件 {STOCK_NAMES_FILE} 失败: {e}")
        merged_df = screened_df
        merged_df['Name'] = '名称未匹配'

    # 4. 应用额外的排除规则
    final_list = []
    print(f"开始应用板块、ST和最终价格检查...")
    for index, row in merged_df.iterrows():
        stock_code = str(row['Code']).zfill(6) # 确保代码是6位字符串
        stock_name = row['Name']
        latest_close = row['Latest_Close']
        
        is_passed, reason = check_stock_code_and_name(stock_code, stock_name, latest_close)
        
        if is_passed:
            final_list.append(row)
        # else:
            # print(f"排除 {stock_code} ({stock_name}): {reason}") # 可以取消注释查看排除详情

    final_df = pd.DataFrame(final_list)
    
    if final_df.empty:
        print("应用所有排除条件后，没有股票符合条件。")
        return

    print(f"最终筛选出 {len(final_df)} 支符合所有条件的股票。")

    # 5. 设置上海时区并生成文件名
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    now_shanghai = datetime.now(shanghai_tz)
    
    # 路径格式: YYYY/MM/filename_timestamp.csv
    output_dir = now_shanghai.strftime("%Y/%m")
    timestamp_str = now_shanghai.strftime("%Y%m%d_%H%M%S")
    output_filename = f"ma_trend_weakness_strict_results_{timestamp_str}.csv"
    output_path = os.path.join(output_dir, output_filename)

    # 6. 保存结果
    os.makedirs(output_dir, exist_ok=True)
    # 重新整理输出列
    final_df[['Code', 'Name', 'Latest_Close', 'Latest_MA20', 'MA20_N_Days_Ago']].to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"筛选结果已成功保存到: {output_path}")

if __name__ == "__main__":
    # 确保 stock_data 目录存在以便进行测试
    if not os.path.exists(STOCK_DATA_DIR):
        print(f"请创建 '{STOCK_DATA_DIR}' 目录并放入股票数据CSV文件。")
    if not os.path.exists(STOCK_NAMES_FILE):
        print(f"请确保 '{STOCK_NAMES_FILE}' 文件存在于根目录。")

    main()
