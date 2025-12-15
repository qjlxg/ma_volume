import pandas as pd
import os
import glob
from datetime import datetime
import pytz
import multiprocessing as mp

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0

# 设置上海时区
SH_TZ = pytz.timezone('Asia/Shanghai')

def is_stacked_multi_cannon(df):
    """
    判断 K 线数据（通常是日线）是否形成了“叠形多方炮”形态。
    形态定义（以阳线代表上涨，阴线代表下跌）：
    1. 连续至少 4 根 K 线。
    2. K1: 阴线或小实体 K 线。
    3. K2, K3: 连续阳线（收盘价 > 开盘价），实体较小。
       - K2, K3 的收盘价和最高价不能大幅突破 K1 的最高价，形成整理。
       - K3 的收盘价高于 K2 的收盘价（或平稳）。
    4. K4: 突破大阳线（收盘价 > 开盘价），实体较大，且突破了 K1/K2/K3 的高点。

    这里简化和量化逻辑：
    - 至少需要 4 根 K 线 (N=4)。
    - K1 (前第四日): 实体不限。
    - K2, K3 (前第三日、第二日): 阳线 (Close > Open)，且高点 H2 < H3 且 H3 < H4。
    - K4 (最新日): 阳线 (Close > Open)，且 Close4 突破 H1, H2, H3 的最高点。
    - 重点关注最近 4 个交易日。
    """
    if len(df) < 4:
        return False

    # 取最近的 4 根 K 线
    df_recent = df.iloc[-4:]
    
    # 检查所有必要的列是否存在
    if not all(col in df_recent.columns for col in ['Open', 'Close', 'High', 'Low']):
        print("缺少必要的 K 线数据列 (Open, Close, High, Low)")
        return False
    
    O, C, H, L = df_recent['Open'].values, df_recent['Close'].values, df_recent['High'].values, df_recent['Low'].values
    
    # K1, K2, K3, K4 的索引是 0, 1, 2, 3

    # 1. K2 和 K3 必须是阳线（Close > Open）
    is_k2_up = C[1] > O[1]
    is_k3_up = C[2] > O[2]
    if not (is_k2_up and is_k3_up):
        return False

    # 2. K4 必须是突破大阳线（Close > Open）
    is_k4_up = C[3] > O[3]
    if not is_k4_up:
        return False

    # 3. K2, K3 形成整理或叠升，实体相对较小
    # 简化：K2/K3 实体小于 K4 实体的一半
    k2_body_size = abs(C[1] - O[1])
    k3_body_size = abs(C[2] - O[2])
    k4_body_size = abs(C[3] - O[3])
    
    if not (k2_body_size < 0.5 * k4_body_size and k3_body_size < 0.5 * k4_body_size):
        return False

    # 4. K4 突破 K1, K2, K3 的最高价
    max_prev_high = max(H[0], H[1], H[2])
    
    # K4 的收盘价必须突破前三根 K 线的最高价
    if C[3] <= max_prev_high:
        return False
        
    # 5. K4 的最新收盘价过滤
    if C[3] < MIN_CLOSE_PRICE:
        return False

    return True

def process_single_file(file_path):
    """处理单个股票数据文件，检查形态并返回代码（如果符合）"""
    stock_code = os.path.basename(file_path).split('.')[0]
    try:
        # 假设 CSV 包含至少 Date, Open, Close, High, Low 列
        df = pd.read_csv(file_path, parse_dates=['Date'])
        # 确保数据按日期排序
        df = df.sort_values(by='Date').reset_index(drop=True)

        if is_stacked_multi_cannon(df):
            print(f"✅ 发现符合形态的股票代码: {stock_code}")
            return stock_code
        
    except Exception as e:
        print(f"❌ 处理文件 {file_path} 出错: {e}")
        
    return None

def main():
    print(f"--- 股票形态扫描器启动 ({datetime.now(SH_TZ).strftime('%Y-%m-%d %H:%M:%S')}) ---")
    
    # 1. 查找所有数据文件
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"未在 '{STOCK_DATA_DIR}' 目录下找到任何 CSV 文件。请确保数据已上传。")
        return

    # 2. 并行处理所有文件
    print(f"开始扫描 {len(all_files)} 个股票文件...")
    pool = mp.Pool(mp.cpu_count())
    found_codes = pool.map(process_single_file, all_files)
    pool.close()
    pool.join()
    
    # 过滤掉 None 值
    found_codes = [code for code in found_codes if code is not None]
    
    if not found_codes:
        print("未找到符合 '叠形多方炮' 形态的股票。")
        return

    # 3. 匹配股票名称
    print(f"共发现 {len(found_codes)} 只符合形态的股票，开始匹配名称...")
    try:
        # 假设 stock_names.csv 格式为 'code', 'name'
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
        names_df = names_df.set_index('code')['name'].to_dict()
    except Exception as e:
        print(f"读取或处理 '{STOCK_NAMES_FILE}' 文件失败: {e}。将只输出代码。")
        names_df = {}
        
    # 4. 组织结果
    results = []
    for code in found_codes:
        name = names_df.get(code, '名称未找到')
        results.append({'股票代码': code, '股票名称': name})
        
    results_df = pd.DataFrame(results)

    # 5. 保存结果
    now = datetime.now(SH_TZ)
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    year_month_dir = now.strftime('%Y/%m')
    
    # 创建输出目录
    output_dir = os.path.join('scan_results', year_month_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # 结果文件名
    output_filename = f'stacked_multi_cannon_{timestamp_str}.csv'
    output_path = os.path.join(output_dir, output_filename)
    
    results_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\n🎉 筛选结果已成功保存到: {output_path}")

if __name__ == "__main__":
    main()
