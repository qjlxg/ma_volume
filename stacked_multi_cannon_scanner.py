import pandas as pd
import os
import glob
import re # 导入正则表达式库用于ST排除
from datetime import datetime
import pytz
import multiprocessing as mp

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSE_PRICE = 5.0
MAX_CLOSE_PRICE = 20.0 # 新增上限过滤

# 设置上海时区
SH_TZ = pytz.timezone('Asia/Shanghai')

# 定义中文列名到英文标准列名的映射 (基于用户提供的格式)
COLUMN_MAPPING = {
    '日期': 'Date',
    '开盘': 'Open',
    '收盘': 'Close',
    '最高': 'High',
    '最低': 'Low',
    '成交量': 'Volume',
    '成交额': 'Amount',
    '股票代码': 'Code' 
}

def is_stacked_multi_cannon(df):
    """
    判断 K 线数据（依赖于重命名后的英文列名：Open, Close, High, Low）是否形成了
    “叠形多方炮”形态。
    
    （形态量化逻辑保持不变）
    """
    if len(df) < 4:
        return False

    # 取最近的 4 根 K 线
    df_recent = df.iloc[-4:]
    
    # 检查所有必要的列是否存在
    required_cols = ['Open', 'Close', 'High', 'Low']
    if not all(col in df_recent.columns for col in required_cols):
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
        
    # 5. K4 的最新收盘价过滤 (新增上限)
    latest_close = C[3]
    if not (MIN_CLOSE_PRICE <= latest_close <= MAX_CLOSE_PRICE):
        return False

    return True


def process_single_file(file_path):
    """处理单个股票数据文件，检查形态并返回代码（如果符合）"""
    stock_code = os.path.basename(file_path).split('.')[0]
    
    # 排除 30 开头的股票代码 (创业板)
    if stock_code.startswith('30'):
        return None
        
    # 排除非深沪A股（主要保留 60/00 开头），但由于数据文件是从 stock_data 读取的，
    # 且已排除 30 开头，这里仅需确保代码是 6位数字即可。
    # 假设您的数据目录只包含股票数据文件。

    try:
        df = pd.read_csv(file_path)
        
        # 1. 重命名列以适应脚本逻辑
        df = df.rename(columns=COLUMN_MAPPING)
        
        required_cols = ['Date', 'Open', 'Close', 'High', 'Low']
        if not all(col in df.columns for col in required_cols):
            return None
        
        # 2. 解析日期并清理 NaN
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date', 'Open', 'Close', 'High', 'Low'])

        # 3. 确保数据按日期排序
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        # 4. 执行形态检查和收盘价过滤
        if is_stacked_multi_cannon(df):
            return stock_code
        
    except Exception as e:
        print(f"❌ 处理文件 {file_path} 出错: {e}")
        
    return None

def filter_st(results_df, names_df):
    """排除名称中含有 *ST 或 ST 的股票"""
    
    # 将名称映射到结果 DataFrame
    name_map = names_df.set_index('code')['name'].to_dict()
    results_df['股票名称'] = results_df['股票代码'].map(name_map)
    
    # 使用正则表达式过滤名称中包含 *ST 或 ST 的股票
    # re.IGNORECASE 忽略大小写
    st_mask = results_df['股票名称'].apply(lambda x: bool(re.search(r'\*?ST', str(x), re.IGNORECASE)))
    
    filtered_df = results_df[~st_mask]
    
    # 统计排除数量并输出
    excluded_count = len(results_df) - len(filtered_df)
    if excluded_count > 0:
        print(f"已根据名称过滤条件排除 {excluded_count} 只 ST/退市风险股票。")
        
    return filtered_df

def main():
    print(f"--- 股票形态扫描器启动 ({datetime.now(SH_TZ).strftime('%Y-%m-%d %H:%M:%S')}) ---")
    
    # 1. 查找所有数据文件
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    if not all_files:
        print(f"未在 '{STOCK_DATA_DIR}' 目录下找到任何 CSV 文件。请确保数据已上传。")
        return

    # 2. 并行处理所有文件 (包含 30 开头的代码排除)
    print(f"开始扫描 {len(all_files)} 个股票文件...")
    pool = mp.Pool(mp.cpu_count())
    found_codes = pool.map(process_single_file, all_files)
    pool.close()
    pool.join()
    
    found_codes = [code for code in found_codes if code is not None]
    
    if not found_codes:
        print("未找到符合 '叠形多方炮' 形态且符合价格/板块过滤条件的股票。")
        return

    # 3. 匹配股票名称并执行 ST 排除 (需要先加载 names_df)
    print(f"初筛得到 {len(found_codes)} 只股票，开始匹配名称并执行 ST 过滤...")
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'code': str})
    except Exception as e:
        print(f"读取 '{STOCK_NAMES_FILE}' 失败: {e}。无法进行 ST 过滤。")
        names_df = pd.DataFrame({'code': [], 'name': []})

    # 组织结果 DataFrame (用于 ST 过滤)
    results_df_raw = pd.DataFrame({'股票代码': found_codes})
    
    # 4. 执行 ST 过滤
    results_df = filter_st(results_df_raw, names_df)
    
    if results_df.empty:
        print("经过 ST 过滤后，没有股票符合条件。")
        return
    
    print(f"最终筛选得到 {len(results_df)} 只符合条件的股票。")

    # 5. 保存结果
    now = datetime.now(SH_TZ)
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    year_month_dir = now.strftime('%Y/%m')
    
    output_dir = os.path.join('scan_results', year_month_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    output_filename = f'stacked_multi_cannon_{timestamp_str}.csv'
    output_path = os.path.join(output_dir, output_filename)
    
    # 确保 '股票代码' 和 '股票名称' 列的顺序
    final_cols = ['股票代码', '股票名称']
    results_df[final_cols].to_csv(output_path, index=False, encoding='utf-8')
    print(f"\n🎉 筛选结果已成功保存到: {output_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"主程序运行失败: {e}")
