import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSING_PRICE = 5.0
MAX_CLOSING_PRICE = 20.0  # 收盘价上限
MAX_PULLBACK_PERCENT = 0.15  # 回调幅度上限 15%
MAX_PULLBACK_DAYS = 30 # 回调时间上限 30 天
# --- 配置结束 ---

def check_stock_code_rules(stock_code, stock_name):
    """
    检查股票代码和名称是否符合深沪A股、非ST、非创业板、非科创板等要求。
    返回 True 表示通过筛选。
    """
    # 1. 排除ST股票 (通过名称)
    if 'ST' in stock_name.upper():
        return False

    # 2. 排除创业板 (30开头)
    if stock_code.startswith('30'):
        return False

    # 3. 只保留深沪A股 (通过代码开头)
    # 沪市A股：60开头 (主板)
    # 深市A股：00开头 (主板)
    # 排除科创板 (68开头)
    if stock_code.startswith('60') and not stock_code.startswith('68'):
        return True
    elif stock_code.startswith('00'):
        return True
    
    # 排除所有其他代码 (如B股、北交所、科创板、基金等)
    return False


def analyze_stock(file_path):
    """
    对单个股票的CSV数据进行回调筛选分析。
    """
    try:
        df = pd.read_csv(file_path)
        
        # 确保数据按日期升序排列
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        # 至少需要数据量来计算30天回调
        if len(df) < MAX_PULLBACK_DAYS + 5: # 预留一些数据裕量
            return None

        stock_code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 1. 检查最新收盘价（价格区间限定）
        latest_close = df['Close'].iloc[-1]
        if not (MIN_CLOSING_PRICE <= latest_close <= MAX_CLOSING_PRICE):
            return None

        # 2. 核心回调逻辑
        
        # 从倒数第二天开始往前寻找阶段性高点
        for i in range(2, len(df)):
            current_close = df['Close'].iloc[-i]
            
            # 确定一个阶段高点：在过去 MAX_PULLBACK_DAYS 范围内，当前价格是否是最高价
            start_index = max(0, len(df) - i - MAX_PULLBACK_DAYS + 1)
            recent_high = df['Close'].iloc[start_index : len(df) - i + 1].max()
            
            if current_close == recent_high:
                high_price = current_close
                high_date = df['Date'].iloc[-i]
                
                # 回调幅度计算
                pullback_percent = (high_price - latest_close) / high_price
                
                # 回调时间计算 (从高点到最新收盘日)
                pullback_days = (df['Date'].iloc[-1] - high_date).days
                
                # 筛选条件：0% < 回调幅度 <= 15%  且  回调时间 <= 30天
                if 0 < pullback_percent <= MAX_PULLBACK_PERCENT and pullback_days <= MAX_PULLBACK_DAYS:
                    return {
                        'Code': stock_code,
                        'Latest_Close': latest_close,
                        'High_Price': high_price,
                        'Pullback_Percent': f"{pullback_percent * 100:.2f}%",
                        'Pullback_Days': pullback_days,
                        'Latest_Date': df['Date'].iloc[-1].strftime('%Y-%m-%d')
                    }
                
                # 找到最近一个高点后即停止，只关注最近一次从高点开始的回调
                break
        
        return None

    except Exception as e:
        # 生产环境中可以启用详细日志：print(f"Error processing file {file_path}: {e}")
        return None

def main():
    # 1. 预加载股票名称用于排除ST、非A股等
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        names_df['Code'] = names_df['Code'].astype(str)
        names_map = names_df.set_index('Code')['Name'].to_dict()
    except FileNotFoundError:
        print(f"致命错误: {STOCK_NAMES_FILE} 未找到。请确保文件存在并包含 'Code' 和 'Name' 列。")
        return

    # 2. 预筛选：排除不符合代码和名称规则的股票
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    
    eligible_files = []
    print(f"Found {len(all_files)} total stock data files.")
    
    for file_path in all_files:
        stock_code = os.path.splitext(os.path.basename(file_path))[0]
        stock_name = names_map.get(stock_code, "") # 获取名称，如果找不到则为空字符串
        
        if check_stock_code_rules(stock_code, stock_name):
            eligible_files.append(file_path)
            
    print(f"After code/name filtering, {len(eligible_files)} files are eligible for technical analysis.")

    if not eligible_files:
        print("No eligible stock data files found. Exiting.")
        return

    # 3. 使用并行处理进行回调和价格筛选
    results = []
    # 使用CPU核心数进行并行
    with Pool(cpu_count()) as pool:
        results = pool.map(analyze_stock, eligible_files)

    screened_stocks = [res for res in results if res is not None]
    
    if not screened_stocks:
        print("No stocks matched all screening criteria (price & pullback).")
        return

    result_df = pd.DataFrame(screened_stocks)

    # 4. 匹配股票名称并格式化输出
    final_df = pd.merge(result_df, names_df, on='Code', how='left')
    final_df['Name'] = final_df['Name'].fillna('名称缺失')
    
    # 5. 保存结果到指定路径 (年月目录, 文件名含时间戳)
    # 使用上海时区（北京时间）
    shanghai_now = datetime.now()
    output_dir = shanghai_now.strftime('%Y%m')
    timestamp = shanghai_now.strftime('%Y%m%d_%H%M%S')
    
    os.makedirs(output_dir, exist_ok=True)
    output_filename = os.path.join(output_dir, f'healthy_pullback_results_{timestamp}.csv')

    # 调整列顺序
    final_df = final_df[['Code', 'Name', 'Latest_Date', 'Latest_Close', 'High_Price', 'Pullback_Percent', 'Pullback_Days']]
    final_df.to_csv(output_filename, index=False, encoding='utf-8')
    print(f"\nSuccessfully screened {len(final_df)} stocks.")
    print(f"Results saved to: {output_filename}")

if __name__ == '__main__':
    main()
