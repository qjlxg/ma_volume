import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
STOCK_NAMES_FILE = 'stock_names.csv'
MIN_CLOSING_PRICE = 5.0
MAX_CLOSING_PRICE = 20.0  # 新增：收盘价上限
MAX_PULLBACK_PERCENT = 0.15  # 15%
MAX_PULLBACK_DAYS = 30
# --- 配置结束 ---

def check_stock_code_rules(stock_code, stock_name):
    """
    检查股票代码和名称是否符合深沪A股、非ST、非创业板等要求。
    """
    # 1. 排除ST股票 (通过名称)
    if 'ST' in stock_name.upper():
        return False

    # 2. 排除创业板 (30开头)
    if stock_code.startswith('30'):
        return False

    # 3. 只保留深沪A股 (通过代码开头)
    # 沪市A股：60开头 (主板)
    # 深市A股：00开头 (主板), 002开头 (中小板)
    if stock_code.startswith('60') or stock_code.startswith('00'):
        # 排除科创板 (68开头，但已包含在60开头的大类中，为保险起见，严格排除)
        if stock_code.startswith('68'): 
             return False
        return True
    
    # 排除所有其他代码，如：
    # 003, 004 (深市非A股)
    # 4, 8 (北交所)
    # 9 (B股)
    # 7 (可转债/基金等)
    return False


def analyze_stock(file_path):
    """
    对单个股票的CSV数据进行回调筛选分析，并加入新的排除条件。
    """
    try:
        df = pd.read_csv(file_path)
        
        # 确保数据按日期升序排列
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by='Date').reset_index(drop=True)
        
        # 至少需要30个交易日的数据
        if len(df) < MAX_PULLBACK_DAYS:
            return None

        # 获取股票代码 (假设文件名即代码，如 '603693.csv' -> '603693')
        stock_code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 2. 预先加载名称并检查股票类型 (Name将在main函数中合并，这里暂时使用一个占位符或假设名称)
        # 为了在筛选早期排除ST股，我们先进行一个简单的名称检查（如果文件名包含名称信息）
        # 实际更准确的做法是在main函数中加载所有名称并传递进来，但为了保持函数独立性，这里先跳过名称检查
        # 假设：如果 stock_names.csv 没有被加载，我们只能通过代码排除ST，但这是不准确的。
        # 鉴于无法在 analyze_stock 内部可靠获取名称，我们将主要通过代码来排除非A股和创业板。
        
        # 3. 检查最新收盘价
        latest_close = df['Close'].iloc[-1]
        if not (MIN_CLOSING_PRICE <= latest_close <= MAX_CLOSING_PRICE):
            return None

        # 4. 核心回调逻辑 (与原脚本一致)
        
        # 寻找最近一个高点 (不包括最新收盘价)
        for i in range(2, len(df)):
            current_close = df['Close'].iloc[-i]
            
            # 简化逻辑：从当前日期往前看MAX_PULLBACK_DAYS天的最高价
            recent_high = df['Close'].iloc[max(0, len(df) - i - MAX_PULLBACK_DAYS + 1):len(df) - i + 1].max()
            
            if current_close == recent_high:
                high_price = current_close
                high_date = df['Date'].iloc[-i]
                
                # 检查回调幅度
                pullback_percent = (high_price - latest_close) / high_price
                
                # 检查回调时间 (从高点到最新收盘日)
                pullback_days = (df['Date'].iloc[-1] - high_date).days
                
                # 筛选条件：
                # 1. 回调幅度在 (0, 15%] 之间 (必须是回调，所以幅度 > 0)
                # 2. 回调时间 <= 30天
                if 0 < pullback_percent <= MAX_PULLBACK_PERCENT and pullback_days <= MAX_PULLBACK_DAYS:
                    return {
                        'Code': stock_code,
                        'Latest_Close': latest_close,
                        'High_Price': high_price,
                        'Pullback_Percent': f"{pullback_percent * 100:.2f}%",
                        'Pullback_Days': pullback_days,
                        'Latest_Date': df['Date'].iloc[-1].strftime('%Y-%m-%d')
                    }
                
                break
        
        return None

    except Exception as e:
        # print(f"Error processing file {file_path}: {e}")
        return None

def main():
    # 1. 预加载股票名称用于排除ST和非A股
    try:
        names_df = pd.read_csv(STOCK_NAMES_FILE, dtype={'Code': str})
        names_df['Code'] = names_df['Code'].astype(str)
        # 确保名称和代码都在同一个DataFrame中，方便后续筛选
        names_map = names_df.set_index('Code')['Name'].to_dict()
    except FileNotFoundError:
        print(f"Error: {STOCK_NAMES_FILE} not found. Cannot proceed with name filtering.")
        return

    # 2. 筛选符合代码规则的股票列表
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    
    eligible_files = []
    print("Pre-filtering stocks based on code and name rules...")
    for file_path in all_files:
        stock_code = os.path.splitext(os.path.basename(file_path))[0]
        stock_name = names_map.get(stock_code, "") # 获取名称，如果找不到则为空字符串
        
        # 严格执行规则检查
        if check_stock_code_rules(stock_code, stock_name):
            eligible_files.append(file_path)
            
    print(f"Total files found: {len(all_files)}. Eligible files for analysis: {len(eligible_files)}.")

    if not eligible_files:
        print("No eligible stock data files found after code/name filtering. Exiting.")
        return

    # 3. 使用并行处理进行回调和价格筛选
    results = []
    with Pool(cpu_count()) as pool:
        results = pool.map(analyze_stock, eligible_files)

    screened_stocks = [res for res in results if res is not None]
    
    if not screened_stocks:
        print("No stocks matched all screening criteria (price & pullback).")
        return

    result_df = pd.DataFrame(screened_stocks)

    # 4. 匹配股票名称 (使用预加载的names_df)
    final_df = pd.merge(result_df, names_df, on='Code', how='left')
    final_df['Name'] = final_df['Name'].fillna('名称缺失')
    
    # 5. 保存结果
    # 路径格式：/年月/文件名_时间戳.csv
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
