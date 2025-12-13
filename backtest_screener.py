# backtest_screener.py

import pandas as pd
import os
from datetime import datetime, timedelta
import glob
import time
# 导入核心筛选逻辑和指标计算函数
from stock_screener_core import calculate_indicators, check_mode_1, check_mode_2, check_mode_3

# --- 回测配置 ---
STOCK_DATA_DIR = 'stock_data' 
OUTPUT_DIR = 'backtest_results'
BACKTEST_START_DATE = datetime(2025, 10, 1) # 回测开始日期 (请根据您的数据调整)
BACKTEST_END_DATE = datetime(2025, 12, 1)   # 回测结束日期 (请根据您的数据调整)
HOLDING_DAYS = 5                            # 持有天数 (N天后卖出)
MAX_STOCKS_TO_PROCESS = 50                  # <--- 限制处理的股票数量
TARGET_MODES = [
    '模式一：底部反转启动型 (买入机会)',
    # '模式二：强势股整理再加速型 (买入机会)', # 默认只回测模式一
] # 目标回测的筛选模式列表

# --- 工具函数 ---

def load_stock_history(file_path, end_date):
    """加载并截取指定股票到指定日期前的历史数据"""
    if not os.path.exists(file_path):
        return None

    df = pd.read_csv(file_path, encoding='utf-8')
    df.rename(columns={
        '日期': '日期', 
        '成交量': '成交量',
        '收盘': '收盘',
        '最高': '最高',
        '最低': '最低'
    }, inplace=True)
    
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.sort_values(by='日期')
    
    # 截取到回测日期的前一天 (即筛选日当天的数据)
    df_filtered = df[df['日期'] <= end_date] 
    
    # 至少需要60天数据来计算MA60和MACD
    if len(df_filtered) < 60:
        return None
        
    return df_filtered

def get_future_price(df_full, screen_date, holding_days):
    """获取 N 天后的卖出价格并计算收益"""
    df_future = df_full[df_full['日期'] > screen_date]
    
    # 假设买入价是筛选日后的第一个交易日收盘价 (这里简化为筛选日当天收盘价，以便与筛选逻辑一致)
    try:
        buy_price = df_full[df_full['日期'] == screen_date]['收盘'].iloc[0]
        
        # 卖出价：获取持有 holding_days 个交易日后的收盘价
        if len(df_future) >= holding_days:
            sell_price = df_future.iloc[holding_days - 1]['收盘']
            
            # 计算收益率
            returns = (sell_price / buy_price - 1) * 100
            return buy_price, sell_price, returns
        else:
            return buy_price, None, 'Not Enough Data'
            
    except IndexError:
        return None, None, 'No Buy Price on Screen Date'

def run_backtest():
    start_time = time.time()
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_files = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    
    if not all_files:
        print(f"错误：未找到 {STOCK_DATA_DIR} 目录下的 CSV 文件。请确保历史数据已准备好。")
        return

    # --- 限制处理的股票数量 ---
    if len(all_files) > MAX_STOCKS_TO_PROCESS:
        all_files = all_files[:MAX_STOCKS_TO_PROCESS]
        print(f"检测到股票文件数量超过 {MAX_STOCKS_TO_PROCESS} 个，本次回测只处理前 {MAX_STOCKS_TO_PROCESS} 个文件。")
    # ---------------------------

    print(f"开始回测 {len(all_files)} 只股票，日期范围: {BACKTEST_START_DATE.date()} 到 {BACKTEST_END_DATE.date()}")
    print(f"回测模式: {TARGET_MODES}，持有天数: {HOLDING_DAYS}")
    
    all_results = []
    
    # 生成回测日期列表
    date_list = []
    current_date = BACKTEST_START_DATE
    while current_date <= BACKTEST_END_DATE:
        if current_date.weekday() < 5: # 仅回测工作日
            date_list.append(current_date)
        current_date += timedelta(days=1)
        
    
    for screen_date in date_list:
        daily_trades = []
        
        for file_path in all_files:
            code = os.path.basename(file_path).split('.')[0]
            
            # 1. 加载并准备回测日的数据
            df_history = load_stock_history(file_path, screen_date)
            if df_history is None:
                continue
            
            # 2. 计算所有技术指标
            df_calc = calculate_indicators(df_history)
            if df_calc is None or len(df_calc) < 2:
                continue

            # 获取回测日的最新数据 (即最后一行)
            df_recent = df_calc.tail(20) # 假设 NUM_DAYS_LOOKBACK=20

            # 3. 执行筛选逻辑 (针对目标模式)
            is_selected = False
            selected_mode = None
            
            if '模式一：底部反转启动型 (买入机会)' in TARGET_MODES and check_mode_1(df_recent, df_calc):
                is_selected = True
                selected_mode = '模式一'
            elif '模式二：强势股整理再加速型 (买入机会)' in TARGET_MODES and check_mode_2(df_recent, df_calc):
                is_selected = True
                selected_mode = '模式二'
            elif '模式三：高风险预警型 (提前跑路)' in TARGET_MODES and check_mode_3(df_recent, df_calc):
                 is_selected = True
                 selected_mode = '模式三 (预警)'
            
            
            if is_selected:
                # 4. 计算收益
                buy_price, sell_price, returns = get_future_price(df_history, screen_date, HOLDING_DAYS)
                
                if isinstance(returns, float):
                    daily_trades.append({
                        'Screen_Date': screen_date.date(),
                        'Code': code,
                        'Mode': selected_mode,
                        'Buy_Price': f'{buy_price:.2f}',
                        'Sell_Price': f'{sell_price:.2f}',
                        'Returns_%': f'{returns:.2f}',
                        'Is_Win': returns > 0
                    })
        
        all_results.extend(daily_trades)
        
        # 打印每日进展到日志
        if daily_trades:
            print(f"| {screen_date.date()} | 交易数: {len(daily_trades)} | 胜率: {(sum(t['Is_Win'] for t in daily_trades)/len(daily_trades)):.1%} |")
        else:
             print(f"| {screen_date.date()} | 未筛选出交易 |")


    # 5. 结果分析
    if not all_results:
        print("\n回测期间未筛选出任何符合条件的交易。")
        return

    results_df = pd.DataFrame(all_results)
    
    total_trades = len(results_df)
    win_rate = results_df['Is_Win'].sum() / total_trades
    
    # 转换为数值以便计算平均收益
    results_df['Returns_Value'] = results_df['Returns_%'].str.replace('%', '').astype(float)
    avg_returns = results_df['Returns_Value'].mean()
    
    end_time = time.time()
    
    print("\n--- 回测结果摘要 ---")
    print(f"总耗时: {(end_time - start_time):.2f} 秒")
    print(f"回测策略: {', '.join(TARGET_MODES)} (持有 {HOLDING_DAYS} 天)")
    print(f"总交易次数: {total_trades}")
    print(f"平均收益率: {avg_returns:.2f}%")
    print(f"胜率 (盈利交易占比): {win_rate:.2%}")
    
    # 6. 保存详细回测结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(OUTPUT_DIR, f'backtest_results_{timestamp}.csv')
    results_df = results_df[['Screen_Date', 'Code', 'Mode', 'Buy_Price', 'Sell_Price', 'Returns_%', 'Is_Win']]
    results_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"详细结果已保存至: {output_path}")

if __name__ == '__main__':
    run_backtest()
