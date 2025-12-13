import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入原始策略脚本中的核心函数
# 假设 ma_volume_strategy.py 和 backtest.py 在同一目录下
from ma_volume_strategy import calculate_indicators, check_c1_golden_cross, check_c4_trend_control

# --- 常量定义 ---
STOCK_DATA_DIR = 'stock_data'
BACKTEST_START_DATE = '2023-01-01'  # 回测开始日期
HOLDING_PERIOD = 20  # 持有期（天），例如 20 个交易日
MAX_WORKERS = 8

# --- 回测核心逻辑 ---

def run_backtest_for_stock(file_path):
    """
    对单个股票数据文件运行回测逻辑。
    
    返回: (总交易次数, 总收益, 胜次数, 败次数)
    """
    try:
        # 读取数据 (使用与原脚本相同的读取方式)
        data = pd.read_csv(
            file_path, 
            header=None, 
            names=['Date', 'Code', 'Open', 'Close', 'High', 'Low', 'Volume', 'Amount', 'Amplitude', 'ChangePct', 'ChangeAmt', 'Turnover'],
            parse_dates=['Date'],
            date_format='%Y-%m-%d'
        )
        
        data = data.sort_values(by='Date').reset_index(drop=True)
        data['Date'] = pd.to_datetime(data['Date'])
        
        # 过滤掉回测开始日期之前的数据，并计算指标
        data = data[data['Date'] >= BACKTEST_START_DATE].copy()
        
        # 必须先计算指标，因为金叉和趋势控制都需要
        data_with_indicators = calculate_indicators(data)
        
        # 如果数据不足，跳过
        if data_with_indicators.empty:
            return 0, 0.0, 0, 0

        # 初始化回测变量
        trades_count = 0
        total_return = 0.0
        win_count = 0
        loss_count = 0
        
        # 确保索引是从0开始的连续整数
        data_with_indicators = data_with_indicators.reset_index(drop=True)
        
        # 从指标计算完毕后（至少30条数据）的第二天开始回溯
        start_index = 1
        
        # 遍历所有可能的交易日作为买入点
        for i in range(start_index, len(data_with_indicators)):
            current_data = data_with_indicators.iloc[:i+1]
            
            # 1. 执行选股逻辑 (使用原策略的 C1 + C4 组合)
            # 注意：这里调用的是原脚本中的函数
            is_golden_cross = check_c1_golden_cross(current_data)
            is_trend_controlled = check_c4_trend_control(current_data)
            
            if is_golden_cross and is_trend_controlled:
                
                # 2. 确定买入日和卖出日
                buy_index = i  # 当天满足条件，次日开盘买入 (简化为当日收盘价买入)
                sell_index = min(i + HOLDING_PERIOD, len(data_with_indicators) - 1)
                
                # 如果持有期结束前数据不够，跳过本次买入
                if sell_index <= buy_index:
                    continue
                
                # 3. 计算收益
                # 简化：买入价为当日收盘价，卖出价为持有期结束日收盘价
                buy_price = data_with_indicators.iloc[buy_index]['Close']
                sell_price = data_with_indicators.iloc[sell_index]['Close']
                
                if buy_price > 0:
                    trades_count += 1
                    trade_return = (sell_price / buy_price) - 1
                    total_return += trade_return
                    
                    if trade_return > 0:
                        win_count += 1
                    else:
                        loss_count += 1
                        
        return trades_count, total_return, win_count, loss_count

    except Exception as e:
        stock_code_match = re.search(r'(\d{6})\.csv$', file_path)
        stock_code = stock_code_match.group(1) if stock_code_match else 'UNKNOWN'
        print(f"Error processing stock {stock_code} in backtest: {e}")
        return 0, 0.0, 0, 0

def main_backtest():
    """主函数：并行扫描所有股票并输出回测结果。"""
    
    if not os.path.isdir(STOCK_DATA_DIR):
        print(f"Error: Stock data directory '{STOCK_DATA_DIR}' not found.")
        return

    # 1. 扫描所有股票数据文件
    all_files = [os.path.join(STOCK_DATA_DIR, f) 
                 for f in os.listdir(STOCK_DATA_DIR) 
                 if f.endswith('.csv') and re.match(r'\d{6}\.csv$', f)]
                 
    if not all_files:
        print("No stock data CSV files found in 'stock_data' directory.")
        return

    print(f"Found {len(all_files)} files. Starting parallel backtesting from {BACKTEST_START_DATE}...")

    # 2. 并行执行回测
    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(run_backtest_for_stock, file): file for file in all_files}
        
        for future in as_completed(future_to_file):
            result = future.result()
            if result:
                all_results.append(result)

    # 3. 汇总结果
    total_trades = sum(r[0] for r in all_results)
    cumulative_return = sum(r[1] for r in all_results)
    total_wins = sum(r[2] for r in all_results)
    total_losses = sum(r[3] for r in all_results)
    
    if total_trades == 0:
        print("\nBacktest completed. No trades were executed under the current strategy and date range.")
        return

    # 4. 计算关键指标
    average_trade_return = cumulative_return / total_trades
    win_rate = total_wins / total_trades if total_trades > 0 else 0
    
    # 5. 报告结果
    print("\n" + "="*50)
    print("📈 **回测结果报告** 📉")
    print(f"策略：金叉启动 (C1) + 趋势控制 (C4)")
    print(f"回测时间范围：{BACKTEST_START_DATE} 至今")
    print(f"持有期：{HOLDING_PERIOD} 个交易日")
    print("---")
    print(f"**总交易次数:** {total_trades}")
    print(f"**总累计收益率:** {cumulative_return:,.2f} ({cumulative_return * 100:.2f}%)")
    print(f"**平均单笔收益率:** {average_trade_return * 100:.2f}%")
    print(f"**胜率 (盈利交易):** {win_rate * 100:.2f}% ({total_wins} 胜 / {total_trades} 总)")
    print("="*50)
    
if __name__ == '__main__':
    main_backtest()
