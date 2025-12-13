import pandas as pd
import glob
import os
from datetime import datetime, timedelta, timezone
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD
import numpy as np

# --- 配置 (引入风控参数) ---
STOCK_DATA_DIR = 'stock_data'
OUTPUT_DIR_BASE = 'backtest_results'
RSI_THRESHOLD = 25 # 优化后的阈值
RSI_PERIOD = 14
MA_PERIOD = 200
PRICE_COLUMN = '收盘'
HIGH_COL = '最高'
LOW_COL = '最低'
HOLDING_DAYS = 5 
# *** 新增风控参数 ***
STOP_LOSS_RATE = -5.0   # 止损线: -5.0%
TAKE_PROFIT_RATE = 15.0 # 止盈线: +15.0%
# ---

# 定义输出结果的中文列名映射表 
OUTPUT_COLUMNS_MAPPING = {
    'StockCode': '股票代码',
    '日期': '信号发生日期',
    PRICE_COLUMN: '信号日收盘价',
    'Calculated_RSI': f'RSI({RSI_PERIOD}日)',
    'Calculated_MA200': f'MA({MA_PERIOD}日)',
    'Calculated_MACD_Histo': 'MACD柱',
    'Calculated_KDJ_J': 'KDJ_J值',
    'Return_5D': f'未来{HOLDING_DAYS}日收益率(%)', 
    '振幅': '振幅',
    '涨跌幅': '涨跌幅',
    '换手率': '换手率'
}
INDICATOR_COLS = ['Calculated_RSI', 'Calculated_MA200', 'Calculated_MACD_Histo', 'Calculated_KDJ_J']


# === 新增：动态收益计算函数 ===
def calculate_dynamic_return(row, sl_rate, tp_rate, holding_days):
    """
    计算在持有期内，考虑止损/止盈后的实际收益率。
    假设买入价为信号日收盘价 (Buy_Price)。
    未来 High/Low 为持有期内的最高/最低价。
    未来 Close 为持有期末的收盘价。
    """
    buy_price = row['Close_Price']
    
    # 计算止损/止盈价格
    sl_price = buy_price * (1 + sl_rate / 100)
    tp_price = buy_price * (1 + tp_rate / 100)
    
    # 获取持有期内的最高和最低价格
    future_high = row[f'Future_{holding_days}D_High']
    future_low = row[f'Future_{holding_days}D_Low']
    
    # 获取持有期末的收盘价
    final_close = row[f'Future_{holding_days}D_Close']
    
    # 1. 判断是否触发止损 (最低价触及止损价)
    if future_low <= sl_price:
        # 确定是否先触发止损。由于是超卖反弹策略，假设止损优先于止盈
        if future_high >= tp_price and abs(tp_price - buy_price) > abs(buy_price - sl_price):
             # 极端情况：如果最高涨幅超过最低跌幅，可能先触及止盈，但简化模型中，我们采用止损优先或看哪个先发生
             # 简化处理：如果最低价跌破止损线，我们就认为止损触发
             return sl_rate 
        
        return sl_rate

    # 2. 判断是否触发止盈 (最高价触及止盈价)
    elif future_high >= tp_price:
        return tp_rate
        
    # 3. 未触发止损/止盈，按固定天数收盘价退出
    elif pd.notna(final_close):
        return (final_close / buy_price - 1) * 100
    
    # 无法计算收益 (数据末尾)
    return np.nan


def convert_to_shanghai_time(dt_utc):
    """将 UTC 时间转换为上海时间 (UTC+8)"""
    utc_tz = timezone.utc
    shanghai_tz = timezone(timedelta(hours=8))
    return dt_utc.astimezone(shanghai_tz)

def run_backtest_analysis():
    """扫描目录, 筛选所有历史信号, 计算回测结果, 并保存报告"""
    
    # 1. 设置时间戳和路径
    now_utc = datetime.utcnow()
    now_shanghai = convert_to_shanghai_time(now_utc)
    timestamp = now_shanghai.strftime('%Y%m%d_%H%M%S')
    year_month_dir = now_shanghai.strftime('%Y/%m')
    output_sub_dir = os.path.join(OUTPUT_DIR_BASE, year_month_dir)
    # 更改文件名以体现止损止盈
    output_filename = f"{timestamp}_BACKTEST_REPORT_{HOLDING_DAYS}D_SL{int(abs(STOP_LOSS_RATE))}TP{int(TAKE_PROFIT_RATE)}.csv" 
    output_path = os.path.join(output_sub_dir, output_filename)
    
    os.makedirs(output_sub_dir, exist_ok=True)
    all_signals_data = []
    
    print(f"Starting backtest analysis with SL: {STOP_LOSS_RATE}% / TP: {TAKE_PROFIT_RATE}%.")
    total_processed_stocks = 0
    
    for file_path in glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')):
        try:
            df = pd.read_csv(file_path)
            
            # 检查所有必需的中文列是否存在
            required_cols = {PRICE_COLUMN, HIGH_COL, LOW_COL, '日期'}
            if not required_cols.issubset(df.columns):
                continue
            
            total_processed_stocks += 1

            # --- 步骤 1: 内部列名标准化及指标计算 ---
            df_temp = df.copy()
            df_temp.rename(columns={
                PRICE_COLUMN: 'Close_Price',
                HIGH_COL: 'High_Price',
                LOW_COL: 'Low_Price'
            }, inplace=True)
            df_temp['Close_Price'] = pd.to_numeric(df_temp['Close_Price'], errors='coerce')

            # 计算所有指标 (与优化版相同)
            rsi_indicator = RSIIndicator(close=df_temp['Close_Price'], window=RSI_PERIOD, fillna=False)
            df_temp['Calculated_RSI'] = rsi_indicator.rsi()
            
            ma_indicator = SMAIndicator(close=df_temp['Close_Price'], window=MA_PERIOD, fillna=False)
            df_temp['Calculated_MA200'] = ma_indicator.sma_indicator()
            
            macd_indicator = MACD(close=df_temp['Close_Price'], fillna=False)
            df_temp['Calculated_MACD_Histo'] = macd_indicator.macd_diff() 
            
            kdj_indicator = StochasticOscillator(high=df_temp['High_Price'], low=df_temp['Low_Price'], close=df_temp['Close_Price'], fillna=False)
            df_temp['Calculated_KDJ_K'] = kdj_indicator.stoch()
            df_temp['Calculated_KDJ_D'] = kdj_indicator.stoch_signal()
            df_temp['Calculated_KDJ_J'] = 3 * df_temp['Calculated_KDJ_K'] - 2 * df_temp['Calculated_KDJ_D']
            
            # --- 步骤 2: 回测收益计算 (新增动态风控所需的未来High/Low/Close) ---
            # 计算 HOLDING_DAYS 个交易日后的收盘价
            df_temp[f'Future_{HOLDING_DAYS}D_Close'] = df_temp['Close_Price'].shift(-HOLDING_DAYS)
            
            # 计算未来 HOLDING_DAYS 周期内的最高价和最低价 (用 rolling window 实现)
            df_temp[f'Future_{HOLDING_DAYS}D_High'] = df_temp['High_Price'].rolling(window=HOLDING_DAYS).max().shift(-HOLDING_DAYS + 1)
            df_temp[f'Future_{HOLDING_DAYS}D_Low'] = df_temp['Low_Price'].rolling(window=HOLDING_DAYS).min().shift(-HOLDING_DAYS + 1)
            
            # *** 应用动态风控收益计算 ***
            df_temp['Return_5D'] = df_temp.apply(
                lambda row: calculate_dynamic_return(row, STOP_LOSS_RATE, TAKE_PROFIT_RATE, HOLDING_DAYS), 
                axis=1
            )
            
            # --- 步骤 3: 筛选所有历史信号 (与优化版相同) ---
            backtest_signals = df_temp.copy()
            
            condition_ma = backtest_signals['Close_Price'] > backtest_signals['Calculated_MA200']
            condition_rsi = backtest_signals['Calculated_RSI'] < RSI_THRESHOLD
            
            backtest_signals['Prev_MACD_Histo'] = backtest_signals['Calculated_MACD_Histo'].shift(1)
            condition_macd_rising = backtest_signals['Calculated_MACD_Histo'] > backtest_signals['Prev_MACD_Histo']
            condition_macd_negative = backtest_signals['Prev_MACD_Histo'] < 0

            condition_kdj = backtest_signals['Calculated_KDJ_J'] > backtest_signals['Calculated_KDJ_K']
            
            final_filter = condition_ma & condition_rsi & condition_macd_rising & condition_macd_negative & condition_kdj

            final_filter = final_filter.fillna(False) 

            filtered_df_temp = backtest_signals[final_filter].copy()
            
            if not filtered_df_temp.empty:
                # 排除数据末尾，无法计算未来收益的信号
                filtered_df_temp.dropna(subset=['Return_5D'], inplace=True) 
                
                if not filtered_df_temp.empty:
                    filtered_df = df.loc[filtered_df_temp.index].copy()
                    
                    for col in INDICATOR_COLS:
                        filtered_df[col] = filtered_df_temp[col]
                    filtered_df['Return_5D'] = filtered_df_temp['Return_5D']
                        
                    stock_code = os.path.basename(file_path).replace('.csv', '')
                    filtered_df.insert(0, 'StockCode', stock_code)
                    all_signals_data.append(filtered_df)
                    print(f" - Found {len(filtered_df)} historical signals for {stock_code}")
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # 4. 合并、计算总体成功率和盈亏指标并保存报告
    if all_signals_data:
        final_df = pd.concat(all_signals_data, ignore_index=True)
        
        # --- 总体统计和盈亏分析 ---
        total_signals = len(final_df)
        
        successful_signals = final_df[final_df['Return_5D'] > 0]
        losing_signals = final_df[final_df['Return_5D'] <= 0] 
        
        successful_count = len(successful_signals)
        losing_count = len(losing_signals)
        total_net_return = final_df['Return_5D'].sum()
        success_rate = successful_count / total_signals * 100 if total_signals > 0 else 0
        
        avg_win_return = successful_signals['Return_5D'].mean() if successful_count > 0 else 0
        avg_loss_return = losing_signals['Return_5D'].mean() * -1 if losing_count > 0 else 0 
        
        if avg_loss_return > 0:
            profit_loss_ratio = avg_win_return / avg_loss_return
        else:
            profit_loss_ratio = float('inf') 
        
        # 排序
        final_df = final_df.sort_values(by=['Calculated_RSI', 'Calculated_KDJ_J'], ascending=[True, False])
        
        # 筛选和重命名列 (汉化)
        columns_to_keep_eng = [k for k in OUTPUT_COLUMNS_MAPPING.keys() if k in final_df.columns]
        
        final_df = final_df[columns_to_keep_eng]
        final_df.rename(columns=OUTPUT_COLUMNS_MAPPING, inplace=True)
        
        final_df.to_csv(output_path, index=False, encoding='utf-8')

        # 打印回测报告 
        print("\n" + "="*50)
        print(f"        🎉 策略回测报告 - 5日持仓 (SL/TP风控版) 🎉")
        print(f"    *** 风控参数: 止损 {STOP_LOSS_RATE}% / 止盈 {TAKE_PROFIT_RATE}% ***")
        print("="*50)
        print(f"    分析股票数量: {total_processed_stocks} 只")
        print(f"    历史信号总数: {total_signals} 个")
        print("-" * 50)
        print(f"    ✅ 策略成功率 (胜率): {success_rate:.2f}%")
        print(f"    累计净收益率: {total_net_return:.2f}% (所有交易收益总和)")
        print(f"    平均盈利 (Avg. Win): +{avg_win_return:.2f}%")
        print(f"    平均亏损 (Avg. Loss): -{avg_loss_return:.2f}%")
        print(f"    🎯 **盈亏比 (R-Factor)**: {profit_loss_ratio:.2f}")
        print("="*50)
        print(f"\n✅ 详细回测结果已保存至: {output_path}")
    else:
        print(f"\n⚠️ 未发现任何符合优化后回测条件的信号。")

if __name__ == "__main__":
    run_backtest_analysis()

