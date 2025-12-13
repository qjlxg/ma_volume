import pandas as pd
import glob
import os
from datetime import datetime, timedelta, timezone
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD
import numpy as np

# --- 配置 (最终优化版) ---
STOCK_DATA_DIR = 'stock_data'
OUTPUT_DIR_BASE = 'backtest_results'
RSI_THRESHOLD = 25              # 维持 RSI < 25
RSI_PERIOD = 14
MA_PERIOD = 200
PRICE_COLUMN = '收盘'
HIGH_COL = '最高'
LOW_COL = '最低'
HOLDING_DAYS = 5 
# *** 新增实战交易成本 ***
TRANSACTION_COST = 0.2          # 双向交易成本 (买入+卖出)，假设为 0.2%
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
    'Return_5D': f'未来{HOLDING_DAYS}日净收益率(%)', # 更改名称体现净收益
    '振幅': '振幅',
    '涨跌幅': '涨跌幅',
    '换手率': '换手率'
}
INDICATOR_COLS = ['Calculated_RSI', 'Calculated_MA200', 'Calculated_MACD_Histo', 'Calculated_KDJ_J']

# (其他辅助函数，如 convert_to_shanghai_time 保持不变)
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
    # 更改文件名以体现成本扣除
    output_filename = f"{timestamp}_BACKTEST_REPORT_{HOLDING_DAYS}D_COST{TRANSACTION_COST}%.csv" 
    output_path = os.path.join(output_sub_dir, output_filename)
    
    os.makedirs(output_sub_dir, exist_ok=True)
    all_signals_data = []
    
    print(f"Starting backtest analysis with fixed 5D holding and Cost: {TRANSACTION_COST}%.")
    total_processed_stocks = 0
    
    for file_path in glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')):
        try:
            df = pd.read_csv(file_path)
            
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

            # 计算所有指标
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
            
            # --- 步骤 2: 回测收益计算 (固定5日退出，并扣除成本) ---
            df_temp[f'Future_{HOLDING_DAYS}D_Close'] = df_temp['Close_Price'].shift(-HOLDING_DAYS)
            
            # 计算毛收益率
            df_temp['Gross_Return'] = (df_temp[f'Future_{HOLDING_DAYS}D_Close'] / df_temp['Close_Price'] - 1) * 100
            
            # *** 扣除交易成本 ***
            df_temp['Return_5D'] = df_temp['Gross_Return'] - TRANSACTION_COST
            
            # --- 步骤 3: 筛选所有历史信号 (RSI<25 & MACD负值抬升) ---
            backtest_signals = df_temp.copy()
            
            condition_ma = backtest_signals['Close_Price'] > backtest_signals['Calculated_MA200']
            condition_rsi = backtest_signals['Calculated_RSI'] < RSI_THRESHOLD # RSI < 25
            
            backtest_signals['Prev_MACD_Histo'] = backtest_signals['Calculated_MACD_Histo'].shift(1)
            condition_macd_rising = backtest_signals['Calculated_MACD_Histo'] > backtest_signals['Prev_MACD_Histo']
            condition_macd_negative = backtest_signals['Prev_MACD_Histo'] < 0 # MACD 负值抬升

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
        
        # 成功定义：净收益 > 0
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
        
        final_df = final_df.sort_values(by=['Calculated_RSI', 'Calculated_KDJ_J'], ascending=[True, False])
        
        columns_to_keep_eng = [k for k in OUTPUT_COLUMNS_MAPPING.keys() if k in final_df.columns]
        
        final_df = final_df[columns_to_keep_eng]
        final_df.rename(columns=OUTPUT_COLUMNS_MAPPING, inplace=True)
        
        final_df.to_csv(output_path, index=False, encoding='utf-8')

        # 打印回测报告 
        print("\n" + "="*50)
        print(f"        🎉 策略回测报告 - 5日持仓 (最终净收益版) 🎉")
        print(f"    *** 交易成本扣除: {TRANSACTION_COST}% ***")
        print("="*50)
        print(f"    分析股票数量: {total_processed_stocks} 只")
        print(f"    历史信号总数: {total_signals} 个")
        print("-" * 50)
        print(f"    ✅ 策略成功率 (净胜率): {success_rate:.2f}%")
        print(f"    累计净收益率: {total_net_return:.2f}% (扣除成本后)")
        print(f"    平均盈利 (Avg. Net Win): +{avg_win_return:.2f}%")
        print(f"    平均亏损 (Avg. Net Loss): -{avg_loss_return:.2f}%")
        print(f"    🎯 **净盈亏比 (R-Factor)**: {profit_loss_ratio:.2f}")
        print("="*50)
        print(f"\n✅ 详细回测结果已保存至: {output_path}")
    else:
        print(f"\n⚠️ 未发现任何符合优化后回测条件的信号。")

if __name__ == "__main__":
    run_backtest_analysis()

