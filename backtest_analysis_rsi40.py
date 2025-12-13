import pandas as pd
import glob
import os
from datetime import datetime, timedelta, timezone
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'
OUTPUT_DIR_BASE = 'backtest_results'
RSI_THRESHOLD = 35 # 【RSI 门槛从 30 调整为 40】
RSI_PERIOD = 14
MA_PERIOD = 200
PRICE_COLUMN = '收盘'
HIGH_COL = '最高'
LOW_COL = '最低'
HOLDING_DAYS = 5 
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

def convert_to_shanghai_time(dt_utc):
    """将 UTC 时间转换为上海时间 (UTC+8)"""
    utc_tz = timezone.utc
    shanghai_tz = timezone(timedelta(hours=8))
    return dt_utc.astimezone(shanghai_tz)

def run_backtest_analysis_rsi40():
    """扫描目录, 筛选所有历史信号 (RSI < 40), 计算回测结果, 并保存报告"""
    
    # 1. 设置时间戳和路径
    now_utc = datetime.utcnow()
    now_shanghai = convert_to_shanghai_time(now_utc)
    timestamp = now_shanghai.strftime('%Y%m%d_%H%M%S')
    year_month_dir = now_shanghai.strftime('%Y/%m')
    output_sub_dir = os.path.join(OUTPUT_DIR_BASE, year_month_dir)
    output_filename = f"{timestamp}_BACKTEST_REPORT_{HOLDING_DAYS}D_RSI{RSI_THRESHOLD}.csv" 
    output_path = os.path.join(output_sub_dir, output_filename)
    
    os.makedirs(output_sub_dir, exist_ok=True)
    all_signals_data = []
    
    print(f"Starting backtest analysis on directory: {STOCK_DATA_DIR} with RSI < {RSI_THRESHOLD}")
    total_processed_stocks = 0
    
    for file_path in glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')):
        try:
            df = pd.read_csv(file_path)
            
            required_cols = {PRICE_COLUMN, HIGH_COL, LOW_COL, '日期'}
            if not required_cols.issubset(df.columns):
                # print(f" - Skipping {file_path}: Missing required columns.")
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

            # 计算所有指标 (保持不变)
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
            
            # --- 步骤 2: 回测收益计算 ---
            df_temp[f'Future_{HOLDING_DAYS}D_Close'] = df_temp['Close_Price'].shift(-HOLDING_DAYS)
            df_temp['Return_5D'] = (df_temp[f'Future_{HOLDING_DAYS}D_Close'] / df_temp['Close_Price'] - 1) * 100
            
            # --- 步骤 3: 筛选所有历史信号 ---
            backtest_signals = df_temp.copy()
            
            # 1. 长期趋势向上 (收盘价 > MA200)
            condition_ma = backtest_signals['Close_Price'] > backtest_signals['Calculated_MA200']
            
            # 2. 短期超卖 (RSI < 40) 
            condition_rsi = backtest_signals['Calculated_RSI'] < RSI_THRESHOLD
            
            # 3. MACD 柱开始抬升 (今天的柱子 > 昨天的柱子)
            backtest_signals['Prev_MACD_Histo'] = backtest_signals['Calculated_MACD_Histo'].shift(1)
            condition_macd = backtest_signals['Calculated_MACD_Histo'] > backtest_signals['Prev_MACD_Histo']
            condition_macd = condition_macd.fillna(False) 
            
            # 4. KDJ J值 > K值 (短期反弹力度)
            condition_kdj = backtest_signals['Calculated_KDJ_J'] > backtest_signals['Calculated_KDJ_K']
            
            final_filter = condition_ma & condition_rsi & condition_macd & condition_kdj

            filtered_df_temp = backtest_signals[final_filter].copy()
            
            if not filtered_df_temp.empty:
                filtered_df_temp.dropna(subset=['Return_5D'], inplace=True) 
                
                if not filtered_df_temp.empty:
                    filtered_df = df.loc[filtered_df_temp.index].copy()
                    
                    for col in INDICATOR_COLS:
                        filtered_df[col] = filtered_df_temp[col]
                    filtered_df['Return_5D'] = filtered_df_temp['Return_5D']
                        
                    stock_code = os.path.basename(file_path).replace('.csv', '')
                    filtered_df.insert(0, 'StockCode', stock_code)
                    all_signals_data.append(filtered_df)
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # 4. 合并、计算总体成功率并保存
    if all_signals_data:
        final_df = pd.concat(all_signals_data, ignore_index=True)
        
        # 确保收益率是数值类型 (处理潜在的 inf/NaN/str 导致平均值计算错误)
        final_df['Return_5D'] = pd.to_numeric(final_df['Return_5D'], errors='coerce')
        final_df.dropna(subset=['Return_5D'], inplace=True)
        
        # --- 总体成功率计算 ---
        total_signals = len(final_df)
        successful_signals = len(final_df[final_df['Return_5D'] > 0])
        success_rate = successful_signals / total_signals * 100 if total_signals > 0 else 0
        
        # 统计盈亏数据
        gains = final_df[final_df['Return_5D'] > 0]['Return_5D']
        losses = final_df[final_df['Return_5D'] <= 0]['Return_5D']
        
        avg_gain = gains.mean() if not gains.empty else 0
        avg_loss = losses.mean() if not losses.empty else 0
        
        # 【BUG 修复：将排序提前到列重命名之前】
        final_df = final_df.sort_values(by=['Calculated_RSI', 'Calculated_KDJ_J'], ascending=[True, False])

        # 打印回测报告
        print("\n" + "="*50)
        print(f"        🎉 策略回测报告 - RSI < {RSI_THRESHOLD} ({HOLDING_DAYS}日持仓) 🎉")
        print("="*50)
        print(f"    分析股票数量: {total_processed_stocks} 只")
        print(f"    🔥 历史信号总数: {total_signals} 个")
        print(f"    ✅ 策略成功率 (胜率): {success_rate:.2f}%")
        print("    💰 平均盈利: +{:.2f}%".format(avg_gain))
        print("    💀 平均亏损: {:.2f}%".format(avg_loss))
        print("    📈 净期望收益 (不含成本): {:.2f}%".format((avg_gain * success_rate / 100) + (avg_loss * (100 - success_rate) / 100)))
        print("="*50)
        
        # 筛选和重命名列 (汉化)
        columns_to_keep_eng = [k for k in OUTPUT_COLUMNS_MAPPING.keys() if k in final_df.columns]
        final_df = final_df[columns_to_keep_eng]
        final_df.rename(columns=OUTPUT_COLUMNS_MAPPING, inplace=True)

        final_df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"\n✅ 详细回测结果已保存至: {output_path}")
    else:
        print(f"\n⚠️ 未发现任何符合回测条件的信号。")

if __name__ == "__main__":
    run_backtest_analysis_rsi40()
