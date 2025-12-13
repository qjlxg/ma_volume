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
RSI_THRESHOLD = 25              # 优化：RSI < 25 (极端超卖)
RSI_PERIOD = 14
MA_PERIOD = 200
PRICE_COLUMN = '收盘'
HIGH_COL = '最高'
LOW_COL = '最低'
HOLDING_DAYS = 5 
# *** 引入实战交易成本 ***
TRANSACTION_COST = 0.2          # 双向交易成本 (买入+卖出)，0.2%
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
    'Return_5D': f'未来{HOLDING_DAYS}日净收益率(%)', 
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

def run_backtest_analysis():
    """扫描目录, 筛选所有历史信号, 计算回测结果, 并保存报告"""
    
    # 1. 设置时间戳和路径
    now_utc = datetime.utcnow()
    now_shanghai = convert_to_shanghai_time(now_utc)
    timestamp = now_shanghai.strftime('%Y%m%d_%H%M%S')
    year_month_dir = now_shanghai.strftime('%Y/%m')
    output_sub_dir = os.path.join(OUTPUT_DIR_BASE, year_month_dir)
    output_filename = f"{timestamp}_BACKTEST_REPORT_A股_无风险股_COST{TRANSACTION_COST}%.csv" 
    output_path = os.path.join(output_sub_dir, output_filename)
    
    os.makedirs(output_sub_dir, exist_ok=True)
    all_signals_data = []
    
    print(f"Starting backtest analysis with fixed 5D holding and Cost: {TRANSACTION_COST}%.")
    print("Applying A-share filter and attempting to filter ST/*ST stocks (based on daily volatility).")
    total_processed_stocks = 0
    total_scanned_stocks = 0
    
    for file_path in glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv')):
        
        stock_code = os.path.basename(file_path).replace('.csv', '')
        total_scanned_stocks += 1
        
        # *** 沪深A股过滤逻辑 ***
        if not (stock_code.startswith('60') or stock_code.startswith('68') or \
                stock_code.startswith('00') or stock_code.startswith('30')):
            continue
            
        try:
            df = pd.read_csv(file_path)
            
            required_cols = {PRICE_COLUMN, HIGH_COL, LOW_COL, '日期', '涨跌幅'}
            if not required_cols.issubset(df.columns):
                continue
            
            # --- 步骤 0: 风险股过滤 (基于涨跌幅限制) ---
            df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')
            
            # 统计近30日内，日涨跌幅绝对值超过 5.5% 的天数 (用来识别非ST股)
            df['High_Volatility_Days'] = (df['涨跌幅'].abs() > 5.5).rolling(window=30).sum()
            
            # 如果近30日内，高波动天数少于3天，我们高度怀疑它是ST股或交易不活跃，直接跳过
            if df['High_Volatility_Days'].max() < 3 and len(df) > 30:
                 # print(f" - Skipping {stock_code}: Suspected low-volatility/ST stock.")
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
            
            df_temp['Gross_Return'] = (df_temp[f'Future_{HOLDING_DAYS}D_Close'] / df_temp['Close_Price'] - 1) * 100
            
            # *** 扣除交易成本 ***
            df_temp['Return_5D'] = df_temp['Gross_Return'] - TRANSACTION_COST
            
            # --- 步骤 3: 筛选所有历史信号 ---
            backtest_signals = df_temp.copy()
            
            # 1. 长期趋势向上 (收盘价 > MA200)
            condition_ma = backtest_signals['Close_Price'] > backtest_signals['Calculated_MA200']
            # 2. 短期极端超卖 (RSI < 25)
            condition_rsi = backtest_signals['Calculated_RSI'] < RSI_THRESHOLD 
            
            # 3. MACD 柱开始抬升 & 必须在负值区域 (空头衰竭)
            backtest_signals['Prev_MACD_Histo'] = backtest_signals['Calculated_MACD_Histo'].shift(1)
            condition_macd_rising = backtest_signals['Calculated_MACD_Histo'] > backtest_signals['Prev_MACD_Histo']
            condition_macd_negative = backtest_signals['Prev_MACD_Histo'] < 0 

            # 4. KDJ J值 > K值 (短期反弹力度)
            condition_kdj = backtest_signals['Calculated_KDJ_J'] > backtest_signals['Calculated_KDJ_K']
            
            # 最终筛选逻辑合并
            final_filter = condition_ma & condition_rsi & condition_macd_rising & condition_macd_negative & condition_kdj

            final_filter = final_filter.fillna(False) 

            filtered_df_temp = backtest_signals[final_filter].copy()
            
            if not filtered_df_temp.empty:
                filtered_df_temp.dropna(subset=['Return_5D'], inplace=True) 
                
                if not filtered_df_temp.empty:
                    filtered_df = df.loc[filtered_df_temp.index].copy()
                    
                    for col in INDICATOR_COLS:
                        filtered_df[col] = filtered_df_temp[col]
                    filtered_df['Return_5D'] = filtered_df_temp['Return_5D']
                        
                    filtered_df.insert(0, 'StockCode', stock_code)
                    all_signals_data.append(filtered_df)
                    # print(f" - Found {len(filtered_df)} historical signals for {stock_code}")
                
        except Exception as e:
            # print(f"Error processing {file_path}: {e}")
             pass

    # 4. 合并、计算总体成功率和盈亏指标并保存报告
    if all_signals_data:
        final_df = pd.concat(all_signals_data, ignore_index=True)
        
        # 将日期列转换为日期格式，以便正确排序
        final_df['日期'] = pd.to_datetime(final_df['日期'], errors='coerce') 

        # *** 关键修改：按日期降序排列 (最新的信号在前) ***
        final_df = final_df.sort_values(by=['日期'], ascending=[False])
        
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
        
        columns_to_keep_eng = [k for k in OUTPUT_COLUMNS_MAPPING.keys() if k in final_df.columns]
        
        final_df = final_df[columns_to_keep_eng]
        final_df.rename(columns=OUTPUT_COLUMNS_MAPPING, inplace=True)
        
        final_df.to_csv(output_path, index=False, encoding='utf-8')

        # 打印回测报告 
        print("\n" + "="*50)
        print(f"        🎉 策略回测报告 - 5日持仓 (沪深A股无风险股净收益版) 🎉")
        print(f"    *** 交易成本扣除: {TRANSACTION_COST}% ***")
        print("="*50)
        print(f"    分析股票数量: {total_scanned_stocks} 只 (其中 {total_processed_stocks} 只为沪深A股/非风险股)")
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
