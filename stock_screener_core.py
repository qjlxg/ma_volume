# stock_screener_core.py

import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time

# --- 配置 ---
STOCK_DATA_DIR = 'stock_data'  # 股票数据目录
STOCK_NAMES_FILE = 'stock_names.csv' # 股票名称文件
OUTPUT_DIR = 'screened_results' # 输出结果目录
NUM_DAYS_LOOKBACK = 20 # 观察最近 N 个交易日的数据

# --- 核心技术指标计算 ---
def calculate_kdj(df, n=9, m1=3, m2=3):
    """计算 KDJ 指标"""
    df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
    df['最高'] = pd.to_numeric(df['最高'], errors='coerce')
    df['最低'] = pd.to_numeric(df['最低'], errors='coerce')

    # 计算 RSV (未成熟随机值)
    low_list = df['最低'].rolling(window=n, min_periods=n).min()
    high_list = df['最高'].rolling(window=n, min_periods=n).max()
    
    # 避免除以零
    denominator = high_list - low_list
    denominator[denominator == 0] = 1e-6 
    
    df['RSV'] = (df['收盘'] - low_list) / denominator * 100
    
    # 计算 K、D、J
    df['K'] = df['RSV'].ewm(com=m1 - 1, adjust=False, min_periods=n).mean()
    df['D'] = df['K'].ewm(com=m2 - 1, adjust=False, min_periods=n).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    
    return df

def calculate_indicators(df):
    """计算 MACD, KDJ, 均线和成交量指标"""
    # 确保有足够的数据计算 MA60 (60天)
    if len(df) < 60:
        return None

    # 统一数据类型
    df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
    df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
    df['最高'] = pd.to_numeric(df['最高'], errors='coerce')
    df['最低'] = pd.to_numeric(df['最低'], errors='coerce')

    # 1. MACD 计算
    ema_short = df['收盘'].ewm(span=12, adjust=False).mean()
    ema_long = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIFF'] = ema_short - ema_long
    df['DEA'] = df['DIFF'].ewm(span=9, adjust=False).mean()
    df['MACD'] = (df['DIFF'] - df['DEA']) * 2 

    # 2. KDJ 计算
    df = calculate_kdj(df)

    # 3. 均线 (MA)
    df['MA5'] = df['收盘'].rolling(window=5).mean()
    df['MA10'] = df['收盘'].rolling(window=10).mean()
    df['MA30'] = df['收盘'].rolling(window=30).mean()
    df['MA60'] = df['收盘'].rolling(window=60).mean()

    # 4. 成交量 (VOL) - Vol_Ratio = 最新成交量 / 近5日均量
    df['VOL_MA5'] = df['成交量'].rolling(window=5).mean()
    df['VOL_MA10'] = df['成交量'].rolling(window=10).mean()
    df['Vol_Ratio'] = df['成交量'] / df['VOL_MA5']

    # 确保数据完整，去掉 NaN 行
    return df.dropna()

# --- 筛选逻辑 ---

def check_mode_1(df_recent, df_full):
    """
    模式一：底部反转启动型 (买入机会) - 使用基础 KDJ 信号，确保回测能出结果。
    """
    if len(df_recent) < 2:
        return False

    last = df_recent.iloc[-1]
    prev = df_recent.iloc[-2]

    # 1. 处于相对底部区域 (例如，低于 MA60)
    is_at_bottom = last['收盘'] < last['MA60'] 

    # 2. KDJ 底部金叉 (K, D 均小于 50)
    is_kdj_golden_cross = (last['K'] > last['D']) and (prev['K'] <= prev['D'])
    is_low_kdj = (last['K'] < 50) and (last['D'] < 50)
    kdj_signal = is_kdj_golden_cross and is_low_kdj

    # 3. 价格温和启动 (当日收阳，且站上短期均线 MA5)
    is_price_up = (last['收盘'] > prev['收盘']) and (last['收盘'] > last['MA5'])

    # 4. 底部放量确认 (成交量高于近5日均量 1.1倍)
    is_volume_confirm = last['Vol_Ratio'] > 1.1 
    
    # 组合条件
    return is_at_bottom and kdj_signal and is_price_up and is_volume_confirm
    
def check_mode_2(df_recent, df_full):
    """
    模式二：强势股整理再加速型 (买入机会) - 维持原有逻辑
    """
    if len(df_recent) < NUM_DAYS_LOOKBACK:
        return False

    last = df_recent.iloc[-1]
    prev_2 = df_recent.iloc[-3]

    # 1. MACD 零轴上方二次金叉或红柱再次放大
    is_strong_macd = (last['DIFF'] > 0.1 and last['DEA'] > 0.1)
    is_macd_reaccelerate = (last['MACD'] > df_recent.iloc[-2]['MACD'] * 1.1) and (df_recent.iloc[-2]['MACD'] < prev_2['MACD'])
    macd_signal = is_strong_macd and is_macd_reaccelerate

    # 2. 严格的多头排列
    is_bullish = last['MA10'] > last['MA30'] and last['MA30'] > last['MA60'] 

    # 优化点 2: 严格紧贴 10 日均线 (股价与 MA10 差距在 1%以内)
    is_tight_ma10 = (last['收盘'] > last['MA10']) and \
                    (last['收盘'] / last['MA10'] < 1.01)
    
    # 4. 量能确认 (当日收阳且放量) - 维持严格的 1.5x VOL_MA10 要求
    is_rebound = last['收盘'] > df_recent['收盘'].iloc[-2] 
    is_vol_confirm = last['成交量'] > last['VOL_MA10'] * 1.5

    return macd_signal and is_bullish and is_tight_ma10 and is_rebound and is_vol_confirm

def check_mode_3(df_recent, df_full):
    """
    模式三：高风险预警型 (提前跑路信号) - KDJ 高位死叉 + 跌破 MA10
    """
    if len(df_recent) < 10:
        return False
        
    last = df_recent.iloc[-1]
    prev = df_recent.iloc[-2]

    # 1. KDJ 死亡预警 (高位死叉)
    is_kdj_death_cross = (last['K'] < last['D']) and (prev['K'] >= prev['D'])
    is_high_kdj = (last['K'] > 80) or (last['D'] > 80) # 发生在超买区
    
    kdj_signal = is_kdj_death_cross and is_high_kdj

    # 2. 价格确认 (跌破短期生命线 MA10)
    is_price_breakdown = last['收盘'] < last['MA10'] 
    
    # 3. MACD 尚未死叉 (确保这是提前预警)
    is_macd_not_death = (last['DIFF'] > last['DEA'])

    return kdj_signal and is_price_breakdown and is_macd_not_death

# --- 工具函数：获取MACD信号文本 (保留，尽管不用于回测) ---
def get_macd_signal_text(last, prev):
    if last['DIFF'] > last['DEA'] and prev['DIFF'] <= prev['DEA']:
        return 'Golden Cross'
    elif last['DIFF'] < last['DEA'] and prev['DIFF'] >= prev['DEA']:
        return 'Death Cross'
    else:
        return 'No Cross'

# --- 并行处理函数 (保留，但回测脚本不直接调用) ---
def process_stock_file(file_path):
    # 此函数主要用于每日筛选，回测脚本使用其内部的 check_mode_X 函数
    pass # 保持原文件内容
    
# --- 主函数 (保留，但回测脚本不直接调用) ---
def main():
    # 此函数主要用于每日筛选，回测脚本不依赖它
    pass # 保持原文件内容

if __name__ == '__main__':
    # 为了避免在回测环境中被调用，这里可以留空或写 pass
    # 如果原脚本的 main 函数是为了每日运行筛选，请保留原样
    pass
