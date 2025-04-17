import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from utils.draw import display_dataframe_in_window

def check_reversal_conditions(
    stock_data,
    # 时间窗口参数调整为更合理的设置
    downtrend_period=365 * 3,  # 调整为4个月（原90天不足）
    reversal_window=21,  # 调整为1个月实际交易日
    ma_short=5,
    ma_long=20,
    volume_compare_window=5,  # 缩短为5日对比
    # 放松阈值参数
    downtrend_slope_threshold=-0.0003,  # 放宽斜率要求
    volume_increase_ratio=1.1,  # 降低成交量倍数
    ma_spread_threshold=0.02,  # 降低均线发散要求
):
    # 数据预处理（调整为更宽松的条件）
    if len(stock_data) < 120:  # 至少需要4个月数据
        return False

    # 创建数据副本避免链式索引警告
    stock_data = stock_data.sort_values("日期").copy()

    # 计算实际交易日窗口（增加容错机制）
    def get_trading_days(days):
        available_days = int(days * 0.7)
        return min(available_days, len(stock_data) - 5)  # 保留至少5日缓冲

    # 1. 下跌趋势判断优化
    reversal_days = get_trading_days(reversal_window)
    if reversal_days >= len(stock_data):
        return False

    downtrend_days = get_trading_days(downtrend_period)
    start_idx = max(0, len(stock_data) - downtrend_days - reversal_days)
    end_idx = len(stock_data) - reversal_days
    downtrend_data = stock_data.iloc[start_idx:end_idx].copy()

    # 趋势斜率计算增加容错
    if len(downtrend_data) < 10:
        return False
    x = np.arange(len(downtrend_data))
    y = downtrend_data["收盘"].values
    slope, intercept = np.polyfit(x, y, 1)
    is_downtrend = slope < downtrend_slope_threshold

    # 2. 反转信号检测优化
    reversal_data = stock_data.iloc[-reversal_days:].copy()  # 明确创建副本

    # 计算均线（增加空值处理）
    reversal_data["MA_short"] = (
        reversal_data["收盘"].rolling(window=ma_short, min_periods=3).mean()
    )
    reversal_data["MA_long"] = (
        reversal_data["收盘"].rolling(window=ma_long, min_periods=5).mean()
    )

    # 均线交叉条件优化
    ma_cross = (
        reversal_data["MA_short"].iloc[-1] > reversal_data["MA_long"].iloc[-1]
    ) & (
        reversal_data["MA_short"].iloc[-3] < reversal_data["MA_long"].iloc[-3]
    )  # 3日前对比

    # 均线发散条件优化
    ma_spread = (reversal_data["MA_short"] - reversal_data["MA_long"]).iloc[-5:].mean()
    is_ma_diverging = ma_spread > ma_spread_threshold and ma_spread > 0

    # 成交量条件优化（使用EMA平滑）
    vol_ema_short = reversal_data["成交量"].ewm(span=5).mean()
    vol_ema_long = reversal_data["成交量"].ewm(span=10).mean()
    is_volume_increase = (
        vol_ema_short.iloc[-5:].mean()
        > vol_ema_long.iloc[-5:].mean() * volume_increase_ratio
    )

    # 趋势线突破优化（动态计算）
    trendline_current = slope * (len(downtrend_data) - 1) + intercept
    current_price = reversal_data["收盘"].iloc[-1]
    is_break_trendline = current_price > trendline_current * 1.015  # 突破1.5%

    # 增加价格动量验证
    price_increase = (
        reversal_data["收盘"].iloc[-1]
        > reversal_data["收盘"].iloc[-5] * 1.05  # 最近5日涨幅超5%
    )

    return (
        is_downtrend
        & ma_cross
        # & is_ma_diverging
        & is_volume_increase
        # & is_break_trendline
        # & price_increase  # 新增动量条件
    )


def process_stock(symbol, name):
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")
        stock_data = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if check_reversal_conditions(stock_data):
            return {
                "代码": symbol,
                "名称": name,
            }
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None
    return None


def filter_stocks():
    df = ak.stock_zh_a_spot_em()
    # 排除nan的数据
    df = df.dropna()
    # 筛选代码60、30开头的股票
    df = df[df["代码"].str.startswith(("60", "00"))]
    symbols = df["代码"].tolist()
    names = df["名称"].tolist()

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_stock, symbol, name)
            for symbol, name in zip(symbols, names)
        ]
        for future in futures:
            result = future.result()
            if result:
                results.append(result)
    display_dataframe_in_window(results)
    return pd.DataFrame(results)


if __name__ == "__main__":
    df = filter_stocks()
    print(df)
