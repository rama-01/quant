import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def check_conditions(
    stock_data,
    # 时间窗口参数（按自然日设定，自动转换为交易日数量）
    ma_window=20,  # 均线周期（默认20日）
    consolidation_lookback=90,  # 横盘观察期（默认3个月）
    breakout_lookback=60,  # 突破观察期（默认2个月）
    amplitude_lookback=90,  # 振幅观察期（默认3个月）
    volume_compare_window=5,  # 成交量对比窗口
    # 阈值参数
    bollinger_threshold=0.08,  # 布林带收缩阈值
    amplitude_threshold=0.3,  # 振幅阈值
    atr_multiplier=0.5,  # ATR突破倍数
):
    """
    改进版选股条件判断，支持动态时间窗口
    参数说明：
    - 所有时间类参数按自然日设定，自动转换为实际交易日数量
    - 阈值参数根据A股历史数据经验值设定
    """

    # 计算实际可用的交易日窗口
    def get_trading_window(days, max_possible=len(stock_data)):
        # 按自然日转换为交易日（假设平均每月21个交易日）
        trading_days = min(int(days * 0.7), max_possible)  # 0.7=21/30
        return max(trading_days, 5)  # 至少保留5个交易日

    # 动态调整各时间窗口
    ma_win = get_trading_window(ma_window)
    con_win = get_trading_window(consolidation_lookback)
    brk_win = get_trading_window(breakout_lookback)
    amp_win = get_trading_window(amplitude_lookback)
    vol_win = get_trading_window(volume_compare_window)

    # 1. 长期横盘条件（布林带收缩度）
    stock_data["MA20"] = stock_data["收盘"].rolling(window=ma_win).mean()
    stock_data["STD20"] = stock_data["收盘"].rolling(window=ma_win).std()
    bollinger_width = (stock_data["STD20"] / stock_data["MA20"]).iloc[-con_win:].mean()

    # 2. 成交量条件（动态窗口EMA）
    stock_data["Volume_EMA5"] = stock_data["成交量"].ewm(span=vol_win).mean()
    stock_data["Volume_EMA20"] = (
        stock_data["成交量"].ewm(span=vol_win * 4).mean()
    )  # 4倍周期对比

    # 3. 平台突破条件（动态窗口ATR）
    stock_data["ATR"] = (
        (stock_data["最高"] - stock_data["最低"])
        .rolling(window=get_trading_window(14))
        .mean()
    )

    # 条件判断
    is_consolidation = bollinger_width < bollinger_threshold

    # 成交量温和放大（动态窗口比较）
    is_volume_growing = (
        stock_data["Volume_EMA5"].iloc[-vol_win:].mean()
        > stock_data["Volume_EMA20"].iloc[-vol_win:].mean()
    ) & (stock_data["Volume_EMA5"].pct_change().iloc[-vol_win:].mean() > 0)

    # 平台突破判断（动态窗口高点和ATR）
    recent_high = stock_data["最高"].iloc[-brk_win:].max()
    is_breakout = (stock_data["收盘"].iloc[-1] > recent_high) & (
        stock_data["收盘"].iloc[-1] - recent_high
        > atr_multiplier * stock_data["ATR"].iloc[-1]
    )

    # 振幅条件（动态窗口）
    recent_low = stock_data["最低"].iloc[-amp_win:].min()
    amplitude = (recent_high - recent_low) / recent_low
    is_low_amplitude = amplitude < amplitude_threshold

    return is_consolidation & is_volume_growing & is_breakout & is_low_amplitude


def process_stock(symbol, name):
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=2000)).strftime("%Y%m%d")
        stock_data = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if check_conditions(stock_data):
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

    return pd.DataFrame(results)


if __name__ == "__main__":
    selected_stocks = filter_stocks()
    print("\n=== 筛选结果 ===")
    print(selected_stocks)
