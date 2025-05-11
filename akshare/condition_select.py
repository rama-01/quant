import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from utils.draw import display_dataframe_in_window


def check_conditions(
    stock_data,
    ma_window=20,  # 均线周期
    consolidation_lookback=60,  # 横盘观察期
    breakout_lookback=20,  # 突破观察期
    amplitude_lookback=60,  # 振幅观察期
    volume_compare_window=10,  # 成交量对比窗口
    # 阈值参数
    bollinger_threshold=0.15,  # 布林带收缩阈值
    amplitude_threshold=0.3,  # 振幅阈值
    atr_multiplier=0.3,  # ATR突破倍数
):
    if len(stock_data) < 250:
        return None

    # 1. 修正布林带收缩条件计算
    stock_data["MA20"] = stock_data["收盘"].rolling(window=ma_window).mean()
    stock_data["STD20"] = stock_data["收盘"].rolling(window=ma_window).std()
    # 计算最近N日布林带宽度的最大值（非均值）
    bollinger_width_max = (
        (stock_data["STD20"] / stock_data["MA20"]).iloc[-consolidation_lookback:].max()
    )
    is_consolidation = bollinger_width_max < bollinger_threshold

    # 2. 修正ATR计算（真实波幅）
    high_low = stock_data["最高"] - stock_data["最低"]
    high_pclose = (stock_data["最高"] - stock_data["收盘"].shift()).abs()
    low_pclose = (stock_data["最低"] - stock_data["收盘"].shift()).abs()
    true_range = pd.concat([high_low, high_pclose, low_pclose], axis=1).max(axis=1)
    stock_data["ATR"] = true_range.rolling(window=ma_window).mean()

    # 3. 优化突破条件逻辑
    recent_high = stock_data["最高"].iloc[-breakout_lookback:-1].max()  # 排除最新日
    is_breakout = stock_data["收盘"].iloc[-1] > recent_high * (
        1 + atr_multiplier * stock_data["ATR"].iloc[-1] / recent_high
    )

    # 4. 振幅条件添加容错机制
    recent_low = stock_data["最低"].iloc[-amplitude_lookback:].min()
    if recent_low <= 0:
        is_low_amplitude = False
    else:
        amplitude = (recent_high - recent_low) / recent_low
        is_low_amplitude = amplitude < amplitude_threshold

    # 5. 成交量放大
    stock_data["Volume_EMA5"] = (
        stock_data["成交量"].ewm(span=volume_compare_window).mean()
    )
    stock_data["Volume_EMA20"] = (
        stock_data["成交量"].ewm(span=volume_compare_window * 4).mean()
    )
    is_volume_growing = (
        stock_data["Volume_EMA5"].iloc[-volume_compare_window:].mean()
        > stock_data["Volume_EMA20"].iloc[-volume_compare_window:].mean()
    ) & (
        stock_data["Volume_EMA5"].pct_change().iloc[-volume_compare_window:].mean() > 0
    )

    # 6.当前价格处于历史20%分位以下
    price_percentile = stock_data["收盘"].quantile(0.2)  # 计算20%分位值
    is_low_percentile = stock_data["收盘"].iloc[-1] < price_percentile

    return is_consolidation & is_low_amplitude & is_volume_growing & is_low_percentile



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
    # 排除值为nan的数据
    df = df[~df["最新价"].isna()]
    df["流通市值"] = pd.to_numeric(df["流通市值"], errors="coerce")
    # 排除创业板、科创板、ST股票、总市值大于500亿的股
    df = df[
        ~(
            df["代码"].str.startswith(("30", "688"))
            | df["名称"].str.startswith(("ST", "*ST"))
        )
    ]

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
    display_dataframe_in_window(selected_stocks)
