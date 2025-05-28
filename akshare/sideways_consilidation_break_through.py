import akshare as ak
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from utils.draw import display_dataframe_in_window


def check_box_breakout_conditions(stock_data, symbol):
    if len(stock_data) < 250:
        return False

    # 计算箱体震荡区间（过去250日最高价和最低价）
    box_high = stock_data["最高"].iloc[-250:].max()
    box_low = stock_data["最低"].iloc[-250:].min()

    # 1. 检查股价是否横盘箱体震荡（价格波动幅度小于一定阈值）
    price_range = (box_high - box_low) / box_low
    # if price_range > 0.3:  # 设置箱体震荡的价格波动阈值为30%
    #     return False

    # 2. 检查成交量是否温和放量（近一周的平均成交量大于过去一个月的平均成交量）
    volume_ema_short = stock_data["成交量"].ewm(span=5).mean()  # 短期EMA
    volume_ema_long = stock_data["成交量"].ewm(span=20).mean()  # 长期EMA
    is_volume_growing = (
        volume_ema_short.iloc[-5:].mean() > volume_ema_long.iloc[-5:].mean()
    )

    if not is_volume_growing:
        return False

    # 3. 检查最近一个交易日是否突破箱体上方阻力位
    latest_close = stock_data["收盘"].iloc[-1]
    if latest_close <= box_high:
        return False

    return True


def process_stock(symbol, name):
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        stock_data = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if check_box_breakout_conditions(stock_data, symbol):
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

    # 过滤条件：
    # 1. 沪深主板，代码以60或00开头
    # 2. 不是ST股
    # 3. 流通市值在20-200亿之间
    df = df[
        df["代码"].str.startswith(("60", "00"))
        & ~df["名称"].str.startswith(("ST", "*ST"))
        & (df["流通市值"] >= 20e8)
        & (df["流通市值"] <= 200e8)
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
