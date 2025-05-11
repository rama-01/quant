import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def get_recent_five_years_data():
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=2 * 365)).strftime("%Y%m%d")
    return start_date, end_date


def calculate_price_changes(stock_data):
    price_changes = []
    n = len(stock_data)
    for i in range(n):
        for j in range(i + 1, n):
            start_price = stock_data["收盘"].iloc[i]
            end_price = stock_data["收盘"].iloc[j]
            change = (end_price - start_price) / start_price
            if change > 1.0:  # 涨幅超过100%
                start_date = stock_data["日期"].iloc[i]
                end_date = stock_data["日期"].iloc[j]
                price_changes.append((start_date, end_date, change))
    return price_changes


def process_stock(symbol, name):
    try:
        start_date, end_date = get_recent_five_years_data()
        stock_data = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if len(stock_data) < 240 * 2:  # 确保有足够的历史数据
            return None

        price_changes = calculate_price_changes(stock_data)
        if price_changes:
            return {"代码": symbol, "名称": name, "区间涨幅": price_changes}
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
    return None


def filter_stocks():
    df = ak.stock_zh_a_spot_em()
    # 排除值为nan的数据
    df = df[~df["最新价"].isna()]
    # 排除创业板、科创板、ST股票
    # df = df[
    #     ~(
    #         df["代码"].str.startswith(("30", "688"))
    #         | df["名称"].str.startswith(("ST", "*ST"))
    #         | (df["流通市值"] > 5e8)
    #     )
    # ]

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

    return results


if __name__ == "__main__":
    selected_stocks = filter_stocks()
    for stock in selected_stocks:
        print(f"代码: {stock['代码']}, 名称: {stock['名称']}")
        for start_date, end_date, change in stock["区间涨幅"]:
            print(f"  区间起止: {start_date} - {end_date}, 涨幅: {change*100:.2f}%")
