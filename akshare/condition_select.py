import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def get_sz_stock_list():
    sz_stock_list = ak.stock_info_sz_name_code(symbol="A股列表")
    sz_stock_list = sz_stock_list[["A股代码", "A股简称"]]
    sz_stock_list.columns = ["symbol", "name"]
    sz_stock_list = sz_stock_list[~sz_stock_list["symbol"].str.startswith("300")]
    return sz_stock_list


def get_sh_stock_list():
    sh_stock_list = ak.stock_info_sh_name_code()
    sh_stock_list = sh_stock_list[["证券代码", "证券简称"]]
    sh_stock_list.columns = ["symbol", "name"]
    return sh_stock_list


def get_market_value():
    market_value = ak.stock_zh_a_spot_em()
    market_value = market_value[["代码", "流通市值"]]
    market_value.columns = ["symbol", "market_value"]
    return market_value


def filter_stocks_by_market_value(df, market_value):
    df = pd.merge(df, market_value, on="symbol", how="inner")
    df = df[df["market_value"] < 5e10]
    return df


def calculate_technical_indicators(stock_data):
    stock_data["MA20"] = stock_data["收盘"].rolling(window=20).mean()
    stock_data["Volume_MA5"] = stock_data["成交量"].rolling(window=5).mean()
    return stock_data


def check_conditions(stock_data):
    low_threshold = stock_data["收盘"].quantile(0.2)
    is_low = stock_data["收盘"].iloc[-1] < low_threshold

    ma20_volatility = stock_data["MA20"].pct_change(fill_method=None).std()
    is_consolidating = ma20_volatility < 0.05

    is_volume_increasing = (
        stock_data["Volume_MA5"].iloc[-1] > stock_data["Volume_MA5"].iloc[-6]
    )
    is_price_breaking = stock_data["收盘"].iloc[-1] > stock_data["MA20"].iloc[-1]

    return is_low and is_consolidating and is_volume_increasing and is_price_breaking


def process_stock(symbol, name, market_value):
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
        stock_data = calculate_technical_indicators(stock_data)
        if check_conditions(stock_data):
            return {
                "symbol": symbol,
                "name": name,
                "market_value": market_value,
                "current_price": stock_data["收盘"].iloc[-1],
                "MA20": stock_data["MA20"].iloc[-1],
                "Volume_MA5": stock_data["Volume_MA5"].iloc[-1],
            }
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
    return None


def filter_stocks():
    sz_stock_list = get_sz_stock_list()
    sh_stock_list = get_sh_stock_list()
    stock_list = pd.concat([sz_stock_list, sh_stock_list], ignore_index=True)

    market_value = get_market_value()
    df = filter_stocks_by_market_value(stock_list, market_value)
    print(df)

    symbols = df["symbol"].tolist()
    names = df["name"].tolist()
    market_values = df["market_value"].tolist()

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_stock, symbol, name, market_value)
            for symbol, name, market_value in zip(symbols, names, market_values)
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
