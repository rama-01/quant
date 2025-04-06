import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def filter_stocks():
    # 获取深交所主板A股（排除创业板）
    sz_stock_list = ak.stock_info_sz_name_code(symbol="A股列表")
    sz_stock_list = sz_stock_list[["代码", "简称"]]
    sz_stock_list.columns = ["symbol", "name"]

    # 排除创业板（300开头）
    sz_stock_list = sz_stock_list[~sz_stock_list["symbol"].str.startswith("300")]

    # 获取上交所A股
    sh_stock_list = ak.stock_info_sh_name_code()
    sh_stock_list = sh_stock_list[["证券代码", "证券简称"]]
    sh_stock_list.columns = ["symbol", "name"]

    # 合并深交所和上交所股票列表
    stock_list = pd.concat([sz_stock_list, sh_stock_list], ignore_index=True)

    # 获取股票流通市值
    market_value = ak.stock_zh_a_spot_em()
    market_value = market_value[["代码", "流通市值"]]
    market_value.columns = ["symbol", "circulating_market_value"]

    # 合并数据
    df = pd.merge(stock_list, market_value, on="symbol", how="inner")

    # 条件1：沪深主板（已通过数据源筛选）
    # 条件2：流通市值在1000亿以下
    df = df[df["circulating_market_value"] < 1e11]  # 1000亿 = 1e11

    # 获取历史数据并计算技术指标
    results = []
    for symbol in df["symbol"]:
        try:
            # 获取近一年的日线数据
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
            stock_data = ak.stock_zh_a_hist(
                symbol=symbol, period="daily", start_date=start_date, end_date=end_date
            )

            # 计算技术指标
            stock_data["MA20"] = (
                stock_data["收盘"].rolling(window=20).mean()
            )  # 20日均线
            stock_data["Volume_MA5"] = (
                stock_data["成交量"].rolling(window=5).mean()
            )  # 5日成交量均线

            # 条件3：股价处于历史低位（当前价格低于历史20%分位数）
            low_threshold = stock_data["收盘"].quantile(0.2)
            is_low = stock_data["收盘"].iloc[-1] < low_threshold

            # 条件4：长期盘整（20日均线波动率小于5%）
            ma20_volatility = stock_data["MA20"].pct_change().std()
            is_consolidating = ma20_volatility < 0.05

            # 条件5：近期温和放量上攻（最近5日成交量均线大于前5日，且价格突破20日均线）
            is_volume_increasing = (
                stock_data["Volume_MA5"].iloc[-1] > stock_data["Volume_MA5"].iloc[-6]
            )
            is_price_breaking = (
                stock_data["收盘"].iloc[-1] > stock_data["MA20"].iloc[-1]
            )

            # 综合筛选
            if (
                is_low
                and is_consolidating
                and is_volume_increasing
                and is_price_breaking
            ):
                results.append(
                    {
                        "symbol": symbol,
                        "name": df[df["symbol"] == symbol]["name"].values[0],
                        "circulating_market_value": df[df["symbol"] == symbol][
                            "circulating_market_value"
                        ].values[0],
                        "current_price": stock_data["收盘"].iloc[-1],
                        "MA20": stock_data["MA20"].iloc[-1],
                        "Volume_MA5": stock_data["Volume_MA5"].iloc[-1],
                    }
                )
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    # 转换为DataFrame并返回
    return pd.DataFrame(results)


# 执行筛选
if __name__ == "__main__":
    selected_stocks = filter_stocks()
    print("\n=== 筛选结果 ===")
    print(selected_stocks)
