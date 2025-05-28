import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime

""""
创业板非st,今日竞价金额大于100万小于1888万，今日竞价涨幅*今日竞价实际换手率大于0.52，自由流通值小于60亿，今日9点25分股价大于或等于9点24分最高价，（9点25分开盘价-9点24分收盘价）/昨日收盘价*100小于3，9点31分分时成交量大于9000手，9点31分大单净量大于0，9点31分分时成交量/9点30分分时成交量大于4

"""


def filter_stock(stock_info):
    """
    根据多维度条件筛选股票
    :param stock_info: 股票信息对象，包含以下字段：
        - symbol: 股票代码（6位数字）
        - yesterday_close: 昨日收盘价
        - free_circulation_value: 自由流通市值（亿元）
    :return: bool - 是否满足所有条件
    """
    symbol = stock_info["symbol"]
    yesterday_close = stock_info["yesterday_close"]
    circulation_shares = stock_info["circulation_shares"]
    free_circulation_value = stock_info["free_circulation_value"]

    # 获取分时数据
    try:
        df = ak.stock_intraday_em(symbol=symbol)
    except Exception as e:
        print(f"获取分时数据失败：{str(e)}")
        return False

    # 数据预处理
    df["时间"] = pd.to_datetime(df["时间"])
    df["成交额"] = df["成交价"] * df["手数"] * 100  # 计算成交金额（元）

    # 条件1：竞价金额大于100万小于1888万
    try:
        # 假设原始数据为纯时间字符串（如 "09:25:00"）
        df["时间"] = pd.to_datetime(df["时间"], format="%H:%M:%S")
    except ValueError:
        # 如果格式不一致，尝试自动解析
        df["时间"] = pd.to_datetime(df["时间"])

    # 条件1：竞价金额大于100万小于1888万
    auction_data = df[
        (df["时间"].dt.hour == 9)
        & (df["时间"].dt.minute == 25)
        & (df["时间"].dt.second == 0)
    ]
    if auction_data.empty:
        return False
    auction_price = auction_data.iloc[0]["成交价"]
    auction_volume = auction_data.iloc[0]["手数"]
    auction_amount = auction_price * auction_volume * 100  # 竞价金额

    if not (1e6 < auction_amount < 18.88e6):
        return False

    # 条件2：竞价涨幅*竞价实际换手率 > 0.52
    auction_change = (auction_price / yesterday_close - 1) * 100  # 竞价涨幅（%）

    # 获取09:24:00-09:24:59数据
    time_24 = df[(df["时间"].dt.hour == 9) & (df["时间"].dt.minute == 24)]
    if time_24.empty:
        return False
    price_24 = time_24.iloc[0]["成交价"]  # 取最早一笔价格
    high_24 = time_24["成交价"].max()  # 取最高价

    if auction_price < high_24:
        return False

    turnover_rate = auction_volume / circulation_shares * 100  # 换手率百分比
    if auction_change * turnover_rate <= 0.52:
        return False

    # 条件3：自由流通值<60亿
    if free_circulation_value >= 60e8:
        return False

    # 条件4：(09:25价-09:24价)/前收盘价*100 <3
    price_diff_ratio = (auction_price - price_24) / yesterday_close * 100
    if price_diff_ratio >= 3:
        return False

    # 条件5：09:31成交量>9000手
    time_31 = df[(df["时间"].dt.hour == 9) & (df["时间"].dt.minute == 31)]
    volume_31 = time_31["手数"].sum()
    if volume_31 <= 9000:
        return False

    # 条件6：09:31大单净量>0
    buy_volume = time_31[time_31["买卖盘性质"] == "买盘"]["成交额"].sum()
    sell_volume = time_31[time_31["买卖盘性质"] == "卖盘"]["成交额"].sum()
    if buy_volume <= sell_volume:
        return False

    # 条件7：09:31成交量/09:30成交量>4
    time_30 = df[(df["时间"].dt.hour == 9) & (df["时间"].dt.minute == 30)]
    volume_30 = time_30["手数"].sum()

    if volume_30 == 0 or volume_31 / volume_30 <= 4:
        return False

    return True


if __name__ == "__main__":
    # 构造测试参数
    stock_info = {
        "symbol": "300815",
        "yesterday_close": 18.60,
        "circulation_shares": 386.83e6,
        "free_circulation_value": 86.34e8,
    }

    try:
        # 执行筛选函数
        result = filter_stock(stock_info)

        print(f"股票{stock_info['symbol']}是否满足条件：{result}")

    except Exception as e:
        print(f"执行异常：{str(e)}")
