import akshare as ak
import pandas as pd
from utils.draw import display_dataframe_in_window


def get_recent_10_days_flow(symbol):
    stock_flow_df = ak.stock_individual_fund_flow(stock=symbol)

    # 获取前10条记录
    recent_10_days = stock_flow_df.tail(10)

    # 转换单位并保留两位小数
    recent_10_days["主力净流入"] = (recent_10_days["主力净流入-净额"] / 1e8).round(2)

    # 仅返回日期和主力净流入列
    return recent_10_days[["日期", "主力净流入"]]


def test():
    merged_data = get_recent_10_days_flow("600094")
    print(merged_data)


if __name__ == "__main__":
    test()
