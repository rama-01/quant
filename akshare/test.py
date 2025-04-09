import akshare as ak
import pandas as pd
from utils.draw import display_dataframe_in_window


def get_recent_10_days_flow(symbol):
    try:
        market = "sh" if symbol.startswith("6") else "sz"
        stock_flow_df = ak.stock_individual_fund_flow(stock=symbol, market=market)

        # if stock_flow_df is None or stock_flow_df.empty:
        # return pd.DataFrame(columns=["代码", "日期", "主力净流入"])

        stock_flow_df = stock_flow_df.sort_values("日期", ascending=True).copy()
        stock_flow_df["日期"] = pd.to_datetime(stock_flow_df["日期"]).dt.date

        recent_10_days = stock_flow_df.tail(10).copy()
        recent_10_days.loc[:, "主力净流入"] = (
            recent_10_days["主力净流入-净额"] / 1e8
        ).round(2)

        return recent_10_days[["日期", "主力净流入"]]
    # 排除新股首日无此数据
    except Exception as e:
        return pd.DataFrame(columns=["代码", "日期", "主力净流入"])


def test():
    result = get_recent_10_days_flow("600036")
    print(result)
    display_dataframe_in_window(result)


if __name__ == "__main__":
    test()
