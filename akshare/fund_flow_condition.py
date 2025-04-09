import akshare as ak
import pandas as pd
from utils.draw import display_dataframe_in_window
from datetime import datetime, timedelta


# 获取个股资金流排名
def get_stock_fund_flow_rank(indicator):
    df = ak.stock_individual_fund_flow_rank(indicator=indicator)
    amount_col = f"{indicator}主力净流入-净额"
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
    df[f"{indicator}主力净流入"] = (df[amount_col] / 1e8).round(2)
    return df[["代码", "名称", f"{indicator}主力净流入"]]


def get_recent_fund_flow():
    df_today = get_stock_fund_flow_rank("今日")
    df_3day = get_stock_fund_flow_rank("3日")
    df_5day = get_stock_fund_flow_rank("5日")
    df_10day = get_stock_fund_flow_rank("10日")

    merged_df = pd.merge(
        df_today[["代码", "名称", "今日主力净流入"]],
        df_3day[["代码", "名称", "3日主力净流入"]],
        on=["代码", "名称"],
        how="inner",
    )
    merged_df = pd.merge(
        merged_df,
        df_5day[["代码", "名称", "5日主力净流入"]],
        on=["代码", "名称"],
        how="inner",
    )
    merged_df = pd.merge(
        merged_df,
        df_10day[["代码", "名称", "10日主力净流入"]],
        on=["代码", "名称"],
        how="inner",
    )
    # 排除30、688开头的股票
    merged_df = merged_df[~merged_df["代码"].str.startswith(("30", "688"))]
    # 排除ST、*ST
    merged_df = merged_df[~merged_df["名称"].str.startswith(("ST", "*ST"))]
    # 排除今日、3日、5日、10日数据中存在nan的数据
    merged_df = merged_df[
        (merged_df["今日主力净流入"].notna())
        & (merged_df["3日主力净流入"].notna())
        & (merged_df["5日主力净流入"].notna())
        & (merged_df["10日主力净流入"].notna())
    ]
    return merged_df


# 添加个股近10个交易日每日主力净流入的数据
def get_recent_10_days_fund_flow(symbol):
    try:
        market = "sh" if symbol.startswith("6") else "sz"
        stock_flow_df = ak.stock_individual_fund_flow(stock=symbol, market=market)

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


def get_merged_fund_flow():
    merged_df = get_recent_fund_flow()
    all_recent_data = []
    for index, row in merged_df.iterrows():
        symbol = row["代码"]
        recent_flows = get_recent_10_days_fund_flow(symbol)
        recent_flows = recent_flows.sort_values("日期", ascending=False)

        # 转换为横向排列的Series
        if not recent_flows.empty and len(recent_flows) == 10:
            flow_dict = {
                f"{i+1}日": val
                for i, val in enumerate(recent_flows["主力净流入"].values)
            }
            flow_dict["代码"] = symbol

            all_recent_data.append(pd.Series(flow_dict))

    # 合并特征到主表
    if all_recent_data:
        recent_features = pd.DataFrame(all_recent_data)
        merged_df = pd.merge(merged_df, recent_features, on="代码", how="left")
    return merged_df


def filter_merged_df(merged_df):
    filtered_df = merged_df[
        (merged_df["今日主力净流入"] > 0)
        & (merged_df["3日主力净流入"] > 0)
        & (merged_df["5日主力净流入"] > 0)
        & (merged_df["10日主力净流入"] > 0)
    ]
    return filtered_df


def test():
    merged_data = get_merged_fund_flow()
    result = filter_merged_df(merged_data)
    display_dataframe_in_window(result)


if __name__ == "__main__":
    test()
