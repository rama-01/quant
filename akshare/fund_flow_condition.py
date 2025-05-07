import akshare as ak
import pandas as pd
from utils.draw import display_dataframe_in_window
from datetime import datetime, timedelta
import concurrent.futures


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


def process_single_stock(symbol):
    recent_flows = get_recent_10_days_fund_flow(symbol)
    recent_flows = recent_flows.sort_values("日期", ascending=False)

    if not recent_flows.empty and len(recent_flows) == 10:
        return {
            "代码": symbol,
            **{
                f"{i+1}日": val
                for i, val in enumerate(recent_flows["主力净流入"].values)
            },
        }


def get_merged_fund_flow():
    merged_df = get_recent_fund_flow()
    all_recent_data = []

    # 使用线程池并行处理（核心优化点）
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for index, row in merged_df.iterrows():
            symbol = row["代码"]
            # 提交任务到线程池
            future = executor.submit(process_single_stock, symbol)
            futures.append(future)

        # 收集结果
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                all_recent_data.append(result)

    if all_recent_data:
        recent_features = pd.DataFrame(all_recent_data)
        merged_df = pd.merge(merged_df, recent_features, on="代码", how="left")
    return merged_df


def test():
    merged_data = get_merged_fund_flow()
    result1 = merged_data[
        (merged_data["今日主力净流入"] > 0)
        & (merged_data["3日主力净流入"] > 0)
        & (merged_data["5日主力净流入"] > 0)
        & (merged_data["10日主力净流入"] > 0)
    ]
    print("近期主力资金净流入", result1)
    display_dataframe_in_window(result1)

    # stock_flow_df = ak.stock_individual_fund_flow(stock=symbol, market=market)
    # print(stock_flow_df)


if __name__ == "__main__":
    test()
