import akshare as ak
import pandas as pd
from utils.draw import display_dataframe_in_window
from datetime import datetime, timedelta


def get_merged_fund_flow():
    """获取并合并当日、3日、5日主力资金流数据"""

    def process_flow_data(indicator):
        df = ak.stock_individual_fund_flow_rank(indicator=indicator)
        amount_col = f"{indicator}主力净流入-净额"
        df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
        df[f"{indicator}主力净流入"] = (df[amount_col] / 1e8).round(2)
        return df[["代码", "名称", f"{indicator}主力净流入"]]

    df_today = process_flow_data("今日")
    df_3day = process_flow_data("3日")
    df_5day = process_flow_data("5日")
    df_10day = process_flow_data("10日")

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

    # 添加个股近10个交易日每日主力净流入的数据
    def get_recent_10_days_flow(symbol):
        stock_flow_df = ak.stock_individual_fund_flow(stock=symbol)

        if stock_flow_df is None:
            return pd.DataFrame(columns=["代码", "日期", "主力净流入"])
        # 确保日期列是 datetime.date 类型
        stock_flow_df["日期"] = pd.to_datetime(stock_flow_df["日期"]).dt.date

        # 获取前10条记录
        recent_10_days = stock_flow_df.tail(10)

        # 转换单位并保留两位小数
        recent_10_days.loc[:, "主力净流入"] = (
            recent_10_days["主力净流入-净额"] / 1e8
        ).round(2)

        # 仅返回日期和主力净流入列
        return recent_10_days[["日期", "主力净流入"]]

    # 拼接近10日主力净流入，表头依次命名为10日、9日...2日、1日
    all_recent_flows = []
    for index, row in merged_df.iterrows():
        symbol = row["代码"]
        recent_flows = get_recent_10_days_flow(symbol)
        recent_flows["代码"] = symbol
        all_recent_flows.append(recent_flows)

    # 合并所有近10个交易日的主力净流入数据
    recent_flows_df = pd.concat(all_recent_flows, ignore_index=True)

    # 将近10个交易日的数据透视为宽格式
    recent_flows_wide = recent_flows_df.pivot(
        index="代码", columns="日期", values="主力净流入"
    ).reset_index()
    recent_flows_wide.columns.name = None

    # 重命名列名以匹配日期格式
    recent_flows_wide.columns = ["代码"] + [f"{i+1}日" for i in range(10)]

    # 合并到主数据框
    merged_df = pd.merge(merged_df, recent_flows_wide, on="代码", how="left")
    return merged_df


def filter_and_format(merged_df):
    """筛选符合条件的股票并格式化输出"""
    filtered_df = merged_df[
        (merged_df["今日主力净流入"] > 0)
        & (merged_df["3日主力净流入"] > 0)
        & (merged_df["5日主力净流入"] > 0)
        & (merged_df["10日主力净流入"] > 0)
    ]
    return filtered_df


def test():
    # merged_data = get_merged_fund_flow()
    # result = filter_and_format(merged_data)
    # display_dataframe_in_window(result)
    get_merged_fund_flow()


if __name__ == "__main__":
    test()
