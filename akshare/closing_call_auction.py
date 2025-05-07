import akshare as ak
import pandas as pd
from datetime import datetime


# 1. 获取沪深主板实时行情
def get_main_board():
    # 获取所有A股实时行情
    spot_df = ak.stock_zh_a_spot_em()

    # 筛选沪深主板（60/00开头）
    main_board = spot_df[
        spot_df["代码"].str.startswith(("60", "00"))
        & ~spot_df["名称"].str.contains("ST")
    ]

    # 过滤nan值
    main_board = main_board.dropna()

    # 过滤上市不足一年的新股
    # mature_stocks = []
    # for code in main_board["代码"]:
    #     ipo_info = ak.stock_ipo_summary_cninfo(symbol=code)
    #     if not ipo_info.empty:
    #         listing_date = pd.to_datetime(ipo_info["上市日期"].iloc[0])
    #         if (pd.Timestamp.today() - listing_date).days >= 365:
    #             mature_stocks.append(code)

    # main_board = main_board[main_board["代码"].isin(mature_stocks)]

    return main_board[
        ["代码", "名称", "最新价", "涨跌幅", "今开", "成交额", "量比", "换手率"]
    ]


# 2. 添加个股近10个交易日每日主力净流入的数据
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

        recent_10_days["代码"] = symbol
        return recent_10_days[["代码", "日期", "主力净流入"]]
    # 排除新股首日无此数据
    except Exception as e:
        return pd.DataFrame(columns=["代码", "日期", "主力净流入"])


def filter_stocks():
    # 获取基础数据
    main_df = get_main_board()

    # 筛选条件1：涨跌幅、分时、量比、换手率
    filtered_df = main_df[
        (main_df["涨跌幅"] > 1)
        & (main_df["涨跌幅"] < 9.5)
        & (main_df["最新价"] > main_df["今开"])
        & (main_df["量比"] > 1)
        & (main_df["换手率"] > 5)
    ]

    # 筛选条件2: 主力净流入
    fund_flow_list = []
    for code in filtered_df["代码"]:
        flow_data = get_recent_10_days_fund_flow(code)
        if not flow_data.empty:
            fund_flow_list.append(flow_data)

    fund_flow_df = pd.concat(fund_flow_list, ignore_index=True)

    # 将每日主力净流入数据转换为宽表格式
    if not fund_flow_df.empty and len(fund_flow_df) == 10:
        fund_flow_wide = fund_flow_df.pivot(
            index="代码", columns="日期", values="主力净流入"
        ).reset_index()

    filtered_df = filtered_df.merge(fund_flow_wide, on="代码", how="left")

    filtered_df = filtered_df[filtered_df[fund_flow_df.iloc[-1]["日期"]] > 0]

    return filtered_df


# 主逻辑
if __name__ == "__main__":
    filter_stocks()
