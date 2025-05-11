import akshare as ak
from utils.draw import display_dataframe_in_window


def get_first_limit_up_stock(date="20250411"):
    stock_zt_pool_em_df = ak.stock_zt_pool_em(date)
    # 昨日涨停股票池
    # stock_zt_pool_previous_em_df = ak.stock_zt_pool_previous_em(date)
    # 过滤昨日涨停股票池，排除重复symbol
    # result = stock_zt_pool_em_df[
    #     ~stock_zt_pool_em_df["代码"].isin(stock_zt_pool_previous_em_df["代码"])
    # ]
    # 筛选连板数=1的股票
    df = stock_zt_pool_em_df[stock_zt_pool_em_df["连板数"] == 1]
    # 排除30、688开头的股票
    df = df[~df["代码"].str.startswith(("30", "688"))]
    # 排除ST、*ST
    df = df[~df["名称"].str.startswith(("ST", "*ST"))]
    return df


if __name__ == "__main__":
    result = get_first_limit_up_stock()
    display_dataframe_in_window(result)
