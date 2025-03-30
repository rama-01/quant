import akshare as ak
from utils.draw import display_dataframe_in_window


def get_sse_summary():
    df = ak.stock_zh_a_spot_em()
    return df


if __name__ == "__main__":
    summary_df = get_sse_summary()
    display_dataframe_in_window(summary_df)
