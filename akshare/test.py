import akshare as ak
from utils.draw import display_dataframe_in_window


def test():
    market_value = ak.stock_zh_a_spot_em()
    print(market_value)


if __name__ == "__main__":
    test()
