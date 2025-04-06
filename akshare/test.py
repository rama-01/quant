import akshare as ak
from utils.draw import display_dataframe_in_window


def test():
    stock_info_sz_name_code_df = ak.stock_info_sz_name_code(symbol="A股列表")
    display_dataframe_in_window(stock_info_sz_name_code_df)


if __name__ == "__main__":
    test()
