import akshare as ak
from utils.draw import display_dataframe_in_window


stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
display_dataframe_in_window(stock_zh_a_spot_em_df)
