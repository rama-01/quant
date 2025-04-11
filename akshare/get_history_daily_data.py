import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from utils.draw import display_dataframe_in_window


def get_sz_stock_list():
    sz_stock_list = ak.stock_info_sz_name_code(symbol="A股列表")
    sz_stock_list = sz_stock_list[["A股代码", "A股简称"]]
    sz_stock_list.columns = ["symbol", "name"]
    sz_stock_list = sz_stock_list[~sz_stock_list["symbol"].str.startswith("30")]
    return sz_stock_list


def get_sh_stock_list():
    sh_stock_list = ak.stock_info_sh_name_code()
    sh_stock_list = sh_stock_list[["证券代码", "证券简称"]]
    sh_stock_list.columns = ["symbol", "name"]
    return sh_stock_list


def get_merged_stock_list():
    sz_stock_list = get_sz_stock_list()
    sh_stock_list = get_sh_stock_list()
    stock_list = pd.concat([sz_stock_list, sh_stock_list], ignore_index=True)
    return stock_list


def get_stock_daily_data(symbol, date_str, name):
    try:
        stock_data = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=date_str,
            end_date=date_str,
            adjust="qfq",
        )
        # 插入股票名称数据列，在股票代码symbol之后
        if stock_data is not None and not stock_data.empty:
            stock_data.insert(1, "名称", name)
            return stock_data
        else:
            return pd.DataFrame(
                columns=[
                    "代码",
                    "名称",
                    "成交额",
                    "涨跌幅",
                    "日期",
                    "最高",
                    "最低",
                    "成交量",
                    "涨跌额",
                    "振幅",
                ]
            )
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return pd.DataFrame(
            columns=[
                "代码",
                "名称",
                "成交额",
                "涨跌幅",
                "日期",
                "最高",
                "最低",
                "成交量",
                "涨跌额",
                "振幅",
            ]
        )


def get_history_daily_data(date_str="20250407"):
    stock_list = get_merged_stock_list()
    symbols = stock_list["symbol"].tolist()
    names = stock_list["name"].tolist()

    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(get_stock_daily_data, symbol, date_str, name)
            for symbol, name in zip(symbols, names)
        ]
        for future in futures:
            result = future.result()
            if not result.empty:
                all_data.append(result)

    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        # 删除日期列、涨跌额数据
        combined_data = combined_data.drop(
            columns=["日期", "最高", "最低", "成交量", "涨跌额", "振幅"]
        )
        # 单位转换
        combined_data["成交额"] = (combined_data["成交额"] / 1e8).round(2)
        # 涨幅大于0
        combined_data = combined_data[combined_data["涨跌幅"] >= 0]
        display_dataframe_in_window(combined_data)


if __name__ == "__main__":
    get_history_daily_data()
