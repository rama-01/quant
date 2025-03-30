import akshare as ak
from utils.draw import display_dataframe_in_window
import matplotlib.pyplot as plt
import pandas as pd


def get_stock_minute_data(stock_code: str) -> pd.DataFrame:
    """
    获取某只股票某天的分钟级别行情数据
    :param stock_code: 股票代码（如 "sh600519"）
    :param date: 日期（格式为 "YYYY-MM-DD"）
    :return: 包含分钟级别行情数据的 DataFrame
    """
    try:
        # 使用 akshare 获取分钟级别行情数据
        df = ak.stock_zh_a_minute(symbol=stock_code, period="1", adjust="qfq")
        return df
    except Exception as e:
        print(f"获取数据失败: {e}")
        return pd.DataFrame()


def plot_price_time(df: pd.DataFrame):
    """
    绘制价格-时间关系的二维图表
    :param df: 包含分钟级别行情数据的 DataFrame
    """
    if df.empty:
        print("数据为空，无法绘制图表")
        return

    # 提取时间和价格列
    df["时间"] = pd.to_datetime(df["day"])
    df.set_index("时间", inplace=True)

    # 绘制二维图表
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df["close"], label="收盘价", color="blue")
    plt.title("股票价格-时间关系图")
    plt.xlabel("时间")
    plt.ylabel("价格")
    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    # 输入股票代码和日期
    stock_code = "sh600519"  # 示例：贵州茅台
    date = "2025-03-28"  # 示例日期

    # 获取分钟级别行情数据
    minute_data = get_stock_minute_data(stock_code)

    # 输出数据表格
    if not minute_data.empty:
        print(minute_data)  # 打印到控制台
        display_dataframe_in_window(minute_data)  # 显示在窗口中

    # 绘制价格-时间关系图
    plot_price_time(minute_data)
