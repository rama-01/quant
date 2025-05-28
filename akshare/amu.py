import akshare as ak
from utils.draw import display_dataframe_in_window
from bs4 import BeautifulSoup
import pandas as pd
import os

# 获取概念资金流排行
df get_stock_fund_flow_concept(symbol="即时"):
    stock_fund_flow_concept_df = ak.stock_fund_flow_concept(symbol)
    return stock_fund_flow_concept_df

# 获取概念成份股行情数据
def extract_rank_table(html_path):
    # 读取HTML文件
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    # 解析HTML
    soup = BeautifulSoup(html_content, "html.parser")

    # 定位目标表格
    table = soup.find("table", class_="m-table m-pager-table")

    # 提取表头
    headers = [th.get_text(strip=True) for th in table.find_all("th")]

    # 提取数据行
    rows = []
    for tr in table.select("tbody tr"):
        row = [td.get_text(strip=True) for td in tr.find_all("td")]
        rows.append(row)

    # 创建DataFrame
    df = pd.DataFrame(rows, columns=headers)
    return df


html_path = os.path.join(os.path.dirname(__file__), "test.html")
rank_df = extract_rank_table(html_path)
print("成分股涨跌排行榜数据：")
print(rank_df.to_string(index=False))


if __name__ == "__main__":
    get_stock_fund_flow_concept()