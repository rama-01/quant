import akshare as ak
import pandas as pd
import numpy as np
import nest_asyncio
import asyncio
import sys
import ssl
import os
from datetime import datetime, timedelta
from utils.draw import display_dataframe_in_window
from concurrent.futures import ThreadPoolExecutor

# ===== 修复关键配置 =====
nest_asyncio.apply()

# Windows 兼容性设置
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 设置全局超时时间
os.environ["AKSHARE_TIMEOUT"] = "10"

# 代理设置（可选）
# os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"
# os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"

# SSL 验证绕过（仅测试环境使用）
ssl._create_default_https_context = ssl._create_unverified_context


# ===== 主逻辑 =====
def get_base_data():
    # 获取实时行情数据
    stock_zh_a_spot_df = ak.stock_zh_a_spot_em()

    # 筛选代码60，00开头的股票/排除ST的股票
    stock_zh_a_spot_df = stock_zh_a_spot_df[
        stock_zh_a_spot_df["代码"].str.startswith(("60", "00"))
        & ~stock_zh_a_spot_df["名称"].str.contains("ST")
    ]

    # 单位转换：流通市值、总市值、成交额转换为亿
    stock_zh_a_spot_df = stock_zh_a_spot_df.assign(
        流通市值=lambda x: x["流通市值"].astype(float) / 1e8,
        总市值=lambda x: x["总市值"].astype(float) / 1e8,
        成交额=lambda x: x["成交额"].astype(float) / 1e8,
    )
    # 获取不同周期资金流向数据
    fund_flow_today = ak.stock_individual_fund_flow_rank(indicator="今日")
    fund_flow_3d = ak.stock_individual_fund_flow_rank(indicator="3日")
    fund_flow_10d = ak.stock_individual_fund_flow_rank(indicator="10日")

    # 重命名字段（直接修改源数据字段名）
    fund_flow_today.rename(columns={"今日主力净流入-净额": "今日净流入"}, inplace=True)
    fund_flow_3d.rename(columns={"3日主力净流入-净额": "3日净流入"}, inplace=True)
    fund_flow_10d.rename(columns={"10日主力净流入-净额": "10日净流入"}, inplace=True)

    # 单位转换：今日、3日、5日、10日资金流向转换为亿
    fund_flow_today["今日净流入"] = fund_flow_today["今日净流入"].astype(float) / 1e8
    fund_flow_3d["3日净流入"] = fund_flow_3d["3日净流入"].astype(float) / 1e8
    fund_flow_10d["10日净流入"] = fund_flow_10d["10日净流入"].astype(float) / 1e8

    # 合并数据
    merged_df = pd.merge(
        stock_zh_a_spot_df[
            [
                "代码",
                "名称",
                "最新价",
                "今开",
                "涨跌幅",
                "换手率",
                "量比",
                "成交额",
                "流通市值",
                "总市值",
                "60日涨跌幅",
                "年初至今涨跌幅",
            ]
        ],
        fund_flow_today[["代码", "今日净流入"]],
        on="代码",
        how="left",
    )

    # 合并历史资金流向数据
    merged_df = pd.merge(
        merged_df, fund_flow_3d[["代码", "3日净流入"]], on="代码", how="left"
    )

    merged_df = pd.merge(
        merged_df, fund_flow_10d[["代码", "10日净流入"]], on="代码", how="left"
    )

    return merged_df


# ===== 其余逻辑保持不变 =====
# 筛选条件实现
def screen_stocks(df):
    # 条件1: 当前价格 >= 开盘价格（趋势向上）
    df = df[df["最新价"] >= df["今开"]]

    # 条件4: 流通市值20亿-500亿
    df = df[(df["流通市值"] > 20) & (df["流通市值"] < 200)]

    # 条件3: 换手率3%-20%
    df = df[(df["换手率"] > 3) & (df["换手率"] < 20)]

    # 条件2: 量比>0.8
    df = df[df["量比"] > 1]

    # 条件6: 主力净流入为正值/10日净流入为正值
    df = df[df["今日净流入"] > 0]
    df = df[df["10日净流入"] > 0]

    # 条件5: 均线多头发散（需获取历史K线数据）
    for code in df["代码"]:
        try:
            # 动态设置日期范围（最近30个交易日）
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

            # 获取历史K线数据（限定时间窗口）
            kline_df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )

            # 计算均线
            kline_df["MA5"] = kline_df["收盘"].rolling(5).mean()
            kline_df["MA10"] = kline_df["收盘"].rolling(10).mean()

            # 判断均线多头排列
            latest = kline_df.iloc[-1]
            prev_day = kline_df.iloc[-2]

            if not (
                latest["MA5"] > latest["MA10"]
                and latest["MA5"] > prev_day["MA5"]
                # and latest["MA5"] - prev_day["MA5"] >= 0.3
            ):
                df = df[df["代码"] != code]
        except:
            continue

    # 条件7: 剔除涨停的股票
    df = df[(df["涨跌幅"] < 8) & (df["涨跌幅"] >= 0)]

    # 条件8: 筹码分布：收盘获利>70%
    cyq_records = []
    stock_codes = df["代码"].copy()  # 创建副本避免迭代时修改

    for code in stock_codes:
        try:
            # 获取筹码分布数据
            cyq_df = ak.stock_cyq_em(symbol=code, adjust="qfq")

            if not cyq_df.empty:
                latest_row = cyq_df.iloc[-1]
                profit_ratio = latest_row["获利比例"]
                concentration_90 = latest_row["90集中度"]
                concentration_70 = latest_row["70集中度"]

                # 获利比例>=70%,90集中度<=0.08,70集中度<=0.10
                if (
                    profit_ratio >= 0.9
                    and concentration_70 <= 0.1
                    and concentration_90 <= 0.15
                ):
                    cyq_records.append(
                        {
                            "代码": code,
                            "获利比例": profit_ratio,
                            "90集中度": concentration_90,
                            "70集中度": concentration_70,
                        }
                    )
                else:
                    # 移除不达标股票
                    df = df[df["代码"] != code]
        except Exception as e:
            print(f"处理{code}筹码数据失败：{str(e)}")
            continue

    # 合并筹码分布数据
    if cyq_records:
        cyq_df = pd.DataFrame(cyq_records)
        df = pd.merge(df, cyq_df, on="代码", how="left")
        # 强制类型转换确保后续计算正确
        df[["获利比例", "90集中度", "70集中度"]] = df[
            ["获利比例", "90集中度", "70集中度"]
        ].astype(float)

    # 条件9：计算3天、5天、10天涨幅

    return df


def tail_strategy():
    base_data = get_base_data()
    result = screen_stocks(base_data)
    display_dataframe_in_window(result)
    return result


if __name__ == "__main__":
    tail_strategy()
