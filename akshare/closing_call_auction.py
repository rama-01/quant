import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.draw import display_dataframe_in_window
import nest_asyncio
import asyncio
import sys
import ssl
import os

# ===== 修复关键配置 =====
nest_asyncio.apply()

# Windows 兼容性设置
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 设置全局超时时间
os.environ["AKSHARE_TIMEOUT"] = "10"

# 代理设置（可选）
os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"

# SSL 验证绕过（仅测试环境使用）
ssl._create_default_https_context = ssl._create_unverified_context

# ===== 主逻辑 =====
def get_base_data():
    # 获取实时行情数据（使用官方异步接口）
    stock_zh_a_spot_df = ak.stock_zh_a_spot_em()

    # 获取资金流向数据（使用官方异步接口）
    fund_flow_df = ak.stock_individual_fund_flow_rank(indicator="今日")

    # 合并数据
    merged_df = pd.merge(
        stock_zh_a_spot_df[
            ["代码", "名称", "最新价", "今开", "流通市值", "成交量", "换手率", "量比"]
        ],
        fund_flow_df[["代码", "今日主力净流入-净额"]],
        on="代码",
    )
    return merged_df

# ===== 其余逻辑保持不变 =====
# 筛选条件实现
def screen_stocks(df):
    # 条件1: 当前价格 > 开盘价格（趋势向上）
    df = df[df["最新价"] > df["今开"]]

    # 条件4: 流通市值20亿-200亿
    df = df[(df["流通市值"] > 20e8) & (df["流通市值"] < 200e8)]

    # 条件3: 换手率5%-20%
    df = df[(df["换手率"] > 5) & (df["换手率"] < 20)]

    # 条件2: 量比>1
    df = df[df["量比"] > 0.8]

    # 条件6: 主力净流入为正值
    df = df[df["今日主力净流入-净额"] > 0]

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
                adjust="qfq"
            )

            # 计算均线
            kline_df["MA5"] = kline_df["收盘"].rolling(5).mean()
            kline_df["MA10"] = kline_df["收盘"].rolling(10).mean()

            # 判断均线多头排列
            latest = kline_df.iloc[-1]
            if not (
                latest["MA5"] > latest["MA10"]
                and latest["MA5"] > kline_df.iloc[-2]["MA5"]
            ):
                df = df[df["代码"] != code]
        except:
            continue

    return df

def tail_strategy():
    base_data = get_base_data()
    result = screen_stocks(base_data)
    display_dataframe_in_window(result)
    return result

tail_strategy()