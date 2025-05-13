import jqdatasdk
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 认证聚宽账号
jqdatasdk.auth("13266873985", "Zx123456")

# 获取所有A股股票池
def get_all_a_stocks():
    return jqdatasdk.get_all_securities(types=["stock"], date=None).index.tolist()

# 获取当前行情数据
def get_current_data_dict(stocks):
    current_data = jqdatasdk.get_current_data()
    return {stock: current_data[stock] for stock in stocks}

# 获取基本面数据（换手率、流通市值）
def get_fundamentals_data(stocks, date=None):
    q = jqdatasdk.query(
        jqdatasdk.fundamental.indicator.code,
        jqdatasdk.fundamental.indicator.turnover_ratio,
        jqdatasdk.fundamental.indicator.circulating_market_cap,
    ).filter(jqdatasdk.fundamental.indicator.code.in_(stocks))
    return jqdatasdk.get_fundamentals(q, date=date)

# 获取历史K线数据（用于计算均线）
def get_kline_data(stock, count=11, frequency="daily"):
    end_date = datetime.now() - timedelta(days=1)  # 使用前一交易日数据
    return jqdatasdk.get_price(
        stock,
        end_date=end_date,
        count=count,
        frequency=frequency,
        fields=["close", "volume"],
    )

# 筛选条件实现
def screen_stocks(stocks):
    filtered_stocks = []

    # 获取当前行情数据
    current_data = get_current_data_dict(stocks)

    # 获取基本面数据（换手率、流通市值）
    fundamentals_df = get_fundamentals_data(stocks)
    fundamentals_df.set_index("code", inplace=True)

    for stock in stocks:
        try:
            # 条件1: 当前价格 > 开盘价格（趋势向上）
            curr = current_data.get(stock)
            if not curr or not curr.open_price or not curr.last_price:
                continue
            if curr.last_price <= curr.open_price:
                continue

            # 条件4: 流通市值20亿-200亿（单位：亿元）
            if "circulating_market_cap" not in fundamentals_df.loc[stock]:
                continue
            market_cap = fundamentals_df.loc[stock]["circulating_market_cap"] / 1e8
            if not (20 < market_cap < 200):
                continue

            # 条件3: 换手率5%-20%
            if "turnover_ratio" not in fundamentals_df.loc[stock]:
                continue
            turnover = fundamentals_df.loc[stock]["turnover_ratio"]
            if not (5 < turnover < 20):
                continue

            # 条件2: 量比>1（当前成交量 / 过去5日平均成交量）
            kline = get_kline_data(stock)
            if len(kline) < 6:
                continue
            recent_volume = kline["volume"][-1]
            avg_volume_5 = kline["volume"][:-1].mean()
            if recent_volume <= avg_volume_5:
                continue

            # 条件5: 均线多头发散（5日线 > 10日线，且趋势向上）
            if len(kline) < 11:
                continue
            close = kline["close"]
            ma5 = close.rolling(5).mean()
            ma10 = close.rolling(10).mean()
            if not (ma5.iloc[-1] > ma10.iloc[-1] and ma5.iloc[-1] > ma5.iloc[-2]):
                continue

            # 条件6: 主力净流入为正值（聚宽无直接API，需使用其他方式获取）
            # 注意：此处为示例逻辑，实际需根据平台数据源实现
            # 示例：假设主力净流入 = 最新成交额 * (换手率 / 100)
            # main_net_inflow = curr.turnover_value * (turnover / 100)
            # if main_net_inflow <= 0:
            #     continue

            # 所有条件满足
            filtered_stocks.append(stock)

        except Exception as e:
            continue

    return filtered_stocks

# 主策略执行
def tail_strategy():
    all_stocks = get_all_a_stocks()
    result = screen_stocks(all_stocks)
    print("尾盘选股结果：")
    print(result)
    return result

# 执行策略
tail_strategy()