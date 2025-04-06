from datetime import datetime
import akshare as ak
import backtrader as bt
import pandas as pd


class MyStrategy(bt.Strategy):
    params = (
        ("maperiod", 5),
        ("printlog", False),
    )

    def __init__(self):
        self.data_close = self.datas[0].close
        self.sma = bt.indicators.SMA(self.data_close, period=self.params.maperiod)
        self.addminperiod(self.params.maperiod + 1)
        self.trade_count = 0

    def next(self):
        if len(self.data_close) < self.params.maperiod:
            return

        crossover = self.data_close[0] > self.sma[0]
        crossunder = self.data_close[0] < self.sma[0]

        if not self.position:
            if crossover:
                self.buy(size=100)
                self.trade_count += 1
        else:
            if crossunder:
                self.sell(size=100)
                self.trade_count += 1

    def stop(self):
        print(f"\n[策略验证] 总交易次数: {self.trade_count}")
        print(f"期末资金: {self.broker.getvalue():.2f}")


def main(code="600036", start_cash=1000000, stake=100, commission_fee=0.001):
    cerebro = bt.Cerebro()

    # ========== 数据获取与处理 ==========
    stock_hfq_df = ak.stock_zh_a_hist(
        symbol=code, adjust="hfq", start_date="20200101", end_date="20231231"
    ).iloc[:, :7]

    # 数据清洗与格式转换
    stock_hfq_df.columns = ["date", "code", "open", "close", "high", "low", "volume"]
    stock_hfq_df = (
        stock_hfq_df.drop(columns=["code"])
        .set_index("date")
        .apply(pd.to_numeric, errors="coerce")
    )

    # 填充缺失交易日并确保连续性
    stock_hfq_df = stock_hfq_df.asfreq("B").ffill()
    stock_hfq_df.index = pd.to_datetime(stock_hfq_df.index)

    # ========== 回测配置 ==========
    data = bt.feeds.PandasData(
        dataname=stock_hfq_df,
        fromdate=datetime(2020, 1, 1),
        todate=datetime(2023, 12, 31),
        timeframe=bt.TimeFrame.Days,
        open="open",
        high="high",
        low="low",
        close="close",
        volume="volume",
    )

    cerebro.adddata(data)
    cerebro.addstrategy(MyStrategy)
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=commission_fee)
    cerebro.addsizer(bt.sizers.FixedSize, stake=stake)

    cerebro.addanalyzer(
        bt.analyzers.TimeReturn,
        _name="_timereturn",
        timeframe=bt.TimeFrame.Days,
        data=0,
    )

    # ========== 执行回测 ==========
    results = cerebro.run(stdstats=False, exactbars=1, silent=True, tradehistory=False)

    # ========== 收益率数据构建 ==========
    strat = results[0]
    timereturn = strat.analyzers._timereturn.get_analysis()

    # 将分析器数据转换为 Pandas Series
    if isinstance(timereturn, dict):
        timereturn = pd.Series(timereturn)
    elif isinstance(timereturn, list):
        timereturn = pd.Series(timereturn)

    # 获取实际数据长度
    data_length = len(strat.data)
    date_index = stock_hfq_df.index[:data_length]

    # 确保收益率数据长度与日期索引一致
    if len(timereturn) < data_length:
        timereturn = timereturn.reindex(range(data_length), fill_value=0)

    return pd.DataFrame({"return_pct": (timereturn * 100).values}, index=date_index)


if __name__ == "__main__":
    try:
        returns_df = main()
        print("\n=== 前3日收益率 ===")
        print(returns_df.head(3))
        print("\n=== 关键统计 ===")
        print(f"总交易日数: {len(returns_df)}")
        print(f"最终收益率: {returns_df['return_pct'].cumsum().iloc[-1]:.2f}%")
    except Exception as e:
        print(f"回测失败: {str(e)}")
