from datetime import datetime
import akshare as ak
import backtrader as bt
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
import pandas as pd

plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置画图时的中文显示
plt.rcParams["axes.unicode_minus"] = False  # 设置画图时的负号显示


class MyStrategy(bt.Strategy):
    """
    主策略程序
    """

    params = (
        ("maperiod", 5),
        ("printlog", True),
    )  # 全局设定交易策略的参数, maperiod 是 MA 均值的长度

    def __init__(self):
        """
        初始化函数
        """
        self.data_close = self.datas[0].close  # 指定价格序列
        self.addminperiod(self.params.maperiod + 1)  # 确保足够数据
        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        # 添加移动均线指标
        self.sma = bt.indicators.SMA(self.data_close, period=self.params.maperiod)

    def next(self):
        """
        主逻辑
        """
        if len(self.data_close) < self.params.maperiod:
            return
        # self.log(f'收盘价, {data_close[0]}')  # 记录收盘价
        if self.order:  # 检查是否有指令等待执行
            return
        # 检查是否持仓
        if not self.position:  # 没有持仓
            # 执行买入条件判断：收盘价格上涨突破15日均线
            if self.data_close[0] > self.sma[0]:
                self.log("BUY CREATE, %.2f" % self.data_close[0])
                # 执行买入
                self.order = self.buy()
        else:
            # 执行卖出条件判断：收盘价格跌破15日均线
            if self.data_close[0] < self.sma[0]:
                self.log("SELL CREATE, %.2f" % self.data_close[0])
                # 执行卖出
                self.order = self.sell()

    def log(self, txt, dt=None, do_print=False):
        """
        Logging function fot this strategy
        """
        if self.params.printlog or do_print:
            dt = dt or self.datas[0].datetime.date(0)
            print("%s, %s" % (dt.isoformat(), txt))

    def notify_order(self, order):
        """
        记录交易执行情况
        """
        # 如果 order 为 submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"买入:\n价格:{order.executed.price},\
                成本:{order.executed.value},\
                手续费:{order.executed.comm}"
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(
                    f"卖出:\n价格：{order.executed.price},\
                成本: {order.executed.value},\
                手续费{order.executed.comm}"
                )
            self.bar_executed = len(self)

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")
        self.order = None

    def notify_trade(self, trade):
        """
        记录交易收益情况
        """
        if not trade.isclosed:
            return
        self.log(f"策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}")

    def stop(self):
        """
        回测结束后输出结果
        """
        self.log(
            "(MA均线： %2d日) 期末总资金 %.2f"
            % (self.params.maperiod, self.broker.getvalue()),
            do_print=True,
        )


def main(code="600036", start_cash=1000000, stake=100, commission_fee=0.001):
    cerebro = bt.Cerebro()  # 初始化回测引擎

    # ========== 数据获取与处理 ==========
    # 获取股票后复权数据
    stock_hfq_df = ak.stock_zh_a_hist(
        symbol=code, adjust="hfq", start_date="20200101", end_date="20231231"
    ).iloc[
        :, :7
    ]  # 取前7列（日期、代码、OHLCV）

    # 列名修正与清理
    stock_hfq_df.columns = ["date", "code", "open", "close", "high", "low", "volume"]
    stock_hfq_df.drop(columns=["code"], inplace=True)

    # 数据类型转换
    numeric_cols = ["open", "close", "high", "low", "volume"]
    stock_hfq_df[numeric_cols] = stock_hfq_df[numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    )
    stock_hfq_df.dropna(inplace=True)

    # 日期索引设置
    stock_hfq_df["date"] = pd.to_datetime(stock_hfq_df["date"])
    stock_hfq_df.set_index("date", inplace=True)

    # ========== 回测配置 ==========
    # 加载数据
    data = bt.feeds.PandasData(
        dataname=stock_hfq_df,
        fromdate=datetime(2020, 1, 1),
        todate=datetime(2023, 12, 31),
        open="open",
        high="high",
        low="low",
        close="close",
        volume="volume",
        openinterest=-1,
    )
    cerebro.adddata(data)

    # 添加策略（关键修改点）
    cerebro.addstrategy(MyStrategy)  # 单策略运行

    # 配置经纪商参数
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=commission_fee)
    cerebro.addsizer(bt.sizers.FixedSize, stake=stake)

    # ========== 添加分析器 ==========
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")  # 收益率分析
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")  # 交易统计
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")  # 回撤分析

    # ========== 执行回测 ==========
    print("\n=== 数据摘要 ===")
    print(f"时间范围: {stock_hfq_df.index.min()} 至 {stock_hfq_df.index.max()}")
    print(f"总K线数量: {len(stock_hfq_df)} 条")
    print("前3行数据:\n", stock_hfq_df.head(3))
    print("\n后3行数据:\n", stock_hfq_df.tail(3))

    print("\n=== 回测开始 ===")
    print(f"期初总资金: {cerebro.broker.getvalue():.2f}")

    # 运行回测
    results = cerebro.run()

    # ========== 结果分析 ==========
    strat = results[0]
    print("\n=== 回测结果 ===")
    print(f"期末总资金: {cerebro.broker.getvalue():.2f}")

    # 打印收益率
    returns_analysis = strat.analyzers.returns.get_analysis()
    print(f"年化收益率: {returns_analysis['rnorm100']:.2f}%")

    # 打印交易统计
    ta = strat.analyzers.ta.get_analysis()
    print(f"总交易次数: {ta.total.closed}")
    print(f"盈利交易占比: {ta.won.total/ta.total.closed:.2%}")

    # 打印最大回撤
    drawdown = strat.analyzers.drawdown.get_analysis()
    print(f"最大回撤: {drawdown.max.drawdown:.2f}%")

    # ========== 可视化 ==========
    # 解决中文显示问题（macOS）
    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS"]
    plt.rcParams["axes.unicode_minus"] = False

    # 绘制价格与均线
    # ========== 可视化 ==========
    # 解决中文显示问题（macOS）
    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS"]
    plt.rcParams["axes.unicode_minus"] = False

    # 创建子图布局
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 10), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
    )

    # 绘制价格与均线
    ax1.plot(stock_hfq_df["close"], label="收盘价", color="#1f77b4")
    ax1.plot(
        stock_hfq_df["close"].rolling(5).mean(),
        label="5日均线",
        color="#ff7f0e",
        linestyle="--",
        linewidth=1.5,
    )
    ax1.set_ylabel("价格 (元)", fontsize=12)
    ax1.set_title(f"{code} 价格走势与策略收益率", fontsize=14, pad=20)
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc="upper left")

    # 计算并绘制累计收益率曲线
    portfolio_value = pd.Series(strat.analyzers.getbyname("returns").rets)
    cumulative_return = (portfolio_value / start_cash - 1) * 100  # 转换为百分比

    # 确保时间索引对齐
    dates = stock_hfq_df.index[: len(cumulative_return)]

    ax2.plot(dates, cumulative_return, label="累计收益率", color="#2ca02c", linewidth=2)
    ax2.fill_between(
        dates,
        cumulative_return,
        where=(cumulative_return >= 0),
        facecolor="#2ca02c",
        alpha=0.2,
    )
    ax2.axhline(0, color="gray", linestyle="--", linewidth=1)
    ax2.set_xlabel("日期", fontsize=12)
    ax2.set_ylabel("收益率 (%)", fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc="upper left")

    # 优化布局
    plt.tight_layout()
    plt.show()

    # 绘制回测结果
    cerebro.plot(style="candlestick", volume=False)


if __name__ == "__main__":
    main(code="600036", start_cash=1000000, stake=100, commission_fee=0.001)
