import akshare as ak
import backtrader as bt
import pandas as pd
import datetime

# ==============================
# 数据获取：使用 akshare 获取 A股主板股票历史数据
# ==============================
def get_stock_data(stock_code, start_date, end_date):
    """
    获取 A股主板股票历史行情数据（前复权）
    :param stock_code: 股票代码（如 "000001"）
    :param start_date: 开始日期（如 "20200101"）
    :param end_date: 结束日期（如 "20240101"）
    :return: pandas.DataFrame 符合 Backtrader 格式要求
    """
    df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    df.rename(columns={
        '日期': 'datetime',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume'
    }, inplace=True)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    return df[['open', 'high', 'low', 'close', 'volume']]

# ==============================
# 策略定义：双均线策略（5日 & 20日）
# ==============================
class DoubleMAStrategy(bt.Strategy):
    params = (
        ('fast_period', 5),
        ('slow_period', 20),
    )

    def __init__(self):
        # 定义两条均线
        self.fast_ma = bt.indicators.SMA(period=self.p.fast_period)
        self.slow_ma = bt.indicators.SMA(period=self.p.slow_period)

    def next(self):
        # 做多信号：5日均线上穿20日均线（金叉）
        if not self.position:
            if self.fast_ma > self.slow_ma and self.fast_ma[-1] <= self.slow_ma[-1]:
                self.buy()

        # 做空信号：5日均线下穿20日均线（死叉）
        elif self.position:
            if self.fast_ma < self.slow_ma and self.fast_ma[-1] >= self.slow_ma[-1]:
                self.close()

# ==============================
# 回测执行函数
# ==============================
def run_backtest(stock_code="000001", start_date="20200101", end_date="20240101"):
    # 获取数据
    data = get_stock_data(stock_code, start_date, end_date)

    # 初始化回测引擎
    cerebro = bt.Cerebro()
    cerebro.addstrategy(DoubleMAStrategy)

    # 加载数据
    data_feed = bt.feeds.PandasData(dataname=data)
    cerebro.adddata(data_feed)

    # 设置初始资金和佣金
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)  # 万1手续费

    # 运行回测
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # 绘制回测结果
    cerebro.plot(iplot=False, style='candle')

if __name__ == '__main__':
    run_backtest()