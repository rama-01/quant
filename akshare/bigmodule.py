from bigmodule import M

# <aistudiograph>


# @param(id="m1", name="initialize")
# 交易引擎：初始化函数, 只执行一次
def m1_initialize_bigquant_run(context):
    import math
    import numpy as np

    from bigtrader.finance.commission import PerOrder

    # 系统已经设置了默认的交易手续费和滑点, 要修改手续费可使用如下函数
    context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    # 预测数据, 通过 options 传入进来, 使用 read_df 函数, 加载到内存 (DataFrame)
    # 设置买入的股票数量, 这里买入预测股票列表排名靠前的5只
    stock_count = 1
    # 每只的股票的权重, 如下的权重分配会使得靠前的股票分配多一点的资金, [0.339160, 0.213986, 0.169580, ..]
    context.stock_weights = np.array(
        [1 / math.log(i + 2) for i in range(0, stock_count)]
    )
    context.stock_weights = context.stock_weights / context.stock_weights.sum()

    # 设置每只股票占用的最大资金比例
    context.max_cash_per_instrument = 1
    context.options["hold_days"] = 1


# @param(id="m1", name="handle_data")
# 回测引擎：每日数据处理函数, 每天执行一次
def m1_handle_data_bigquant_run(context, data):
    # 按日期过滤得到今日的预测数据
    ranker_prediction = context.data[
        context.data.date == data.current_dt.strftime("%Y-%m-%d")
    ]

    # 1. 资金分配
    # 平均持仓时间是hold_days, 每日都将买入股票, 每日预期使用 1/hold_days 的资金
    # 实际操作中, 会存在一定的买入误差, 所以在前hold_days天, 等量使用资金；之后, 尽量使用剩余资金（这里设置最多用等量的1.5倍）
    is_staging = (
        context.trading_day_index < context.options["hold_days"]
    )  # 是否在建仓期间（前 hold_days 天）
    cash_avg = context.portfolio.portfolio_value / context.options["hold_days"]
    cash_for_buy = min(context.portfolio.cash, (1 if is_staging else 1.5) * cash_avg)
    cash_for_sell = cash_avg - (context.portfolio.cash - cash_for_buy)
    positions = {
        e: p.amount * p.last_sale_price for e, p in context.portfolio.positions.items()
    }

    # 2. 生成卖出订单：hold_days天之后才开始卖出；对持仓的股票, 按机器学习算法预测的排序末位淘汰
    if not is_staging and cash_for_sell > 0:
        equities = {e: e for e, p in context.portfolio.positions.items()}
        instruments = list(
            reversed(
                list(
                    ranker_prediction.instrument[
                        ranker_prediction.instrument.apply(lambda x: x in equities)
                    ]
                )
            )
        )

        for instrument in instruments:
            context.order_target(instrument, 0)
            cash_for_sell -= positions[instrument]
            if cash_for_sell <= 0:
                break

    # 3. 生成买入订单：按机器学习算法预测的排序, 买入前面的stock_count只股票
    buy_cash_weights = context.stock_weights
    buy_instruments = list(ranker_prediction.instrument[: len(buy_cash_weights)])
    max_cash_per_instrument = (
        context.portfolio.portfolio_value * context.max_cash_per_instrument
    )
    for i, instrument in enumerate(buy_instruments):
        cash = cash_for_buy * buy_cash_weights[i]
        if cash > max_cash_per_instrument - positions.get(instrument, 0):
            # 确保股票持仓量不会超过每次股票最大的占用资金量
            cash = max_cash_per_instrument - positions.get(instrument, 0)
        if cash > 0:
            context.order_value(instrument, cash)


# @module(position="-357,-563", comment="""""", comment_collapsed=True)
m2 = M.input_features_dai.v30(
    mode="""表达式""",
    expr="""score
position
-- input_2.close / input_1.close
""",
    expr_tables="""tianzhu2_c_f70_50_y36""",
    extra_fields="""date, instrument""",
    order_by="""date, instrument""",
    expr_drop_na=True,
    extract_data=False,
    m_name="""m2""",
)

# @module(position="-356,-431", comment="""抽取预测数据""", comment_collapsed=True)
m3 = M.extract_data_dai.v17(
    sql=m2.data,
    start_date="""2024-06-01""",
    start_date_bound_to_trading_date=True,
    end_date="""2025-02-26""",
    end_date_bound_to_trading_date=True,
    before_start_days=90,
    debug=False,
    m_name="""m3""",
)

# @module(position="-321,-337", comment="""""", comment_collapsed=True)
m4 = M.data_sort.v6(
    input_ds=m3.data,
    sort_by="""position""",
    group_by="""date""",
    keep_columns="""--""",
    ascending=True,
    m_name="""m4""",
)

# @module(position="-314,-255", comment="""""", comment_collapsed=True)
m1 = M.bigtrader.v30(
    data=m4.sorted_data,
    start_date="""""",
    end_date="""""",
    initialize=m1_initialize_bigquant_run,
    handle_data=m1_handle_data_bigquant_run,
    capital_base=1000000,
    frequency="""daily""",
    product_type="""股票""",
    rebalance_period_type="""交易日""",
    rebalance_period_days="""1""",
    rebalance_period_roll_forward=True,
    backtest_engine_mode="""标准模式""",
    before_start_days=0,
    volume_limit=1,
    order_price_field_buy="""open""",
    order_price_field_sell="""close""",
    benchmark="""沪深300指数""",
    plot_charts=True,
    debug=False,
    backtest_only=False,
    m_name="""m1""",
)
# </aistudiograph>
