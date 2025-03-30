from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
import pandas as pd

# 上证50成分股代码列表
sz50_stocks = [
    "600030",
    "600050",
    "600104",
    "600111",
    "600132",
    "600196",
    "600276",
    "600309",
    "600340",
    "600519",
    "600547",
    "600600",
    "600887",
    "600905",
    "601006",
    "601088",
    "601166",
    "601186",
    "601211",
    "601229",
    "601288",
    "601318",
    "601328",
    "601390",
    "601398",
    "601601",
    "601628",
    "601668",
    "601688",
    "601766",
    "601818",
    "601857",
    "601888",
    "601939",
    "601988",
    "601989",
    "603993",
    "600000",
    "600016",
    "600028",
    "600036",
    "600048",
    "600100",
    "600110",
    "600150",
    "600271",
    "600362",
    "600518",
    "600585",
    "600690",
    "600741",
    "600837",
    "600999",
    "601087",
    "601111",
    "601138",
    "601238",
    "601313",
    "601326",
    "601393",
    "601629",
    "601633",
    "601728",
    "601800",
    "601810",
    "601858",
    "601872",
    "601878",
    "601901",
    "601919",
    "601985",
    "603986",
]


def get_realtime_quotes(stock_codes):
    api = TdxHq_API()
    with api.connect("211.147.225.98", 7709):
        quotes = []
        for code in stock_codes:
            try:
                quote = api.get_security_quotes(
                    [(0, code) if code.startswith("6") else (1, code)]
                )[0]
                quotes.append(
                    {
                        "code": quote.code,
                        "name": quote.name,
                        "last_close": quote.last_close,
                        "open": quote.open,
                        "high": quote.high,
                        "low": quote.low,
                        "price": quote.price,
                        "volume": quote.volume,
                        "amount": quote.amount,
                        "bid1": quote.bid1,
                        "ask1": quote.ask1,
                    }
                )
            except Exception as e:
                print(f"获取数据失败: {e}")
        return pd.DataFrame(quotes)


if __name__ == "__main__":
    # 获取上证50成分股实时行情数据
    sz50_quotes = get_realtime_quotes(sz50_stocks)
    print(sz50_quotes)
    # 将数据保存到CSV文件
    sz50_quotes.to_csv("sz50_quotes.csv", index=False)
