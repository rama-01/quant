import akshare as ak

df = ak.stock_zh_a_spot_em()
# 排除最新价为nan的数据
df = df.dropna()
df = df[df["代码"].str.startswith(("60", "00"))]

print(df)
