import jqdatasdk

jqdatasdk.auth("13266873985", "Zx123456")

result = jqdatasdk.get_price(
    "000001.XSHE",
    start_date="2025-01-01",
    end_date="2025-01-31",
    frequency="daily",
)
print(result)
