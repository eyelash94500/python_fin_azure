import json
import numpy
import pandas
import requests

# API位置
start_time = 946656000  # 2000/1/1
end_time = 1600272000  # 2020/9/17
stock_code = 2317
stock_market = "TW"
address = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_code}.{stock_market}?period1={start_time}&period2={end_time}&interval=1d&events=history&=hP2rOschxO0"

# 使用requests 來跟遠端 API server 索取資料
response = requests.get(address)

# 序列化資料回報
data = json.loads(response.text)

# 把json格式資料放入pandas中
df = pandas.DataFrame(
    data["chart"]["result"][0]["indicators"]["quote"][0],
    index=pandas.to_datetime(
        numpy.array(data["chart"]["result"][0]["timestamp"]) * 1000 * 1000 * 1000
    ),
    columns=["open", "high", "low", "close", "volume"],
)

# 寫成csv
df.to_csv(f"{stock_code}_{start_time}_{end_time}.csv")
