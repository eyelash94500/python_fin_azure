import requests
import json

stock = "2330"
date = "202009"
address = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock}"

# 取得資料
response = requests.get(address)
# 解析
# 有幾個部分：stat, date, title, fields, data, notes
data = response.text  # 這是json格式的資料
a_json = json.loads(data)  # 轉成dict
print(a_json)
