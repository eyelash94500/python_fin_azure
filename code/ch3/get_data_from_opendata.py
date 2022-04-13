import requests
import pandas
from io import StringIO

# 資料說明：https://data.gov.tw/dataset/11549
# API位置
address = "http://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"

# 取得資料
response = requests.get(address)

# 解析
data = response.text
mystr = StringIO(data)
df = pandas.read_csv(mystr, header=None)

# 建立新dataframe
new_headers = df.iloc[0]  # 第一行當作header
df = df[1:]  # 拿掉第一行的資料
df.columns = new_headers  # 設定資料欄位的名稱
print(df)
