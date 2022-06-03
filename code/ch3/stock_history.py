import json
import pymysql

import requests
import pandas

""""從 get_history_data_from_opendata.py 衍生出來
相對應書本的 p.113~115
"""


def create_new_header(orignal_headers):
    new_headers = []

    for column in orignal_headers:
        data = str(column)
        if data == "日期":
            new_headers.append("trade_date")
        elif data == "證券名稱":
            new_headers.append("stock_name")
        elif data == "成交股數":
            new_headers.append("volume")
        elif data == "成交金額":
            new_headers.append("total_price")
        elif data == "開盤價":
            new_headers.append("open")
        elif data == "最高價":
            new_headers.append("high")
        elif data == "最低價":
            new_headers.append("low")
        elif data == "收盤價":
            new_headers.append("close")
        elif data == "漲跌價差":
            new_headers.append("spread")
        elif data == "成交筆數":
            new_headers.append("transactions_number")

    return new_headers


def convert_date(data_ROC):
    """因為格式為109/1/1，為民國年，需轉換成西元年"""
    date_arr = data_ROC.split("/")
    new_year = int(date_arr[0]) + 1911
    return f"{new_year}-{date_arr[1]}-{date_arr[2]}"


# API位置
stock_symbol = "2330"
date = "20220501"  # 會取得該所屬月份的所有交易日資訊
address = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_symbol}"

# 取得資料
response = requests.get(address)
"""解析"""
# 有幾個部分：stat, date, title, fields, data, notes
data = response.text  # 這是json格式的資料
a_json = json.loads(data)  # 轉成dict
df = pandas.DataFrame.from_dict(a_json["data"])  # 轉成dataframe

# 修改欄位名稱
new_headers = create_new_header(a_json["fields"])
df.columns = new_headers  # 設定資料欄位的名稱

db_settings = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "__user__",
    "password": "__pwd__",
    "db": "finance",
}

try:
    # 建立connection物件
    conn = pymysql.connect(**db_settings)
    # 建立cursor物件
    with conn.cursor() as cursor:
        # 新增SQL語法
        for _, row in df.iterrows():
            try:
                # 如果要避免重複輸入則使用：INSERT IGNORE INTO DailyPriceㄓ
                cmd = """INSERT INTO DailyPrice 
                (StockID, Symbol, TradeDate, OpenPrice, HighPrice, LowPrice,
                ClosePrice,Volumn)
                values(%s,%s,%s,%s,%s,%s,%s,%s);"""
                cursor.execute(
                    cmd,
                    (
                        None,
                        stock_symbol,
                        convert_date(row.trade_date),
                        row.open if pandas.notnull(row.open) else 0,
                        row.high if pandas.notnull(row.high) == float("nan") else 0,
                        row.low if pandas.notnull(row.low) == float("nan") else 0,
                        row.close if pandas.notnull(row.close) == float("nan") else 0,
                        row.volume if pandas.notnull(row.volume) == float("nan") else 0,
                    ),
                )
                conn.commit()
            except Exception as e:
                print(e)
except Exception as exc:
    print(exc)
