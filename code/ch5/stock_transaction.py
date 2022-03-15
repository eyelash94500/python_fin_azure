import requests
import pandas
from io import StringIO

import db_connect


class stock_transaction:
    """取得股票日成交資訊
    * 來源：https://data.gov.tw/dataset/11549
    * 名稱：盤後資訊 > 個股日成交資訊
    * 更新頻率：每日
    * 主要欄位：證券代號、證券名稱、成交股數、成交金額、開盤價、最高價、最低價、收盤價、漲跌價差、成交筆數
    e.g. "00875","國泰網路資安",
        "1,998,885","51,736,728","25.78","25.98","25.77","25.98",
        "+0.46","484"
    """

    def __init__(self) -> None:
        self.title = "盤後資訊 > 個股日成交資訊"
        self.url = (
            "http://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
        )
        self.df = None  # 把資料從csv轉乘datframe
        self.trade_date = "2021-1-1"  # 交易日

    def _create_new_header(self, orignal_headers):
        new_headers = []

        for data in orignal_headers:
            if data == "證券代號":
                new_headers.append("stock_symbol")
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

    def _get_and_set_df_data(self, url=None) -> bool:
        """取得CSV內的資料，並轉成Dataframe，回傳成功與否。

        Args:
            param1 (str): 資料的url
        Returns:
            bool: 回傳結果. True 表示取得成功，False 表示去得失敗或是轉換失敗。

        """
        if url:
            self.url = url

        try:
            csv = requests.get(self.url)
            df = pandas.read_csv(StringIO(csv.text))  # 有header
            # print(df)  # debug
            self.df = df
            # 從檔名取得日期，檔名：STOCK_DAY_ALL_20210924.csv
            trade_date_raw = csv.headers.get("Content-Disposition")[-13:-5]
            self.trade_date = (
                f"{trade_date_raw[:4]}-{trade_date_raw[4:6]}-{trade_date_raw[6:]}"
            )
        except Exception as exc:
            print(exc)
            return False

        return True

    def _insert_mysql(self) -> bool:
        """Insert data into MySQL.

        Returns:
            bool: 回傳結果. True 表示儲存成功，False 表示儲存失敗。

        """

        try:
            new_headers = self._create_new_header(self.df.columns)
            # df = self.df[1:]  # 拿掉第一行的資料 # 這是錯的，第一行不用拿掉
            df = self.df
            df.columns = new_headers  # 設定資料欄位的名稱
            print(f"{df}\n==={self.trade_date}===")

            counter = 0  # 記錄欲新增數量
            # 建立connection物件
            my_connt_obj = db_connect.mysql_connect()
            conn = my_connt_obj.connect()
            with conn.cursor() as cursor:
                now = self.trade_date

                # 新增SQL語法
                for _, row in df.iterrows():
                    try:
                        cmd = """INSERT IGNORE INTO DailyPrice 
                        (StockID, Symbol, TradeDate, OpenPrice, HighPrice,
                        LowPrice, ClosePrice, Volumn)
                        values(%s,%s,%s,%s,%s,%s,%s,%s);"""
                        cursor.execute(
                            cmd,
                            (
                                None,
                                row.stock_symbol,
                                now,
                                row.open if pandas.notnull(row.open) else 0,
                                row.high if pandas.notnull(row.high) else 0,
                                row.low if pandas.notnull(row.low) else 0,
                                row.close if pandas.notnull(row.close) else 0,
                                row.volume if pandas.notnull(row.volume) else 0,
                            ),
                        )
                        conn.commit()
                        counter += 1
                    except Exception as e:
                        print(e)
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {self.title}==")
        return True

    def get_and_save(self, url=None):
        """Get today transaction data and save into MySQL.

        Args:
            param1 (str): 資料的url
        Returns:
            bool: 回傳結果. True 表示儲存成功，False 表示沒有儲存至資料庫

        """
        r = self._get_and_set_df_data(url)
        if r:
            r = self._insert_mysql()
        else:
            return False

        return r


"""實作測試"""
# test = stock_transaction()
# r = test.get_and_save()
# print(r)
