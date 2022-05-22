import os
from io import StringIO
import pymysql

import requests
import pandas


""""相對應書本的 p.115~117
"""


class daily_market_info:
    """Get the CSV which is the data TAIEX's open, high, low
    and close price from API.
    * Name: 盤後資訊 > 每日市場成交資訊
    * Download from: https://www.twse.com.tw/exchangeReport/FMTQIK?response=open_data
    * Character encoding: utf8
    * File type: CSV

    範本：每日市場成交資訊_202205.csv
    CSV:
        columns:
            日期,成交股數,成交金額,成交筆數,發行量加權股價指數,漲跌點數
        data:
            110/01/04,9,339,297,176,349,548,269,131,2,722,333,14,902.03,169.50
    """

    def __init__(self) -> None:
        self.title = "盤後資訊 > 每日市場成交資訊"
        self.url = "https://www.twse.com.tw/exchangeReport/FMTQIK?response=open_data"
        self.df = None  # 把資料從csv轉乘datframe

    def _get_csv_data(self, url=None, path=None) -> bool:
        """Get the data from CSV and turn it to Datafram.
        Return success or not.

        Args:
            param1 (str): 資料的url
        Returns:
            bool: 回傳結果. True 表示取得成功，False 表示取得失敗或是轉換失敗。

        """

        try:
            if path is None:
                if url:
                    self.url = url

                csv = requests.get(self.url)  # 從網路取得CSV檔案
                df = pandas.read_csv(StringIO(csv.text))  # 有header
            else:
                # 從CSV檔取得資料，且編碼為big-5
                # df = pandas.read_csv(path, encoding="big5")
                # 從CSV檔取得資料，已改成非big-5編碼
                df = pandas.read_csv(path)
            # print(df.tail())  # debug
            self.df = df
        except Exception as exc:
            print(exc)
            return False

        return True

    def _insert_mysql(self) -> bool:
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
            with conn.cursor() as cursor:
                # 新增SQL語法
                for _, row in self.df.iterrows():
                    if pandas.isnull(row[0]):
                        break
                    trade_date = f"{str(int(row[0]/10000)+1911)}-{str(row[0])[3:5]}-{str(row[0])[5:8]}"
                    cmd = f"""INSERT IGNORE INTO StockTransactionInfo
                    (TradeDate,
                    TranscationQty, TranscationAmount, TranscationCount,
                    Taiex, ChangePoint)
                    VALUES('{trade_date}',
                    '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]});"""
                    cursor.execute(cmd)
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {self.title}==")
        return True

    def get_and_save(self, url=None, path=None) -> bool:
        """Get csv data and save into MySQL.

        Args:
            param1 (str): 資料的路徑
        Returns:
            bool: 回傳結果. True 表示儲存成功，False 表示沒有儲存至資料庫。

        """

        if url:
            print("=資料從網路來（非預設）=")
            r = self._get_csv_data(url)
        else:
            if path:  # 代表是從檔案來的
                print("=資料從檔案來=")
                r = self._get_csv_data(path=path)
            else:
                print("=資料從網路來=")
                r = self._get_csv_data()

        if r:
            r = self._insert_mysql()
            return True
        else:
            return False


"""實作測試"""
"""Path"""
csv_data = daily_market_info()
csv_path = os.path.join(os.path.dirname(__file__), "每日市場成交資訊_202205.csv")
r = csv_data.get_and_save(path=csv_path)
if r:
    print("success")
else:
    print("False")
