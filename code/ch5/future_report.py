import requests
from io import StringIO

import pandas

import db_connect


class future_report:
    """Get the CSV which is recorded the future trading info.

    * Name: 期貨每日交易行情
    * Info: https://data.gov.tw/dataset/11319
    * Download from: https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=DailyMarketReportFut
    * Character encoding: big5
    * File type: CSV

    CSV:
        columns:
            日期、契約、到期月份(週別)、
            開盤價、最高價、最低價、最後成交價、
            漲跌價、漲跌%、合計成交量、結算價、
            未沖銷契約數、最後最佳買價、最後最佳賣價、
            歷史最高價、歷史最低價、是否因訊息面暫停交易、交易時段、
            價差對單式委託成交量
        data:
            20220221,TX,202203,
            18001,18242,17970,18210,
            -17,-0.09%,95874,18212,
            74692,18210,18212,
            18545,14054,,一般
            ,
    """

    def __init__(self) -> None:
        self.title = "期貨每日交易行情"
        self.url = "	https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=DailyMarketReportFut"
        self.df = None  # 把資料從csv轉成datframe

    def _get_csv_data(self, url=None) -> bool:
        """Get the data from CSV and turn it to Datafram.
        Return success or not.

        Args:
            param1 (str): 資料的url
        Returns:
            bool: The return value. True 表示取得成功，False 表示去得失敗或是轉換失敗。

        """

        if url:
            self.url = url

        try:
            csv = requests.get(self.url, verify=False)
            df = pandas.read_csv(StringIO(csv.text))  # 有header
            print(df.tail())  # debug
            self.df = df
        except Exception as exc:
            print(exc)
            return False

        return True

    def _insert_mysql(self) -> bool:
        """Insert data into MySQL.

        Returns:
            bool: 回傳結果. True 表示儲存成功，False 表示儲存失敗。

        """

        counter = 0  # 計數器：計算新增了幾筆

        try:
            # 建立connection物件
            my_connt_obj = db_connect.mysql_connect()
            conn = my_connt_obj.connect()
            with conn.cursor() as cursor:
                # 新增SQL語法
                for _, row in self.df.iterrows():
                    trade_date = str(row[0])
                    trade_date = "{}-{}-{}".format(
                        trade_date[:4], trade_date[4:6], trade_date[6:]
                    )

                    # 用%s的方式把「None」放進去
                    # 並且檢查是否有空值，如果有的話也放None
                    cmd = """INSERT IGNORE INTO FutureDailyReport
                    (TradeDate, Contract, ContractMonthWeek, 
                    OpenPrice, HighPrice, LowPrice, ClosePrice,
                    `Change`, ChangeRatio,
                    Volume, SettlementPrice,
                    OpenInterest, BestBid, BestAsk,
                    HistoricalHigh, HistoricalLow, 
                    TradingHalt, TradingSession, VolumeSpread)
                    VALUES (%s, %s, %s, 
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, 
                    %s, %s, %s);"""

                    cursor.execute(
                        cmd,
                        (
                            trade_date,
                            row[1],
                            row[2],
                            row[3] if row[3] != "-" else None,
                            row[4] if row[4] != "-" else None,
                            row[5] if row[5] != "-" else None,
                            row[6] if row[6] != "-" else None,
                            row[7] if row[7] != "-" else None,
                            float(row[8].strip("%")) / 100 if row[8] != "-" else None,
                            row[9] if row[9] != "-" else None,
                            row[10]
                            if row[10] != "-" and pandas.notnull(row[10])
                            else None,
                            row[11] if row[11] != "-" else None,
                            row[12] if row[11] != "-" else None,
                            row[13] if row[11] != "-" else None,
                            row[14] if row[11] != "-" else None,
                            row[15] if row[11] != "-" else None,
                            row[16] if pandas.notnull(row[16]) else None,
                            row[17],
                            row[18] if pandas.notnull(row[18]) else None,
                        ),
                    )

                    counter += 1
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish:{self.title}－{counter}==")
        return True

    def get_and_save(self, url=None) -> bool:
        """Get csv data and save into MySQL.

        Args:
            param1 (str): 資料的url
        Returns:
            bool: 回傳結果. True 表示儲存成功，False 表示沒有儲存至資料庫。

        """

        r = self._get_csv_data(self.url if url else None)

        if r:
            r = self._insert_mysql()
            return True
        else:
            return False


"""實作測試"""
# test = future_report()
# r = test.get_and_save()
# print(r)
