import os
import requests
from io import StringIO

import pandas

import db_connect


class legal_daily_future_option_history:
    """Get the CSV which is recorded the data which 3 legal-person traded on
    future and option from Taifex's CSV.

    * Name: 區分期貨與選擇權二類-依日期（資料下載專區）
    * Download from: https://www.taifex.com.tw/cht/3/futAndOptDateView?menuid1=03
    * Character encoding: big5
    * File type: CSV

    CSV:
        columns:
            日期,身份別,
            期貨多方交易口數,選擇權多方交易口數,
            期貨多方交易契約金額(千元),選擇權多方交易契約金額(千元),
            期貨空方交易口數,選擇權空方交易口數,
            期貨空方交易契約金額(千元),選擇權空方交易契約金額(千元),
            期貨多空交易口數淨額,選擇權多空交易口數淨額,
            期貨多空交易契約金額淨額(千元),選擇權多空交易契約金額淨額(千元),

            期貨多方未平倉口數,選擇權多方未平倉口數,
            期貨多方未平倉契約金額(千元),選擇權多方未平倉契約金額(千元),
            期貨空方未平倉口數,選擇權空方未平倉口數,
            期貨空方未平倉契約金額(千元),選擇權空方未平倉契約金額(千元),
            期貨多空未平倉口數淨額,選擇權多空未平倉口數淨額,
            期貨多空未平倉契約金額淨額(千元),選擇權多空未平倉契約金額淨額(千元)
        data:
            20210713,自營商,
            49823,278180,52199999,
            705529,51537,262514,
            59173868,659540,-1714,
            15666,-6973869,45989,
            113981,169564,115945548,
            954505,145842,145264,
            71963914,921040,-31861,
            24300,43981634,33465

    """

    def __init__(self) -> None:
        self.title = "區分期貨與選擇權二類-依日期（資料下載專區）"
        # 預設值，用來當範本
        self.url = os.path.join(os.path.dirname(__file__), "20180907-20210907.csv")
        self.df = None  # 把資料從csv轉成datframe

    def _get_csv_data(self, url=None) -> bool:
        """Get the data from CSV and turn it to Datafram.
        Return success or not.

        Args:
            param1 (str): 歷史資料CSV路徑
        Returns:
            bool: 回傳結果. True 表示取得成功，False 表示去得失敗或是轉換失敗。

        """

        # 設定url
        if url:
            self.url = url

        try:
            df = pandas.read_csv(self.url, encoding="big5")  # 有header
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
                    cmd = f"""INSERT IGNORE INTO LegalDailyFutureOption 
                    (TradeDate, TradeGroup,
                    FutureLongQty, OptionLongQty,
                    FutureLongAmount, OptionLongAmount,
                    FutureShortQty, OptionShortQty,
                    FutureShortAmount, OptionShortAmount,
                    FutureNetQty, OptionNetQty,
                    FutureNetAmount, OptionNetAmount,
                    FutureLongOIQty, OptionLongOIQty,
                    FutureLongOIAmount, OptionLongOIAmount,
                    FutureShortOIQty, OptionShortOIQty,
                    FutureShortOIAmount, OptionShortOIAmount,
                    FutureOINetQty, OptionOINetQty,
                    FutureOINetAmount, OptionOINetAmount)
                    VALUES("{trade_date}", "{row[1]}",
                    {row[2]}, {row[3]},
                    {row[4]}, {row[5]},
                    {row[6]}, {row[7]},
                    {row[8]}, {row[9]},
                    {row[10]}, {row[11]},
                    {row[12]}, {row[13]},
                    {row[14]}, {row[15]},
                    {row[16]}, {row[17]},
                    {row[18]}, {row[19]},
                    {row[20]}, {row[21]},
                    {row[22]}, {row[23]},
                    {row[24]}, {row[25]});"""
                    cursor.execute(cmd)

                    counter += 1
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {counter}==")
        return True

    def get_and_save(self, url=None) -> bool:
        """Get csv data and save into MySQL.

        Args:
            param1 (str): 資料的路徑
        Returns:
            bool: 回傳結果. True 表示儲存成功，False 表示沒有儲存至資料庫。

        """

        if url:
            r = self._get_csv_data(os.path.join(os.path.dirname(__file__), url))
        else:
            r = self._get_csv_data(url=None)

        if r:
            r = self._insert_mysql()
            return True
        else:
            return False


"""實作測試"""
# test = legal_daily_future_option_history()
# r = test.get_and_save()
# print(r)


class legal_daily_future_option:
    """Get the CSV which is recorded the data which 3 legal-person traded on
    future and option from open data.

    * Name: 三大法人-區分期貨與選擇權二類-依日期
    * Download from: https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate
    * Character encoding: big5
    * File type: CSV

    CSV:
        columns:
            日期,身份別,
            期貨多方交易口數,選擇權多方交易口數,
            期貨多方交易契約金額(千元),選擇權多方交易契約金額(千元),
            期貨空方交易口數,選擇權空方交易口數,
            期貨空方交易契約金額(千元),選擇權空方交易契約金額(千元),
            期貨多空交易口數淨額,選擇權多空交易口數淨額,
            期貨多空交易契約金額淨額(千元),選擇權多空交易契約金額淨額(千元),

            期貨多方未平倉口數,選擇權多方未平倉口數,
            期貨多方未平倉契約金額(千元),選擇權多方未平倉契約金額(千元),
            期貨空方未平倉口數,選擇權空方未平倉口數,
            期貨空方未平倉契約金額(千元),選擇權空方未平倉契約金額(千元),
            期貨多空未平倉口數淨額,選擇權多空未平倉口數淨額,
            期貨多空未平倉契約金額淨額(千元),選擇權多空未平倉契約金額淨額(千元)
        data:
            20210713,自營商,
            49823,278180,52199999,
            705529,51537,262514,
            59173868,659540,-1714,
            15666,-6973869,45989,
            113981,169564,115945548,
            954505,145842,145264,
            71963914,921040,-31861,
            24300,43981634,33465

    """

    def __init__(self) -> None:
        self.title = "三大法人-區分期貨與選擇權二類-依日期"
        self.url = "https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate"
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
                    cmd = f"""INSERT IGNORE INTO LegalDailyFutureOption 
                    (TradeDate, TradeGroup,
                    FutureLongQty, OptionLongQty,
                    FutureLongAmount, OptionLongAmount,
                    FutureShortQty, OptionShortQty,
                    FutureShortAmount, OptionShortAmount,
                    FutureNetQty, OptionNetQty,
                    FutureNetAmount, OptionNetAmount,
                    FutureLongOIQty, OptionLongOIQty,
                    FutureLongOIAmount, OptionLongOIAmount,
                    FutureShortOIQty, OptionShortOIQty,
                    FutureShortOIAmount, OptionShortOIAmount,
                    FutureOINetQty, OptionOINetQty,
                    FutureOINetAmount, OptionOINetAmount)
                    values('{"{}-{}-{}".format(trade_date[:4],trade_date[4:6],trade_date[6:])}', 
                    '{row[1]}',
                    {row[2]}, {row[3]},
                    {row[4]}, {row[5]},
                    {row[6]}, {row[7]},
                    {row[8]}, {row[9]},
                    {row[10]}, {row[11]},
                    {row[12]}, {row[13]},
                    {row[14]}, {row[15]},
                    {row[16]}, {row[17]},
                    {row[18]}, {row[19]},
                    {row[20]}, {row[21]},
                    {row[22]}, {row[23]},
                    {row[24]}, {row[25]});"""
                    cursor.execute(cmd)

                    counter += 1
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {counter}==")
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
# test = legal_daily_future_option()
# r = test.get_and_save()
# print(r)
