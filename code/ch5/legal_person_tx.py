import requests
from io import StringIO

import pandas

import db_connect


class legal_tx:
    """Get the CSV which is recorded the data which 3 legal-person traded on
    future and option from open data.

    * Name: 三大法人-區分各期貨契約-依日期
    * Info: https://data.gov.tw/dataset/11596
    * Download from: https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate
    * Character encoding: big5
    * File type: CSV

    CSV:
        columns:
            日期、商品名稱、身份別、
            多方交易口數、多方交易契約金額(千元)、
            空方交易口數、空方交易契約金額(千元)、
            多空交易口數淨額、多空交易契約金額淨額(千元)、
            多方未平倉口數、多方未平倉契約金額(千元)、
            空方未平倉口數、空方未平倉契約金額(千元)、
            多空未平倉口數淨額、多空未平倉契約金額淨額(千元)
        data:
            20220218,臺股期貨,自營商,
            7812,28391996,
            7634,27748664,
            178,643333,
            18298,66659446,
            3962,14424786,
            14336,52234660
    """

    def __init__(self) -> None:
        self.title = "三大法人-區分各期貨契約-依日期"
        self.url = "https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate"
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

                    cmd = f"""INSERT IGNORE INTO LegalFuture 
                    (TradeDate, SecurityName, TradeGroup, 
                    LongQty, LongAmount, 
                    ShortQty, ShortAmount, 
                    NetQty, NetAmount, 
                    LongOIQty, LongOIAmount, 
                    ShortOIQty, ShortOIAmount, 
                    OINetQty, OINetAmount
                    )
                    VALUES('{trade_date}', '{row[1]}', '{row[2]}',
                    {row[3]}, {row[4]},
                    {row[5]}, {row[6]}, 
                    {row[7]}, {row[8]}, 
                    {row[9]}, {row[10]}, 
                    {row[11]}, {row[12]},
                    {row[13]},{row[14]});"""
                    cursor.execute(cmd)

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
# test = legal_tx()
# r = test.get_and_save()
# print(r)
