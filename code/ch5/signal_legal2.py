from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas
import db_connect


class fin_signal:
    def __init__(self):
        self.df_legal = None

    def _get_db_data(self):
        # 建立與MySQL的連線
        my_connt_obj = db_connect.mysql_connect()
        conn = my_connt_obj.connect()
        # 概算一年前的日期
        date_year = datetime.today() - relativedelta(weeks=52)
        sql_script = (
            f"SELECT * FROM LegalDailyFutureOption WHERE TradeDate > '{date_year}'"
        )
        self.df_legal = pandas.read_sql(sql_script, con=conn)

    def _get_signal_7_1(self) -> tuple:
        """signal_7_1: 外資期貨近年最多留倉數
        近一年最多留倉數，有可能是空單

        Return:
            int: 留倉數量
        """

        # 取得外資資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 取得留倉數
        last_2 = df_investment["FutureOINetQty"]

        # 取得最大數
        r = last_2.max()

        return ("外資期貨近年最多留倉數", r)

    def _get_signal_8_1(self) -> tuple:
        """signal_8_1: 外資期貨近年最少留倉數
        近一年最少留倉數，有可能是多單

        Return:
            int: 留倉數量
        """

        # 取得外資資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 取得留倉數
        last_2 = df_investment["FutureOINetQty"]

        # 取得最小數
        r = last_2.min()

        return ("外資期貨近年最少留倉數", r)

    def _get_signal_7_2(self) -> tuple:
        """signal_7_2: 自營商期貨近年最大留倉數
        近一年最多留倉數，有可能是空單

        Return:
            int: 留倉數量
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 取得留倉數
        last_2 = df_investment["FutureOINetQty"]

        # 取得最大數
        r = last_2.max()

        return ("自營商期貨近年最大留倉數", r)

    def _get_signal_8_2(self) -> tuple:
        """signal_8_2: 自營商期貨近年最少留倉數
        近一年最少留倉數，有可能是多單

        Return:
            int: 留倉數量
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 取得留倉數
        last_2 = df_investment["FutureOINetQty"]

        # 取得最小數
        r = last_2.min()

        return ("自營商期貨近年最少留倉數", r)

    def _get_signal_7_3(self) -> tuple:
        """signal_7_3: 投信期貨近年最大留倉數
        近一年最多留倉數，有可能是空單

        Return:
            int: 留倉數量
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 取得留倉數
        last_2 = df_investment["FutureOINetQty"]

        # 取得最大數
        r = last_2.max()

        return ("投信期貨近年最大留倉數", r)

    def _get_signal_8_3(self) -> tuple:
        """signal_8_1: 投信期貨近年最少留倉數
        近一年最少留倉數，有可能是多單

        Return:
            int: 留倉數量
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 取得留倉數
        last_2 = df_investment["FutureOINetQty"]

        # 取得最小數
        r = last_2.min()

        return ("投信期貨近年最少留倉數", r)

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (
            self._get_signal_7_1(),
            self._get_signal_8_1(),
            self._get_signal_7_2(),
            self._get_signal_8_2(),
            self._get_signal_7_3(),
            self._get_signal_8_3(),
        )
        return r


obj = fin_signal()
obj._get_db_data()
signal_list = obj.get_signal()
for title, signal in signal_list:
    print(f"* {title}: {signal}")
