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

    def _get_signal_3_1(self):
        """signal_3_1: 外資期貨留倉是否空單還是多單
        最近一次交易日資料做比較

        Return:
            bool: True:多單；False：空單
        """

        # 取得外資資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return ("外資期貨留倉是多單", r)

    def _get_signal_4_1(self):
        """signal_4_1: 外資期貨留倉數量是否增加
        最近一次交易日與前一次的期貨留倉數量做比較

        Return:
            bool: True:增加；False：減少
        """

        # 取得外資資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 比對
        r = True if abs(last_2.iloc[1]) >= abs(last_2.iloc[0]) else False

        return ("外資期貨留倉數量是否增加", r)

    def _get_signal_5_1(self) -> tuple:
        """signal_5_1: 外資期貨留倉數量變化量
        最近一次交易日與前一次的期貨留倉數量做相減

        Return:
            int: 顯示為變化數量
        """

        # 取得外資資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 後面天數減前面天數
        r = last_2.iloc[1] - last_2.iloc[0]

        return ("外資期貨留倉數量變化量", r)

    def _get_signal_6_1(self) -> tuple:
        """signal_6_1: 外資期貨留倉數變動比例
        最近一次交易日與前一次的期貨留倉數量做相減

        Return:
            float: 顯示小數點
        """

        # 取得外資資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 變動比率
        r = abs((last_2.iloc[1] - last_2.iloc[0]) / last_2.iloc[0])

        return ("外資期貨留倉數變動率", r)

    def _get_signal_3_2(self):
        """signal_3_2: 自營商期貨留倉是否空單還是多單
        最近一次交易日資料做比較

        Return:
            bool: True:多單；False：空單
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return ("自營商期貨留倉是多單", r)

    def _get_signal_4_2(self):
        """signal_4_2: 自營商期貨留倉數量是否增加
        最近一次交易日與前一次的期貨留倉數量做比較

        Return:
            bool: True:增加；False：減少
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 比對
        r = True if abs(last_2.iloc[1]) >= abs(last_2.iloc[0]) else False

        return ("自營商期貨留倉數量是否增加", r)

    def _get_signal_5_2(self) -> tuple:
        """signal_5_2: 自營商期貨留倉數量變化多少
        最近一次交易日與前一次的期貨留倉數量做相減

        Return:
            int: 顯示為變化數量
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 後面天數減前面天數
        r = last_2.iloc[1] - last_2.iloc[0]

        return ("自營商期貨留倉數量變化量", r)

    def _get_signal_6_2(self) -> tuple:
        """signal_6_2: 自營商期貨留倉數變動比例
        最近一次交易日與前一次的期貨留倉數量做相減

        Return:
            float: 顯示小數點
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 變動比率
        r = abs((last_2.iloc[1] - last_2.iloc[0]) / last_2.iloc[0])

        return ("自營商期貨留倉數量變化率", r)

    def _get_signal_3_3(self):
        """signal_3_3: 投信商期貨留倉是否空單還是多單
        最近一次交易日資料做比較

        Return:
            bool: True:多單；False：空單
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return ("投信商期貨留倉是多單", r)

    def _get_signal_4_3(self):
        """signal_4_3: 投信期貨留倉數量是否增加
        最近一次交易日與前一次的期貨留倉數量做比較

        Return:
            bool: True:增加；False：減少
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 比對
        r = True if abs(last_2.iloc[1]) >= abs(last_2.iloc[0]) else False

        return ("投信期貨留倉數量是否增加", r)

    def _get_signal_5_3(self) -> tuple:
        """signal_5_3: 投信期貨留倉數量變化多少
        最近一次交易日與前一次的期貨留倉數量做相減

        Return:
            int: 顯示為變化數量
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 後面天數減前面天數
        r = last_2.iloc[1] - last_2.iloc[0]

        return ("投信期貨留倉變化量", r)

    def _get_signal_6_3(self) -> tuple:
        """signal_6_3: 投信期貨留倉數變動比例
        最近一次交易日與前一次的期貨留倉數量做相減

        Return:
            float: 顯示小數點
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 倒數兩筆
        last_2 = df_investment["FutureOINetQty"][-2:]

        # 變動比率
        r = abs((last_2.iloc[1] - last_2.iloc[0]) / last_2.iloc[0])

        return ("投信期貨留倉數變動率", r)

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (
            self._get_signal_3_1(),
            self._get_signal_4_1(),
            self._get_signal_5_1(),
            self._get_signal_6_1(),
            self._get_signal_3_2(),
            self._get_signal_4_2(),
            self._get_signal_5_2(),
            self._get_signal_6_2(),
            self._get_signal_3_3(),
            self._get_signal_4_3(),
            self._get_signal_5_3(),
            self._get_signal_6_3(),
        )
        return r


obj = fin_signal()
obj._get_db_data()
signal_list = obj.get_signal()
for title, signal in signal_list:
    print(f"* {title}: {signal}")
