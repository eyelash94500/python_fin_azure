from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas

import db_connect


class fin_signal:
    def __init__(self):
        self.df_taiex = None
        self.df_legal = None
        self.df_stock = None
        self.df_legal_tx = None
        self.df_future = None

    def _get_db_data(self):
        # 建立與MySQL的連線
        my_connt_obj = db_connect.mysql_connect()
        conn = my_connt_obj.connect()

        # 概算一年前的日期
        date_year = datetime.today() - relativedelta(weeks=52)
        date_year_str = date_year.strftime("%Y-%m-%d")
        # 加權指數資料
        sql_script = (
            f"SELECT * FROM StockTransactionInfo WHERE TradeDate > '{date_year_str}'"
        )
        self.df_taiex = pandas.read_sql(sql_script, con=conn)

        # 取得三大法人期選的資料
        sql_script = (
            f"SELECT * FROM LegalDailyFutureOption WHERE TradeDate > '{date_year_str}'"
        )
        self.df_legal = pandas.read_sql(sql_script, con=conn)

        # 取得股價資訊
        sql_script = f"SELECT * FROM DailyPrice WHERE TradeDate > '{date_year_str}'"
        self.df_stock = pandas.read_sql(sql_script, con=conn)

        # 取得臺指資訊
        sql_script = f"SELECT * FROM LegalFuture WHERE TradeDate > '{date_year_str}'"
        self.df_legal_tx = pandas.read_sql(sql_script, con=conn)

        # 取得期貨交易資訊
        sql_script = (
            f"SELECT * FROM FutureDailyReport WHERE TradeDate > '{date_year_str}'"
        )
        self.df_future = pandas.read_sql(sql_script, con=conn)

        # 關閉連線
        conn.close()

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (
            self._get_stock_ma_accross(),
            self._get_taiex(),
            self._get_taiex_1(),
            self._get_taiex_2(),
            self._get_taiex_3(),
            self._get_taiex_4(),
            self._get_signal_1(),
            self._get_signal_2(),
            self._get_signal_3_1(),
            self._get_signal_4_1(),
            self._get_signal_5_1(),
            self._get_signal_6_1(),
            self._get_signal_7_1(),
            self._get_signal_8_1(),
            self._get_signal_3_2(),
            self._get_signal_4_2(),
            self._get_signal_5_2(),
            self._get_signal_6_2(),
            self._get_signal_7_2(),
            self._get_signal_8_2(),
            self._get_signal_3_3(),
            self._get_signal_4_3(),
            self._get_signal_5_3(),
            self._get_signal_6_3(),
            self._get_signal_7_3(),
            self._get_signal_8_3(),
            self._get_signal_2330(),
            self._get_signal_9_1(),
            self._get_signal_10_1(),
            self._get_signal_9_2(),
            self._get_signal_10_2(),
            self._get_signal_9_3(),
            self._get_signal_10_3(),
            self._get_signal_11(),
            self._get_signal_12(),
        )

        return r

    def _get_stock_ma_accross(self):
        """stock_ma_accross: 股票 5 SMA 穿過 20 SMA
        當股票為 5 SMA 穿過 20 SMA 的時候列出該股票

        Return:
            list: 股票陣列
        """

        # 取得不重複的商品代碼
        stocks = self.df_stock["Symbol"].unique()

        # 要回傳的 list，紀錄訊號燈結果
        r_list = []
        # 執行檢查各股票是否 5 SMA > 20 SMA
        for stock in stocks:
            # 取得該股票資料
            df_stock = self.df_stock.loc[self.df_stock["Symbol"] == stock]

            # 製作5日移動平均數
            close_price_5 = df_stock["ClosePrice"].rolling(5, min_periods=1).mean()

            # 製作20日移動平均數
            close_price_20 = df_stock["ClosePrice"].rolling(20, min_periods=1).mean()

            r = True if close_price_5.iloc[-1] > close_price_20.iloc[-1] else False

            if r:
                r_list.append(stock)

        return ("5 SMA > 20 SMA的股票", r_list)

    def _get_taiex(self):
        """taiex: 最後一個交易日的加權指數"""

        r = self.df_taiex[-1:]

        return (f"交易日－{r['TradeDate'].iloc[0]} 的加權指數", r["Taiex"].iloc[0])

    def _get_taiex_1(self):
        """taiex_1: 取得近五筆加權指數收盤價，最高的值"""

        last_5_data = self.df_taiex[-5:]
        r = last_5_data["Taiex"].max()

        return (f"近期加權指數最高為", r)

    def _get_taiex_2(self):
        """taiex_2: 近期收盤價最高的值與最後交易日的差異數"""

        last_5_data = self.df_taiex[-5:]

        # last_5_data["Taiex"].max()  = 18338.05
        # last_5_data["Taiex"].iloc[-1]= 18310.94
        # r = 27.110000000000582
        # r = last_5_data["Taiex"].max()-last_5_data["Taiex"].iloc[-1]
        r = (
            round(last_5_data["Taiex"].max() * 100)
            - round(last_5_data["Taiex"].iloc[-1] * 100)
        ) / 100

        return (f"近期加權指數最高與目前的差", r)

    def _get_taiex_3(self):
        """taiex_3: 取得近五筆加權指數收盤價，最低的值"""

        last_5_data = self.df_taiex[-5:]
        r = last_5_data["Taiex"].min()

        return (f"近期加權指數最低為", r)

    def _get_taiex_4(self):
        """taiex_4: 近期收盤價最低的值與最後交易日的差異數"""

        last_5_data = self.df_taiex[-5:]
        r = (
            round(last_5_data["Taiex"].min() * 100)
            - round(last_5_data["Taiex"].iloc[-1] * 100)
        ) / 100

        return (f"近期加權指數最低與目前的差", r)

    def _get_signal_1(self):
        """signal_1: 最近交易日，是近期六天最大的交易金額
        最近的一個交易日，比前五日的最大的金額更大
        """

        # 取得最近六、五筆資訊
        last_6_data = self.df_taiex[-6:]
        last_5_data = last_6_data["TranscationAmount"][0:-1]

        r = (
            True
            if last_6_data["TranscationAmount"].iloc[-1] > last_5_data.max()
            else False
        )

        return ("最近交易日是近期六天最大的交易金額", r)

    def _get_signal_2(self):
        """signal_2: 顯示交易是否熱絡
        最近的一個交易日，比前五日的平均金額更大

        Return:
            bool: True表示熱絡，False表示不熱絡
        """

        # 取得最近六、五筆資訊
        last_6_data = self.df_taiex[-6:]
        last_5_data = last_6_data["TranscationAmount"][0:-1]

        r = (
            True
            if last_6_data["TranscationAmount"].iloc[-1] > last_5_data.mean()
            else False
        )

        return ("最近的一個交易日比前五日平均金額更大", r)

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

    def _get_signal_2330(self) -> tuple:
        """signal_2330: 5 SMA > 20 SMA
        近期 5 日平均比 20 日還要高

        Return:
            bool: True:高；False：低
        """

        # 取得台積電資料，代號：2330
        df_2330 = self.df_stock.loc[self.df_stock["Symbol"] == "2330"]

        # 製作5日移動平均數
        close_price_5 = df_2330["ClosePrice"].rolling(5, min_periods=1).mean()

        # 製作20日移動平均數
        close_price_20 = df_2330["ClosePrice"].rolling(20, min_periods=1).mean()

        r = True if close_price_5.iloc[-1] > close_price_20.iloc[-1] else False

        return ("台積電是否 5 SMA > 20 SMA", r)

    def _get_signal_9_1(self) -> tuple:
        """signal_9_1: 外資臺指留倉多空單
        最近一次交易日資料做比較

        Return:
            bool: True:多單；False：空單
        """

        # 取得外資資料
        df_investment_group = self.df_legal_tx.loc[
            self.df_legal_tx["TradeGroup"] == "外資及陸資"
        ]
        df_investment = df_investment_group.loc[
            df_investment_group["SecurityName"] == "臺股期貨"
        ]

        # 取得最後一筆資料
        last_investment = df_investment["OINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return ("外資臺指期貨留倉是多單", r)

    def _get_signal_10_1(self) -> tuple:
        """signal_10_1: 外資臺指留倉數變動比例
        最近一次交易日與前一次的臺指期貨留倉數量做相減

        Return:
            float: 顯示小數點
        """

        # 取得外資資料
        df_investment_group = self.df_legal_tx.loc[
            self.df_legal_tx["TradeGroup"] == "外資及陸資"
        ]
        df_investment = df_investment_group.loc[
            df_investment_group["SecurityName"] == "臺股期貨"
        ]

        if len(df_investment["OINetQty"]) >= 2:
            # 倒數兩筆
            last_2 = df_investment["OINetQty"][-2:]

            # 變動比率
            r = (last_2.iloc[1] - last_2.iloc[0]) / last_2.iloc[0]
        else:
            r = 0

        return ("外資臺指期貨留倉數變動率", r)

    def _get_signal_9_2(self) -> tuple:
        """signal_9_2: 自營商臺指留倉多空單
        最近一次交易日資料做比較

        Return:
            bool: True:多單；False：空單
        """

        # 取得自營商資料
        df_investment_group = self.df_legal_tx.loc[
            self.df_legal_tx["TradeGroup"] == "自營商"
        ]
        df_investment = df_investment_group.loc[
            df_investment_group["SecurityName"] == "臺股期貨"
        ]

        # 取得最後一筆資料
        last_investment = df_investment["OINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return ("自營商臺指期貨留倉是多單", r)

    def _get_signal_10_2(self) -> tuple:
        """signal_10_1: 自營商臺指留倉數變動比例
        最近一次交易日與前一次的臺指期貨留倉數量做相減

        Return:
            float: 顯示小數點
        """

        # 取得自營商資料
        df_investment_group = self.df_legal_tx.loc[
            self.df_legal_tx["TradeGroup"] == "自營商"
        ]
        df_investment = df_investment_group.loc[
            df_investment_group["SecurityName"] == "臺股期貨"
        ]

        if len(df_investment["OINetQty"]) >= 2:
            # 倒數兩筆
            last_2 = df_investment["OINetQty"][-2:]

            # 變動比率
            r = (last_2.iloc[1] - last_2.iloc[0]) / last_2.iloc[0]
        else:
            r = 0

        return ("自營商臺指期貨留倉數變動率", r)

    def _get_signal_9_3(self) -> tuple:
        """signal_9_2: 投信臺指留倉多空單
        最近一次交易日資料做比較

        Return:
            bool: True:多單；False：空單
        """

        # 取得投信資料
        df_investment_group = self.df_legal_tx.loc[
            self.df_legal_tx["TradeGroup"] == "投信"
        ]
        df_investment = df_investment_group.loc[
            df_investment_group["SecurityName"] == "臺股期貨"
        ]

        # 取得最後一筆資料
        last_investment = df_investment["OINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return ("投信臺指期貨留倉是多單", r)

    def _get_signal_10_3(self) -> tuple:
        """signal_10_1: 投信臺指留倉數變動比例
        最近一次交易日與前一次的臺指期貨留倉數量做相減

        Return:
            float: 顯示小數點
        """

        # 取得投信資料
        df_investment_group = self.df_legal_tx.loc[
            self.df_legal_tx["TradeGroup"] == "投信"
        ]
        df_investment = df_investment_group.loc[
            df_investment_group["SecurityName"] == "臺股期貨"
        ]

        if len(df_investment["OINetQty"]) >= 2:
            # 倒數兩筆
            last_2 = df_investment["OINetQty"][-2:]

            # 變動比率
            r = (last_2.iloc[1] - last_2.iloc[0]) / last_2.iloc[0]
        else:
            r = 0

        return ("投信臺指期貨留倉數變動率", r)

    def _get_signal_11(self) -> tuple:
        """signal_11: 小臺未平倉數
        取得所有小臺的為平倉數量，包含跨月單

        Return:
            float: 顯示小數點
        """
        # 取得最新的交易日
        last_trade_date = self.df_future["TradeDate"].max()

        # 取得小臺留倉總數，代碼為MTX
        df_mtx = self.df_future.loc[self.df_future["Contract"] == "MTX"]
        df_mtx_last_date = df_mtx.loc[df_mtx["TradeDate"] == last_trade_date]
        r = 0
        for qty in df_mtx_last_date["OpenInterest"]:
            r = r + (qty if pandas.notnull(qty) else 0)

        return ("小臺總留倉數", r)

    def _get_signal_12(self) -> tuple:
        """signal_12: 小臺散戶指數
        為運用總小臺指未平倉數減去三大法人小臺指留倉數之差，除以總小臺未平倉數的比例。

        Return:
            float: 顯示小數點
        """
        # 取得最新的交易日
        last_trade_date = self.df_legal_tx["TradeDate"].max()

        # 取得三大法人小臺留倉數
        df_legal_mtx = self.df_legal_tx.loc[
            self.df_legal_tx["SecurityName"] == "小型臺指期貨"
        ]
        df_legal_mtx_last_date = df_legal_mtx.loc[
            df_legal_mtx["TradeDate"] == last_trade_date
        ]
        legal_total = 0
        for qty in df_legal_mtx_last_date["OINetQty"]:
            legal_total = legal_total + qty

        # 取得小臺留倉總數，代碼為MTX
        df_mtx = self.df_future.loc[self.df_future["Contract"] == "MTX"]
        df_mtx_last_date = df_mtx.loc[df_mtx["TradeDate"] == last_trade_date]
        mtx_total = 0
        for qty in df_mtx_last_date["OpenInterest"]:
            mtx_total = mtx_total + (qty if pandas.notnull(qty) else 0)

        if mtx_total == 0 or mtx_total is None:
            r = 0
        else:
            r = float(mtx_total - legal_total) / mtx_total

        return ("小臺散戶指數", r)


""""實作測試"""
signal = fin_signal()
signal._get_db_data()
# # signal.show_data()
# # r = signal.get_signal()
r = signal._get_stock_ma_accross()

print(r)
