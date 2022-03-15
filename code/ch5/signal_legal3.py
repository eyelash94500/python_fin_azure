from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas
import db_connect


class fin_signal:
    def __init__(self):
        self.df_legal_tx = None

    def _get_db_data(self):
        # 建立與MySQL的連線
        my_connt_obj = db_connect.mysql_connect()
        conn = my_connt_obj.connect()
        # 概算一年前的日期
        date_year = datetime.today() - relativedelta(weeks=52)
        date_year_str = date_year.strftime("%Y-%m-%d")
        # 取得臺指資訊
        sql_script = f"SELECT * FROM LegalFuture WHERE TradeDate > '{date_year_str}'"
        self.df_legal_tx = pandas.read_sql(sql_script, con=conn)

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

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (
            self._get_signal_9_1(),
            self._get_signal_10_1(),
            self._get_signal_9_2(),
            self._get_signal_10_2(),
            self._get_signal_9_3(),
            self._get_signal_10_3(),
        )
        return r


obj = fin_signal()
obj._get_db_data()
signal_list = obj.get_signal()
for title, signal in signal_list:
    print(f"* {title}: {signal}")
