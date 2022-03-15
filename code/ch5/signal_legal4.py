from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas
import db_connect


class fin_signal:
    def __init__(self):
        self.df_legal_tx = None
        self.df_future = None

    def _get_db_data(self):
        # 建立與MySQL的連線
        my_connt_obj = db_connect.mysql_connect()
        conn = my_connt_obj.connect()
        # 概算一年前的日期
        date_year = datetime.today() - relativedelta(weeks=52)
        date_year_str = date_year.strftime("%Y-%m-%d")
        # 取得期貨交易資訊
        sql_script = (
            f"SELECT * FROM FutureDailyReport WHERE TradeDate > '{date_year_str}'"
        )
        self.df_future = pandas.read_sql(sql_script, con=conn)

        # 取得臺指資訊
        sql_script = f"SELECT * FROM LegalFuture WHERE TradeDate > '{date_year_str}'"
        self.df_legal_tx = pandas.read_sql(sql_script, con=conn)

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

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (
            self._get_signal_11(),
            self._get_signal_12(),
        )
        return r


obj = fin_signal()
obj._get_db_data()
signal_list = obj.get_signal()
for title, signal in signal_list:
    print(f"* {title}: {signal}")
