from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas
import db_connect


class fin_signal:
    def __init__(self):
        self.df_stock = None

    def _get_db_data(self):
        # 建立與MySQL的連線
        my_connt_obj = db_connect.mysql_connect()
        conn = my_connt_obj.connect()
        # 概算一年前的日期
        date_year = datetime.today() - relativedelta(weeks=52)
        date_year_str = date_year.strftime("%Y-%m-%d")
        # 取得股價資訊
        sql_script = f"SELECT * FROM DailyPrice WHERE TradeDate > '{date_year_str}'"
        self.df_stock = pandas.read_sql(sql_script, con=conn)

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

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (self._get_signal_2330(),)
        return r


obj = fin_signal()
obj._get_db_data()
signal_list = obj.get_signal()
for title, signal in signal_list:
    print(f"* {title}: {signal}")
