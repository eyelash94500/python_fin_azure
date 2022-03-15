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

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (self._get_stock_ma_accross(),)
        return r


obj = fin_signal()
obj._get_db_data()
signal_list = obj.get_signal()
for title, signal in signal_list:
    print(f"* {title}: {signal}")
