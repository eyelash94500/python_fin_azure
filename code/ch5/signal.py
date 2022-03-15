from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas
import pymysql


class fin_signal:
    def __init__(self):
        self.df_taiex = None

    def _get_db_data(self):
        # 建立與MySQL的連線
        self.db_settings = {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "__user__",
            "password": "__pwd",
            "db": "finance",
        }
        conn = pymysql.connect(**self.db_settings)
        # 概算一年前的日期
        date_year = datetime.today() - relativedelta(weeks=52)
        date_year_str = date_year.strftime("%Y-%m-%d")
        # 加權指數資料
        sql_script = (
            f"SELECT * FROM StockTransactionInfo WHERE TradeDate > '{date_year_str}'"
        )
        self.df_taiex = pandas.read_sql(sql_script, con=conn)

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

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (
            self._get_taiex(),
            self._get_taiex_1(),
            self._get_taiex_2(),
            self._get_taiex_3(),
            self._get_taiex_4(),
        )
        return r


obj = fin_signal()
obj._get_db_data()
signal_list = obj.get_signal()
for title, signal in signal_list:
    print(f"* {title}: {signal}")
