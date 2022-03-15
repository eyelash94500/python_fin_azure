import pandas
from talib import abstract

import db_connect


class trade_strategy:
    def __init__(self, stock_code="2330"):
        self.df_stock = None

        # 開始就取得股價資料
        self._get_db_data(stock_code)

    def _get_db_data(self, stock_code="2330"):
        # 建立與MySQL的連線
        my_connt_obj = db_connect.mysql_connect()
        conn = my_connt_obj.connect()

        # 取得股價資訊
        sql_script = f"SELECT * FROM DailyPrice WHERE Symbol = '{stock_code}' ORDER BY TradeDate;"
        self.df_stock = pandas.read_sql(sql_script, con=conn)
        self.df_stock = self.df_stock.set_index("TradeDate")

        # 關閉連線
        conn.close()

    def do_strategy(self):
        df = self.df_stock.rename(columns={"ClosePrice": "close"})

        # abstract.RSI(df).plot()  # for test

        df_rsi = abstract.RSI(df)

        stock_inventory = 0  # 記錄庫存
        profit = 0  # 總利潤
        trade_his = []  # 交易歷史記錄，格式為：(Trade date, trade type, price, RSI)

        for trade_date, rsi_value in df_rsi.iteritems():
            if pandas.isnull(rsi_value):
                continue

            if rsi_value < 35 and stock_inventory == 0:
                stock_inventory = 1
                price = self.df_stock.loc[self.df_stock.index == trade_date][
                    "ClosePrice"
                ][0]

                trade_his.append(
                    (trade_date.strftime("%Y-%m-%d"), "B", price, rsi_value)
                )
            elif rsi_value > 65 and stock_inventory == 1:
                stock_inventory = 0
                price = self.df_stock.loc[self.df_stock.index == trade_date][
                    "ClosePrice"
                ][0]

                this_profit = price - trade_his[-1][2]
                profit = profit + this_profit

                trade_his.append(
                    (trade_date.strftime("%Y-%m-%d"), "S", price, rsi_value)
                )

        print(f"=>trade times:{len(trade_his)}, profit:{profit}")
        print(trade_his)


"""===執行策略==="""
a = trade_strategy("2603")  # 長榮:2603
a.do_strategy()
