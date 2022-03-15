from datetime import datetime
from future_report import future_report
from legal_person import legal_daily_future_option
from legal_person_tx import legal_tx
from market_info import daily_market_info
from stock_transaction import stock_transaction
from fin_signal import fin_signal


class daily_transaction:
    """Save the daily transaction info to database."""

    def save_to_db(self):
        print(f"=Start: {datetime.now().strftime('%H%M%S')}")
        legal_daily = legal_daily_future_option()
        market_data = daily_market_info()
        stock_trans_data = stock_transaction()
        legal_daily_tx = legal_tx()
        future_report_data = future_report()

        r = legal_daily.get_and_save()
        if r is False:
            print(f"error:{legal_daily.__str__}")

        r = market_data.get_and_save()
        if r is False:
            print(f"error:{market_data.__str__}")

        r = stock_trans_data.get_and_save()
        if r is False:
            print(f"error:{stock_trans_data.__str__}")

        r = legal_daily_tx.get_and_save()
        if r is False:
            print(f"error:{legal_daily_tx.__str__}")

        r = future_report_data.get_and_save()
        if r is False:
            print(f"error:{future_report_data.__str__}")

        print(f"=Finish: {datetime.now().strftime('%H%M%S')}")


class signal:
    """Show the signal"""

    def __init__(self) -> None:
        self.fin_signal = fin_signal()
        self.fin_signal._get_db_data()  # get the row data

    def show_signal(self) -> None:
        """Create and show all the signal"""

        signal_list = self.fin_signal.get_signal()
        self._show_singnal(signal_list)

    def _show_singnal(self, signal_list) -> None:
        """Show the signal function"""

        print(f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("＝＝訊號燈＝＝")
        for title, signal in signal_list:
            print(f"* {title}: {signal}")


"""實作測試"""
# 匯入資料
worker = daily_transaction()
worker.save_to_db()

# 訊號燈印出
signal = signal()
signal.show_signal()
