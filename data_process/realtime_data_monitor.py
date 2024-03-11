import threading
import datetime
from . import data_processing
from config import config_manager
from .data_manager import DataManager
from strategy import analysis

configManager = config_manager.ConfigManager()
dataManager = DataManager()
interval2seconds = configManager.get_config("timing")


class RealTimeDataMonitor:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            cls._instance.__init__()
        return cls._instance

    def __init__(self):

        if not hasattr(self, 'coin_data'):
            self.update_time = {}  # symbol last updated time

    def update_price(self, coin, interval, time):
        '''
        update the price data and indicators
        reset the timer to start the next update

        :param coin: coin name
        :param interval: update interval
        :param time: the time of the update

        :return: void
        '''
        new_data = data_processing.get_specific_time_kline(coin, interval, time)
        symbol = coin + interval
        dataManager.update_indicators(coin, interval, new_data)
        if interval == '1m':
            analysis.analyze(coin, interval)
        self.update_time[symbol] = time
        self.set_timer(coin, interval)

    def set_timer(self, coin, interval):
        '''
        set the timer for the next update

        :param coin: coin name
        :param interval: update interval
        :return: void
        '''
        # calculate next update time
        update_interval = interval2seconds[interval]
        symbol = coin + interval
        last_update_time = self.update_time[symbol] / 1000  # convert to seconds
        update_time = last_update_time + update_interval
        now_time = datetime.datetime.now().timestamp()
        next_update_in = update_time - now_time

        if next_update_in < 0:
            next_update_in = 0

        # set timer(add extra 3 seconds to ensure the next update is after the update time)
        timer = threading.Timer(next_update_in + 3, lambda: self.update_price(coin, interval, int(update_time * 1000)))
        print(f"Next update for {symbol} at {update_time}")
        timer.start()

    def add_coin(self, coin, interval, last_update_time):
        symbol = coin + interval
        self.update_time[symbol] = last_update_time
        self.set_timer(coin, interval)
