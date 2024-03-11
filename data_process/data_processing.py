import os
import pandas as pd
from config import config_manager
from binance.spot import Spot
from .realtime_data_monitor import RealTimeDataMonitor
from .data_manager import DataManager

# Get the realtime data monitor instance
realtime_monitor = RealTimeDataMonitor.get_instance()
# Get the config
config = config_manager.ConfigManager()
coin_list = config.get_config("coin_list")
intervals = config.get_config("intervals")
client = Spot()


def check_data_folder(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    return 1


def check_data_file(file_path):
    if not os.path.exists(file_path):
        return False
    return True


def init_indicator_data(coin_list, intervals):
    data_manager = DataManager()
    for coin in coin_list:
        for interval, ideal_data_count in intervals.items():
            # get two times the ideal data count to ensure we have enough data to initialize and update
            total_data = client.klines(coin, interval, limit=ideal_data_count * 2)

            # split the data into initial and recent data
            initial_data = total_data[:ideal_data_count]
            recent_data = total_data[ideal_data_count:]

            initial_data_dataset = pd.DataFrame(initial_data,
                                                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                                         'close_time',
                                                         'quote_asset_volume', 'number_of_trades',
                                                         'taker_buy_base_asset_volume',
                                                         'taker_buy_quote_asset_volume', 'ignore'])

            recent_data_dataset = pd.DataFrame(recent_data,
                                               columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                                        'close_time',
                                                        'quote_asset_volume', 'number_of_trades',
                                                        'taker_buy_base_asset_volume',
                                                        'taker_buy_quote_asset_volume', 'ignore'])
            # numerical conversion
            initial_data_dataset = initial_data_dataset.apply(pd.to_numeric)
            recent_data_dataset = recent_data_dataset.apply(pd.to_numeric)

            # init the indicators
            data_manager.initialize_indicators(coin, interval, initial_data_dataset)

            # update the indicators
            for index, row in recent_data_dataset.iterrows():
                row_df = pd.DataFrame(row).transpose()
                data_manager.update_indicators(coin, interval, row_df)
                # set the last update time
                if index == len(recent_data_dataset) - 1:
                    realtime_monitor.add_coin(coin, interval, row['timestamp'])
                    print(f"{coin} {interval} start to update from {row['timestamp']}")
    return


def process_depth_data():
    for coin in coin_list:
        depth_data = client.depth(coin)
        bids = pd.DataFrame(depth_data["bids"], columns=["Price", "Quantity"])
        asks = pd.DataFrame(depth_data["asks"], columns=["Price", "Quantity"])

    return 1


def get_specific_time_kline(coin, interval, time):
    kline = client.klines(coin, interval, limit=1, endTime=time)
    dataset = pd.DataFrame(kline,
                           columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                    'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                                    'taker_buy_quote_asset_volume', 'ignore'])
    dataset = dataset.apply(pd.to_numeric)
    return dataset


def process_data():
    init_indicator_data(coin_list, intervals)
    # process_depth_data()
    return 1
