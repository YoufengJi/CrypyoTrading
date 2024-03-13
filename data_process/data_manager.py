import pandas as pd
import os


class DataManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DataManager, cls).__new__(cls, *args, **kwargs)
            cls._instance.coin_data = {}
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'is_initialized'):
            self.is_initialized = True

    def initialize_indicators(self, coin, interval, historical_data, short_window=12, long_window=26, signal_window=9):
        symbol = coin + interval
        self.coin_data[symbol] = historical_data.copy()
        self.coin_data[symbol]['date'] = pd.to_datetime(self.coin_data[symbol]['timestamp'], unit='ms').dt.strftime(
            '%Y-%m-%d %H:%M:%S')

        # EMA,MACD
        self.coin_data[symbol]['short_ema'] = self.coin_data[symbol]['close'].ewm(span=short_window,
                                                                                  adjust=False).mean()
        self.coin_data[symbol]['long_ema'] = self.coin_data[symbol]['close'].ewm(span=long_window, adjust=False).mean()
        self.coin_data[symbol]['macd'] = self.coin_data[symbol]['short_ema'] - self.coin_data[symbol]['long_ema']
        self.coin_data[symbol]['signal_line'] = self.coin_data[symbol]['macd'].ewm(span=signal_window,
                                                                                   adjust=False).mean()
        self.coin_data[symbol]['histogram'] = self.coin_data[symbol]['macd'] - self.coin_data[symbol]['signal_line']

        # RSI
        delta = self.coin_data[symbol]['close'].diff()
        self.coin_data[symbol]['gain'] = (delta.where(delta > 0, 0)).ewm(span=14, adjust=False).mean()
        self.coin_data[symbol]['loss'] = (-delta.where(delta < 0, 0)).ewm(span=14, adjust=False).mean()
        rs = self.coin_data[symbol]['gain'] / self.coin_data[symbol]['loss']
        self.coin_data[symbol]['rsi'] = 100 - (100 / (1 + rs))

        # Bollinger Bands
        self.coin_data[symbol]['20sma'] = self.coin_data[symbol]['close'].rolling(window=20).mean()
        self.coin_data[symbol]['20std'] = self.coin_data[symbol]['close'].rolling(window=20).std()
        self.coin_data[symbol]['upper_band'] = self.coin_data[symbol]['20sma'] + (self.coin_data[symbol]['20std'] * 2)
        self.coin_data[symbol]['lower_band'] = self.coin_data[symbol]['20sma'] - (self.coin_data[symbol]['20std'] * 2)

        # MA
        self.coin_data[symbol]['5ma'] = self.coin_data[symbol]['close'].rolling(window=5).mean()
        self.coin_data[symbol]['10ma'] = self.coin_data[symbol]['close'].rolling(window=10).mean()
        self.coin_data[symbol]['20ma'] = self.coin_data[symbol]['close'].rolling(window=20).mean()

        # ADX
        self.coin_data[symbol]['tr'] = self.coin_data[symbol].apply(
            lambda row: max(
                abs(row['high'] - row['low']),
                abs(row['high'] - self.coin_data[symbol]['close'].shift(1)[row.name]),  # 使用shift(1)的结果
                abs(row['low'] - self.coin_data[symbol]['close'].shift(1)[row.name])
            ), axis=1)
        self.coin_data[symbol]['atr'] = self.coin_data[symbol]['tr'].rolling(window=14).mean()
        self.coin_data[symbol]['plus_dm'] = self.coin_data[symbol]['high'].diff()
        self.coin_data[symbol]['minus_dm'] = self.coin_data[symbol]['low'].diff(-1)
        self.coin_data[symbol]['plus_dm'] = self.coin_data[symbol]['plus_dm'].apply(lambda x: max(x, 0))
        self.coin_data[symbol]['minus_dm'] = self.coin_data[symbol]['minus_dm'].apply(lambda x: max(x, 0))
        self.coin_data[symbol]['plus_di'] = 100 * (
                self.coin_data[symbol]['plus_dm'].rolling(window=14).mean() / self.coin_data[symbol]['atr'])
        self.coin_data[symbol]['minus_di'] = 100 * (
                self.coin_data[symbol]['minus_dm'].rolling(window=14).mean() / self.coin_data[symbol]['atr'])
        di_diff = abs(self.coin_data[symbol]['plus_di'] - self.coin_data[symbol]['minus_di'])
        di_sum = self.coin_data[symbol]['plus_di'] + self.coin_data[symbol]['minus_di']
        self.coin_data[symbol]['adx'] = 100 * (di_diff / di_sum).rolling(window=14).mean()

        # put date as the first column
        columns = [col for col in self.coin_data[symbol].columns if col not in ['date']]
        columns = ['date'] + columns
        self.coin_data[symbol] = self.coin_data[symbol][columns]

        # write to file
        if not os.path.exists(f'data/indicators/{coin}'):
            os.makedirs(f'data/indicators/{coin}')
        with open(f'data/indicators/{coin}/{interval}.csv', 'w') as f:
            self.coin_data[symbol].to_csv(f, header=True, index=False)

        # keep 50 rows in memory
        self.coin_data[symbol] = self.coin_data[symbol].tail(50)

    def update_indicators(self, coin, interval, new_info, short_window=12, long_window=26, signal_window=9):
        symbol = coin + interval
        new_info.loc[:, 'timestamp'] = new_info['timestamp'].astype('int64')
        new_info['date'] = pd.to_datetime(new_info['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')

        # add to coin_data
        self.coin_data[symbol] = self.coin_data[symbol]._append(new_info, ignore_index=True)

        # update last row MACD
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'short_ema'] = \
            self.coin_data[symbol]['close'].ewm(span=short_window, adjust=False).mean().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'long_ema'] = \
            self.coin_data[symbol]['close'].ewm(span=long_window, adjust=False).mean().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'macd'] = \
            self.coin_data[symbol]['short_ema'].iloc[-1] - self.coin_data[symbol]['long_ema'].iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'signal_line'] = \
            self.coin_data[symbol]['macd'].ewm(span=signal_window, adjust=False).mean().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'histogram'] = \
            self.coin_data[symbol]['macd'].iloc[-1] - self.coin_data[symbol]['signal_line'].iloc[-1]

        # update last row RSI
        delta = self.coin_data[symbol]['close'].diff()
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'gain'] = \
            (delta.where(delta > 0, 0)).ewm(span=14, adjust=False).mean().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'loss'] = \
            (-delta.where(delta < 0, 0)).ewm(span=14, adjust=False).mean().iloc[-1]
        rs = self.coin_data[symbol]['gain'].iloc[-1] / self.coin_data[symbol]['loss'].iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'rsi'] = 100 - (100 / (1 + rs))

        # update last row Bollinger Bands
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], '20sma'] = \
            self.coin_data[symbol]['close'].rolling(window=20).mean().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], '20std'] = \
            self.coin_data[symbol]['close'].rolling(window=20).std().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'upper_band'] = \
            self.coin_data[symbol]['20sma'].iloc[-1] + (self.coin_data[symbol]['20std'].iloc[-1] * 2)
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], 'lower_band'] = \
            self.coin_data[symbol]['20sma'].iloc[-1] - (self.coin_data[symbol]['20std'].iloc[-1] * 2)

        # update last row MA
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], '5ma'] = \
            self.coin_data[symbol]['close'].rolling(window=5).mean().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], '10ma'] = \
            self.coin_data[symbol]['close'].rolling(window=10).mean().iloc[-1]
        self.coin_data[symbol].loc[self.coin_data[symbol].index[-1], '20ma'] = \
            self.coin_data[symbol]['close'].rolling(window=20).mean().iloc[-1]

        # update last row ADX
        # calculate TR
        tr = max(abs(new_info['high'].iloc[0] - new_info['low'].iloc[0]),
                 abs(new_info['high'].iloc[0] - self.coin_data[symbol]['close'].iloc[-2]),
                 abs(new_info['low'].iloc[0] - self.coin_data[symbol]['close'].iloc[-2]))

        self.coin_data[symbol].at[self.coin_data[symbol].index[-1], 'tr'] = tr
        # calculate ATR
        atr = self.coin_data[symbol]['tr'].rolling(window=14).mean().iloc[-1]
        # update ATR
        self.coin_data[symbol].at[self.coin_data[symbol].index[-1], 'atr'] = atr
        # calculate +DM and -DM
        plus_dm = max(new_info['high'].iloc[0] - self.coin_data[symbol]['high'].iloc[-2], 0)
        minus_dm = max(self.coin_data[symbol]['low'].iloc[-2] - new_info['low'].iloc[0], 0)
        # update +DM and -DM
        self.coin_data[symbol].at[self.coin_data[symbol].index[-1], 'plus_dm'] = plus_dm
        self.coin_data[symbol].at[self.coin_data[symbol].index[-1], 'minus_dm'] = minus_dm
        # update +DI and -DI
        plus_di = 100 * (self.coin_data[symbol]['plus_dm'].rolling(window=14).mean().iloc[-1] / atr)
        minus_di = 100 * (self.coin_data[symbol]['minus_dm'].rolling(window=14).mean().iloc[-1] / atr)
        self.coin_data[symbol].at[self.coin_data[symbol].index[-1], 'plus_di'] = plus_di
        self.coin_data[symbol].at[self.coin_data[symbol].index[-1], 'minus_di'] = minus_di
        # update ADX
        adx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        self.coin_data[symbol].at[self.coin_data[symbol].index[-1], 'adx'] = adx

        # put date as the first column
        columns = [col for col in self.coin_data[symbol].columns if col not in ['date']]
        columns = ['date'] + columns
        self.coin_data[symbol] = self.coin_data[symbol][columns]

        # write to file
        with open(f'data/indicators/{coin}/{interval}.csv', 'a') as f:
            self.coin_data[symbol].tail(1).to_csv(f, header=False, index=False)

        self.coin_data[symbol] = self.coin_data[symbol].tail(50)

    def get_latest_data(self, symbol):
        return self.coin_data[symbol].tail(1)
