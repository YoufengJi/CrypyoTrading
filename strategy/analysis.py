import os
from datetime import datetime

from data_process import data_manager
from tools import speaker

data_getter = data_manager.DataManager()


def check_data_folder(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)


def calculate_signal_score(macd, macd_signal, price, ma, rsi, bollinger_upper, bollinger_lower, adx, plus_di, minus_di):
    score = 0

    score += 10 if macd > macd_signal else -10

    score += 10 if price > ma else -10

    if rsi < 30:
        score += 20
    elif rsi > 70:
        score -= 20

    if price < bollinger_lower:
        score += 20
    elif price > bollinger_upper:
        score -= 20

    if adx > 25:
        if plus_di > minus_di:
            score += 30
        elif minus_di > plus_di:
            score -= 30

    signal_score = max(min(score, 100), -100)

    return signal_score


def analyze_trend(coin):
    latest_data = data_getter.get_latest_data(coin + '4h')
    macd = latest_data['macd'].iloc[-1]
    macd_signal = latest_data['signal_line'].iloc[-1]
    price = latest_data['close'].iloc[-1]
    ma = latest_data['20ma'].iloc[-1]
    rsi = latest_data['rsi'].iloc[-1]
    bollinger_upper = latest_data['upper_band'].iloc[-1]
    bollinger_lower = latest_data['lower_band'].iloc[-1]
    adx = latest_data['adx'].iloc[-1]
    plus_di = latest_data['plus_di'].iloc[-1]
    minus_di = latest_data['minus_di'].iloc[-1]
    score = calculate_signal_score(macd, macd_signal, price, ma, rsi, bollinger_upper, bollinger_lower, adx, plus_di,
                                   minus_di)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    check_data_folder(f'data/strategy/{coin}/4h')
    with open(f'data/strategy/{coin}/4h/signal_score_log.txt', 'a') as f:
        f.write(f"{current_time} - {coin} 4h signal score: {score}\n")
    if score > 50:
        return True


def confirm_trend(coin):
    latest_data = data_getter.get_latest_data(coin + '1h')
    macd = latest_data['macd'].iloc[-1]
    macd_signal = latest_data['signal_line'].iloc[-1]
    price = latest_data['close'].iloc[-1]
    ma = latest_data['20ma'].iloc[-1]
    rsi = latest_data['rsi'].iloc[-1]
    bollinger_upper = latest_data['upper_band'].iloc[-1]
    bollinger_lower = latest_data['lower_band'].iloc[-1]
    adx = latest_data['adx'].iloc[-1]
    plus_di = latest_data['plus_di'].iloc[-1]
    minus_di = latest_data['minus_di'].iloc[-1]
    score = calculate_signal_score(macd, macd_signal, price, ma, rsi, bollinger_upper, bollinger_lower, adx, plus_di,
                                   minus_di)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    check_data_folder(f'data/strategy/{coin}/1h')
    with open(f'data/strategy/{coin}/1h/signal_score_log.txt', 'a') as f:
        f.write(f"{current_time} - {coin} 1h signal score: {score}\n")
    if score > 50:
        return True


def find_entry_points(coin):
    latest_data = data_getter.get_latest_data(coin + '15m')
    macd = latest_data['macd'].iloc[-1]
    macd_signal = latest_data['signal_line'].iloc[-1]
    price = latest_data['close'].iloc[-1]
    ma = latest_data['20ma'].iloc[-1]
    rsi = latest_data['rsi'].iloc[-1]
    bollinger_upper = latest_data['upper_band'].iloc[-1]
    bollinger_lower = latest_data['lower_band'].iloc[-1]
    adx = latest_data['adx'].iloc[-1]
    plus_di = latest_data['plus_di'].iloc[-1]
    minus_di = latest_data['minus_di'].iloc[-1]
    score = calculate_signal_score(macd, macd_signal, price, ma, rsi, bollinger_upper, bollinger_lower, adx, plus_di,
                                   minus_di)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    check_data_folder(f'data/strategy/{coin}/15m')
    with open(f'data/strategy/{coin}/15m/signal_score_log.txt', 'a') as f:
        f.write(f"{current_time} - {coin} 15m signal score: {score}\n")
    if score > 50:
        return True


def confirm_entry_points(coin):
    latest_data = data_getter.get_latest_data(coin + '1m')
    macd = latest_data['macd'].iloc[-1]
    macd_signal = latest_data['signal_line'].iloc[-1]
    price = latest_data['close'].iloc[-1]
    ma = latest_data['20ma'].iloc[-1]
    rsi = latest_data['rsi'].iloc[-1]
    bollinger_upper = latest_data['upper_band'].iloc[-1]
    bollinger_lower = latest_data['lower_band'].iloc[-1]
    adx = latest_data['adx'].iloc[-1]
    plus_di = latest_data['plus_di'].iloc[-1]
    minus_di = latest_data['minus_di'].iloc[-1]
    score = calculate_signal_score(macd, macd_signal, price, ma, rsi, bollinger_upper, bollinger_lower, adx, plus_di,
                                   minus_di)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    check_data_folder(f'data/strategy/{coin}/1m')
    with open(f'data/strategy/{coin}/1m/signal_score_log.txt', 'a') as f:
        f.write(f"{current_time} - {coin} 1m signal score: {score}\n")
    if score > 50:
        return True


def analyze(coin, interval):
    print(f"Analyzing {coin} {interval}")
    if interval == '1m':
        if confirm_entry_points(coin):
            notification(coin)
    elif interval == '15m':
        if find_entry_points(coin):
            if confirm_entry_points(coin):
                notification(coin)
    elif interval == '1h':
        if confirm_trend(coin):
            if find_entry_points(coin):
                if confirm_entry_points(coin):
                    notification(coin)
    else:
        if analyze_trend(coin):
            if confirm_trend(coin):
                if find_entry_points(coin):
                    if confirm_entry_points(coin):
                        notification(coin)

    return


def notification(coin):
    speaker.speak(f"indicators shows it's time to buy {coin} now")
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    with open('strategy/notifications_log.txt', 'a') as f:
        f.write(f"{current_time} -  buy {coin} \n")

    return
