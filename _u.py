from _c import *
import pandas as pd
import numpy as np
import pickle
import requests
import os
import ccxt



# Indicator

def indicator_bollinger_band_width(data, n=20, k=2):
    data['MA'] = data['close'].rolling(window=n).mean()
    data['Std'] = data['close'].rolling(window=n).std()
    data['UpperBand'] = data['MA'] + (k * data['Std'])
    data['LowerBand'] = data['MA'] - (k * data['Std'])
    data['BBW'] = (data['UpperBand'] - data['LowerBand']) / data['MA']
    return data

def indicator_fibonacci(pb, ph):
    
    pzr = (ph - pb * 1.618) / (1 - 1.618)
    p02 = pzr - ((pzr - pb) * 0.236)
    p03 = pzr - ((pzr - pb) * 0.382)
    p05 = pzr - ((pzr - pb) * 0.5)
    p06 = pzr - ((pzr - pb) * 0.618)
    p07 = pzr - ((pzr - pb) * 0.786)
    px1 = pzr - ((pzr - pb) * 1.618)
    px2 = pzr - ((pzr - pb) * 2.618)
    px3 = pzr - ((pzr - pb) * 3.618)
    px4 = pzr - ((pzr - pb) * 4.618)

    return pzr, p02, p03, p05, p06, p07, px1, px2, px3, px4


def indicator_volume_oscillator(data, short_window, long_window):
    short_ma = data.ewm(span=short_window, min_periods=short_window).mean()
    long_ma = data.ewm(span=long_window, min_periods=long_window).mean()
    volume_oscillator = ((short_ma - long_ma) / long_ma) * 100
    return volume_oscillator


def indicator_ema(data, window):
    ema = data.ewm(span=window, adjust=False).mean()
    return ema


def indicator_macd(data, short_window, long_window, signal_window):
    short_ema = data.ewm(span=short_window, adjust=False).mean()
    long_ema = data.ewm(span=long_window, adjust=False).mean()
    macd_line = short_ema - long_ema
    signal_line = macd_line.ewm(span=signal_window, adjust=False).mean()
    macd_histogram = macd_line - signal_line
    return macd_line, signal_line, macd_histogram


def indicator_rsi(data, window):
    diff = data.diff(1)
    up = diff.where(diff > 0, 0)
    down = -diff.where(diff < 0, 0)
    avg_gain = up.rolling(window=window).mean()
    avg_loss = down.rolling(window=window).mean()
    avg_gain = up.ewm(alpha=(1/window), min_periods=window).mean()
    avg_loss = down.ewm(alpha=(1/window), min_periods=window).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def indicator_ma(data, window):
    return data.rolling(window=window).mean()


def indicator_angle(data, window):
    data['ma'] = data['close'].rolling(window=window).mean()
    slopes = np.gradient(data['ma'])
    angles = np.degrees(np.arctan(slopes))
    data['ma_angle'] = angles

    return data


# Line

def line_message(msg):
    print(msg)
    requests.post(LINE_URL, headers={'Authorization': 'Bearer ' + LINE_TOKEN}, data={'message': msg})



# Etc

def save_xlsx(url, df):
    df.to_excel(url)


def load_xlsx(url):
    return pd.read_excel(url)


def save_file(url, obj):
    with open(url, 'wb') as f:
        pickle.dump(obj, f)


def load_file(url):
    with open(url, 'rb') as f:
        return pickle.load(f)
    

def delete_file(url):
    if os.path.exists(url):
        for file in os.scandir(url):
            os.remove(file.path)


def get_qty(crnt_p, max_p):
    q = int(max_p / crnt_p)
    return 1 if q == 0 else q


def get_ror(pv, nv, pr=1, pf=0.001, spf=0):
    cr = ((nv - (nv * pf) - (nv * spf)) / (pv + (pv * pf)))
    return cr * pr