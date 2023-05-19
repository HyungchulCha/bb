from _c import *
from _u import *
from dateutil.relativedelta import *
import os
import copy
import time
import datetime
import threading
import ccxt


class BotBinance():


    def __init__(self):

        self.is_aws = True
        self.access_key = BN_ACCESS_KEY_AWS if self.is_aws else BN_ACCESS_KEY_NAJU
        self.secret_key = BN_SECRET_KEY_AWS if self.is_aws else BN_SECRET_KEY_NAJU
        self.bnc = ccxt.binance(config={'apiKey': self.access_key, 'secret': self.secret_key, 'enableRateLimit': True})
        
        self.q_l = []
        self.b_l = []
        self.r_l = []
        self.o_l = {}

        self.time_order = None
        self.time_rebalance = None

        self.bool_start = False
        self.bool_balance = False
        self.bool_order = False
        
        self.prc_ttl = 0
        self.prc_lmt = 0
        self.prc_buy = 0

        self.const_up = 377500
        self.const_up = 3750
        self.const_dn = 12.5

    
    def init_per_day(self):

        if self.bool_balance == False:

            tn = datetime.datetime.now()
            tn_0 = tn.replace(hour=0, minute=0, second=0)
            tn_d = int(((tn - tn_0).seconds) % 300)
            print(f'{tn_d} Second')

            if tn_d <= 150:
                time.sleep(300 - tn_d - 150)
            else:
                time.sleep(300 - tn_d + 150)

            self.bool_balance = True

        print('##############################')

        self.bnc = ccxt.binance(config={'apiKey': self.access_key, 'secret': self.secret_key, 'enableRateLimit': True})
        
        self.q_l = self.get_filter_ticker()
        prc_ttl, prc_lmt, _, bal_lst  = self.get_balance_info()
        self.b_l = list(set(self.q_l + bal_lst))
        self.r_l = list(set(bal_lst).difference(self.q_l))
        self.prc_ttl = prc_ttl if prc_ttl < self.const_up else self.const_up
        self.prc_lmt = prc_lmt if prc_ttl < self.const_up else prc_lmt - (prc_ttl - self.const_up)
        prc_buy = self.prc_ttl / (len(self.q_l) * 4)
        self.prc_buy = prc_buy if prc_buy > self.const_dn else self.const_dn

        if os.path.isfile(FILE_URL_TIKR_3M):
            self.o_l = load_file(FILE_URL_TIKR_3M)
        else:
            self.o_l = {}
            save_file(FILE_URL_TIKR_3M, self.o_l)

        for tk in self.b_l:
            if not (tk in self.o_l):
                self.get_tiker_data_init(tk)

        if self.prc_lmt < self.prc_buy:
            line_message('BotBinance Insufficient Balance !!!')

        int_prc_ttl = int(self.prc_ttl)
        int_prc_lmt = int(self.prc_lmt)
        len_bal_lst = len(self.b_l)

        line_message(f'BotBinance \nTotal Price : {int_prc_ttl:,} USDT \nLimit Price : {int_prc_lmt:,} USDT \nSymbol List : {len_bal_lst}')

        __tn = datetime.datetime.now()
        __tn_min = __tn.minute % 5
        __tn_sec = __tn.second

        self.time_rebalance = threading.Timer(300 - (60 * __tn_min) - __tn_sec + 150, self.init_per_day)
        self.time_rebalance.start()


    def stock_order(self):

        if self.bool_order == False:

            tn = datetime.datetime.now()
            tn_0 = tn.replace(hour=0, minute=0, second=0)
            tn_d = int(((tn - tn_0).seconds) % 300)
            time.sleep(300 - tn_d)
            self.bool_order = True

        _tn = datetime.datetime.now()

        # self.get_remain_cancel(self.b_l)

        _, _, bal_lst, _ = self.get_balance_info()
        sel_lst = []

        for symbol in self.b_l:

            df = self.strategy_rsi(self.gen_bnc_df(symbol, '5m', 120))
            is_df = not (df is None)

            if is_df:
                
                df_h = df.tail(2).head(1)
                close = df_h['close'].iloc[-1]
                rsi = df_h['rsi'].iloc[-1]
                rsi_prev = df_h['rsi_prev'].iloc[-1]
                volume_osc = df_h['volume_osc'].iloc[-1]

                cur_prc = float(close)
            
                is_symbol_bal = symbol in bal_lst
                is_psb_sel = (is_symbol_bal and (cur_prc * bal_lst[symbol]['b'] > self.const_dn))
                ol_bool_buy = copy.deepcopy(self.o_l[symbol]['bool_buy'])
                is_nothing = ol_bool_buy and ((not is_symbol_bal) or (is_symbol_bal and (cur_prc * bal_lst[symbol]['b'] < self.const_dn)))

                if is_nothing:
                    self.get_tiker_data_init(symbol)

                if is_psb_sel and ol_bool_buy:

                    bl_balance = copy.deepcopy(bal_lst[symbol]['b'])
                    ol_buy_price = copy.deepcopy(self.o_l[symbol]['buy_price'])
                    ol_quantity_ratio = copy.deepcopy(self.o_l[symbol]['quantity_ratio'])
                    ol_bool_sell = copy.deepcopy(self.o_l[symbol]['bool_sell'])
                    ol_70_position = copy.deepcopy(self.o_l[symbol]['70_position'])
                    sell_qty = bl_balance * (1 / ol_quantity_ratio)
                    is_psb_sel_div = (cur_prc * sell_qty) > self.const_dn

                    if (not is_psb_sel_div) and ol_quantity_ratio > 1:
                        sell_qty = bl_balance * (1 / ol_quantity_ratio - 1)

                    if rsi <= 50 and ol_bool_sell:
                        self.bnc.create_market_sell_order(symbol=symbol, amount=bl_balance)
                        self.get_tiker_data_init(symbol)

                        _ror = get_ror(ol_buy_price, cur_prc)
                        print(f'Sell - Symbol: {symbol}, Profit: {round(_ror, 4)}')
                        sel_lst.append({'c': '[S] ' + symbol, 'r': round(_ror, 4)})

                    elif rsi >= 70 and ((ol_70_position == '70_down') or (ol_70_position == '70_up' and (rsi_prev < rsi))):
                        self.bnc.create_market_sell_order(symbol=symbol, amount=sell_qty)
                        self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio - 1
                        self.o_l[symbol]['bool_sell'] = True

                        if (not is_psb_sel_div) and ol_quantity_ratio > 1:
                            self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio - 2

                        if self.o_l[symbol]['quantity_ratio'] == 0:
                            self.get_tiker_data_init(symbol)

                        _ror = get_ror(ol_buy_price, cur_prc)
                        print(f'Sell - Symbol: {symbol}, Profit: {round(_ror, 4)}')
                        sel_lst.append({'c': '[S] ' + symbol, 'r': round(_ror, 4)})
                        

                if (rsi <= 30) and (rsi_prev > rsi) and (volume_osc > 0):
                    
                    is_psb_ord = float(self.bnc.fetch_balance()['USDT']['free']) > self.prc_buy
                    is_remain_symbol = symbol in self.r_l
                    buy_qty = float(self.prc_buy / cur_prc)

                    if is_psb_ord and (not is_remain_symbol):

                        self.bnc.create_market_buy_order(symbol=symbol, amount=buy_qty)
                        ol_bool_buy = copy.deepcopy(self.o_l[symbol]['bool_buy'])
                        ol_quantity_ratio = copy.deepcopy(self.o_l[symbol]['quantity_ratio'])

                        if ol_bool_buy:
                            self.o_l[symbol]['buy_price'] = cur_prc
                            self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio + 1
                        else:
                            self.o_l[symbol] = {
                                'bool_buy': True,
                                'buy_price': cur_prc,
                                'quantity_ratio': 2,
                                'bool_sell': False,
                                '70_position': ''
                            }

                        print(f'Buy - Symbol: {symbol}, Balance: {buy_qty}')
                        sel_lst.append({'c': '[B] ' + symbol, 'r': (buy_qty)})


                self.o_l[symbol]['70_position'] = '70_down' if rsi < 70 else '70_up'


        save_file(FILE_URL_TIKR_3M, self.o_l)
        # print(self.o_l)

        sel_txt = ''
        for sl in sel_lst:
            sel_txt = sel_txt + '\n' + str(sl['c']) + ' : ' + str(sl['r'])

        __tn = datetime.datetime.now()
        __tn_min = __tn.minute % 5
        __tn_sec = __tn.second

        self.time_backtest = threading.Timer(300 - (60 * __tn_min) - __tn_sec, self.stock_order)
        self.time_backtest.start()

        int_prc_ttl = int(self.prc_ttl)
        str_start = _tn.strftime('%Y/%m/%d %H:%M:%S')
        str_end = __tn.strftime('%Y/%m/%d %H:%M:%S')

        line_message(f'BotBinance \nStart : {str_start}, \nEnd : {str_end}, \nTotal Price : {int_prc_ttl:,} USDT {sel_txt}')
    

    # Tiker Data Init
    def get_tiker_data_init(self, tk):
        self.o_l[tk] = {
            'bool_buy': False,
            'buy_price': 0,
            'quantity_ratio': 0,
            'bool_sell': False,
            '70_position': ''
        }


    # Spot, USDT Filter Ticker
    def get_filter_ticker(self):
        mks = self.bnc.load_markets()
        tks = []
        lst = []

        for mk in mks:
            if \
            mk.endswith('/USDT') and \
            mks[mk]['active'] == True and \
            mks[mk]['info']['status'] == 'TRADING' and \
            mks[mk]['info']['isSpotTradingAllowed'] == True and \
            'SPOT' in mks[mk]['info']['permissions'] \
            :
                tks.append(mk)

        for tk in tks:
            resp = self.bnc.fetch_ticker(tk)
            lst.append({'t': tk, 'v': float(resp['info']['quoteVolume'])})
        
        lst = sorted(lst, key=lambda x: x['v'])[-40:]
        lst = [t['t'] for t in lst]

        return lst
    

    # Strategy RSI
    def strategy_rsi(self, df):
        if not (df is None):
            df['rsi'] = indicator_rsi(df['close'], 14)
            df['rsi_prev'] = df['rsi'].shift()
            df['volume_osc'] = indicator_volume_oscillator(df['volume'], 5, 10)
            return df
    

    # Generate Dataframe
    def gen_bnc_df(self, tk, tf, lm):
        ohlcv = self.bnc.fetch_ohlcv(tk, timeframe=tf, limit=lm)
        if not (ohlcv is None) and len(ohlcv) >= lm:
            df = pd.DataFrame(ohlcv, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
            pd_ts = pd.to_datetime(df['datetime'], utc=True, unit='ms')
            pd_ts = pd_ts.dt.tz_convert("Asia/Seoul")
            pd_ts = pd_ts.dt.tz_localize(None)
            df.set_index(pd_ts, inplace=True)
            df = df[['open', 'high', 'low', 'close', 'volume']]
            return df
        

    # Balance Code List
    def get_balance_info(self):
        balance = self.bnc.fetch_balance()
        bal_ttl = balance['total']
        bal_lst = balance['info']['balances']
        bal_fre = float(balance['USDT']['free'])
        prc = 0
        obj = {}
        lst = []
        for bl in bal_lst:
            free = float(bl['free'])
            asst = bl['asset']
            tikr = asst + '/USDT'
            if free > 0 and asst != 'USDT':
                obj[tikr] = {
                    'b': free,
                }
                cls = self.bnc.fetch_ticker(tikr)['close']
                prc = prc + (cls * bal_ttl[asst])
                lst.append(tikr)
        prc = prc + bal_fre

        return prc, bal_fre, obj, lst
    
    
    # Not Signed Cancel Order
    def get_remain_cancel(self, l):
        for _l in l:
            rmn_lst = self.bnc.fetch_open_orders(_l)
            print(rmn_lst)
            if len(rmn_lst) > 0:
                for rmn in rmn_lst:
                    if rmn['status'] == 'open':
                        self.bnc.cancel_order(rmn['info']['orderId'], _l)

    
    # All Sell
    def all_sell_order(self):
        _, _, bal_lst, _  = self.get_balance_info()
        for bl in bal_lst:
            cls = self.bnc.fetch_ticker(bl)['close']
            prc = cls * bal_lst[bl]['b']
            if prc > 10:
                resp = self.bnc.create_market_sell_order(bl, bal_lst[bl]['b'])
                print(resp)
                time.sleep(0.25)


if __name__ == '__main__':

    bb = BotBinance()
    # bb.init_per_day()
    # bb.stock_order()
    # bb.all_sell_order()

    while True:

        try:

            tn = datetime.datetime.now()
            tn_start = tn.replace(hour=0, minute=0, second=0)

            if tn >= tn_start and bb.bool_start == False:
                bb.init_per_day()
                bb.stock_order()
                bb.bool_start = True

        except Exception as e:

            line_message(f"BotBinance Error : {e}")
            break