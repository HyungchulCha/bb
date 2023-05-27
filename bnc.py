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
        self.t_l = []
        self.o_l = {}

        self.time_order = None
        self.time_rebalance = None

        self.bool_start = False
        self.bool_balance = False
        self.bool_order = False
        
        self.prc_ttl = 0
        self.prc_lmt = 0
        self.prc_buy = 0

        self.const_up = 375000
        self.const_dn = 12.5


    def top_tier(self):

        sym_arr = []
        pft_arr = []
        obj_lst = {}
        bal_obj = {}
        tks = self.get_filter_ticker(True)

        for code in tks:

            df = self.strategy_rsi(self.gen_bnc_df(code, '5m', 12*24*3))

            obj_lst[code] = {
                'bool_buy': False,
                'buy_price': 0,
                'quantity_ratio': 0,
                'bool_sell': False,
                '70_position': ''
            }
            
            if not df is None:
                _ror = 1
                bal_obj[code] = {'b': 0}

                for i, row in df.iterrows():
                    
                    rsi = row['rsi']
                    rsi_prv = row['rsi_prev']
                    vol_osc = row['volume_osc']
                    cur_prc = float(row['close'])

                    if (rsi <= 30) and (rsi_prv > rsi) and (vol_osc > 0):

                        bb = copy.deepcopy(obj_lst[code]['bool_buy'])

                        if bb:
                            bp = copy.deepcopy(obj_lst[code]['buy_price'])
                            qr = copy.deepcopy(obj_lst[code]['quantity_ratio'])
                            obj_lst[code]['buy_price'] = ((bp * (qr - 1)) + cur_prc) / qr
                            obj_lst[code]['quantity_ratio'] = qr + 1
                        else:
                            obj_lst[code] = {
                                'bool_buy': True,
                                'buy_price': cur_prc,
                                'quantity_ratio': 2,
                                'bool_sell': False,
                                '70_position': ''
                            }

                    if obj_lst[code]['bool_buy'] == True:

                        bp = float(copy.deepcopy(obj_lst[code]['buy_price']))
                        qr = copy.deepcopy(obj_lst[code]['quantity_ratio'])
                        bs = copy.deepcopy(obj_lst[code]['bool_sell'])
                        p7 = copy.deepcopy(obj_lst[code]['70_position'])

                        if rsi <= 50 and bs:
                            _ror = get_ror(bp, cur_prc, _ror)
                            obj_lst[code] = {
                                'bool_buy': False,
                                'buy_price': 0,
                                'quantity_ratio': 0,
                                'bool_sell': False,
                                '70_position': ''
                            }

                        elif rsi >= 70 and ((p7 == '70_down') or (p7 == '70_up' and (rsi_prv <= rsi))):
                            _ror = get_ror(bp, cur_prc, _ror)
                            obj_lst[code]['quantity_ratio'] = qr - 1
                            obj_lst[code]['bool_sell'] = True

                            if obj_lst[code]['quantity_ratio'] == 0:
                                obj_lst[code] = {
                                    'bool_buy': False,
                                    'buy_price': 0,
                                    'quantity_ratio': 0,
                                    'bool_sell': False,
                                    '70_position': ''
                                }

                    obj_lst[code]['70_position'] = '70_down' if rsi < 70 else '70_up'

                pft_per = round(((_ror - 1) * 100), 2)

                sym_arr.append(code)
                pft_arr.append(pft_per)

        pft_df = pd.DataFrame({'code': sym_arr, 'profit': pft_arr})
        pft_df = pft_df.loc[(pft_df['profit'] > 0)]
        pft_df = pft_df.sort_values('profit', ascending=False)
        print(pft_df)
        ttr_li = pft_df.head(80)['code'].to_list()
        save_file(FILE_URL_TPTR_3M, ttr_li)

    
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

        # tn_tt = datetime.datetime.now()

        # if ((tn_tt.hour % 3) == 2) and tn_tt.minute == 57 and (30 <= tn_tt.second < 35):
        #     self.top_tier()
        #     self.t_l = load_file(FILE_URL_TPTR_3M)
        #     line_message(f'BotBinance \nToday Top-Tier \n{self.t_l}')

        print('##############################')

        self.bnc = ccxt.binance(config={'apiKey': self.access_key, 'secret': self.secret_key, 'enableRateLimit': True})
        
        self.t_l = load_file(FILE_URL_TPTR_3M)
        self.q_l = self.get_filter_ticker()
        prc_ttl, prc_lmt, _, bal_lst  = self.get_balance_info()
        self.b_l = list(set(self.q_l + bal_lst))
        self.r_l = list(set(bal_lst).difference(self.q_l))
        self.prc_ttl = prc_ttl if prc_ttl < self.const_up else self.const_up
        self.prc_ttl = 17000
        self.prc_lmt = prc_lmt if prc_ttl < self.const_up else prc_lmt - (prc_ttl - self.const_up)
        prc_buy = self.prc_ttl / 500
        self.prc_buy = prc_buy if prc_buy > self.const_dn else self.const_dn

        if os.path.isfile(FILE_URL_TIKR_3M):
            self.o_l = load_file(FILE_URL_TIKR_3M)
        else:
            self.o_l = {}
            save_file(FILE_URL_TIKR_3M, self.o_l)

        for tk in self.b_l:
            if not (tk in self.o_l):
                self.get_tiker_data_init(tk)
                
        for _tk in self.o_l:
            if (self.o_l[_tk]['bool_buy'] == True) and (not (_tk in self.b_l)):
                self.get_tiker_data_init(_tk)

        if self.prc_lmt < self.prc_buy:
            line_message('BotBinance Insufficient Balance !!!')

        int_rel_ttl = int(prc_ttl)
        int_prc_ttl = int(self.prc_ttl)
        int_prc_lmt = int(self.prc_lmt)
        len_qnt_lst = len(self.q_l)

        line_message(f'BotBinance \nT : {int_prc_ttl:,} USDT \nR : {int_rel_ttl:,} USDT \nL : {int_prc_lmt:,} USDT \nS : {len_qnt_lst}')

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
        cur_pft = []

        for symbol in self.b_l:

            df = self.strategy_rsi(self.gen_bnc_df(symbol, '5m', 120))

            if not (df is None):
                
                dfh = df.tail(2).head(1)
                rsi = dfh['rsi'].iloc[-1]
                rsi_prv = dfh['rsi_prev'].iloc[-1]
                vol_osc = dfh['volume_osc'].iloc[-1]
                cur_prc = float(dfh['close'].iloc[-1])

                str_rsi = round(rsi, 2)
                str_rsi_prv = round(rsi_prv, 2)
                str_vol_osc = round(vol_osc, 2)
                print(f'{symbol} : RSI - {str_rsi}, RSI_P - {str_rsi_prv}, VO - {str_vol_osc}')
                
                
                bal_sym = symbol in bal_lst
                psb_sel = (bal_sym and (cur_prc * bal_lst[symbol]['b'] > self.const_dn))
                bb = copy.deepcopy(self.o_l[symbol]['bool_buy'])
                nt = bb and ((not bal_sym) or (bal_sym and (cur_prc * bal_lst[symbol]['b'] < self.const_dn)))

                if bb:
                    _bp = copy.deepcopy(self.o_l[symbol]['buy_price'])
                    _cur_ror = get_ror(_bp, cur_prc)
                    _cur_ror = round(_cur_ror, 4)
                    cur_pft.append(f'{symbol} : {_cur_ror}')

                if nt:
                    self.get_tiker_data_init(symbol)

                if psb_sel and bb:
                    tb = copy.deepcopy(bal_lst[symbol]['b'])
                    bp = copy.deepcopy(self.o_l[symbol]['buy_price'])
                    qr = copy.deepcopy(self.o_l[symbol]['quantity_ratio'])
                    bs = copy.deepcopy(self.o_l[symbol]['bool_sell'])
                    p7 = copy.deepcopy(self.o_l[symbol]['70_position'])
                    sq = (tb * (1 / qr))
                    psb_sel_div = (cur_prc * sq) > self.const_dn

                    if (not psb_sel_div) and qr > 1:
                        sq = tb * (1 / (qr - 1))

                    tb = round(tb, 8)
                    sq = round(sq, 8)

                    if rsi <= 50 and bs:
                        res = self.bnc.create_market_sell_order(symbol=symbol, amount=tb)
                        if res['info']['status'] == 'FILLED':
                            self.get_tiker_data_init(symbol)
                            _ror = get_ror(bp, cur_prc)
                            print(f'Sell - Symbol: {symbol}, Profit: {round(_ror, 4)}')
                            sel_lst.append({'c': '[E] ' + symbol, 'r': round(_ror, 4)})

                    elif rsi >= 70 and ((p7 == '70_down') or (p7 == '70_up' and (rsi_prv <= rsi))):
                        res = self.bnc.create_market_sell_order(symbol=symbol, amount=sq)
                        if res['info']['status'] == 'FILLED':
                            self.o_l[symbol]['quantity_ratio'] = qr - 1
                            self.o_l[symbol]['bool_sell'] = True

                            if (not psb_sel_div) and qr > 1:
                                self.o_l[symbol]['quantity_ratio'] = qr - 2

                            if self.o_l[symbol]['quantity_ratio'] == 0:
                                self.get_tiker_data_init(symbol)

                            _ror = get_ror(bp, cur_prc)
                            print(f'Sell - Symbol: {symbol}, Profit: {round(_ror, 4)}')
                            sel_lst.append({'c': '[S] ' + symbol, 'r': round(_ror, 4)})
                        

                if (rsi <= 30) and (rsi_prv > rsi) and (vol_osc > 0):
                    
                    psb_ord = float(self.bnc.fetch_balance()['USDT']['free']) > self.prc_buy
                    rmn_sym = symbol in self.r_l
                    bq = float(self.prc_buy / cur_prc)
                    bq = round(bq, 8)

                    if psb_ord and ((not rmn_sym) or (rmn_sym and (self.o_l[symbol]['bool_buy'] == True))):
                        res = self.bnc.create_market_buy_order(symbol=symbol, amount=bq)
                        if res['info']['status'] == 'FILLED':
                            bb = copy.deepcopy(self.o_l[symbol]['bool_buy'])
                            bp = copy.deepcopy(self.o_l[symbol]['buy_price'])
                            qr = copy.deepcopy(self.o_l[symbol]['quantity_ratio'])

                            if bb:
                                self.o_l[symbol]['buy_price'] = ((bp * (qr - 1)) + cur_prc) / qr
                                self.o_l[symbol]['quantity_ratio'] = qr + 1
                            else:
                                self.o_l[symbol] = {
                                    'bool_buy': True,
                                    'buy_price': cur_prc,
                                    'quantity_ratio': 2,
                                    'bool_sell': False,
                                    '70_position': ''
                                }

                            print(f'Buy - Symbol: {symbol}, Balance: {bq}')
                            sel_lst.append({'c': '[B] ' + symbol, 'r': (bq)})


                self.o_l[symbol]['70_position'] = '70_down' if rsi < 70 else '70_up'


        save_file(FILE_URL_TIKR_3M, self.o_l)
        for o in self.o_l:
            if self.o_l[o]['bool_buy'] == True:
                print(o, self.o_l[o])

        sel_txt = ''
        for sl in sel_lst:
            sel_txt = sel_txt + '\n' + str(sl['c']) + ' : ' + str(sl['r'])

        cur_pft_frst = True
        cur_pft_text = ''
        for cp in cur_pft:
            if cur_pft_frst:
                cur_pft_text = cur_pft_text + '\n' + cp
                cur_pft_frst = False
            else:
                cur_pft_text = cur_pft_text + ' / ' + cp

        __tn = datetime.datetime.now()
        __tn_min = __tn.minute % 5
        __tn_sec = __tn.second

        self.time_backtest = threading.Timer(300 - (60 * __tn_min) - __tn_sec, self.stock_order)
        self.time_backtest.start()

        str_start = _tn.strftime('%Y/%m/%d %H:%M:%S')
        str_end = __tn.strftime('%Y/%m/%d %H:%M:%S')

        # line_message(f'BotBinance \nS : {str_start} \nE : {str_end} {cur_pft_text} {sel_txt}')
        line_message(f'BotBinance \nS : {str_start} \nE : {str_end} {sel_txt}')
    

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
    def get_filter_ticker(self, init=False):
        mks = self.bnc.load_markets()
        tks = []

        for mk in mks:
            if \
            mk.endswith('/USDT') and \
            mks[mk]['active'] == True and \
            mks[mk]['info']['status'] == 'TRADING' and \
            mks[mk]['info']['isSpotTradingAllowed'] == True and \
            'SPOT' in mks[mk]['info']['permissions'] \
            :
                
                _tks = self.bnc.fetch_ticker(mk)
                if float(_tks['info']['priceChangePercent']) > 0:
                    tks.append({'t': mk, 'c': float(_tks['info']['priceChangePercent'])})

        #         if not init:
        #             if mk in self.t_l:
        #                 tks.append(mk)
        #         else:
        #             tks.append(mk)

        # return tks

        # if len(tks) > 100:
        #     _lst = sorted(tks, key=lambda t: t['c'])[-100:]
        # else:
        _lst = sorted(tks, key=lambda t: t['c'])[::-1]
        lst = [l['t'] for l in _lst]

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