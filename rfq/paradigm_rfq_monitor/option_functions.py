import math
import numpy as np
from scipy.stats import norm
from functools import lru_cache
import requests
import datetime as dt
import json
from dflog import get_logger

class RFQ:
    def __init__(self, params: dict):
        self.params = params
        self.future_price_data = self.get_future_price()
        self.iv_data = self.get_iv()
        self.type_dict = {'P': 'Put', 'C': 'Call'}  # for format switch

    # third party data (future prices and volatilities)
    @lru_cache
    def get_future_price(self):
        req = requests.get('https://defi-bot.bastioncb.com:453/api/getAllFutures?exchange=deribit')
        txt = eval(req.text)
        data = txt['data']
        return data

    @lru_cache
    def get_iv(self):
        req = requests.get('https://defi-bot.bastioncb.com:453/api/getMarkVols?raw=True')
        txt = eval(req.text)
        data = txt['data']
        return data['DB']

    def analysis(self):
        today = dt.date.today()
        data = self.params['data']
        legs = data['legs']
        #print(legs)
        option_price, delta_all, gamma_all, theta_all, vega_all = 0, 0, 0, 0, 0
        port_dict = {}
        for leg in legs:
            instrument = leg['instrument_name']  # str
            instruments = instrument.split('-')  # list
            crypto = instruments[0]  # eg: 'BTC'
            expiry_date = instruments[1]  # eg: '30JUN23'
            strike = instruments[2]  # int
            option_type = instruments[3]  # 'P' or 'C'
            option_type = self.type_dict[option_type]  # 'Put' or 'Call'

            
            future_prices = self.future_price_data[crypto]
            try:  # sometimes KeyError will raise, so if this occurs, skip this leg.
                future_price = future_prices[expiry_date]  
            except KeyError:
                continue
            ivs = self.iv_data[crypto]
            sigma = ivs[expiry_date][strike]  # BS的input之一  这个数据里面的strike是string形式，我也不知道为什么哈哈哈哈哈

            expiry_date = dt.datetime.strptime(expiry_date, '%d%b%y').date()
            T = (expiry_date - today).days / 365
            oc = OptionCalculation(s0=future_price, K=eval(strike), T=T, r=0, sigma=sigma, option_type=option_type)
            ratio = eval(leg['ratio'])
            side = leg['side']
            if side == 'BUY':
                m = 1
            elif side == 'SELL':
                m = -1
            else:
                m = 0 

        
            expiry_date = dt.datetime.strftime(expiry_date, '%y%m%d')  # another format for checking
            response = check(crypto=str(crypto), expiry_date=expiry_date, strike=int(strike),
                             option_type=str(option_type), forward_price=float(future_price),vol_shift=0)
            #JSON data
            data = json.loads(response.text)

            port_dict[instrument] = oc.res_dict

            option_price += ratio * m * oc.option_price
            delta_all += ratio * m * oc.Delta
            gamma_all += ratio * m * oc.Gamma
            theta_all += ratio * m * oc.Theta
            vega_all += ratio * m * oc.Vega
            # rho_all += ratio * m * oc.Rho

        port_dict['strategy'] = {
            'price': option_price,
            'delta': delta_all,
            'gamma': gamma_all,
            'theta': theta_all,
            'vega': vega_all
        }

        # print('Total price of the RFQ is %.2f dollars!' % option_price)
        # print('Delta of the RFQ is %.4f' % delta_all)
        # print('Gamma of the RFQ is %.4f' % gamma_all)
        # print('Theta of the RFQ is %.4f' % theta_all)
        # print('Vega of the RFQ is %.4f' % vega_all)
        # print('Rho of the RFQ is %.4f' % rho_all)
        log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                                  file_prefix="info",live_stream=True)
        log2.info("Strategy data acquired")
        return port_dict
    
    def acquire_data(self):
        log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                                  file_prefix="info",live_stream=True)
        log2.info("Acquire data")

        data = self.params['data']
        legs = data['legs']
        port_dict = {}
        #print(legs)
        for leg in legs:
            instrument = leg['instrument_name']  # str
            instruments = instrument.split('-')  # list
            crypto = instruments[0]  # eg: 'BTC'
            expiry_date = instruments[1]  # eg: '30JUN23'
            strike = instruments[2]  # int
            option_type = instruments[3]  # 'P' or 'C'
            #option_type = self.type_dict[option_type]  # 'Put' or 'Call'

            # Need to use third-party data obtained from the request library
            future_prices = self.future_price_data[crypto]
            #print(future_prices)
            try:  # sometimes KeyError will raise, so if this occurs, skip this leg.
                future_price = future_prices[expiry_date]  
            except KeyError:
                continue           
            expiry_date = dt.datetime.strptime(expiry_date, '%d%b%y').date()
            expiry_date = dt.datetime.strftime(expiry_date, '%y%m%d')  # another format for checking
            #response = check(crypto=str(crypto), expiry_date=expiry_date, strike=int(strike),
            #                 option_type=str(option_type), forward_price=float(future_price),vol_shift=0)
            response = check(crypto=str(crypto), expiry_date=expiry_date, strike=int(strike),
                             option_type=str(option_type), forward_price=float(future_price),vol_shift=0)
            #file = open("data.txt", "a")
            #print(response.json(),file=file)
            rep = response.json()
            #port_dict[leg] = {'rep':rep}
            port_dict[f'{leg}'] = {'rep':rep}
            log2.info("Return data")
        return port_dict


#def check(crypto='ETH', expiry_date='230728', strike=3000, option_type='C', forward_price=1876.25 ,vol_shift = 0):
def check(crypto, expiry_date, strike, option_type, forward_price , vol_shift=0):
    #print(crypto, expiry_date, strike, option_type, forward_price)
    'to check whether our calculation was right'
    url = 'http://18.140.178.150:8050/thetanutsOptionsRisk'
    data = [{
        "requestID": 1,
        "instrument": crypto + '-' + expiry_date + '-' + str(strike) + '-' + option_type,
        "risk_coin_symbol": crypto,
        "reference_coin_price": forward_price,
        "risk_coin_price": forward_price,
        "forward_price": forward_price,
        "expiry_time_UTC": int(dt.datetime.strptime(expiry_date, '%y%m%d').timestamp()),
        "vol_shift": vol_shift
    }]
    response = requests.post(url, data=json.dumps(data), headers={"Content-Type": "application/json"}, timeout=3)
    #print(response.json())
    
    return response


class OptionCalculation:
    def __init__(self, s0, K, T, r=0, sigma=0.5, option_type='Call'):
        """
        calc the price through BS model, and calc all the greeks
        :param option_type: 'Call' or 'Put'
        :param s0: actually, this is the future price
        :param K: strike price
        :param T: time(as days)
        :param r: we set it to 0 because we use future price
        :param sigma: IV
        :return: price, Delta, Gamma, Vega, Theta, Rho
        """
        self.s0 = s0
        self.K = K
        self.T = T
        self.r = r
        self.sigma = sigma
        self.option_type = option_type
        self.d1, self.d2 = self.d()
        if self.option_type == 'Call':
            self.n = 1  # orientation
        else:
            self.n = -1
        self.option_price = self.black_scholes()
        self.Delta = self.delta()
        self.Gamma = self.gamma()
        self.Theta = self.theta()
        self.Vega = self.vega()
        self.Rho = self.rho()
        self.res_dict = {
            'price': self.option_price,
            'forward price': self.s0,
            'IV': self.sigma,
            'delta': self.Delta,
            'gamma': self.Gamma,
            'vega': self.Vega,
            'theta': self.Theta
        }

    @lru_cache()
    def d(self):
        # calc d1
        try:
            a1 = math.log(self.s0 / self.K) + (self.r + self.sigma ** 2 / 2) * self.T
            a2 = self.sigma * math.sqrt(self.T)
            if a2!=0:
                d1 = a1 / a2
            else:
                d1 = a1
            # calc d2
            d2 = d1 - self.sigma * math.sqrt(self.T)
        except:
            d1=1
            d2=1
        return d1, d2

    def black_scholes(self):
        option_price = np.NAN
        if self.option_type == 'Call':
            option_price = self.s0 * norm.cdf(self.d1) - self.K * math.exp(-self.r * self.T) * norm.cdf(self.d2)
        elif self.option_type == 'Put':
            option_price = self.K * math.exp(-self.r * self.T) * norm.cdf(-self.d2) - self.s0 * norm.cdf(-self.d1)
        return option_price

    def delta(self):
        return self.n * norm.cdf(self.n * self.d1)

    def gamma(self):
        return norm.pdf(self.d1) / (self.s0 * self.sigma * math.sqrt(self.T))

    def vega(self):
        return self.s0 * norm.pdf(self.d1) * math.sqrt(self.T)

    def theta(self):
        left = self.s0 * norm.pdf(self.d1) * self.sigma / (2 * math.sqrt(self.T))
        right = self.r * self.K * math.exp(-self.r * self.T) * norm.cdf(self.n * self.d2)
        Theta = -left + right
        return Theta

    def rho(self):
        return self.n * self.K * self.T * math.exp(-self.r * self.T) * norm.cdf(self.n * self.d2)
