'''
    Live data source from bastion MDC
'''

__author__ = "Michael Vasquez"
__email__ = "mv@bastiontrade.com"

import asyncio
import json
import time
#import csv
#import pandas as pd

import requests
from websocket import create_connection
import pandas as pd
from dash import Dash, html, dcc,dash_table
from dash.dependencies import Output, Input
from functools import lru_cache
import datetime as dt
import threading
from dash import Dash
import logging
from collections import deque

# from memory_profiler import profile
logger = logging.Logger('options_ob_monitor')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


lock = threading.Lock()

class ThreadStream:

    def __init__(self, result, exchange) -> None:
        self.result = result
        self.buffer_q = deque()
        self.exchange = exchange

    def _get_instruments(self, options=True, exchange='Deribit'):
        '''
        Get all available instruments from Helios on Deribit
            Format will be slightly different for other exchanges

        returns json
            'success': bool
            'result': 'currencyPairs'
            'timeStamp': int
        '''
        url = 'https://market.beyondalpha.io/ITS/Market/QuerySymbols'
        header = {"accept": "text/plain", "Content-Type": "application/json"}

        if exchange == 'Deribit':
            data = {
                    "exchangeName": exchange,
                    "tradingMarket": "Main",
                    "exchangeTradingMarket": "Option" if options else "Main"
                    }
        elif exchange == 'Okex':
            data = {
                    "exchangeName": exchange,
                    "tradingMarket": "Main",
                    "exchangeTradingMarket": "Option"
                    }
        elif exchange == 'Binance':
            data = {
                    "exchangeName": exchange,
                    "tradingMarket": "Deriv",
                    "exchangeTradingMarket": "Option"
                    }

        response = requests.post(url, headers=header, data=json.dumps(data), timeout=5).json()
        return response

    def get_instruments(self, options=True, exchange='Deribit', tries=0):
        ''' gets DB instruments from Helios as json wrapper in case request fails
    ETH-USD-231229-1800-P,T02:46:48.7605794, 1.90s, 1.93s, {'asks': [[0.086, 252.0, 21.672]], 'bids': [[0.085, 25.0, 2.125]]}
    ETH-USD-231229-2000-C,T02:46:48.7605794, 27.24s, 27.27s, {'asks': [[0.0915, 20.0, 1.83]], 'bids': [[0.087, 0.1, 0.0087]]}
            sleeps 1 second and retries
        '''
        response = self._get_instruments(options=options, exchange=exchange)

        if 'success' in response.keys():
            if (not response['success']) and (tries < 5):
                time.sleep(1)
                response = self.get_instruments(options=options, exchange=exchange, tries=tries+1)
            elif response['success']:
                return response
            else:
                raise RuntimeError(f"Failed getting Bastion Market Instruments: {response}")
            
    def stream_cex_markets(self, book_depth: int = 2):
        logger.info(f"streaming data... {self.exchange}")
        if self.exchange == 'Deribit':
            stream_options ={
                "op":"sub",
                "arg":{"tp":"snapshot",
                    "es": [{"ex":"Deribit","tm": "Main", "s": self.instruments}],
                        "dp":book_depth, "ims":100}}
        elif self.exchange == 'Okex':
            stream_options ={
                "op":"sub",
                "arg":{"tp":"snapshot",
                    "es": [{"ex":"Okex","tm": "Main", "s": self.instruments}],
                        "dp":book_depth, "ims":100}}
        elif self.exchange == 'Binance':
            stream_options ={
                "op":"sub",
                "arg":{"tp":"snapshot",
                    "es": [{"ex":"Binance","tm": "Deriv", "s": self.instruments}],
                        "dp":book_depth, "ims":100}}
        try:  
            auth = json.dumps(stream_options)
            time.sleep(3)
            ws = create_connection("wss://market.beyondalpha.io/market")
            ws.send(auth)
            logger.info(f"{self.exchange} connect the websockets.")
        except Exception as e:
            logger.error(f"There's something wrong with websocket, not the program. {str(e)}")
            raise e
        
        while True:
            time.sleep(0.01)
            response = ws.recv()
            self.buffer_q.append(response)

    def handle_buffer(self):
        self.last_check_len_ts = time.time()
        while True:
            #log1.info("Received a response from the web.")
            buffer_q_len = len(self.buffer_q)
            if buffer_q_len == 0:
                time.sleep(0.01)
                continue
            if time.time() - self.last_check_len_ts > 10 and buffer_q_len>20:
                logger.info(f"{self.exchange} buffer_q_len: {buffer_q_len}")
                self.last_check_len_ts = time.time()
            
            response = self.buffer_q.popleft()
            dfn =pd.DataFrame(columns=('Time','Exchange','Instrument','Asks','Bids'))
            #raise Exception("test")

            if response != 'Connect success':
                response = json.loads(response)
                if 'tp' in response.keys():
                    #log1.info("handle data......")
                    exchangem = response['exchange']
                    exchangeSymbolAlias = response['exchangeSymbolAlias']
                    asks = response['data']['asks']
                    bids = response['data']['bids']
                            
                    a = []
                    b = []
                    tp =  exchangeSymbolAlias.split('-')[0]
                    if tp == "ETH":
                        size = 2500
                    else:
                        size = 150
                    

                    for ask in asks:
                        if ask[1]>size:
                            a.append(str(ask))
                    for bid in bids:
                        if bid[1]>size:   
                            b.append(str(bid))
                    ask1 = "\n".join(a)
                    bid1 = "\n".join(b)
                    
                    with lock:
                        result = self.result.copy()
                        if len(ask1) == 0 and len(bid1) == 0:
                            # global result
                            
                            exchange_to_match = exchangem
                            instrument_to_match = exchangeSymbolAlias
                            # Finds matching rows based on the values in the specified columns
                            result.reset_index(drop=True, inplace=True)
                            matching_rows = result.loc[(result['Exchange'] == exchange_to_match) & (result['Instrument'] == instrument_to_match)]#0.0058MB
                            
                            if not matching_rows.empty:  
                                result = result.drop(matching_rows.index)
                                result.reset_index(drop=True, inplace=True)
                            
                        else:
                            time1 = dt.datetime.now()
                            time2 = time1 + dt.timedelta(hours=8)
                            
                            ress = {'Time':[time2],'Exchange':[exchangem],'Instrument': [exchangeSymbolAlias],'Asks': [ask1],'Bids':[bid1]}
                            dfs = pd.DataFrame(ress)#0.009596 MB
                            
                            dfn = dfn._append(dfs)#0.007824 MB
                            
                        dfn = dfn.drop_duplicates(subset=['Exchange','Instrument'], keep='last', inplace=False)
                        
                        result = pd.concat([result, dfn])#0.007824 MB
                        #tracemalloc.start()
                        result.reset_index(drop=True, inplace=True)
                        result.drop_duplicates(subset=['Exchange','Instrument'], keep='last', inplace=True)#0.009418 MB-0.01005 MB
                        #current_memory, peak_memory = tracemalloc.get_traced_memory()
                        # log3.info(f"Current memory usage within the function: {current_memory / 10**6} MB")
                        # log3.info(f"Peak memory usage within the function: {peak_memory / 10**6} MB")
                        # tracemalloc.stop()

                        #### clear self.result and update the dataframe inplace
                        self.result.drop(self.result.index,inplace=True)
                        for row, item in result.iterrows():
                            self.result.loc[row] = item

                now = dt.datetime.now()
                hour = now.hour
                minute = now.minute
                second = now.second
                # if you want to test, please set the hour to exchange update time. This is UTC+8:00.
                if hour == 8 and  minute == 3 and 0 <= second <5:
                    try:
                        with lock:
                            self.result.drop(self.result.index,inplace=True)
                        logger.info("The current time is 4pm and the instrument list needs to be updated.")
                        break
                    except Exception as e:
                        logger.error(f"The current time is 4pm, but don't jump out of the loop. {str(e)}")
                        raise e
                    #log1.info("Successful insertion of data")
            
    def run(self):
        resp = self.get_instruments(options=True, exchange=self.exchange)
        my_list = resp['result']['symbols']
        self.instruments = [item for item in my_list if 'ETH' in item or 'BTC' in item]
        t1 = threading.Thread(target=self.stream_cex_markets,name=f"stream_{self.exchange}",daemon=True) 
        t1.start() 
        self.handle_buffer()
        logging.info(f"{self.exchange} thread is over.")


class BookMonitor:
    def __init__(self) -> None:
        self.last_log_time = None
        # shared df
        self.result =pd.DataFrame(columns=('Time','Exchange','Instrument','Asks','Bids'))
        logger.info("BookMonitor Init")

    
    def run_logger_task(self, df):
        current_time = time.time()

        if self.last_log_time is None or current_time - self.last_log_time >= 5 * 60:
            logger.info(f"dataframe.size:{df.size}")
            self.last_log_time = current_time

    def update_Table(self, df):
        with lock:
            dash_df = self.result.copy()

        df1 = pd.DataFrame.from_dict(dash_df)
        df1.drop_duplicates(subset=['Exchange','Instrument'], keep='last', inplace=True)
        self.run_logger_task(df1)
        df = df1.iloc[::-1]
        df.reset_index(drop=True, inplace=True)
        table =  dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            style_cell={
                'whiteSpace': 'pre-wrap',
                'text_align': 'center', 
            },
            data=df.to_dict('records'))
        return table
               
    def threading1(self):
        while True:
            try:
                logger.info("Begin reacquisition of data")
                ts = ThreadStream(self.result, exchange="Okex")
                ts.run() 
                time.sleep(1)
            except Exception as e:
                logger.error(f"Okex failed to get data:{str(e)}")  
                time.sleep(30)

    def threading2(self):
        while True:
            try:
                logger.info("Begin reacquisition of data")
                ts = ThreadStream(self.result, exchange="Deribit")
                ts.run() 
                time.sleep(1)

            except Exception as e:
                logger.error(f"Deribit failed to get data:{str(e)}") 
                time.sleep(30) 

    def threading3(self):
        while True:
            try:
                logger.info("Begin reacquisition of data")
                ts = ThreadStream(self.result, exchange="Binance")
                ts.run() 
                time.sleep(1)

            except Exception as e:
                logger.error(f"Binance failed to get data:{str(e)}") 
                time.sleep(30)

    def main(self):
        t1 = threading.Thread(target=self.threading1,name="fun_thread1",daemon=True) 
        t1.start() 
        
        t2 = threading.Thread(target=self.threading2,name="fun_thread2",daemon=True)
        t2.start()

        t3 = threading.Thread(target=self.threading3,name="fun_thread3",daemon=True)
        t3.start() 

        external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
        self.app = Dash(__name__, external_stylesheets=external_stylesheets)
        self.app.enable_dev_tools(dev_tools_silence_routes_logging=True)
        self.app.layout = html.Div(
            children=[
                html.Span(
                    id='hearttime',
                    children="Format: [price, size, price*size]"
                ),
                html.Div(id='output'),
                dcc.Interval(
                id='interval-component',
                interval = 0.5*1000,
                n_intervals=0)
            ])
        self.app.callback(Output('output', 'children'),
                Input('interval-component', 'n_intervals'))(self.update_Table)
        self.app.run_server(debug=False, host="0.0.0.0", port='8051')

if __name__=='__main__':
    
    bm_bot = BookMonitor()
    bm_bot.main()

    # result =pd.DataFrame(columns=('Time','Exchange','Instrument','Asks','Bids'))
    # ts = ThreadStream(result)
    # resp = ts.get_instruments(options=True, exchange="Deribit")
    # my_list = resp['result']['symbols']
    # tund = [item for item in my_list if 'ETH' in item or 'BTC' in item]
    # asyncio.run(ts.stream_cex_markets(tund, exchange="Deribit"))
    # time.sleep(1)
    # logger.info("Begin reacquisition of data")
