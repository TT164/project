"""
Description:
    Paradigm WebSocket Asyncio Example.

    Product: DRFQv2

Usage:
    python3.9 paradigm-drfqv2-ws-example.py

Requirements:
    - websocket-client >= 1.2.1
"""

# built ins
import asyncio
import datetime
import sys
import json
from time import sleep
from typing import Dict

# installed
import websockets
import os, sys
from option_functions import RFQ
from dflog import get_logger

PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PATH not in sys.path:   sys.path.append(PATH)

log1 = get_logger(name='log',fmt = '%(asctime)s | %(levelname)s | %(message)s',
                  file_prefix="data",live_stream=True)
log2 = get_logger(name='log',fmt = '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                  file_prefix="info",live_stream=True)
log3 = get_logger(name='msg',fmt = '%(asctime)s | %(levelname)s | %(message)s',
                  file_prefix="msg",live_stream=False)
# from data.para_config import *

class mainLog:
    def __init__(
            self,
            ws_connection_url: str,
            access_key: str
    ) -> None:
        # Async Event Loop
        self.loop =  asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        """ 
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())#Thread and Asynchronous Conflict Join

        self.loop = asyncio.get_event_loop()
        asyncio.set_event_loop(self.loop)#Thread and Asynchronous Conflict Join
        """

        # Instance Variables
        self.ws_connection_url: str = ws_connection_url + f'?api-key={access_key}'
        self.access_key: str = access_key
        self.websocket_client: websockets.WebSocketClientProtocol = None

        # Start Primary Coroutine
        self.loop.run_until_complete(
            self.ws_manager()
        )
        #self.db = self.ws_manager().db
    
    
    async def ws_manager(self) -> None:
        async with websockets.connect(
                self.ws_connection_url,
                ping_interval=None,
                compression=None,
                close_timeout=60
        ) as self.websocket_client:
            
            # Maintain Heartbeat
            self.loop.create_task(
                self.maintain_heartbeat()
            )
                
            # Subscribe to the specified WebSocket Channel
            self.loop.create_task(
                self.ws_operation(
                    operation='subscribe',
                    ws_channel='rfqs'
                )
            )

            while self.websocket_client.open:
                message: bytes = await self.websocket_client.recv()
                message: Dict = json.loads(message)
                
                #logging.info(message)
                if 'params' in message:
                    params = message['params']
                else:
                    log2.info(f"{message}")
                    continue
                log2.info(f"{message}")
                rfq = RFQ(params=params)
                db=rfq.analysis()
                #rfq.acquire_data()
                #message = {**message,**db} 
                aa = rfq.acquire_data() 
                message = {**message,**db,**aa}
                
                log1.info(message)
                sleep(0.5)
            else:
                log2.error("WebSocket connection has broken.")

    async def ws_operation(
            self,
            operation: str,
            ws_channel: str
    ) -> None:
        """
        `subscribes` or `unsubscribes` to the
        specified WebSocket Channel.
        """
        msg: Dict = {
            "id": 2,
            "jsonrpc": "2.0",
            "method": operation,
            "params": {
                "channel": ws_channel
            }
        }

        await self.websocket_client.send(
            json.dumps(
                msg
            )
        )

    async def maintain_heartbeat(self) -> None:
        """
        Send a heartbeat message to keep the connection alive.
        """
        msg: Dict = json.dumps(
            {
                "id": 10,
                "jsonrpc": "2.0",
                "method": "heartbeat"
            }
        )

        while True:
            try:
                await self.websocket_client.send(msg)
                send_time = datetime.datetime.now()
                log3.info(f"{send_time}")
                await asyncio.sleep(5)
            except Exception as e:              
                log2.error(f"Maintain Heartbeat---{str(e)}")
                await asyncio.sleep(5)

def runLog():
    # Paradigm LIVE WebSocket Connection URL
    ws_connection_url: str = 'wss://ws.api.prod.paradigm.trade'
    # Paradigm TEST WebSocket Connection URL
    #ws_connection_url: str = 'wss://ws.api.testnet.paradigm.trade'

    # FSPD Version + Product URL
    ws_connection_url += '/v2/drfq/'


    # Paradigm Access Key
    try:
        access_key = os.getenv("access_key")
        log2.info('get access_key')  
    except:
        log2.error("Don't get access_key")

    mainLog(
        ws_connection_url=ws_connection_url,
        access_key=access_key
    )  

if __name__=="__main__":
    runLog()