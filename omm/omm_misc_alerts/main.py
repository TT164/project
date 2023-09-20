from collections import defaultdict
import os
import threading
import time
import psycopg2
from skpy import Skype
from datetime import datetime,timedelta
import logging
import numpy as np

HOST = os.getenv("host")
PORT = os.getenv("port")
DATABASE = os.getenv("database")
USER = os.getenv("user")
PASSWORD = os.getenv("password")
ACCOUNT_USER = os.getenv("account_user")
ACCOUND_PSD = os.getenv("account_psd")

class VolAlert:
    def __init__(self) -> None:
        self.last_alert_run_ts = 0
        self.last_ex_alert_run_ts = 0
        self.send_bn_time = 0
        self.send_ok_time = 0
        self.send_db_time = 0
        self.last_bn = 0
        self.last_ok = 0
        self.last_db = 0
        
        self.host = HOST
        self.port = PORT
        self.database = DATABASE
        self.user = USER
        self.password = PASSWORD
        self.account_user = ACCOUNT_USER
        self.account_psd = ACCOUND_PSD

        self.skype = Skype(f"{self.account_user}", f"{self.account_psd}")
        self.connection = None

    def connect_to_database(self):
        while True:
            try:
                # Attempting to establish a database connection
                self.connection = psycopg2.connect(
                    host = self.host,
                    port = self.port,
                    database = self.database,
                    user = self.user,
                    password = self.password,
                    
                )
                break  #
            except (psycopg2.Error, Exception) as e:
                logger.error("Database connection error:", str(e))
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)  # Wait for 5 seconds and try connecting again

    def run(self):
        thread1 = threading.Thread(target=self.run_job_one)
        thread2 = threading.Thread(target=self.run_job_second)

        thread1.start()
        thread2.start()
        
    def db_iv_data(self):
        # Connect to PostgreSQL database
        try:
            cursor = self.connection.cursor()
        except:
            self.connect_to_database()
            cursor = self.connection.cursor()

      
        # Query the first row of data with the latest timestamp
        latest_timestamp_query = "SELECT MAX(timestamp) FROM db_iv_deltak"
        cursor.execute(latest_timestamp_query)
        latest_timestamp_row = cursor.fetchone()[0]

        # Calculate time range
        start_time = latest_timestamp_row - timedelta(minutes = 5)

        # Executing Queries Using a Time Range
        query = "SELECT token,maturity, t2m, d50, timestamp FROM db_iv_deltak WHERE timestamp >= %s ORDER BY timestamp DESC"
        t1 = time.time()
        cursor.execute(query, (start_time,))
        t2 = time.time()
        t = round(t2-t1,3)
        logger.info(f"db_iv_deltak database execution query time {t}s")
        #raise ValueError("Invalid value")
        # Retrieve query results
        result = cursor.fetchall()
            
        logger.info("Finished capturing data")

        # Create a dictionary to store data with the same token and property combination
        data_dict = defaultdict(dict)

        # Process query results
        for row in result:
            token, maturity, t2m, d50, timestamp = row
            
            # Create a combination key
            key = (token, maturity)

            # If the combination key already exists, update the latest and oldest d50 values
            if latest_timestamp_row == timestamp: # t
                data_dict[key]["d50_now"] = d50
                data_dict[key]["t2m_now"] = t2m
            else: # t - 1
                data_dict[key]["d50_old"] = d50
                data_dict[key]["t2m_old"] = t2m

        # Calculate the d50 difference for each combination key and print the result
        for key, dict_ in data_dict.items():
            token, maturity = key
            if len(dict_) > 2:
                d50_diff = dict_["d50_now"] - dict_["d50_old"]
                vega = 0.004 * np.sqrt(float(dict_["t2m_now"]))
                res = float(d50_diff) * vega
                
                if res > 0.0025:
                    logger.info(f"Maturity: {maturity}, Token: {token}, d50 diff: {d50_diff}")
                    try:
                        skype = Skype(f"{self.account_user}", f"{self.account_psd}")
                        group = skype.chats.chat("19:3400f99a7d194eeb97853b858039f284@thread.skype")
                        # Send messages to group chat
                        message = f'{token}-{maturity} increased {res}bps'
                        group.sendMsg(message)
                        time.sleep(0.5)
                    except Exception as e:
                        logger.error(str(e))
                    
                elif res < -0.0025:
                    logger.info(f"Maturity: {maturity}, Token: {token}, d50 diff: {d50_diff}")
                    try:
                        skype = Skype(f"{self.account_user}", f"{self.account_psd}")
                        group = skype.chats.chat("19:3400f99a7d194eeb97853b858039f284@thread.skype")
                        # Send messages to group chat
                        message = f'{token}-{maturity} decreased {res}bps'
                        group.sendMsg(message)
                        time.sleep(0.5)
                    except Exception as e:
                        logger.error(str(e))
            else:
                continue
            logger.info(f"{maturity}-{token} do not reach the threshold")
            
    def extrxlog_data(self):
        try:
            cursor = self.connection.cursor()
        except:
            self.connect_to_database()
            cursor = self.connection.cursor()

        current_time = datetime.now()
        five_minutes_ago = current_time - timedelta(hours=8,minutes=5)

        query = "SELECT timestamp,source,exchange,edge_usd_deribit FROM extrxlog WHERE timestamp >= %s ORDER BY timestamp DESC"
        t1 = time.time()
        cursor.execute(query, (five_minutes_ago,))
        t2 = time.time()
        t = round(t2-t1,3)
        logger.info(f"extrxlog database execution query time {t}s")
        results = cursor.fetchall()

        # cursor.close()
        # conn.close()

        total_bn = 0
        total_db = 0
        total_ok = 0
        for row in results:
            timestamp,source,exchange,edge_usd_deribit = row
            if exchange == "BN":
                total_bn += abs(edge_usd_deribit)
            elif exchange == "OK":
                total_ok += abs(edge_usd_deribit)
            elif exchange == "DB" and source == "screen":
                total_db += abs(edge_usd_deribit)
        group = self.skype.chats.chat("19:3400f99a7d194eeb97853b858039f284@thread.skype")
        
        if total_bn > 10000:
            t = datetime.now()
            acquire_time = t.strftime("%Y-%m-%d %H:%M:%S")
            message = f"{acquire_time} Binance edge > 10000 in 5 mins"
            if time.time() - self.send_bn_time >= 180 and total_bn != self.last_bn:
                self.last_bn = total_bn
                logger.info(message)
                group.sendMsg(message)
                self.send_bn_time = time.time()
                logger.info("Successfully sent a message")
                time.sleep(0.5)
        if total_ok > 10000:
            t = datetime.now()
            acquire_time = t.strftime("%Y-%m-%d %H:%M:%S")
            message = f"{acquire_time} Okex edge > 10000 in 5 mins"
            if time.time() - self.send_ok_time >= 180 and total_ok != self.last_ok:
                self.last_ok = total_ok
                logger.info(message)
                group.sendMsg(message)
                self.send_ok_time = datetime.now()
                logger.info("Successfully sent a message")
                time.sleep(0.5)
        if total_db > 50000:
            t = datetime.now()
            acquire_time = t.strftime("%Y-%m-%d %H:%M:%S")
            message = f"{acquire_time} Deribit edge > 50000 in 5 mins"
            if time.time() - self.send_db_time >= 180 and total_db != self.last_db:
                self.last_db = total_db
                logger.info(message)
                group.sendMsg(message)
                self.send_db_time = datetime.now()
        

    # Define scheduled tasks
    def run_job_one(self):
        while True:
            try:
                time.sleep(5)
                current_minute = int(time.strftime("%M")) 
                if current_minute % 5 == 0 and time.time() - self.last_alert_run_ts > 280:
                    self.db_iv_data()
                    self.last_alert_run_ts = time.time()     
            except Exception as e:
                logger.error(str(e)) 
                time.sleep(5)

    def run_job_second(self):
        while True:
            try:
                time.sleep(1)
                if time.time() - self.last_ex_alert_run_ts >= 30:
                    self.extrxlog_data()
                    self.last_ex_alert_run_ts = time.time()     
            except Exception as e:
                logger.error(str(e)) 
                time.sleep(5)


if __name__ == '__main__':
    logger = logging.Logger('omm_misc_alerts')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    alert_1 = VolAlert()
    alert_1.run()
