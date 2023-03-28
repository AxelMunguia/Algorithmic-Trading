import logging
import typing

import time

from database import Hdf5Client
from exchanges.binance import BinanceClient
from exchanges.ftx import FtxClient
from utils import *


# Get logger
logger = logging.getLogger()

def collect_all(client: typing.Union[BinanceClient, FtxClient], exchange:str, symbol: str):
    
    h5_db = Hdf5Client(exchange)
    h5_db.create_dataset(symbol)
    
    oldest_ts, most_recent_ts = h5_db.get_first_last_timestamp(symbol)
    
    # Initial Request
    if oldest_ts is None:
        # Minus 6000 because current candle is not finished (60 seconds but in miliseconds)
        data = client.get_historical_data(symbol, end_time = int(time.time() * 1_000 - 6_000))
    
        # Check if data
        if len(data) == 0:
            logger.warning(f"{exchange}: {symbol}: No initial data found")
            return None
        else:
            format_input = (exchange, symbol, len(data), ms_to_dt(data[0][0]), ms_to_dt(data[-1][0]))
            logger.info("{}: {}: Collected {} intial data from {} to {}".format(*format_input))
        
        # Update
        oldest_ts = data[0][0]
        most_recent_ts = data[-1][0] 
        # Write Data
        h5_db.write_data(symbol, data)
    
    data_to_insert = []
    # Most Recent Data    
    while True:
        data = client.get_historical_data(symbol, start_time = int(most_recent_ts + 6_0000))
        # In case an error occurs for the request
        if data is None:
            time.sleep(4) 
            continue
        
        # To know if there is no more info to ask for
        if len(data)<2:
            break
        
        # Don't take the last one cause it can be the unfinished current minute
        data = data[:-1]
        
        data_to_insert += data
        
        if len(data_to_insert) >= 10_000:
            # Write Data
            h5_db.write_data(symbol, data_to_insert)
            data_to_insert.clear()
            
        
        if data[-1][0] > most_recent_ts:
            most_recent_ts = data[-1][0]
            
        # Inform 
        format_input = (exchange, symbol, len(data), ms_to_dt(data[0][0]), ms_to_dt(data[-1][0]))
        logger.info("{}: {}: Collected {} recent data from {} to {}".format(*format_input))

        # Sleep so does not crash because of binance
        time.sleep(1.1)
        
    # Write Data
    h5_db.write_data(symbol, data_to_insert)
    data_to_insert.clear()
    
    # Older Data
    while True:
         data = client.get_historical_data(symbol, end_time = int(oldest_ts - 6_0000))
         # In case an error occurs for the request
         if data is None:
             time.sleep(4) 
             continue
         
         # To know if there is no more info to ask for
         if len(data) == 0:
             logger.info("{}: {}: Stopped older data data collection because no data was found before {}".format(exchange,
                                                                                                                 symbol,
                                                                                                                 ms_to_dt(oldest_ts)))
             break
         else:
             data_to_insert += data
             
         if len(data_to_insert) >= 10_000:
            # Write Data
            h5_db.write_data(symbol, data_to_insert)
            data_to_insert.clear()
            
        
         # Check
         if data[-1][0] < most_recent_ts:
             oldest_ts = data[0][0]
             
         # Inform 
         format_input = (exchange, symbol, len(data), ms_to_dt(data[0][0]), ms_to_dt(data[-1][0]))
         logger.info("{}: {}: Collected {} older data from {} to {}".format(*format_input))
         
         # Sleep so does not crash because of binance
         time.sleep(1.1)
    # Write Data
    h5_db.write_data(symbol, data_to_insert)
    
    
    
    
    
