# Download from ->  https://portal.hdfgroup.org/display/support/HDFView+3.1.2#files
# Download File -> HDFView-3.1.2-win10_64-vs16.zip (sha256)	Pre-built binary	Windows 10 64-bit
# Download File from same page -> hdfview.bat (Windows) (Put it in the same folder of program)

import h5py
import typing
import numpy as np
import logging
import pandas as pd
import time

logger = logging.getLogger()

class Hdf5Client:
    
    def __init__(self, exchange: str):
        self.hf = h5py.File(f"data/{exchange}.h5", mode = "a") # Append Data
        # Flush to skip errors
        self.hf.flush()
        
        
    def create_dataset(self, symbol: str):
        if symbol not in self.hf.keys():
            self.hf.create_dataset(symbol, (0,6), maxshape = (None, 6), dtype = "float64")
            
            
    def write_data(self, symbol: str, data: typing.List[typing.Tuple]):
        
        oldest_ts, most_recent_ts = self.get_first_last_timestamp(symbol)
        
        if oldest_ts is None:
            oldest_ts = float("inf")
            most_recent_ts = 0             
        
        filtered_data = []
        # Order data and filter
        for d in data:
            if d[0] < oldest_ts:
                filtered_data.append(d)
            elif d[0] > most_recent_ts:
                filtered_data.append(d)
        
        if len(filtered_data) == 0:
            logger.warning(f"[+] No data to insert for {symbol}")
            return
        
        data_array = np.array(data)
        
        self.hf[symbol].resize(self.hf[symbol].shape[0] + data_array.shape[0], axis = 0) 
        self.hf[symbol][-data_array.shape[0]:] = data_array
        # Flush
        self.hf.flush()
    
    
    def get_data(self, symbol:str, from_time: int, to_time:int) -> typing.Union[None, pd.DataFrame]:
        
        start_query = time.time()
        
        existing_data = self.hf[symbol][:]
        
        if len(existing_data) == 0:
            return None
        
        data = sorted(existing_data, key = lambda x:x[0])
        data = np.array(data)
        
        df = pd.DataFrame(data, columns = ["timestamp", "open", "high", "low", "close", "volume"])
        df = df[(df["timestamp"] >= from_time) & (df["timestamp"] >= from_time)]
        
        df["timestamp"] = pd.to_datetime(df["timestamp"].values.astype(np.int64), unit = "ms")
        df.set_index("timestamp", drop = True, inplace = True)
        
        query_time = time.time() - start_query
        
        logger.info("Retrieved %s %s data from database in %s seconds", len(df), symbol, query_time)
        
        return df
    
    
    def get_first_last_timestamp(self, symbol: str) -> typing.Union[typing.Tuple[None, None], typing.Tuple[float, float]]:
        existing_data = self.hf[symbol][:]
        
        if len(existing_data) == 0:
            return None, None
        
        first_ts = min(existing_data[:, 0]) # min(existing_data, key = lambda x:x[0])[0]
        last_ts = max(existing_data[:, 0]) # max(existing_data, key = lambda x:x[0])[0]
        
        return first_ts, last_ts