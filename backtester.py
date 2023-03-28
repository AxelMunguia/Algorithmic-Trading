from database import Hdf5Client
from utils import resample_timeframe, START_PARAMS
import strategies.obv, strategies.ichimoku, strategies.support_resistance
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", 1_000)

def run(exchange: str, symbol: str, strategy: str, tf: str, from_time: int, to_time: int):
    
    params_des = START_PARAMS[strategy.lower()]
    
    params = {}
    
    for key, value in params_des.items():
        while True:
            try:
                params[key] = value["type"](input(value["name"] + ":"))
                break
            except ValueError:
                continue
    
    if strategy == "obv":
        h5_db = Hdf5Client(exchange)
        data = h5_db.get_data(symbol, from_time, to_time)
        data = resample_timeframe(data, tf)
        
        pnl, max_dd= strategies.obv.backtest(data, ma_period = params["ma_period"])
        
        return pnl, max_dd
        
    elif strategy == "ichimoku":
        h5_db = Hdf5Client(exchange)
        data = h5_db.get_data(symbol, from_time, to_time)
        data = resample_timeframe(data, tf)
        data.columns = ["Open", "High", "Low", "Close", "Volume"]
        
        pnl, max_dd = strategies.ichimoku.backtest(data, tenkan_period = params["kijun"], kijun_period = params["tenkan"])
        
        return pnl, max_dd
        
    elif strategy == "sup_res":
        h5_db = Hdf5Client(exchange)
        data = h5_db.get_data(symbol, from_time, to_time)
        data = resample_timeframe(data, tf)
        # data.columns = ["Open", "High", "Low", "Close", "Volume"]
        
        pnl, max_dd = strategies.support_resistance.backtest(data, min_points = params["min_points"], min_diff_points = params["min_diff_points"],
                                                     rounding_nb = params["rounding_nb"], take_profit = params["take_profit"], stop_loss = params["stop_loss"]) 
        
        return pnl, max_dd