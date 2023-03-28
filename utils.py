import datetime
import pandas as pd

TF_EQUIV = {"1m": "1Min", "5m": "5Min", "15m": "15Min", "30m": "30Min", "1h": "1H", "4h": "4H",
            "12h": "12H", "1d": "D"}

START_PARAMS = {
    
    "obv": {
            "ma_period": {"name": "MA Period", "type": int, "min": 2, "max": 200}
    },
    "ichimoku": {
        "kijun": {"name": "Kijun Period", "type": int, "min": 2, "max": 200},
        "tenkan": {"name": "Tenkan Period", "type": int, "min": 2, "max": 200}
    },
    "sup_res": {
        "min_points": {"name": "Min. Points", "type": int, "min": 2, "max": 200},
        "min_diff_points": {"name": "Min. Difference Between Points", "type": int, "min": 2, "max": 100},
        "rounding_nb": {"name": "Rounding Number", "type": float, "min": 10, "max": 500, "decimals": 2},
        "take_profit": {"name": "Take Profit %", "type": float, "min": 1, "max": 40, "decimals": 2},
        "stop_loss": {"name": "Stop Loss %", "type": float, "min": 2, "max": 200, "decimals": 2},
    },    
}


def ms_to_dt(ms: int) -> datetime.datetime:
    return datetime.datetime.utcfromtimestamp(ms / 1_000)

def resample_timeframe(data: pd.DataFrame, tf: str) -> pd.DataFrame:
    
    return data.resample(TF_EQUIV[tf]).agg({
        "open": "first", # From all prices between t and t + tf keep the first
        "high": "max", # From all prices between t and t + tf keep the max
        "low": "min", # From all prices between t and t + tf keep the min
        "close": "last",  # From all prices between t and t + tf keep the last
        "volume": "sum"  # From all prices between t and t + tf keep the sum of all of them
        })