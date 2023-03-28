

class BacktestResult:
    
    def __init__(self):
        self.pnl: float = 0.0
        self.max_dd: float = 0.0
        self.parameters: dict = dict()
        self.dominated_by: int = 0
        self.dominates: list = []
        self.rank: int = 0
        self.crowding_distance: float = 0.0
    
    def __repr__(self):
        return f"PNL = {round(self.pnl, 2)}, Max. Drawdown = {round(self.max_dd, 2)}, Parameters = {self.parameters}, " \
               f"Rank = {self.rank}, Crowding Distance = {self.crowding_distance}"
        
    def reset_results(self):
        self.dominated_by = 0
        self.dominates.clear()
        self.rank = 0
        self.crowding_distance = 0.0
    
