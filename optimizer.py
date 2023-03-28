from utils import START_PARAMS
import typing
from database import Hdf5Client
from utils import resample_timeframe
from models import BacktestResult
import random
import strategies.obv, strategies.ichimoku, strategies.support_resistance
from copy import deepcopy

class Nsga2:
    
    def __init__(self, exchange: str, symbol: str, strategy: str, tf: str, from_time: int, to_time: int,
                 population_size: int):
        # Define
        self.exchange = exchange
        self.symbol = symbol
        self.strategy = strategy
        self.tf = tf
        self.from_time = from_time
        self.to_time = to_time
        self.population_size = population_size
        self.params_data = START_PARAMS[self.strategy]
        self.population_params = []
        
        if self.strategy in ["obv", "ichimoku", "sup_res"]:
            h5_db = Hdf5Client(exchange)
            self.data = h5_db.get_data(self.symbol, self.from_time, self.to_time)
            self.data = resample_timeframe(self.data, self.tf)
            
            
    def create_initial_population(self) -> typing.List[BacktestResult]:
        
        """
        Creates Initial Population
        """
        # Population
        population = []
        
        while len(population) < self.population_size:
            backtest = BacktestResult()
            for key, value in self.params_data.items():
                if value["type"] == int:
                    backtest.parameters[key] = random.randint(value["min"], value["max"])
                elif value["type"] == float:
                    backtest.parameters[key] = round(random.uniform(value["min"], value["max"]), value["decimals"])
                
            if backtest not in population:
                population.append(backtest)
                self.population_params.append(backtest.parameters)
            
        return population
    
    
    def crowding_distance(self, evaluated_population: typing.List[BacktestResult]) -> typing.List[BacktestResult]:
        # Get Crowding Distance for each individual once thier pnl and max_dd were calculated
        # A grater crowding distance means better.
        for objective  in ["pnl", "max_dd"]:
            # Sort Evaluated Population
            population = sorted(evaluated_population, key = lambda x: getattr(x, objective))
            # Get min and max value
            min_value = getattr(min(population, key = lambda x: getattr(x, objective)), objective)
            max_value = getattr(max(population, key = lambda x: getattr(x, objective)), objective)
            # Exclude First and last one since they have the highest and lowest value
            # More than two values can be inf since a for is being used
            population[0].crowding_distance = float("inf")
            population[-1].crowding_distance = float("inf")
            for i in range(1, len(population) - 1):
                distance = getattr(population[i + 1], objective) - getattr(population[i - 1], objective)
                # Avoiding Division by 0
                if max_value - min_value != 0:
                    # Best individuals will have a bigger crowding distance since denominator will be smaller
                    distance = distance / (max_value - min_value)
                    population[i].crowding_distance += distance
                
        return population
            
   
    def create_new_population(self, fronts: typing.List[typing.List[BacktestResult]]) -> typing.List[typing.List[BacktestResult]]:
        # Create next generation of population
        new_pop = []
        # The code will choose the individuals with best performance
        for front in fronts:
            # Loop front by front untill we create a new population with the best individuals,
            # so if in F1 (front with best performance has 5 individuals 15 will be left)
            # so the code will continue to F2 to find the next n best individuals untill 
            # a new population is formed
            if len(new_pop) + len(front) > self.population_size:
                max_individuals = self.population_size - len(new_pop)
                if max_individuals > 0:
                    new_pop += sorted(front, key = lambda x: getattr(x, "crowding_distance"))[-max_individuals:]
            else:
                new_pop += front
   
        return new_pop
    
    
    def create_offspring_population(self, population: typing.List[BacktestResult]) -> typing.List[BacktestResult]:
        
        offspring_pop = []
        
        # Offspring Population length must be equal to population_size
        while len(offspring_pop) != self.population_size:
            parents: typing.List[BacktestResult] = []
            # Select 2 parents to create one new child with the best of the two parents
            for i in range(2):
                random_parents = random.sample(population, k = 2)
                # Check who's the best parent
                if random_parents[0].rank != random_parents[1].rank:
                    best_parent = min(random_parents, key = lambda x: getattr(x, "rank"))
                else:
                    best_parent = max(random_parents, key = lambda x: getattr(x, "crowding_distance"))
                # Append the parent with the best qualities
                parents.append(best_parent)
            new_child = BacktestResult()
            new_child.parameters = deepcopy(parents[0].parameters)
            
            # Croosover 
            number_of_crossovers = random.randint(1, len(self.params_data))
            # Choose sample among params from each indicator
            params_to_cross = random.sample(list(self.params_data.keys()), k = number_of_crossovers)
            
            # (It can change from 1 to all params from first parent)
            for p in params_to_cross:
                new_child.parameters[p] = deepcopy(parents[1].parameters[p])
        
            # Mutation
            number_of_mutations = random.randint(0, len(self.params_data))
            params_to_change = random.sample(list(self.params_data.keys()), k = number_of_mutations)
            
            for p in params_to_change:
                mutation_strength = random.uniform(-2, 2)
                new_child.parameters[p] = self.params_data[p]["type"](new_child.parameters[p] * (1 + mutation_strength))
                # Check boundaries
                new_child.parameters[p] = max(new_child.parameters[p], self.params_data[p]["min"])
                new_child.parameters[p] = min(new_child.parameters[p], self.params_data[p]["max"])
                
                # Round
                if self.params_data[p]["type"] == float:
                    new_child.parameters[p] = round(new_child.parameters[p], self.params_data[p]["decimals"])
            
            # Check params must be according the strategy
            new_child.parameters = self._params_constraints(new_child.parameters)
            
            # Check if and individual of the new population is not repeated
            if new_child.parameters not in self.population_params:
                offspring_pop.append(new_child)
                self.population_params.append(new_child.parameters)
                
        return offspring_pop 
    
    
    def _params_constraints(self, params: typing.Dict) -> typing.Dict:
        if self.strategy == "obv":
            pass
        elif self.strategy == "sup_res":
            pass
        elif self.strategy == "ichimoku":
            params["kinju"] = max(params["kijun"] + 1, params["tenkan"])
            
        return params
    
    
    def non_dominated_sorting(self, population: typing.Dict[int, BacktestResult]) -> typing.List[typing.List[BacktestResult]]:
        fronts = []
        # Compare all against all to find the pareto frontier (non-dominated individuals)
        for id_1, indiv_1 in population.items():
            for id_2, indiv_2 in population.items():
                # Check if indiv_1 dominates others individuals
                if indiv_1.pnl >= indiv_2.pnl and indiv_1.max_dd <= indiv_2.max_dd \
                    and (indiv_1.pnl > indiv_2.pnl or indiv_1.max_dd < indiv_2.max_dd):
                    # Add to indiv_1 dominates own list
                    indiv_1.dominates.append(id_2) # id_2 is a number [0, 1, 2, ..., len(random_samples_created)]
                elif indiv_2.pnl >= indiv_1.pnl and indiv_2.max_dd <= indiv_1.max_dd \
                    and (indiv_2.pnl > indiv_1.pnl or indiv_2.max_dd < indiv_1.max_dd):    
                    # Add to indiv_2 dominates own list
                    indiv_1.dominated_by += 1
            if indiv_1.dominated_by == 0:
                if len(fronts) == 0:
                    fronts.append([])
                fronts[0].append(indiv_1)
                # It will affect the value added to fronts since they're the same
                indiv_1.rank = 0
                
        # Find the next most dominating individuals    
        i = 0
        while True:
            fronts.append([])
            for indiv_1 in fronts[i]:
                # Decrease one to find the new non-dominated individual
                for indiv_2_id in indiv_1.dominates:
                    population[indiv_2_id].dominated_by -= 1
                    if population[indiv_2_id].dominated_by == 0:
                        # Check to not duplicate
                        if population[indiv_2_id] not in fronts[i + 1]:
                            fronts[i + 1].append(population[indiv_2_id])
                            # It will affect the value added to fronts since they're the same
                            population[indiv_2_id].rank = i + 1
            # Check if a new non-dominated individual was found
            if len(fronts[i + 1]) > 0:
                i += 1
            # Non-dominated individual was found
            else:
                del fronts[-1]
                break
            
        return fronts
            
            
    def evaluate_population(self, population_individuals: typing.List[BacktestResult]) -> typing.List[BacktestResult]:
        if self.strategy == "obv":
            for bt in population_individuals:
                bt.pnl, bt.max_dd= strategies.obv.backtest(self.data, ma_period = bt.parameters["ma_period"])
                # If no profit, insert -inf so it does not keep in the algorithm when optimizing
                if bt.pnl == 0:
                        bt.pnl = -float("inf")
                        bt.max_dd = float("inf")
                        
            return population_individuals
            
        elif self.strategy == "ichimoku":
            self.data.columns = ["Open", "High", "Low", "Close", "Volume"]
            for bt in population_individuals:
                bt.pnl, bt.max_dd = strategies.ichimoku.backtest(self.data, tenkan_period = bt.parameters["kijun"], kijun_period = bt.parameters["tenkan"])
                # If no profit, insert -inf so it does not keep in the algorithm when optimizing
                if bt.pnl == 0:
                    bt.pnl = -float("inf")
                    bt.max_dd = float("inf")
                    
            self.data.columns = ["open", "high", "low", "close", "volume"]
            
            return population_individuals
            
        elif self.strategy == "sup_res":
            for bt in population_individuals:
                bt.pnl, bt.max_dd = strategies.support_resistance.backtest(self.data, min_points = bt.parameters["min_points"], min_diff_points = bt.parameters["min_diff_points"],
                                                             rounding_nb = bt.parameters["rounding_nb"], take_profit = bt.parameters["take_profit"], stop_loss = bt.parameters["stop_loss"]) 
                # If no profit, insert -inf so it does not keep in the algorithm when optimizing
                if bt.pnl == 0:
                    bt.pnl = -float("inf")
                    bt.max_dd = float("inf")
                    
            return population_individuals