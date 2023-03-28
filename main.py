# logging provides a flexible framework for emitting log messages from Python programs
import logging
import datetime
from exchanges.binance import BinanceClient
from exchanges.ftx import FtxClient
from data_collector import collect_all
from utils import TF_EQUIV
import backtester, optimizer
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 300)
pd.set_option("display.width", 1_000)

# Set logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Define formatter
formatter = logging.Formatter("%(asctime)s %(levelname)s :: %(message)s")

# Define stream logger for error
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

# Define File Handler for logger
filename = "info.log"
file_handler = logging.FileHandler(filename)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG) # DEBUG will give more information

# Add to logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

# To show INFO log
# logger.info("This is an info log") # Show in console
# logger.debug("This is log file") # Show in file info.log

if __name__ == '__main__':
    # Mode
    while True:
        mode = input("Choose the program code (data / backtest / optimize): ").lower()
        if mode in ["data", "backtest", "optimize"]:
            break
    # Select exchanges
    while True:
        exchange = input("Choose an exchange (ftx / binance): ").lower()
        if exchange in ["ftx", "binance"]:
            break
    
    if exchange == "binance":
        # Instantiate
        client = BinanceClient(True)
        # Get historical data
        # print(len(client.get_historical_data("BTCUSDT")))
    elif exchange == "ftx":
        client = FtxClient()
        # Get historical data
        # print(client.get_historical_data("BTC-PERP"))
    
    while True:
        symbol = input("Choose a symbol: " + "(" + " / ".join(client.symbols) + "): ").upper()
        if symbol in client.symbols:
            break
        
    if mode == "data":
        collect_all(client, exchange, symbol)
        
    elif mode in ["backtest", "optimize"]:
        # Strategies
        strategies = ["obv", "ichimoku", "sup_res"]
        # Choose one
        while True:
            stra = input(f"Choose a strategy ({' / '.join(strategies)}): ").lower()
            if stra in strategies:
                break

        # Choose timeframe
        while True:
            tf = input(f"Choose a timeframe: ({' / '.join(TF_EQUIV.keys())}): ").lower()
            if tf in TF_EQUIV:
                break            
            
        # From
        while True:
            from_time = input("Backtest from (yyyy-mm-dd or Press Enter: ").lower()
            if from_time == "":
                from_time = 0
                break
            else:
                try:
                    from_time = int(datetime.datetime.strptime(from_time, "%Y-%m-%d").timestamp() * 1_000)
                    break
                except:
                    continue
        # To
        while True:
            to_time = input("Backtest to (yyyy-mm-dd or Press Enter").lower()
            if to_time == "":
                to_time = int(datetime.datetime.now().timestamp() * 1_000)
                break
            else:
                try:
                    to_time = int(datetime.datetime.strptime(to_time, "%Y-%m-%d").timestamp() * 1_000)
                    break
                except:
                    continue
        if mode == "backtest":
            # Backtest
            print(backtester.run(exchange, symbol, stra, tf, from_time, to_time))
            
        elif mode == "optimize":
            
            # Population size
            while True:
               try:
                   pop_size = int(input("Choose a population size: "))
                   break
               except ValueError:
                   continue
                  
            # Iterations
            while True:
                   try:
                       generations = int(input("Choose a number of  generations: "))
                       break
                   except ValueError:
                       continue
            # Nsga2
            
            # Initialize
            nsga2 = optimizer.Nsga2(exchange, symbol, stra, tf, from_time, to_time, pop_size)
            
            # Create Population
            initial_population = nsga2.create_initial_population()
            
            # Evaluate
            evaluated_population = nsga2.evaluate_population(initial_population)
            
            # Add crowding distance to see which are better
            # This crowding distance will be used to create a new sample with the 
            # best of two parents chosen randomly
            p_population = nsga2.crowding_distance(evaluated_population)
            
            g = 0
            while g < generations:
                # Create an ofspring populatin. It is supposed to be better from 
                # intial population, since it is taking the best from 2 parents
                # chosen randomly.
                
                # Add offspring population (A "better" one)
                q_population = nsga2.create_offspring_population(p_population)
                
                # Evaluate new offspring population
                q_population = nsga2.evaluate_population(q_population)
                
                # Add populations
                r_population = p_population + q_population
                
                # Remove previous params from strategies for new generation
                nsga2.population_params.clear()
                
                # Reset all params except pnl and max_dd
                # They will be reset because sample is now bigger and they must be
                # computed again
                i = 0
                population = dict()
                for bt in r_population:
                    bt.reset_results()
                    nsga2.population_params.append(bt.parameters)
                    population[i] = bt
                    i += 1

                # Find non-dominated individuals (F1, F2, F3) according to its levels
                # F1 Frontier dominates F2 Frontier, F2 Frontier dominates F3 Frontier, ... until non-dominated individuals are found
                fronts = nsga2.non_dominated_sorting(population)
                
                # Get crowding distance of old population and the new "better" one
                # but by fronts
                for j in range(len(fronts)):
                    fronts[j] = nsga2.crowding_distance(fronts[j])

                p_population = nsga2.create_new_population(fronts)

                
                print(f"{float((g + 1)/generations)} %", end = "\n")
                
                g += 1
            
            print("\n")
            
                
            # Print fronts
            for individual in p_population:
                print("\n", individual)
