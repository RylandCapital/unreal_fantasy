import numpy as np
from multiprocessing import Pool

from classes.fantasy_labs import Fantasylabs
from classes.optimizer import balanced
from classes.dataiku_nhl import FantasylabsNHL

from utils.slates import slates


'''scraping'''
dates = list(slates['fanduel']['nfl'].keys())

def run_scrape(date):
    Fantasylabs(site='fanduel', sport='nfl', date=date).scrape(historical=True)

if __name__ == "__main__":
    with Pool(len(dates)) as p:  
        p.map(run_scrape, dates)


'''optimizer'''
dates = list(slates['draftkings']['nhl'].keys())
lists = []
for i in np.arange(0,len(dates),2):
  try:
    lists.append([dates[i],dates[i+1]])
  except:
    lists.append([dates[i]])

def run_optimization(list_of_dates):
    balanced(list_of_dates, 'nhl', 'draftkings', True, 1000, live_tag='')

if __name__ == "__main__":
    with Pool(len(lists)) as p:  
        p.map(run_optimization, lists)


'''dataiku'''
dates = list(slates['fanduel']['nhl'].keys())[50:]

def run_build(date):
    FantasylabsNHL(site='fanduel', date=date, historical=True).build()

if __name__ == "__main__":
    with Pool(len(dates)) as p:  
        p.map(run_build, dates)



