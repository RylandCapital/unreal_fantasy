from multiprocessing import Pool

from classes.fantasy_labs import Fantasylabs
from utils.slates import slates


dates = list(slates['draftkings']['nhl'].keys())[50:]

def run_scrape(date):
    Fantasylabs(site='draftkings', sport='nhl', date=date).scrape(historical=True)

if __name__ == "__main__":
    with Pool(len(dates)) as p:  
        p.map(run_scrape, dates)

Fantasylabs(site='draftkings', sport='nhl', date='1.14.23').scrape(historical=True)

'''need draftkings 50 & 52 there was a length error'''