import numpy as np
from multiprocessing import Pool

from classes.fantasy_labs import Fantasylabs
from classes.optimizer import balanced
from classes.dataiku_nhl import DataikuNHL
from classes.dataiku_nfl import DataikuNFL
from classes.ticket import Ticket
from classes.post_slate import PostSlate

from utils.slates import slates
from utils.uploader import upload



#############################################################################
#HISTOIRCAL TOOLS


'''scraping'''
def run_scrape(date):
    Fantasylabs(site='fanduel', sport='nfl', date='date').scrape(historical=True, delete_dups=False)

if __name__ == "__main__":

    dates = list(slates['fanduel']['nfl'].keys())

    with Pool(len(dates)) as p:  
        p.map(run_scrape, dates)


'''optimizer'''
def run_optimization(list_of_dates):
    balanced(list_of_dates, 'nfl', 'fanduel', True, 1000, live_tag='')

if __name__ == "__main__":

    dates = list(slates['draftkings']['nfl'].keys())

    lists = []
    for i in np.arange(0,len(dates),2):
        try:
            lists.append([dates[i],dates[i+1]])
        except:
            lists.append([dates[i]])

    with Pool(len(lists)) as p:  
        p.map(run_optimization, lists)


'''dataiku'''
def run_build(date):
    DataikuNFL(site='draftkings', date=date, historical=True).build()

if __name__ == "__main__":

    dates = list(slates['draftkings']['nfl'].keys())

    with Pool(len(dates)) as p:  
        p.map(run_build, dates)


'''upload ticket creation'''
if __name__ == "__main__":
    upload(site='fanduel', sport='nhl', historical=True)

##############################################################################










#############################################################################
#LIVE GAMEDAY TOOLS


'''scraping'''
if __name__ == "__main__":
    Fantasylabs(site='fanduel', sport='nfl', date='9.6.23').scrape(historical=False, delete_dups=False, site_file='week22023')


'''optimizer'''
def run_optimization_live(live_tag):
    balanced(['9.10.23'], 'nfl', 'fanduel', False, 50000, live_tag=live_tag, sabersim=False)

if __name__ == "__main__":
    letters = [
               'a','b','c','d','e',
               'f','g','h','i','j',
               'k','l','m','n','o',
               'p','q','r','s','t',
               'u','v','w','x','y'
               ]
    
    with Pool(len(letters)) as p:  
        p.map(run_optimization_live, letters)


'''dataiku'''
def run_build_live(live_tag):
    DataikuNFL(site='fanduel', date='9.6.23', historical=False, live_tag=live_tag).build()

if __name__ == "__main__":

    letters = [
               'a','b','c','d','e',
               'f','g','h','i','j',
               'k','l','m','n','o',
               'p','q','r','s','t',
               'u','v','w','x','y'
               ]

    with Pool(len(letters)) as p:  
        p.map(run_build_live, letters)


'''upload ticket creation'''
if __name__ == "__main__":
    upload(site='fanduel', sport='nfl', historical=False)

##############################################################################











#############################################################################
#TICKET CREATION

if __name__ == "__main__":
    ids = Ticket(
            '9.6.23',
            'draftkings', 
            'nfl', 
            site_file='week12023'
            )\
            .optimize_upload_file(
                roster_size=150, 
                pct_from_opt_proj=.808,#.808 
                max_pct_own=.33,
                other_site_min=0, 
                sabersim_only=False,
                removals=[28792643,'92765-169776',28792693,'92765-39716',
                          28792401,'92765-70027', 28792713, '92765-33260',
                          28792703])



##############################################################################
#POST SLATE REVIEW
if __name__ == "__main__":
    PostSlate(
            '9.6.23',
            'fanduel', 
            'nfl', 
            site_file='week12023'
            )\
            .anaylze(removals=[28792643,'92765-169776',28792693,'92765-39716',
                          28792401,'92765-70027', 28792713, '92765-33260',
                          28792703],
                     pct_from_opt_proj=.808,
                     max_pct_own=.33, 
                     other_site_min_compare=51000)