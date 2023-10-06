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
    Fantasylabs(site='fanduel', sport='nfl', date='9.27.23').scrape(historical=True, delete_dups=False)

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
    Fantasylabs(site='draftkings', sport='nfl', date='9.27.23').scrape(historical=False, delete_dups=False, site_file='week42023')


'''optimizer'''
def run_optimization_live(live_tag):
    balanced(['9.27.23'], 'nfl', 'draftkings', False, 90000, live_tag=live_tag, sabersim=False)
    balanced(['9.27.23'], 'nfl', 'fanduel', False, 90000, live_tag=live_tag, sabersim=False)
    if live_tag=='a':
        balanced(['9.27.23'], 'nfl', 'draftkings', False, 100, live_tag='a', sabersim=True)

if __name__ == "__main__":
    letters = [
               'a','b','c','d','e',
               'f','g','h','i','j',
               'k','l','m','n','o',
               'p','q','r','s','t',
               'u','v','w','x','y',
               'z'
               ]
    
    with Pool(len(letters)) as p:  
        p.map(run_optimization_live, letters)


'''dataiku'''
def run_build_live(live_tag):
    DataikuNFL(site='draftkings', date='9.27.23', historical=False, live_tag=live_tag).build()
    DataikuNFL(site='fanduel', date='9.27.23', historical=False, live_tag=live_tag).build()

if __name__ == "__main__":

    letters = [
               'a','b','c','d','e',
               'f','g','h','i','j',
               'k','l','m','n','o',
               'p','q','r','s','t',
               'u','v','w','x','y',
               'z'
               ]

    with Pool(len(letters)) as p:  
        p.map(run_build_live, letters)


'''upload ticket creation'''
if __name__ == "__main__":
    upload(site='draftkings', sport='nfl', historical=False)
    upload(site='fanduel', sport='nfl', historical=False)


##############################################################################











#############################################################################
#TICKET CREATION

if __name__ == "__main__":
    ids = Ticket(
            '9.27.23',
            'draftkings', 
            'nfl', 
            site_file='week42023'
            )\
            .optimize_upload_file(
                roster_size=10, 
                pct_from_opt_proj=.90, #.808
                max_pct_own=.33,
                other_site_min=0, 
                sabersim_only=False,
                h2h=False,
                h2h_rank=.99, #team ownership and floor pct rank, only for H2H=True
                min_team_sal=50000, # min team salary used, only for H2H=True
                removals=[29816345, '94274-54604',
                           29816337, '94274-54879',
                             29816403, '94274-80001',
                             29816705, '94274-56018',
                               29817139, '94274-47870',
                               29816821, '94274-112192',
                               29816429, '94274-71845',
                               29816749, '94274-86687',
                               29816344, '94274-24849',
                               29816841, '94274-73111'])





##############################################################################
#POST SLATE REVIEW
if __name__ == "__main__":
    PostSlate(
            '9.27.23',
            'draftkings', 
            'nfl', 
            site_file='week42023'
            )\
            .anaylze(removals=[29816345, '94274-54604',
                           29816337, '94274-54879',
                             29816403, '94274-80001',
                             29816705, '94274-56018',
                               29817139, '94274-47870',
                               29816821, '94274-112192',
                               29816429, '94274-71845',
                               29816749, '94274-86687',
                               29816344, '94274-24849',
                               29816841, '94274-73111'],
                     pct_from_opt_proj=.91,
                     max_pct_own=.33, 
                     other_site_min_compare=60200,
                     sabersim_only=False)