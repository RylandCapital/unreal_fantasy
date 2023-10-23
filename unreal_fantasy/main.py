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
    Fantasylabs(site='draftkings', sport='nfl', date=date).scrape(historical=True, delete_dups=True)

if __name__ == "__main__":

    dates = list(slates['draftkings']['nfl'].keys())

    with Pool(len(dates)) as p:  
        p.map(run_scrape, dates)


'''optimizer'''
def run_optimization(list_of_dates):
    balanced(list_of_dates, 'nfl', 'draftkings', True, 1000, live_tag='')

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
    DataikuNFL(site='fanduel', date=date, historical=True).build()

if __name__ == "__main__":

    dates = list(slates['fanduel']['nfl'].keys())[30:]

    with Pool(len(dates)) as p:  
        p.map(run_build, dates)


'''upload ticket creation'''
if __name__ == "__main__":
    upload(site='fanduel', sport='nfl', historical=True)
    upload(site='draftkings', sport='nfl', historical=True)

##############################################################################










#############################################################################
#LIVE GAMEDAY TOOLS


'''scraping'''
if __name__ == "__main__":
    Fantasylabs(site='draftkings', sport='nfl', date='10.18.23').scrape(historical=False, delete_dups=False, site_file='week72023')


'''optimizer'''
def run_optimization_live(live_tag):
    #balanced(['10.18.23'], 'nfl', 'draftkings', False, 90000, live_tag=live_tag, sabersim=False)
    balanced(['10.18.23'], 'nfl', 'fanduel', False, 45000, live_tag=live_tag, sabersim=False)
    if live_tag=='a':
        balanced(['10.18.23'], 'nfl', 'draftkings', False, 100, live_tag='a', sabersim=True)

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
    DataikuNFL(site='draftkings', date='10.18.23', historical=False, live_tag=live_tag).build()
    DataikuNFL(site='fanduel', date='10.18.23', historical=False, live_tag=live_tag).build()

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
            '10.18.23',
            'fanduel', 
            'nfl', 
            site_file='week72023'
            )\
            .optimize_upload_file(
                roster_size=2, 
                pct_from_opt_proj=.858,
                max_pct_own=1.00,
                other_site_min=50100, 
                sabersim_only=False,
                h2h=False,
                h2h_rank=.99, #team ownership and floor pct rank, only for H2H=True
                min_team_sal=50000, # min team salary used, only for H2H=True
                removals=[30330590, '94948-63519',
                          30330690, '94948-45229',
                          30330886, '94948-73048'])






##############################################################################
#POST SLATE REVIEW
if __name__ == "__main__":
    PostSlate(
            '10.18.23',
            'fanduel', 
            'nfl', 
            site_file='week72023'
            )\
            .anaylze(removals=[30330590, '94948-63519',
                          30330690, '94948-45229',
                          30330886, '94948-73048'],
                     pct_from_opt_proj=.808, #this will be raised higher for comparison, keep at .808
                     max_pct_own=.50, 
                     other_site_min_compare=50100,
                     sabersim_only=False)