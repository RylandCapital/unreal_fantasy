import time
import os
import sys

import pandas as pd
import numpy as np

from selenium import webdriver
from selenium.webdriver.common.keys import Keys

sys.path.append('p:\\10_CWP Trade Department\\Ryland\\unreal_fantasy\\unreal_fantasy\\')
from utils.slates import slates

from dotenv import load_dotenv

load_dotenv()
#fantasy labs username
FLUSER = os.getenv("FLUSER")
#fantasy labs password
FLPASS = os.getenv("FLPASS")

'''
available sites

['draftkings',
 'fanduel',
]

available sports

nhl
nfl

 '''

class Fantasylabs:
  def __init__(self, site, sport, date):
    self.site = site.lower()
    self.sport = sport.lower()
    self.date = date

  def load_window(self):

    fire = webdriver.FirefoxProfile()
    fire.set_preference("http.response.timeout", 5)
    fire.set_preference("dom.max_script_run_time", 5)
    driver = webdriver.Firefox(firefox_profile=fire)
    webpage = r"https://www.fantasylabs.com/articles/"
    driver.get(webpage)
    time.sleep(2)
    driver.find_element('xpath','/html/body/div[1]/nav/div/div[3]/div[2]/a[2]').click()
    time.sleep(2)
    driver.find_element('xpath', '/html/body/div[3]/form[1]/div[2]/div/input').send_keys(FLUSER)
    time.sleep(2)
    driver.find_element('xpath', '/html/body/div[3]/form[1]/div[3]/div/input').send_keys(FLPASS)
    time.sleep(2)
    driver.find_element('xpath', '/html/body/div[3]/form[1]/div[4]/button').click()
    time.sleep(2)
    driver.get(r"https://www.fantasylabs.com/{0}/player-models/?date={1}".format(self.sport,self.date))
    time.sleep(2)
    driver.find_element('xpath', '/html/body/article/section[1]/div[1]/div[5]/div[1]/a[1]').click()
    time.sleep(2)
    sites = [
       i.lower() for i in driver.find_elements('xpath', '/html/body/article/section[1]/div[1]/div[5]/div[2]')[0].text.split('\n')
       ]
    site_position = sites.index(self.site)
    driver.find_element('xpath', '/html/body/article/section[1]/div[1]/div[5]/div[2]/div[{0}]'.format(site_position+1)).click()
    time.sleep(2)
    driver.set_window_size(3000, 2200)
    driver.set_context("chrome")
    win = driver.find_element('tag name', "html")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    win.send_keys(Keys.CONTROL + "-")
    driver.set_context("content")

    return driver
  
  #historical ids are integers to keep dataiku trainsets in order
  def scrape(self, historical=True, delete_dups=False):

    driver = self.load_window()
    time.sleep(10)

    #go to the date we are currently scraping
    print('\n\n\ncurrently scraping date : {0} for {1}'.format(self.date, self.sport))
    driver.get(r"https://www.fantasylabs.com/{0}/player-models/?date={1}".format(self.sport,self.date))
    time.sleep(5)
    driver.find_element('xpath','/html/body/article/section[1]/div[1]/div[7]/div[1]/a[1]').click()
    time.sleep(1)

    #locate and select correct slate
    if (self.sport=='nhl') & (self.site=='fanduel'):
      main_locate = pd.Series(['Main' in sub for sub in driver.find_element('xpath','/html/body/article/section[1]/div[1]/div[7]/div[2]').text.split('\n')])
      main_locate = main_locate[main_locate==True].index[0] + 1
    if (self.sport=='nhl') & (self.site=='draftkings'):
      main_locate = pd.Series(['7:00PM ET - ' in sub for sub in driver.find_element('xpath','/html/body/article/section[1]/div[1]/div[7]/div[2]').text.split('\n')])
      main_locate = main_locate[main_locate==True].index[0] + 1

    if (self.sport=='nfl') & (self.site=='fanduel'):
      main_locate = pd.Series(['Main' in sub for sub in driver.find_element('xpath','/html/body/article/section[1]/div[1]/div[7]/div[2]').text.split('\n')])
      main_locate = main_locate[main_locate==True].index[0] + 1
    if (self.sport=='nfl') & (self.site=='draftkings'):
      main_locate = pd.Series(['1:00PM ET - ' in sub for sub in driver.find_element('xpath','/html/body/article/section[1]/div[1]/div[7]/div[2]').text.split('\n')])
      main_locate = main_locate[main_locate==True].index[0] + 1
    
    time.sleep(1)
    driver.find_element('xpath', '/html/body/article/section[1]/div[1]/div[7]/div[2]/div[{0}]'.format(main_locate)).click()
    time.sleep(5)

    '''
    Now we are ready to scrape
    '''
    position_dict = {
      'nhl':{
        'fanduel':[['C','W','D','FLEX','G'],['rating', 'name', 'salary', 'pos', 'min', 'max'],15,2],
        'draftkings':[['C','W','D','G','FLEX'],['rating', 'name', 'salary', 'pos', 'min', 'max'],15,2]

      },
      'nfl':{
        'fanduel':[['QB','RB','WR','TE','FLEX','D'],['rating', 'name', 'salary', 'team', 'opp'],13,-1],
        'draftkings':[['QB','RB','WR','TE','FLEX','D'],['rating', 'name', 'salary', 'team', 'opp'],13,-1],
      }
    }

    positions = position_dict[self.sport][self.site][0]
    left_columns = position_dict[self.sport][self.site][1]
    feature_sections = position_dict[self.sport][self.site][2] #this needs to be different for NFL-D, if (p=='D') & (sport=='nfl'):
    column_control = position_dict[self.sport][self.site][3]
    
    if historical==True:
       id = slates[self.site][self.sport][self.date]['slate_id']
    else:
       id = self.date

    dfs = []
    for p in positions:
      if p != 'FLEX':
        while True:
          try:
              print(p)  
              driver.refresh()
              time.sleep(5)
              driver.find_element('xpath','/html/body/article/section[1]/div[1]/div[7]/div[1]/a[1]').click()
              time.sleep(1)
              driver.find_element('xpath', '/html/body/article/section[1]/div[1]/div[7]/div[2]/div[{0}]'.format(main_locate)).click()
              time.sleep(1)
              driver.find_element('xpath', '//*[@id="models-filters"]/div/nav/ul/li[{0}]'.format(positions.index(p)+1)).click()
              time.sleep(2)

              num_players = int(driver.find_element('xpath','/html/body/article/section[2]/section/div[4]/section/div[1]/div/div/ul/li[2]/a/span').text)
                      
              columns = []
              column_names = left_columns
              for n, t in zip(column_names, np.arange(2,7)):
                  column = pd.DataFrame([driver.find_element('xpath','/html/body/article/section[2]/section/div[4]/section/div[2]/div[1]/div/div/div[1]/div[2]/div/div[1]/div/div[2]/div[1]/div/div[{0}]/div[{1}]'.format(i,t)).text for i in np.arange(1,num_players+1)], columns =[n]) #uses last 2 divs (row then column, for examplle its row 1 column)
                  columns.append(column) 
              left = pd.concat(columns, axis=1) 

              rcs = []
              if (p=='D') & (self.sport=='nfl'):
                 feature_sections=11
              for i in np.arange(1,feature_sections):
                  rc = driver.find_element('xpath','/html/body/article/section[2]/section/div[4]/section/div[2]/div[1]/div/div/div[1]/div[2]/div/div[1]/div/div[1]/div[2]/div/div[{0}]'.format(i)).text.split('\n')            
                  rc2 = [(rc[0]+'_'+i).replace(' ','').lower() for i in rc]
                  if i==column_control:
                      [rcs.append(l) for l in rc2]
                  else:
                      [rcs.append(l) for l in rc2[1:]]
              
              rcolumns = []
              rcolumn_names = rcs
              len_names = len(rcolumn_names)
              for n, t in zip(rcolumn_names, np.arange(1,len_names+1)):
                  try:
                      rcolumn = pd.DataFrame([driver.find_element('xpath','/html/body/article/section[2]/section/div[4]/section/div[2]/div[1]/div/div/div[1]/div[2]/div/div[1]/div/div[2]/div[2]/div/div/div[{0}]/div[{1}]'.format(i,t)).text for i in np.arange(1,num_players+1)], columns =[n]) #uses last 2 divs (row then column, for examplle its row 1 column)
                      rcolumns.append(rcolumn) 
                  except:
                      rcolumn = pd.DataFrame([driver.find_element('xpath','/html/body/article/section[2]/section/div[4]/section/div[2]/div[1]/div/div/div[1]/div[2]/div/div[1]/div/div[2]/div[2]/div/div/div[{0}]/div[{1}]'.format(i,t)).text for i in np.arange(1,num_players+1)], columns =[n]) #uses last 2 divs (row then column, for examplle its row 1 column)
                      rcolumns.append(rcolumn) 
                      
              right = pd.concat(rcolumns, axis=1)

              final = pd.concat([left.reset_index(drop=True), right.reset_index(drop=True)], axis=1)
              final['slate_id'] = id
              final['pos'] = p
              # final.to_excel(r'C:\Users\rmathews\Downloads\{0}.xlsx'.format(p))

              dfs.append(final)

              break
          except:
            print("An error occurred: on position: {0}".format(p))
            print("Retrying...")
            
    master = pd.concat(dfs, sort=False).reset_index(drop=True)
    master['slate_id'] = master['slate_id'].astype(str)

    for i in master.columns:
      if i not in ['name','projections_projown']:
          master[i] = master[i].str.replace('$','')
          master[i] = master[i].str.replace('%','')
          master[i] = master[i].str.replace('@','')
          master[i] = master[i].str.replace(' ','')
          master[i] = master[i].str.replace('  ','')
          try:
              master[i]=master[i].apply(lambda x: x.replace('','0') if len(str(x))==0 else x).fillna(0)
          except:
              pass

    master['Last Name_master'] = master['name'].apply(lambda x: x.lower())
    master['City Name_master'] = master['name'].apply(lambda x: x.lower())
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace('st. ', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace('-', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' iii', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' ii', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' iv', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' v', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' jr.', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' sr.', ''))
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' sr.', ''))
    master['First Name_master'] = master['Last Name_master'].apply(lambda x: x.split(' ')[0])
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.split(' ')[1] if len(x.split(' '))>1 else x.split(' ')[0])
    master['Last Name_master'] = master['Last Name_master'].apply(lambda x: x.replace(' ', ''))
    master['First Name_master'] = master['First Name_master'].str.lower().apply(lambda x: x.replace(' ', '')[0])



    if self.sport == 'nhl':
      master['opp'] = master['team_opp'].str.split('-').apply(lambda x: x[0])
      master['lines_full'] = master['lines_full'].apply(lambda x: x[0])
      master['time_b2b'] = master['time_b2b'].apply(lambda x: x[0] if len(x)>0 else x)

      master['RylandID_master'] =  master['Last Name_master'] + master['salary'].astype(str) + master['pos'].str.lower() + master['First Name_master']

    if self.sport == 'nfl':
      master['name'] = master['name'].apply(lambda x: x.replace(' Defense', ''))
      # master['projections_projown'] = master['projections_projown'].apply(lambda x: x.replace('','0-0') if len(x)==0 else x).apply(lambda x: x.split('-')[1])
      master['RylandID_master'] = np.where(master['pos'] == 'D', master['City Name_master'] + + master['salary'].astype(str),  master['Last Name_master'] + master['salary'].astype(str) + master['pos'].str.lower() + master['First Name_master'])
    

    if delete_dups==True:
      master=master.drop_duplicates('RylandID_master', keep='first') 
    master.index = master['RylandID_master']  


    if historical==True:
       hist='historical'
    else:
       hist='live'
    master.to_csv(r'C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv'.format(self.site,self.sport,hist,id))


    

    
    





  

  

    
    

    
    
    




