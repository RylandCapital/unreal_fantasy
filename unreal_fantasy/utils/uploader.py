import os
import pandas as pd



def upload(site, sport, historical):

    if historical==True:
         hist = 'historical' 
    else:
         hist = 'live'
    
    mypath = "C:\\Users\\rmathews\\.unreal_fantasy\\dataiku\\{0}\\{1}\\{2}\\".format(
            site,
            sport,
            hist)
    
    onlyfiles = [f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f))]
    file = pd.concat([pd.read_csv(mypath + f, compression='gzip').sort_values('lineup') for f in onlyfiles])

    file.to_csv(
    'C:\\Users\\rmathews\\.unreal_fantasy\\uploads\\upload_{0}_{1}_{2}.csv'.format(site,sport,hist)
    )