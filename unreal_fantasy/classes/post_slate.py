import sys 
import os
import pandas as pd
import numpy as np

sys.path.append('p:\\10_CWP Trade Department\\Ryland\\unreal_fantasy\\unreal_fantasy\\')
from classes.ticket import Ticket
from utils.slates import slates


class PostSlate:
    
    '''slate_date'''
    def __init__(self, slate_date, site, sport, site_file):

        self.sport = sport
        self.site = site
        self.slate_date = slate_date
        self.site_file= site_file

    def salary_aggregate(self):
       # make a file with dk/fanduel salaries matched up

       fd = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            'fanduel',
            self.sport,
            'live',
            str(self.slate_date)))
       fd = fd.rename(columns={'Salary':'fanduel_Salary'})
       fd = fd.rename(columns={'Unnamed: 0':'fanduel_ArbID'})
 
       
       dk = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            'draftkings',
            self.sport,
            'live',
            str(self.slate_date)))
       dk = dk.rename(columns={'Salary':'draftkings_Salary'})
       dk = dk.rename(columns={'Unnamed: 0':'draftkings_ArbID'})

       

       if self.sport == 'nhl':
         dk['TeamAbbrev'] = np.where(dk['TeamAbbrev']=='WAS', 'WSH', dk['TeamAbbrev'])
         dk['TeamAbbrev'] = np.where(dk['TeamAbbrev']=='CLS', 'CBJ', dk['TeamAbbrev'])
       
       
       if self.sport == 'nfl':
         fd['Nickname'] = np.where(fd['Position']=='D', fd['Last Name'], fd['Nickname'])
         dk['Position'] = np.where(dk['Position']=='DST', 'D', dk['Position'])
         dk['TeamAbbrev'] = np.where(dk['TeamAbbrev']=='JAX', 'JAC', dk['TeamAbbrev'])


       dk['combo_id'] = dk['Name'].str.lower().str.replace(' ','').str.replace('-','').str.replace('.','') +\
                        dk['TeamAbbrev']
       dk['combo_id'] = np.where(dk['pos']=='D', dk['team'], dk['combo_id'])
      
       fd['combo_id'] = fd['Nickname'].str.lower().str.replace(' ','').str.replace('-','').str.replace('.','')+\
                       fd['Team']
       fd['combo_id'] = np.where(fd['pos']=='D', fd['team'], fd['combo_id'])
      
       dk.set_index('combo_id', inplace=True)
       fd.set_index('combo_id', inplace=True)


       final = fd.join(dk[['draftkings_ArbID', 'draftkings_Salary']]).sort_values('salary', ascending=False)
       dkmiss = final[final['draftkings_Salary'].isnull()]
       fdmiss = final[final['fanduel_Salary'].isnull()]

       ##export debug to check unmatched salaries
       dkmiss.to_csv(r'C:\Users\rmathews\.unreal_fantasy\debug_arb_dkmiss.csv')
       fdmiss.to_csv(r'C:\Users\rmathews\.unreal_fantasy\debug_arb_fdmiss.csv')

       return final[['fanduel_ArbID','fanduel_Salary','draftkings_ArbID','draftkings_Salary']]

    def prepare(self, remove=True, removals=[]):
        

        predictions = pd.read_csv('C:\\Users\\rmathews\\.unreal_fantasy\\_live_projections\\{0}_{1}.csv'.format(self.site, self.sport))
        predictions.rename(columns={'proba_1.0':'proba_1'}, inplace=True)
        predictions = predictions.sort_values(by='proba_1', ascending=False)
        predictions = predictions.sort_values(by='lineup',ascending=False) 

        optimized_path = 'C:\\Users\\rmathews\\.unreal_fantasy\\optimizations\\{0}\\{1}\\live\\'.format(self.site, self.sport)
        onlyfiles = [f for f in os.listdir(optimized_path) if os.path.isfile(os.path.join(optimized_path, f))]
        teams = pd.concat([pd.read_csv(optimized_path + f, compression='gzip').sort_values('lineup',ascending=False) for f in onlyfiles])
        #trim teams to only ones represented by dataiku preditions
        teams = teams[teams['lineup'].isin(predictions['lineup'].unique())]
        
        '''join salary arb info, ***need both fanduel and draftkings scrapes'''
        stats = self.salary_aggregate()
        #stats = final[['fanduel_ArbID','fanduel_Salary','draftkings_ArbID','draftkings_Salary']].copy() #for debuggin
        stats.set_index('{0}_ArbID'.format(self.site), inplace=True)
        teams = teams.set_index('Unnamed: 0.1').join(stats, how='inner', lsuffix='_ot').reset_index()

        #confirm 9 only 
        nine_confirm = teams.groupby('lineup').apply(lambda x: len(x))
        teams = teams.set_index('lineup').loc[nine_confirm[nine_confirm==9].index.tolist()].reset_index()

        #remove any removals
        if remove==True:
            removedf = pd.DataFrame(teams.groupby('lineup').apply(lambda x: sorted(x['Id'].tolist())))
            removedf['isremove'] = removedf[0].apply(lambda x: len(list(set(x).intersection(set(removals)))))
            keepers = removedf[removedf['isremove']==0].index.unique().tolist()
            teams = teams.set_index('lineup').loc[keepers].reset_index()

        #join team pool and predictions
        picks = predictions[['lineup', 'proba_1']].set_index('lineup').join(teams.set_index('lineup'), how='inner')
        picks['proba_rank'] = picks['proba_1'].rank(method='max', ascending=False)/9
        picks['check4max'] = picks.groupby(level=0)['team'].value_counts().max(level=0)
        picks = picks[picks['check4max']<4]
        picks.sort_values(by='proba_1', ascending=False, inplace=True)

        actualdf = pd.read_csv('C:\\Users\\rmathews\\.unreal_fantasy\\fantasylabs\\{0}\\{1}\\historical\\{2}.csv'.format(self.site, self.sport, slates[self.site][self.sport][self.slate_date]['slate_id']))
        actualdf.set_index('RylandID_master', inplace=True)
        if self.sport == 'nfl':
            actualdf = actualdf[['projections_actpts']]
        else:
            actualdf = actualdf[['proj_actpts']]

        postdf = picks.reset_index().set_index('RylandID').join(actualdf, how='left')
        postdf = postdf.set_index('lineup')
        if self.sport == 'nfl':
            postdf['team_actual'] = postdf.groupby('lineup')['projections_actpts'].sum()
        else:
            postdf['team_actual'] = postdf.groupby('lineup')['proj_actpts'].sum()
        if self.sport == 'nfl':
            postdf['team_proj'] = postdf.groupby('lineup')['projections_proj'].sum()
        else:
            postdf['team_proj'] = postdf.groupby('lineup')['proj_proj'].sum()
        flip_dict = {'fanduel':'draftkings', 'draftkings':'fanduel'}
        postdf['other_site_salary'] = postdf.groupby('lineup')['{0}_Salary'.format(flip_dict[self.site])].sum()

        #########

        return postdf
    
    def anaylze(self, removals=[], pct_from_opt_proj=.808, max_pct_own=.33, other_site_min_compare=60100):

        postdf = self.prepare(remove=True, removals=removals)
        postdf_trim = postdf[~postdf.index.duplicated(keep='first')]
        max_score = postdf['team_actual'].max()
        max_score_proba = postdf[postdf['team_actual']==max_score]['proba_1'].iloc[0]
        max_score_proba_rank = postdf[postdf['team_actual']==max_score]['proba_rank'].iloc[0]
        proba_act_corr = postdf['proba_1'].corr(postdf['team_actual'])
        actual_other_salary_corr = postdf_trim['other_site_salary'].corr(postdf_trim['team_actual'])

        pool_top_150_proj = postdf_trim.sort_values(by='team_proj', ascending=False)[['team_proj', 'team_actual']].iloc[:150]['team_actual'].describe()
        pool_top_150_proba = postdf_trim.sort_values(by='proba_1', ascending=False)[['proba_1', 'team_actual']].iloc[:150]['team_actual'].describe()


        ticket_ids = Ticket(
            self.slate_date,
            self.site, 
            self.sport, 
            site_file = self.site_file
            )\
            .optimize_upload_file(
                roster_size=150, 
                pct_from_opt_proj=pct_from_opt_proj,#.808 
                max_pct_own=max_pct_own,
                other_site_min=0, 
                sabersim_only=False,
                removals=removals)
        
        postdf2 = self.prepare(remove=False)
        postdf2_trim = postdf2[~postdf2.index.duplicated(keep='first')]

        
        ticket_stats = postdf2_trim.loc[ticket_ids][['proba_1', 'team_actual']].iloc[:150]['team_actual'].describe()

        ticket_ids = Ticket(
            self.slate_date,
            self.site, 
            self.sport, 
            site_file = self.site_file
            )\
            .optimize_upload_file(
                roster_size=150, 
                pct_from_opt_proj=pct_from_opt_proj,#.808 
                max_pct_own=max_pct_own,
                other_site_min=other_site_min_compare, 
                sabersim_only=False,
                removals=removals)

        ticket_stats_wsitemin = postdf2_trim.loc[ticket_ids][['proba_1', 'team_actual']].iloc[:150]['team_actual'].describe()

        report = pd.concat([pool_top_150_proj, pool_top_150_proba, ticket_stats, ticket_stats_wsitemin], axis=1).round(2)
        report.columns = ['pool top 150 projd', 'pool top 150 proba', 'ticket', 'ticket w other site min']

        report.loc[''] = ''
        report.loc[' '] = ''
        report.loc['top pool score', 'pool top 150 projd'] = max_score.round(2)
        report.loc['top pool score proba', 'pool top 150 projd'] = max_score_proba.round(2)
        report.loc['max score proba rank', 'pool top 150 projd'] = max_score_proba_rank.round(2)
        report.loc['proba act corr', 'pool top 150 projd'] = proba_act_corr.round(2)
        report.loc['actual other salary corr', 'pool top 150 projd'] = actual_other_salary_corr.round(2)

        report.to_csv(r'C:\\Users\\rmathews\\.unreal_fantasy\\postslate\\{0}_{1}_{2}.csv'.format(self.site, self.sport, self.slate_date))

        return report



    