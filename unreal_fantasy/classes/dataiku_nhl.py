import pandas as pd
import numpy as np
import sys

sys.path.append('p:\\10_CWP Trade Department\\Ryland\\unreal_fantasy\\unreal_fantasy\\')
from classes.optimizer import Optimize
from utils.slates import slates




class DataikuNHL:

    #date is strdate whether live or 
    def __init__(self, site, date, historical, live_tag=''):
        self.site = site 
        self.date = date
        self.historical = historical
        self.live_tag = live_tag

        if historical==True:
            self.hist = 'historical' 
            self.cur_week = slates[site]['nhl'][date]['slate_id']
        else:
            self.hist = 'live'
            self.cur_week = date


    def build(self):

        #features of the projected optimized team (helps describe current slate)
        opt_team = Optimize(sport='nhl', site=self.site, historical=self.historical).projected_optimal(self.cur_week)
        opt_team_score = opt_team.projected()/9
        opt_team_std = opt_team.projected_std()

        if self.historical == True:
                file = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\optimizations\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nhl',
                self.hist,
                str(self.cur_week)),
                compression='gzip').sort_values('lineup')
        else:
                file = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\optimizations\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nhl',
                self.hist,
                str(self.live_tag)),
                compression='gzip').sort_values('lineup')
             
                
        try:
            file.drop('Position', axis=1, inplace=True)
        except:
            pass
        try:       
            file.rename(columns={'pos':'Position'}, inplace=True)
        except:
            pass
            
        file = file.dropna(subset=['lineup'])

            
        file['games'] = (file['team_team'] + file['opp']).str.replace(
                '@', '').str.split('')

        file['games2'] = (file['team_team'] + '_' + file['opp']).str.replace('@','').str.split('_')

        file['opp'] = file['opp'].str.replace('@', '')
        file['games'] = file['games'].apply(lambda x: sorted(x))
        file['games'] = file['games'].apply(lambda x: ''.join(x))
        file['slot'] = file.groupby(['lineup', 'position'])['salary'].rank(
                method='max',ascending=False)
        file['slot'] = file['Position'] + file['slot'].astype(int).astype(str)

        
    
        lineups = file.groupby('lineup')
        salaries = lineups['salary'].sum()
        teamstackgroup = file.groupby(['lineup', 'team_team'])
        
        min_stats = lineups.apply(lambda x: x.sort_values('salary').iloc[0])[['salary',
                                                                                'proj_pts/sal',
                                                                                'stats-15_toi',
                                                                                'stats-month_toi',
                                                                                'stats-month_pptoi',
                                                                                'vegas_pts']]
        min_stats.columns = [i+'_min_salary_player' for i in min_stats.columns]

        lencheck = lineups.apply(lambda x: len(x)).value_counts()
        if (lencheck.index[0]==9) & (len(lencheck)==9):
            raise NameError('Team Lengths Not All 9')
        
        team_means = lineups[
                'proj_proj',
                'proj_ceiling',
                'proj_floor',
                'proj_pts/sal',
                'proj_proj+/-', 
                'vegas_pts', 
                'vegas_o/u',
                'trends_opp+/-', 
                'vegas_ml',
                'vegas_opppts', 
                'fantasymonth_ppg',
                'lines_full',
                'lines_pp',
                'time_rest',
                'time_b2b',
                'fantasymonth_consistency',
                'stats-month_corsifor',
                'stats-month_s+blk',
                'stats-month_s',
                'stats-15_corsifor',
                'stats-15_s+blk',
                'stats-15_s',
                'stats-15_blk',
                'stats-15_satt',
                'stats-15_ppsatt',
                'stats-15_toi',
                'stats-15_pptoi',
                'stats-15_ppg',
                'stats-15_ppa', 
                'teamstats-month_oppg',
                'teamstats-month_opps',
                'teamstats-month_pks',
                'teamstats-month_pk%',
                'teamstats-month_opppps',
                'teamstats-month_opppp%',
                #new
                'teamstats-month_pp%',
                'stats-month_toi',
                'stats-month_pptoi',
                'stats-month_wins',
                'stats-month_g',
                'stats-month_a'
                #/new

                ].mean()
        team_means.columns = [i+'_mean' for i in team_means.columns]


        team_stds = lineups[
                'proj_proj',
                'proj_ceiling',
                'proj_floor',
                'proj_pts/sal',
                'proj_proj+/-', 
                'vegas_pts', 
                'vegas_o/u',
                'trends_opp+/-', 
                'vegas_ml',
                'vegas_opppts', 
                'fantasymonth_ppg',
                'lines_full',
                'lines_pp',
                'time_rest',
                'time_b2b',
                'fantasymonth_consistency',
                'stats-month_corsifor',
                'stats-month_s+blk',
                'stats-month_s',
                'stats-15_corsifor',
                'stats-15_s+blk',
                'stats-15_s',
                'stats-15_blk',
                'stats-15_satt',
                'stats-15_ppsatt',
                'stats-15_toi',
                'stats-15_pptoi',
                'stats-15_ppg',
                'stats-15_ppa', 
                'teamstats-month_oppg',
                'teamstats-month_opps',
                'teamstats-month_pks',
                'teamstats-month_pk%',
                'teamstats-month_opppps',
                'teamstats-month_opppp%',
                #new
                'teamstats-month_pp%',
                'stats-month_toi',
                'stats-month_pptoi',
                'stats-month_wins',
                'stats-month_g',
                'stats-month_a'
                #/new         
                ].std()
        team_stds.columns = [i+'_std' for i in team_stds.columns]
        
      
            
        sal_std = lineups['salary'].std()/file.drop_duplicates(subset=['name',
                        'proj_proj'], keep='first')['salary'].std()
        
        salary_info = file['salary'].describe()
        plyrs_eq_0 = lineups.apply(lambda x: len(x[x['salary']==salary_info['min']]))
        plyrs_0= lineups.apply(lambda x: len(x[x['salary']<salary_info['25%']]))
        plyrs_less_5 = lineups.apply(lambda x: len(x[x['salary']<(salary_info['min'])+(salary_info['std'])]))
        plyrs_less_10 = lineups.apply(lambda x: len(x[x['salary']<salary_info['50%']]))
        plyrs_less_25 = lineups.apply(lambda x: len(x[x['salary']<salary_info['75%']]))
        plyrs_abv_90 = lineups.apply(lambda x: len(x[x['salary']>salary_info['mean']]))
        plyrs_abv_99 = lineups.apply(lambda x: len(x[x['salary']>=(salary_info['max']-100)]))
        plyrs_eq_1 = lineups.apply(lambda x: len(x[x['salary']==salary_info['max']]))

        '''GAMES TEAM INFO'''
        maxplayersfrom1team = lineups.apply(
                lambda x: x['team_team'].value_counts().iloc[0])
        num_games_represented = lineups.apply(
                lambda x: len(x['games'].unique()))
        
    
        #part 1 (agg the game stack stat)
        numberofgamestacks = lineups.apply(lambda x: len(x['games'].value_counts()[
                x['games'].value_counts()>1].index.values))
        
        '''TEAM STACKS'''
        print('building teamStack analysis')
        #part 1 (agg the game stack stat)
        numberofteamstacks = lineups.apply(lambda x: len(x['team_team'].value_counts()[
                x['team_team'].value_counts()>1].index.values))
        
        team_stack_strings = teamstackgroup.apply(
                lambda x: x['slot'].tolist() if len(x)>1
                else [])
        
        team_stack_salaries = teamstackgroup.apply(
                lambda x: x['salary'].sum()/len(x) if len(x)>1
                else 0) 
        
        team_stack_ou = teamstackgroup.apply(
                lambda x: x['vegas_o/u'].sum()/len(x) if len(x)>1
                else 0)

        team_stack_pts = teamstackgroup.apply(
                lambda x: x['vegas_pts'].sum()/len(x) if len(x)>1
                else 0)
    
        team_stack_ml = teamstackgroup.apply(
                lambda x: x['vegas_ml'].sum()/len(x) if len(x)>1
                else 0)

        teamstackdf = pd.DataFrame(team_stack_strings).join(
                pd.DataFrame(team_stack_salaries), rsuffix='_salary').join(
                pd.DataFrame(team_stack_ou)).join(
                pd.DataFrame(team_stack_pts), rsuffix='_pts').join(
                pd.DataFrame(team_stack_ml), rsuffix='_ml')  
        teamstackdf.columns = ['0', '0_salary', 0, 'pts', 'ml']
                    

        teamstackdf['0'] = teamstackdf['0'].apply(lambda x: ''.join(
                sorted(x)) if len(x)>1 else 0)

        teamstackdf = teamstackdf.sort_values(['lineup','0_salary'],
            ascending=[False,False])
                
                
        #part 2 (out agged stats into lists sorted by stack string)
        team_stack_strings2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['0'] \
                                    .tolist())
        
        team_stack_salaries2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['0_salary'] \
                                    .tolist())
        
        team_stack_ou2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0][0] \
                                    .tolist())

        team_stack_pts2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['pts'] \
                                    .tolist())
        
        team_stack_ml2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['ml'] \
                                    .tolist())
        
        #part 3 create feature column by stack string
        team_stack1 = team_stack_strings2.apply(lambda x: x[0] 
                                                if len(x)>=1 else 'NA')
        team_stack2 = team_stack_strings2.apply(lambda x: x[1]
                                                if len(x)>=2 else 'NA')
        team_stack3 = team_stack_strings2.apply(lambda x: x[2] 
                                                if len(x)>=3 else 'NA')
        team_stack4 = team_stack_strings2.apply(lambda x: x[3] 
                                                if len(x)==4 else 'NA')
        
        team_stack1salary = team_stack_salaries2.apply(lambda x: x[0]
                                                if len(x)>=1 else 0)
        team_stack2salary = team_stack_salaries2.apply(lambda x: x[1] 
                                                if len(x)>=2 else 0)
        team_stack3salary = team_stack_salaries2.apply(lambda x: x[2]
                                                if len(x)>=3 else 0)
        team_stack4salary = team_stack_salaries2.apply(lambda x: x[3]
                                                if len(x)==4 else 0)
        
        team_stack1ou = team_stack_ou2.apply(lambda x: x[0]
                                                if len(x)>=1 else 0)
        team_stack2ou = team_stack_ou2.apply(lambda x: x[1] 
                                                if len(x)>=2 else 0)
        team_stack3ou = team_stack_ou2.apply(lambda x: x[2]
                                                if len(x)>=3 else 0)
        team_stack4ou = team_stack_ou2.apply(lambda x: x[3]
                                                if len(x)==4 else 0)

        team_stack1pts = team_stack_pts2.apply(lambda x: x[0]
                                                if len(x)>=1 else 0)
        team_stack2pts= team_stack_pts2.apply(lambda x: x[1] 
                                                if len(x)>=2 else 0)
        team_stack3pts = team_stack_pts2.apply(lambda x: x[2]
                                                if len(x)>=3 else 0)
        team_stack4pts = team_stack_pts2.apply(lambda x: x[3]
                                                if len(x)==4 else 0)

        team_stack1ml = team_stack_ml2.apply(lambda x: x[0]
                                                if len(x)>=1 else 0)
        team_stack2ml= team_stack_ml2.apply(lambda x: x[1] 
                                                if len(x)>=2 else 0)
        team_stack3ml = team_stack_ml2.apply(lambda x: x[2]
                                                if len(x)>=3 else 0)
        team_stack4ml = team_stack_ml2.apply(lambda x: x[3]
                                                if len(x)==4 else 0)


        position_numbers = lineups['Position'].value_counts()
        cnumbers = position_numbers.loc[:,'C',:]
        wnumbers = position_numbers.loc[:,'W',:]
        defnumbers = position_numbers.loc[:,'D',:]

        min_salary = lineups['salary'].min()
        min_proj = lineups['proj_proj'].min()
        min_ptssal = lineups['proj_pts/sal'].min()

        max_salary = lineups['salary'].max()
        max_proj = lineups['proj_proj'].max() 
        max_ptssal = lineups['proj_pts/sal'].max()

        analysis = pd.concat([
                                  team_means,
                                  team_stds,
                                  #new
                                  min_stats,
                                  #new
                                  sal_std,
                                  plyrs_eq_0,
                                  plyrs_0,
                                  plyrs_less_5,
                                  plyrs_less_10,
                                  plyrs_less_25,
                                  plyrs_abv_90,
                                  plyrs_abv_99,
                                  plyrs_eq_1, 
                                  maxplayersfrom1team,
                                  num_games_represented,
                                  numberofgamestacks,
                                  numberofteamstacks,
                                  team_stack1salary,
                                  team_stack2salary,
                                  team_stack3salary, 
                                  team_stack4salary,
                                  team_stack1ou,
                                  team_stack2ou,
                                  team_stack3ou,
                                  team_stack4ou,
                                  team_stack1pts,
                                  team_stack2pts,
                                  team_stack3pts,
                                  team_stack4pts,
                                  team_stack1ml,
                                  team_stack2ml,
                                  team_stack3ml,
                                  team_stack4ml,
                                  cnumbers,
                                  wnumbers,
                                  defnumbers,
                                  min_salary,
                                  min_proj,
                                  min_ptssal,
                                  max_salary,
                                  max_proj,
                                  max_ptssal,
                                  salaries,
                                  team_stack1,
                                  team_stack2,
                                  team_stack3,
                                  team_stack4,
                                                             
                                ], axis=1)
             
        analysis.columns =  \
                    team_means.columns.tolist() + \
                    team_stds.columns.tolist() + \
                    min_stats.columns.tolist() + \
                    ['salary_std',
                    'plyrs_eq_0',
                    'plyrs_<_0',
                    'plyrs_less_5',
                    'plyrs_less_10',
                    'plyrs_less_25',
                    'plyrs_abv_90',
                    'plyrs_abv_99',
                    'plyrs_eq_1',
                    'maxplayersfrom1team',
                    'num_games_represented',
                    'numberofgamestacks',
                    'numberofteamstacks',
                    'team_stack1salary',
                    'team_stack2salary',
                    'team_stack3salary', 
                    'team_stack4salary',
                    'team_stack1ou',
                    'team_stack2ou',
                    'team_stack3ou',
                    'team_stack4ou',
                    'team_stack1pts',
                    'team_stack2pts',
                    'team_stack3pts',
                    'team_stack4pts',
                    'team_stack1ml',
                    'team_stack2ml',
                    'team_stack3ml',
                    'team_stack4ml',
                    'numberofcenters',
                    'numberofwingers',
                    'numberofdefensemen',
                    'min_salary',
                    'min_proj',
                    'min_ptssal',
                    'max_salary',
                    'max_proj',
                    'max_ptssal',
                    'team_salary_raw',
                    'team_stack1',
                    'team_stack2',
                    'team_stack3',
                    'team_stack4',
                    ]
        
        analysis['slate_optimal_projected'] = opt_team_score*9
        analysis['proj_from_opt_proj'] = ((analysis['proj_proj_mean']*9)/analysis['slate_optimal_projected'])
    
        #new
        analysis['pct_opt_proj*team_salary'] = analysis['team_salary_raw']*analysis['proj_from_opt_proj']

        analysis['proj_from_opt_per_games'] = (analysis['proj_from_opt_proj']/int(len(file['team_team'].unique())))

        analysis.loc[:,'proj_proj_mean':'max_ptssal'] = analysis.loc[:,'proj_proj_mean':'max_ptssal'].rank(pct=True)
        analysis['opt_team_projpts_std'] = opt_team_std
        analysis['number_teams_on_slate'] = int(len(file['team_team'].unique())/2)
        if self.historical==True:
                analysis['actual_sum'] = lineups['actual'].sum() 
        analysis = analysis.reset_index()
        analysis['week'] = self.slate_id
        analysis['id'] = analysis['week'].astype(str) + \
        analysis['lineup'].astype(str) 
        if self.historical==True:
                analysis['ismilly'] = np.where(analysis['actual_sum']>(slates[self.site]['nhl'][str(self.date)]['winning_score']*.999), 1,0)
        
        if self.historical==True:
                #export to folder and ready for dataiku upload
                analysis.to_csv(r"C:\Users\rmathews\.unreal_fantasy\dataiku\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nhl',
                self.hist,
                str(self.cur_week)),
                compression='gzip', index=False) 
        else:
                analysis.to_csv(r"C:\Users\rmathews\.unreal_fantasy\dataiku\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nhl',
                self.hist,
                str(self.live_tag)),
                compression='gzip', index=False) 
        
        return analysis











        






        


  