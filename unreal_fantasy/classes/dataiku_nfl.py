import pandas as pd
import numpy as np
import sys
import re

sys.path.append('p:\\10_CWP Trade Department\\Ryland\\unreal_fantasy\\unreal_fantasy\\')
from classes.optimizer import Optimize
from utils.slates import slates




class DataikuNFL:

    #date is strdate whether live or historical
    def __init__(self, site, date, historical, live_tag=''):
        self.site = site 
        self.date = date
        self.historical = historical
        self.live_tag = live_tag
        

        if historical==True:
            self.hist = 'historical' 
            self.cur_week = slates[site]['nfl'][date]['slate_id']
        else:
            self.hist = 'live'
            self.cur_week = date


    def build(self):

        #features of the projected optimized team (helps describe current slate)
        opt_team = Optimize(sport='nfl', site=self.site, historical=self.historical).projected_optimal(self.cur_week)
        opt_team_score = opt_team.projected()/9
        opt_team_std = opt_team.projected_std()

        if self.historical == True:
                file = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\optimizations\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nfl',
                self.hist,
                str(self.cur_week)),
                compression='gzip').sort_values('lineup')
        else:
                file = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\optimizations\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nfl',
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
        file = file[sorted(file.columns.tolist())]
        #OAK/LV clean
        file['team2'] = np.where(file['team']=='OAK', 'LV', file['team'])
        #projected_own clean
        file['projections_projown'] = file['projections_projown'].fillna('0-0')
        file['projections_projown'] = file['projections_projown'].apply(lambda x: x.replace('','0-0') if len(str(x))==0 else x).apply(lambda x: x.split('-')[1] if len(re.findall("-", str(x)))>0 else x)
        file['projections_projown'] = file['projections_projown'].astype(float) + 1 #so no zeroes
            
        file['games'] = (file['team'] + file['opp']).str.replace(
                    '@', '').str.split('')
        file['games2'] = (file['team'] + '_' + file['opp']).str.replace('@','').str.split('_')

        file['opp'] = file['opp'].str.replace('@', '')
        file['games'] = file['games'].apply(lambda x: sorted(x))
        file['games'] = file['games'].apply(lambda x: ''.join(x))
        file['slot'] = file.groupby(['lineup', 'Position'])['salary'].rank(
                method='max',ascending=False)
        file['slot'] = file['Position'] + file['slot'].astype(int).astype(str)

        lineups = file.groupby('lineup')
        rblineups = file[file['Position'] == 'RB'].groupby('lineup')
        wrlineups = file[file['Position'] == 'WR'].groupby('lineup')
        telineups = file[file['Position'] == 'TE'].groupby('lineup')
        qblineups = file[file['Position'] == 'QB'].groupby('lineup')
        dlineups = file[file['Position'] == 'D'].groupby('lineup')
        nodlineups = file[file['Position'] != 'D'].groupby('lineup')
        gamestacksgroup = file.groupby(['lineup', 'games'])
        teamstackgroup = file.groupby(['lineup', 'team'])
        
        lencheck = lineups.apply(lambda x: len(x)).value_counts()
        if (lencheck.index[0]==9) & (len(lencheck)==9):
            raise NameError('Team Lengths Not All 9')
        



        team_sums = lineups['projections_proj',
                            'projections_projown',
                            'projections_proj+/-',
                            'vegas_pts',
                            'vegas_o/u',
                            'trends_opp+/-', 
                            'trends_snaps',
                            'vegas_spread', 
                            'rushingyear_successrate', 
                            'defenseyear_passsucc',
                            'defenseyear_takeaway%'].sum()
          
        
        team_prods = (lineups[['projections_projown']].apply(lambda x: (x/100).prod())*100000000).round(5) #times 100M to normalize
    
        rbown = rblineups['projections_projown'].sum()
        wrown = wrlineups['projections_projown'].sum()
        teown = telineups['projections_projown'].sum()
        qbown = qblineups['projections_projown'].sum()
        down =  dlineups['projections_projown'].sum()
        
        sal_std = lineups['salary'].std()/file.drop_duplicates(subset=['name',
                        'projections_proj'], keep='first')['salary'].std()
        
        salary_info = file['salary'].describe()
        plyrs_eq_0 = lineups.apply(lambda x: len(x[x['salary']==salary_info['min']]))
        plyrs_0= lineups.apply(lambda x: len(x[x['salary']<salary_info['25%']]))
        plyrs_less_5 = lineups.apply(lambda x: len(x[x['salary']<(salary_info['min'])+(salary_info['std'])]))
        plyrs_less_10 = lineups.apply(lambda x: len(x[x['salary']<salary_info['50%']]))
        plyrs_less_25 = lineups.apply(lambda x: len(x[x['salary']<salary_info['75%']]))
        plyrs_abv_90 = lineups.apply(lambda x: len(x[x['salary']>salary_info['mean']]))
        plyrs_abv_99 = lineups.apply(lambda x: len(x[x['salary']>=(salary_info['max']-100)]))
        plyrs_eq_1 = lineups.apply(lambda x: len(x[x['salary']==salary_info['max']]))

        '''SLOTS INFORMATION''' 
        #rb flex
        slot_cols = [
                     'salary', 
                     'trends_snaps',
                     'trends_opp+/-',
                     'vegas_pts',
                     'projections_proj',
                     'projections_proj+/-', 
                     'projections_ceiling',
                     'vegas_spread', 
                     'projections_projown', 
                     'team2', 
                     'projections_projsacks',
                     'passingyear_int%',
                     'trends_bargain',
                     'projections_leverage',
                     'vegas_opppts'
                     ]
        
        def flexerror(x, row, extra=[]):
            try:
                return x.sort_values(by='salary',
                                    ascending=False).iloc[row][slot_cols+extra]
            except:
                return pd.Series([0]*len(slot_cols+extra),
                                    index = slot_cols+extra)
            
        rbextras = ['redzoneyear_succ%', 'redzoneyear_td%']
        rbslot1 = rblineups.apply(lambda x: x.sort_values(by='salary',
                                    ascending=False).iloc[0])[slot_cols+rbextras]
        rbslot1.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        rbslot2 = rblineups.apply(lambda x: x.sort_values(by='salary',
                                     ascending=False).iloc[1])[slot_cols+rbextras]
        rbslot2.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        rbslot3 = rblineups.apply(lambda x: flexerror(x,2,rbextras))
        rbslot3.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                         axis=1)
        
        #wr slots
        wrextras = ['redzoneyear_td%','marketshareyear_recyds','marketshareyear_rectgts','marketshareyear_rectd']
        wrslot1 = wrlineups.apply(lambda x: x.sort_values(by='salary',
                                    ascending=False).iloc[0])[slot_cols+wrextras]
        wrslot1.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        
        wrslot2 = wrlineups.apply(lambda x: x.sort_values(by='salary',
                                    ascending=False).iloc[1])[slot_cols+wrextras]
        wrslot2.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        
        wrslot3 = wrlineups.apply(lambda x: x.sort_values(by='salary',
                                    ascending=False).iloc[2])[slot_cols+wrextras]
        wrslot3.drop(['team2', 'projections_projsacks', 'passingyear_int%'],
                        inplace=True, axis=1)
        
        
        wrslot4 = wrlineups.apply(lambda x: flexerror(x,3,wrextras))
        wrslot4.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        #te slots
        teextras = ['redzoneyear_td%','marketshareyear_recyds','marketshareyear_rectgts','marketshareyear_rectd']
        teslot1 = telineups.apply(lambda x: x.sort_values(by='salary',
                                    ascending=False).iloc[0])[slot_cols+teextras]
        teslot1.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        teslot2 = telineups.apply(lambda x: flexerror(x, 1,teextras))
        teslot2.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        #qb slots
        qbslot1 = qblineups.apply(lambda x: x.sort_values(by='salary',
                                    ascending=False).iloc[0])[slot_cols]
        qbslot1.drop(['team2', 'projections_projsacks', 'passingyear_int%'], inplace=True,
                        axis=1)
        
        #d slots
        dslot1 = dlineups.apply(lambda x: x.sort_values(by='salary',
                                    ascending=False).iloc[0])[slot_cols]
        dslot1.drop('team2', inplace=True, axis=1)




        maxplayersfrom1team = lineups.apply(
                    lambda x: x['team'].value_counts().iloc[0])
        num_games_represented = lineups.apply(
                lambda x: len(x['games'].unique()))
        opponents = nodlineups.apply(
                lambda x: x['opp'].tolist())
        defense = dlineups.apply(
                lambda x: x['team'].tolist())
        off_def_df = pd.DataFrame(opponents, columns=['opp']).join(
                pd.DataFrame(defense, columns=['team']))
        is_playing_d = off_def_df.apply(lambda row: bool(set(row['team']) & \
                                                            set(row['opp'])), axis=1)
        chalk_players =  lineups.apply(
                lambda x: len(x[x['projections_projown']>=20]))
        



        numberofgamestacks = lineups.apply(lambda x: len(x['games'].value_counts()[
                    x['games'].value_counts()>1].index.values))
            
        game_stack_strings = gamestacksgroup.apply(
                lambda x: x['slot'].tolist() if len(x)>1
                else [])
        
        game_stack_salaries = gamestacksgroup.apply(
                lambda x: x['salary'].sum()/len(x) if len(x)>1
                else 0) 
        
        game_stack_ou = gamestacksgroup.apply(
                lambda x: x['vegas_o/u'].sum()/len(x) if len(x)>1
                else 0)
        
        gamestackdf = pd.DataFrame(game_stack_strings).join(
                pd.DataFrame(game_stack_salaries), rsuffix='_salary').join \
                (pd.DataFrame(game_stack_ou)).reset_index()
        

        gamestackdf['0'] = gamestackdf['0'].apply(lambda x: ''.join(
                sorted(x)) if len(x)>1 else 0)
        
        
        gamestackdf = gamestackdf.sort_values(['lineup','0'])
                
                
        #part 2 (out agged stats into lists sorted by stack string)
        game_stack_strings2 = gamestackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['0'] \
                                    .tolist())
        
        game_stack_salaries2 = gamestackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['0_salary'] \
                                    .tolist())
        
        game_stack_ou2 = gamestackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0][0] 
                                    .tolist())
        
        #part 3 create feature column by stack string
        game_stack1 = game_stack_strings2.apply(lambda x: x[0] 
                                                if len(x)>=1 else 0)
        game_stack2 = game_stack_strings2.apply(lambda x: x[1]
                                                if len(x)>=2 else 0)
        game_stack3 = game_stack_strings2.apply(lambda x: x[2] 
                                                if len(x)>=3 else 0)
        game_stack4 = game_stack_strings2.apply(lambda x: x[3] 
                                                if len(x)==4 else 0)
        
        game_stack1salary = game_stack_salaries2.apply(lambda x: x[0]
                                                if len(x)>=1 else 0)
        game_stack2salary = game_stack_salaries2.apply(lambda x: x[1] 
                                                if len(x)>=2 else 0)
        game_stack3salary = game_stack_salaries2.apply(lambda x: x[2]
                                                if len(x)>=3 else 0)
        game_stack4salary = game_stack_salaries2.apply(lambda x: x[3]
                                                if len(x)==4 else 0)
        
        game_stack1ou = game_stack_ou2.apply(lambda x: x[0]
                                                if len(x)>=1 else 0)
        game_stack2ou = game_stack_ou2.apply(lambda x: x[1] 
                                                if len(x)>=2 else 0)
        game_stack3ou = game_stack_ou2.apply(lambda x: x[2]
                                                if len(x)>=3 else 0)
        game_stack4ou = game_stack_ou2.apply(lambda x: x[3]
                                                if len(x)==4 else 0)
        

        numberofteamstacks = lineups.apply(lambda x: len(x['team'].value_counts()[
                    x['team'].value_counts()>1].index.values))
            
        team_stack_strings = teamstackgroup.apply(
                lambda x: x['slot'].tolist() if len(x)>1
                else [])

        team_stack_game = teamstackgroup.apply(
                lambda x: x['games'].tolist()[0] if len(x)>1
                else [])

        team_stack_game_raw = teamstackgroup.apply(
                lambda x: x['games2'].tolist()[0] if len(x)>0
                else [])
        

        team_stack_comeback = pd.concat([team_stack_game, team_stack_game_raw, team_stack_strings], axis=1)
        team_stack_comeback['opp'] = team_stack_comeback[1].apply(lambda x: x[1])
        team_stack_comeback['isteamstack'] = team_stack_comeback[2].apply(lambda x: len(x))
        team_stack_comeback['teamtemp'] = team_stack_comeback.index.get_level_values(1)
        lineup_teams = lineups.apply(lambda x: x['team'].tolist())
        lineup_teams.name = 'lineup_teams'
        team_stack_comeback = team_stack_comeback.join(lineup_teams)
        team_stack_comeback['player_opp_same_team'] = team_stack_comeback.groupby(level=[0,1]).apply(
                lambda x: len(set([x['opp'].iloc[0]]).intersection(x['lineup_teams'].iloc[0])))
        team_stack_comeback['comeback'] = np.where((team_stack_comeback['player_opp_same_team']==1) & (team_stack_comeback['isteamstack']>0),1,0)
        team_stack_comeback = team_stack_comeback['comeback']



        team_stack_salaries = teamstackgroup.apply(
                    lambda x: x['salary'].sum()/len(x) if len(x)>1
                    else 0) 
            
        team_stack_ou = teamstackgroup.apply(
                lambda x: x['vegas_o/u'].sum()/len(x) if len(x)>1
                else 0)

        team_stack_fpo = teamstackgroup.apply(
                lambda x: x['fantasymonth_fp/oppor'].sum()/len(x) if len(x)>1
                else 0)

        teamstackdf = pd.DataFrame(team_stack_strings).join(
                    pd.DataFrame(team_stack_salaries), rsuffix='_salary').join \
                    (pd.DataFrame(team_stack_ou))
        teamstackdf = teamstackdf.join(pd.DataFrame(team_stack_game, columns=['stack_game']))
        teamstackdf = teamstackdf.join(pd.DataFrame(team_stack_comeback, columns=['comeback']))

        teamstackdf = teamstackdf.join(pd.DataFrame(team_stack_fpo, columns=['stack_fpo'])).reset_index()

        teamstackdf['0'] = teamstackdf['0'].apply(lambda x: ''.join(
                sorted(x)) if len(x)>1 else 0)

        teamstackdf = teamstackdf.sort_values(['lineup','0'])
                
                
        team_stack_strings2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0].sort_values(['0'])['0'] \
                                    .tolist())

        team_stack_games2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0].sort_values(['0'])['stack_game'] \
                                    .tolist())
        team_stack_games2 = team_stack_games2.apply(lambda x: len(x)==len(set(x)))
    
        team_stack_comeback2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['comeback'].sum())
        
        
        team_stack_salaries2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0]['0_salary'] \
                                    .tolist())
        
        team_stack_ou2 = teamstackdf.groupby('lineup').apply(
                lambda x: x[x[0]!=0][0] \
                                    .tolist())
        team_stack_fpo2 = teamstackdf.groupby('lineup').apply(
                    lambda x: x[x[0]!=0]['stack_fpo'].sum()/len(x[x[0]!=0]))
        
        team_stack1 = team_stack_strings2.apply(lambda x: x[0] 
                                                    if len(x)>=1 else 'none')
        team_stack2 = team_stack_strings2.apply(lambda x: x[1]
                                                if len(x)>=2 else 'none')
        team_stack3 = team_stack_strings2.apply(lambda x: x[2] 
                                                if len(x)>=3 else 'none')
        team_stack4 = team_stack_strings2.apply(lambda x: x[3] 
                                                if len(x)==4 else 'none')
        
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

        head_to_head_team_stacks = pd.DataFrame(team_stack_games2.copy(), columns=['head_to_head_team_stacks'])
        head_to_head_team_stacks['head_to_head_team_stacks'] = np.where(head_to_head_team_stacks['head_to_head_team_stacks']==False,1,0)
        head_to_head_team_stacks = head_to_head_team_stacks['head_to_head_team_stacks'] 
        
        #flex analysis
        whose_in_flex = lineups.apply(lambda x: x['Position'].value_counts() \
                                        .subtract(
        pd.Series([1,2,3,1,1], index=['QB','RB','WR','D','TE'])).sort_values() \
                                        .index[-1])
                                        
        #%thrown to positions   
        throw_2rb = lineups['passingyear_%rb'].sum()
        throw_2wr = lineups['passingyear_%wr'].sum()
        throw_2te = lineups['passingyear_%te'].sum()  

        #rolling month fantasy points per snap / fantasy points per opporunity 
        points_per_opp = lineups['fantasymonth_fp/oppor'].sum()
        points_per_snap = lineups['fantasymonth_fp/snap'].sum()
        opps_per_snap = points_per_snap/points_per_opp

        analysis = pd.concat([
                                  team_sums,
                                  team_prods,
                                  rbown,
                                  wrown,
                                  teown,
                                  qbown,
                                  down,
                                  sal_std,
                                  plyrs_eq_0,
                                  plyrs_0,
                                  plyrs_less_5,
                                  plyrs_less_10,
                                  plyrs_less_25,
                                  plyrs_abv_90,
                                  plyrs_abv_99,
                                  plyrs_eq_1, 
                                  rbslot1,
                                  rbslot2,
                                  rbslot3, #],axis=1)
                                  wrslot1,
                                  wrslot2,
                                  wrslot3,
                                  wrslot4,
                                  teslot1,
                                  teslot2,
                                  qbslot1,
                                  dslot1,
                                  maxplayersfrom1team,
                                  num_games_represented,
                                  is_playing_d,
                                  chalk_players,
                                  numberofgamestacks,
                                  game_stack1,
                                  game_stack2,
                                  game_stack3,
                                  game_stack4,
                                  game_stack1salary,
                                  game_stack2salary,
                                  game_stack3salary, 
                                  game_stack4salary,
                                  game_stack1ou,
                                  game_stack2ou,
                                  game_stack3ou,
                                  game_stack4ou,
                                  numberofteamstacks,
                                  team_stack1,
                                  team_stack2,
                                  team_stack3,
                                  team_stack4,
                                  team_stack1salary,
                                  team_stack2salary,
                                  team_stack3salary, 
                                  team_stack4salary,
                                  team_stack1ou,
                                  team_stack2ou,
                                  team_stack3ou,
                                  team_stack4ou,
                                  team_stack_fpo2,
                                  head_to_head_team_stacks,
                                  team_stack_comeback2,
                                  whose_in_flex,
                                  throw_2rb,
                                  throw_2wr,
                                  throw_2te,
                                  #points_per_opp,
                                  #points_per_snap,
                                  #opps_per_snap
                                  
                                ], axis=1)
        
        analysis.columns = [
                    'team_proj',
                    'team_proj_own',
                    'team_proj+-',
                    'team_pts',
                    'team_ou',
                    'team_opp+-',
                    'team_snaps',
                    'team_spread',
                    'pass_succ',
                    'rush_succ',
                    'takeaway%',
                    'team_proj_own_prod',
                    'team_rbown',
                    'team_wrown',
                    'team_teown',
                    'team_qbown',
                    'team_down',
                    'salary_std',
                    'plyrs_eq_0',
                    'plyrs_<_0',
                    'plyrs_less_5',
                    'plyrs_less_10',
                    'plyrs_less_25',
                    'plyrs_abv_90',
                    'plyrs_abv_99',
                    'plyrs_eq_1',
                    'rb1_salary',
                    'rb1_snaps',
                    'rb1_opp+-',
                    'rb1_pts',
                    'rb1_proj',
                    'rb1_proj+-',
                    'rb1_ceil',
                    'rb1_spread',
                    'rb1_own',
                    'rb1_bargain',
                    'rb1_leverage',
                    'rb1_optpts',
                    'rb1_rz_succ%',
                    'rb1_rz_td_pct',
                    'rb2_salary',
                    'rb2_snaps',
                    'rb2_opp+-',
                    'rb2_pts',
                    'rb2_proj',
                    'rb2_proj+-',
                    'rb2_ceil',
                    'rb2_spread',
                    'rb2_own',
                    'rb2_bargain',
                    'rb2_leverage',
                    'rb2_optpts',
                    'rb2_rz_succ%',
                    'rb2_rz_td_pct',
                    'rb3_salary',
                    'rb3_snaps',
                    'rb3_opp+-',
                    'rb3_pts',
                    'rb3_proj',
                    'rb3_proj+-',
                    'rb3_ceil',
                    'rb3_spread',
                    'rb3_own',
                    'rb3_bargain',
                    'rb3_leverage',
                    'rb3_optpts',
                    'rb3_rz_succ%',
                    'rb3_rz_td_pct',
                    'wr1_salary',
                    'wr1_snaps',
                    'wr1_opp+-',
                    'wr1_pts',
                    'wr1_proj',
                    'wr1_proj+-',
                    'wr1_ceil',
                    'wr1_spread',
                    'wr1_own',
                    'wr1_bargain',
                    'wr1_leverage',
                    'wr1_optpts',
                    'wr1_rz_td_pct',
                    'wr1_rec_yds%',
                    'wr1_rec_trgts%',
                    'wr1_rec_td%',
                    'wr2_salary',
                    'wr2_snaps',
                    'wr2_opp+-',
                    'wr2_pts',
                    'wr2_proj',
                    'wr2_proj+-',
                    'wr2_ceil',
                    'wr2_spread',
                    'wr2_own',
                    'wr2_bargain',
                    'wr2_leverage',
                    'wr2_optpts',
                    'wr2_rz_td_pct',
                    'wr2_rec_yds%',
                    'wr2_rec_trgts%',
                    'wr2_rec_td%',
                    'wr3_salary',
                    'wr3_snaps',
                    'wr3_opp+-',
                    'wr3_pts',
                    'wr3_proj',
                    'wr3_proj+-',
                    'wr3_ceil',
                    'wr3_spread',
                    'wr3_own',
                    'wr3_bargain',
                    'wr3_leverage',
                    'wr3_optpts',
                    'wr3_rz_td_pct',
                    'wr3_rec_yds%',
                    'wr3_rec_trgts%',
                    'wr3_rec_td%',
                    'wr4_salary',
                    'wr4_snaps',
                    'wr4_opp+-',
                    'wr4_pts',
                    'wr4_proj',
                    'wr4_proj+-',
                    'wr4_ceil',
                    'wr4_spread',
                    'wr4_own',
                    'wr4_bargain',
                    'wr4_leverage',
                    'wr4_optpts',
                    'wr4_rz_td_pct',
                    'wr4_rec_yds%',
                    'wr4_rec_trgts%',
                    'wr4_rec_td%',
                    'te1_salary',
                    'te1_snaps',
                    'te1_opp+-',
                    'te1_pts',
                    'te1_proj',
                    'te1_proj+-',
                    'te1_ceil',
                    'te1_spread',
                    'te1_own',
                    'te1_bargain',
                    'te1_leverage',
                    'te1_optpts',
                    'te1_rz_td_pct',
                    'te1_rec_yds%',
                    'te1_rec_trgts%',
                    'te1_rec_td%',
                    'te2_salary',
                    'te2_snaps',
                    'te2_opp+-',
                    'te2_pts',
                    'te2_proj',
                    'te2_proj+-',
                    'te2_ceil',
                    'te2_spread',
                    'te2_own',
                    'te2_bargain',
                    'te2_leverage',
                    'te2_optpts',
                    'te2_rz_td_pct',
                    'te2_rec_yds%',
                    'te2_rec_trgts%',
                    'te2_rec_td%',
                    'qb1_salary',
                    'qb1_snaps',
                    'qb1_opp+-',
                    'qb1_pts',
                    'qb1_proj',
                    'qb1_proj+-',
                    'qb1_ceil',
                    'qb1_spread',
                    'qb1_own',
                    'qb1_bargain',
                    'qb1_leverage',
                    'qb1_optpts',
                    'd1_salary',
                    'd1_snaps',
                    'd1_opp+-',
                    'd1_pts',
                    'd1_proj',
                    'd1_proj+-',
                    'd1_ceil',
                    'd1_spread',
                    'd1_own',
                    'd1_bargain',
                    'd1_leverage',
                    'd1_optpts',
                    'proj_sacks',
                    'int%',
                    'maxplayersfrom1team',
                    'num_games_represented',
                    'is_playing_d',
                    'chalk_players',
                    'numberofgamestacks',
                    'game_stack1',
                    'game_stack2',
                    'game_stack3',
                    'game_stack4',
                    'game_stack1salary',
                    'game_stack2salary',
                    'game_stack3salary', 
                    'game_stack4salary',
                    'game_stack1ou',
                    'game_stack2ou',
                    'game_stack3ou',
                    'game_stack4ou',
                    'numberofteamstacks',
                    'team_stack1',
                    'team_stack2',
                    'team_stack3',
                    'team_stack4',
                    'team_stack1salary',
                    'team_stack2salary',
                    'team_stack3salary', 
                    'team_stack4salary',
                    'team_stack1ou',
                    'team_stack2ou',
                    'team_stack3ou',
                    'team_stack4ou',
                    'team_stack_fpo2',
                    'head_to_head_stacks',
                    'comeback',
                    'whose_in_flex',
                    'throw_2rb',
                    'throw_2wr',
                    'throw_2te',
                    #'points_per_opp',
                    #'points_per_snap',
                    #'opps_per_snap'
                    ]
        
        analysis['pct_of_optimal_projected'] = lineups['projections_proj'].sum()/(opt_team_score*9)
        
        analysis.loc[:,'team_proj':'num_games_represented'] = analysis.loc[:,'team_proj':'num_games_represented'].rank(pct=True)
        analysis.loc[:,'game_stack1salary':'game_stack4ou'] = analysis.loc[:,'game_stack1salary':'game_stack4ou'].rank(pct=True)
        analysis.loc[:,'team_stack1salary':'team_stack_fpo2'] = analysis.loc[:,'team_stack1salary':'team_stack_fpo2'].rank(pct=True)
        analysis.loc[:,'throw_2rb':'throw_2te'] = analysis.loc[:,'throw_2rb':'throw_2te'].rank(pct=True)

        if self.historical==True:
                analysis['actual_sum'] = lineups['actual'].sum()
                ##analysis['lineup'] = lineups['actual'].first() 
        
        analysis = analysis.reset_index()
        analysis['week'] = self.cur_week
        analysis['id'] = analysis['week'].astype(str) + \
        analysis['lineup'].astype(str) 

        if self.historical==True:
            analysis['ismilly'] = np.where(analysis['actual_sum']>(slates[self.site]['nfl'][str(self.date)]['winning_score']*.999), 1,0)

        if self.historical == True: 
                analysis.to_csv(r"C:\Users\rmathews\.unreal_fantasy\dataiku\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nfl',
                self.hist,
                str(self.cur_week)),
                compression='gzip', index=False) 
        else:
                analysis.to_csv(r"C:\Users\rmathews\.unreal_fantasy\dataiku\{0}\{1}\{2}\{3}.csv.gz".format(
                self.site,
                'nfl',
                self.hist,
                str(self.live_tag)),
                compression='gzip', index=False) 
             
        
        return analysis



            



            



        
       











        






        


  