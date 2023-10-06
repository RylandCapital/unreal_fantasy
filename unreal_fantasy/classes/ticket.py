import os
import sys
import numpy as np
import pandas as pd
import time
import csv
import statistics

from ortools.linear_solver import pywraplp

sys.path.append('p:\\10_CWP Trade Department\\Ryland\\unreal_fantasy\\unreal_fantasy\\')
from classes.optimizer import Optimize



class Player:
  def __init__(self, opts):
      
      self.proba1 = round(float(opts['proba_1']),4)
      self.rank = int(float(opts['proba_rank']))
      self.lineup = str(opts['lineup'])
      self.team_proj = round(float(opts['team_proj']),4)
      self.team_pm = round(float(opts['team_+/-']),4)
      self._1 = str(opts['1'])
      self._2 = str(opts['2'])
      self._3 = str(opts['3'])
      self._4 = str(opts['4'])
      self._5 = str(opts['5'])
      self._6 = str(opts['6'])
      self._7 = str(opts['7'])
      self._8 = str(opts['8'])
      self._9 = str(opts['9'])
      self.pred_owns = []
      self.lock = False
      self.ban = False
       
   
  def __repr__(self):
    return "[{0},{1},{2},{3}]".format(self.proba1,self.rank,self.lineup,self.team_proj)
                                    
class Roster:

  POSITION_ORDER = {
    "TEAM": 1,
  }

  def __init__(self):
    self.players = []

  def add_player(self, player):
    self.players.append(player)
  
  def sum_actual(self):
     return round(sum(map(lambda x: x.proba1, self.players)),2)
  
  def mean_actual(self):
     return round(statistics.mean(map(lambda x: x.proba1, self.players)),4)

  def min_actual(self):
     return min(map(lambda x: x.proba1, self.players))

  def max_actual(self):
     return max(map(lambda x: x.proba1, self.players))
  
  def min_proj(self):
     return min(map(lambda x: x.team_proj, self.players))
  
  def max_proj(self):
     return max(map(lambda x: x.team_proj, self.players))
  
  def mean_proj(self):
     return round(statistics.mean(map(lambda x: x.team_proj, self.players)),2)

  def min_pm(self):
     return min(map(lambda x: x.team_pm, self.players))
  
  def max_pm(self):
     return max(map(lambda x: x.team_pm, self.players))
  
  def mean_pm(self):
     return round(statistics.mean(map(lambda x: x.team_pm, self.players)),2)
    
  

  def __repr__(self):
    s = "Sum Proba1: %s" % self.sum_actual()
    s += "\nMin Proba1: %s" % self.min_actual()
    s += "\nMax Proba1: %s" % self.max_actual()
    s += "\nMean Proba1: %s" % self.mean_actual()
    s += "\nMin Proj: %s" % self.min_proj()
    s += "\nMax Proj: %s" % self.max_proj()
    s += "\nMean Proj: %s" % self.mean_proj()
    s += "\nMin Plus/Minus: %s" % self.min_pm()
    s += "\nMax Plus/Minus: %s" % self.max_pm()
    s += "\nMean Plus/Minus: %s" % self.mean_pm()
    return s

class Ticket:
    
    '''slate_date'''
    def __init__(self, slate_date, site, sport, site_file=''):

        self.sport = sport
        self.site = site
        self.slate_date = slate_date
        self.site_file = site_file

    def salary_aggregate(self):
       # make a file with dk/fanduel salaries matched up

       fd = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            'fanduel',
            self.sport,
            'live',
            str(self.slate_date)))
       fd = fd.rename(columns={'Salary':'fanduel_Salary'})
       fd = fd.rename(columns={'Unnamed: 0':'fanduel_ArbID'})
       fd['fanduel_ArbID'] = fd['RylandID_master'].copy()
 
       
       dk = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            'draftkings',
            self.sport,
            'live',
            str(self.slate_date)))
       dk = dk.rename(columns={'Salary':'draftkings_Salary'})
       dk = dk.rename(columns={'Unnamed: 0':'draftkings_ArbID'})
       dk['draftkings_ArbID'] = dk['RylandID_master'].copy()

       

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

    def prepare(self, num_top_probas=100000, removals=[], sabersim=False):
        

        predictions = pd.read_csv('C:\\Users\\rmathews\\.unreal_fantasy\\_live_projections\\{0}_{1}.csv'.format(self.site, self.sport))
        predictions.rename(columns={'proba_1.0':'proba_1'}, inplace=True)
        if (sabersim==True) & (self.sport=='nfl'):
           predictions['ss_check'] = predictions['lineup'].str[-3:]
           predictions = predictions[predictions['ss_check']=='_ss'].sort_values(by='proba_1', ascending=False)
        else:
           predictions = predictions.sort_values(by='proba_1', ascending=False).iloc[:num_top_probas]
        predictions = predictions.sort_values(by='lineup',ascending=False) 

        optimized_path = 'C:\\Users\\rmathews\\.unreal_fantasy\\optimizations\\{0}\\{1}\\live\\'.format(self.site, self.sport)
        onlyfiles = [f for f in os.listdir(optimized_path) if os.path.isfile(os.path.join(optimized_path, f))]
        teams = pd.concat([pd.read_csv(optimized_path + f, compression='gzip').sort_values('lineup',ascending=False) for f in onlyfiles])
        #trim teams to only ones represented by dataiku preditions
        teams = teams[teams['lineup'].isin(predictions['lineup'].unique())]
        teams['Unnamed: 0.1'] = teams['Unnamed: 0'].copy()
        
        '''join salary arb info, ***need both fanduel and draftkings scrapes'''
        stats = self.salary_aggregate()
        stats.set_index('{0}_ArbID'.format(self.site), inplace=True)
        teams = teams.set_index('Unnamed: 0.1').join(stats, how='inner', lsuffix='_ot').reset_index()

        #confirm 9 only 
        nine_confirm = teams.groupby('lineup').apply(lambda x: len(x))
        teams = teams.set_index('lineup').loc[nine_confirm[nine_confirm==9].index.tolist()].reset_index()

        removedf = pd.DataFrame(teams.groupby('lineup').apply(lambda x: sorted(x['Id'].tolist())))
        removedf['isremove'] = removedf[0].apply(lambda x: len(list(set(x).intersection(set(removals)))))
        keepers = removedf[removedf['isremove']==0].index.unique().tolist()
        teams = teams.set_index('lineup').loc[keepers].reset_index()

        #join teams and predicttions
        picks = predictions[['lineup', 'proba_1']].set_index('lineup').join(teams.set_index('lineup'), how='inner')
        picks['proba_rank'] = picks['proba_1'].rank(method='max', ascending=False)/9
        picks['check4max'] = picks.groupby(level=0)['team'].value_counts().max(level=0)
        picks = picks[picks['check4max']<4]
        picks.sort_values(by='proba_1', ascending=False, inplace=True)


        #########

        return picks #, stats
    
    def run(self, roster_size=150, own_limits=''):

      solver = pywraplp.Solver('FD', pywraplp.Solver.CBC_MIXED_INTEGER_PROGRAMMING)
      all_players = []  

      with open(r'C:\Users\rmathews\.unreal_fantasy\_live_projections\optimtkttemp.csv', 'r') as csvfile:
         csvdata = csv.DictReader(csvfile, skipinitialspace=True)
         for row in csvdata:
            all_players.append(Player(row)) #this is adding a TEAM, each row is a TEAM at this level

      variables = []
      all_players = np.random.choice(all_players, size=int(len(all_players))
            , replace=False)
      for player in all_players:
         if player.lock:
            variables.append(solver.IntVar(1, 1, player.lineup))
         elif player.ban:
            variables.append(solver.IntVar(0, 0, player.lineup))
         else:      
            variables.append(solver.IntVar(0, 1, player.lineup))
         
      objective = solver.Objective()
      objective.SetMaximization()
      
      for i, player in enumerate(all_players):
         objective.SetCoefficient(variables[i], player.proba1)
         
      size_cap = solver.Constraint(roster_size, roster_size)
      for variable in variables:
         size_cap.SetCoefficient(variable, 1)

      for position, min_limit, max_limit in own_limits:
         position_cap = solver.Constraint(min_limit, max_limit)

         #variables[i] (i is integer) returns lineup_46259_28
         for i, player in enumerate(all_players):
            #position = 
            if position == player._1:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._2:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._3:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._4:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._5:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._6:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._7:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._8:
               position_cap.SetCoefficient(variables[i], 1)
            if position == player._9:
               position_cap.SetCoefficient(variables[i], 1)

      solution = solver.Solve()

      if solution == solver.OPTIMAL:
         roster = Roster()

         for i, player in enumerate(all_players):
            if variables[i].solution_value() == 1:
               roster.add_player(player)

      else:
         print("No solution :(")
         
      return roster

    def file_construct(self, picks):

      ticket = picks.copy()
      ticket.rename(columns={'Nickname':'Name'}, inplace=True)
      ##ticket.to_csv(r'C:\Users\rmathews\.unreal_fantasy\ticket_debug.csv')

      opt_team = Optimize(self.sport, self.site, False).projected_optimal(self.slate_date)
      opt_team_score = opt_team.actual()

      selections = []
      exposures = dict(zip(ticket['Name'].unique().tolist(),'0'*len(ticket['Name'].unique().tolist())))

      if self.sport == 'nhl':
         for i,n in zip(ticket.index.unique(), np.arange(len(ticket.index.unique()))):
               ticket_cols = ['C','C','W','W','D','D','FLEX','FLEX','G']
               df = ticket.loc[i][['pos','Id','Name','proba_1',
               'other_site_salary', 
               'Salary', 'proj_proj']].sort_values('Id')
               id2 = sorted(df['Id'].values)
               id2_names = sorted(df['Name'].values)

               proj = df['proba_1'].iloc[0]
               proj_pts = df['proj_proj'].sum()
               other_site_salary = df['other_site_salary'].iloc[0]
               salary = df['Salary'].sum()

               df = df[['pos','Id']].sort_values('pos')
               df.set_index('pos', inplace=True)

               sections = []
               for l in df.index.unique():
                  t = pd.DataFrame(df.loc[l])
               if len(t) > 2:
                  t.index = [l,l] + ['FLEX']*(len(t)-2)
                  sections.append(t)
               elif len(t) == 1:
                  sections.append(t.T)
               else:
                  sections.append(t)

               df = pd.concat(sections).loc[ticket_cols].drop_duplicates('Id').T
               df['id2'] = str(id2)
               df['name'] = str(id2_names)
               df['proba_1'] = proj
               df['projected'] = proj_pts
               df['pct_optimal'] = round(proj_pts/opt_team_score,2)
               df['Salary'] = salary
               df['other_site_salary'] = other_site_salary

               update = [exposures.update({i:float(exposures[i])+1}) for i in id2_names]
               selections.append(df)
      else:
         for i,n in zip(ticket.index.unique(), np.arange(len(ticket.index.unique()))):
               ticket_cols = ['QB','RB','RB','WR','WR','WR','TE','FLEX','D']
               df = ticket.loc[i][['pos','Id','Name','proba_1',
               'other_site_salary', 
               'Salary', 'projections_proj']].sort_values('Id')
               id2 = sorted(df['Id'].values)
               id2_names = sorted(df['Name'].values)

               proj = df['proba_1'].iloc[0]
               proj_pts = df['projections_proj'].sum()
               other_site_salary = df['other_site_salary'].iloc[0]
               salary = df['Salary'].sum()

               df = df[['pos','Id']].sort_values('pos')
               df.set_index('pos', inplace=True)

               sections = []
               for l in df.index.unique():
                  t = pd.DataFrame(df.loc[l])
                  posit = t.index[0]
                  if len(t) == 4:
                     t.index = [l,l,l] + ['FLEX']
                     sections.append(t)
                  if (len(t) == 3) & (posit == 'WR'):
                     sections.append(t)
                  if (len(t) == 3) & (posit == 'RB'):
                     t.index = [l,l] + ['FLEX']
                     sections.append(t)
                  if (len(t) == 2) & (posit == 'RB'):
                     sections.append(t)
                  if (len(t) == 2) & (posit == 'TE'):
                     t.index = [l] + ['FLEX']
                     sections.append(t)
                  elif len(t) == 1:
                     sections.append(t.T)
                  else:
                     sections.append(t)

               df = pd.concat(sections).loc[ticket_cols].drop_duplicates('Id').T
               df['id2'] = str(id2)
               df['name'] = str(id2_names)
               df['proba_1'] = proj
               df['projected'] = proj_pts
               df['pct_optimal'] = round(proj_pts/opt_team_score,2)
               df['Salary'] = salary
               df['other_site_salary'] = other_site_salary
               df['lineup'] = i

               update = [exposures.update({i:float(exposures[i])+1}) for i in id2_names]
               selections.append(df)

  

      upload = pd.concat(selections)
      #remove duplicate teams (id2)
      upload = upload.sort_values(by='proba_1', ascending=False).drop_duplicates('id2',keep='first')
      upload.drop('id2', axis=1).to_csv('C:\\Users\\rmathews\\.unreal_fantasy\\tickets\\{0}_{1}_{2}_ticket.csv'.format(self.site, self.sport, self.slate_date))
      exposuresdf = (pd.DataFrame.from_dict(exposures,orient='index').astype(float).sort_values(by=0, ascending=False)/len(selections)*100).round(1)
      try:
         exposuresdf = exposuresdf.join(ticket[['Name', 'team', 'pos', 'Salary', 'proj_proj']].set_index('Name')).drop_duplicates().sort_values(by=0, ascending=False)
      except:
         exposuresdf = exposuresdf.join(ticket[['Name','projections_projown', 'team', 'pos', 'Salary', 'projections_proj']].set_index('Name')).drop_duplicates().sort_values(by=0, ascending=False)
      exposuresdf.columns = ['My Ownership', 'Projected Ownership', 'Team', 'Position', 'Salary', 'Projected Points']
      exposuresdf.to_csv('C:\\Users\\rmathews\\.unreal_fantasy\\tickets\\{0}_{1}_{2}_exposures.csv'.format(self.site, self.sport, self.slate_date))


      return upload, exposuresdf

    def optimize_upload_file(self, roster_size=150, pct_from_opt_proj=.808, max_pct_own=.33, other_site_min=0, sabersim_only=False, h2h=False, h2h_rank=.99, min_team_sal=50000, removals=[]):

       
      picks = self.prepare(num_top_probas=100000, removals=removals, sabersim=sabersim_only)
      picks['team_proj'] = picks.groupby(level=0)['actual'].sum()
      try: #NHL
         picks['team_+/-'] = picks.groupby(level=0)['proj_proj+/-'].sum()
         picks['team_floor'] = picks.groupby(level=0)['proj_floor'].sum().rank(pct=True)
         picks['team_pts/sal'] = picks.groupby(level=0)['proj_pts/sal'].sum().rank(pct=True)
         picks['team_projown'] = picks.groupby(level=0)['proj_projown'].sum().rank(pct=True)
         picks['team_salary'] = picks.groupby(level=0)['{0}_Salary'.format(self.site)].sum()
      except: #NFL
         picks['team_+/-'] = picks.groupby(level=0)['projections_proj+/-'].sum()
         picks['team_floor'] = picks.groupby(level=0)['projections_floor'].sum().rank(pct=True)
         picks['team_pts/sal'] = picks.groupby(level=0)['projections_pts/sal'].sum().rank(pct=True)
         picks['team_projown'] = picks.groupby(level=0)['projections_projown'].sum().rank(pct=True)
         picks['team_salary'] = picks.groupby(level=0)['{0}_Salary'.format(self.site)].sum()
      if h2h==True:
         picks = picks[(picks['team_floor']>h2h_rank)&(picks['team_projown']>h2h_rank)&(picks['team_salary']>=min_team_sal)]
      #need salary arb stuff
      flip_dict = {'fanduel':'draftkings', 'draftkings':'fanduel'}
      picks['other_site_salary'] = picks.groupby(level=0)['{0}_Salary'.format(flip_dict[self.site])].sum()

      player_list = pd.DataFrame(picks.groupby(level=0).apply(lambda x: x['RylandID'].tolist()))
      player_list[1] = player_list[0].apply(lambda x: x[0])
      player_list[2] = player_list[0].apply(lambda x: x[1])
      player_list[3] = player_list[0].apply(lambda x: x[2])
      player_list[4] = player_list[0].apply(lambda x: x[3])
      player_list[5] = player_list[0].apply(lambda x: x[4])
      player_list[6] = player_list[0].apply(lambda x: x[5])
      player_list[7] = player_list[0].apply(lambda x: x[6])
      player_list[8] = player_list[0].apply(lambda x: x[7])
      player_list[9] = player_list[0].apply(lambda x: x[8])
      player_list.drop(0,axis=1, inplace=True)
      picks = picks.join(player_list)

      picks = picks.reset_index().set_index('name').reset_index(drop=True).sort_values(by='lineup').set_index('lineup')

      teams = picks.groupby(level=0).first()
      teams = teams.sort_values(by='proba_1', ascending=False)

      optimaldf = Optimize(self.sport, self.site, False).projected_optimal(self.slate_date)
      teams=teams[teams['team_proj']>=optimaldf.actual()*pct_from_opt_proj]
      #need salary arb stuff
      teams=teams[teams['other_site_salary']>=other_site_min]
      teams.sort_values(by='proba_1', ascending=False).to_csv(r'C:\Users\rmathews\.unreal_fantasy\_live_projections\optimtkttemp.csv')

      owndict = picks[['index','proba_1']].drop_duplicates('index').set_index('index').to_dict()
      own_limits = []
      for i in owndict['proba_1'].keys():
         entry = ["{0}".format(i), int(-1), int(roster_size*max_pct_own)]
         own_limits.append(entry)

      team = self.run(roster_size=roster_size, own_limits=own_limits)
      players = team.players
      ids = [i.lineup for i in players]

      picks = picks.loc[ids]

      self.file_construct(picks)

      return ids








    

