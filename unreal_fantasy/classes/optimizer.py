import numpy as np
import pandas as pd
import csv
import sys

from ortools.linear_solver import pywraplp

sys.path.append('p:\\10_CWP Trade Department\\Ryland\\unreal_fantasy\\unreal_fantasy\\')
from utils.slates import slates


class Player:
  
  ''''''  

  def __init__(self, opts, sport, historical):

    if sport == 'nhl':
        self.name = opts['RylandID_master']
        self.position = opts['pos'].upper()
        self.salary = int(float((opts['salary'])))
        self.theo_actual = float(np.random.randint(-100,100)) 
        if historical == True:
            self.actual = float(opts['proj_actpts'])
        else:
            self.actual = float(opts['proj_proj'])
        self.plusminus = float(opts['proj_proj+/-'])
        self.proj = float(opts['proj_proj'])
        self.team = str(opts['team_team'])
        self.opp = str(opts['opp'])

    if sport == 'nfl':
        self.name = opts['RylandID_master']
        self.position = opts['pos'].upper()
        self.salary = int(float((opts['salary'])))
        self.theo_actual = float(np.random.randint(-100,100)) 
        if historical == True:
            self.actual = float(opts['projections_actpts'])
        else:
            self.actual = float(opts['projections_proj'])
        self.plusminus = float(opts['projections_proj+/-'])
        self.proj = float(opts['projections_proj'])
        if historical == False:
            self.projown = float(opts['projections_projown'])
        self.team = str(opts['team'])
        self.opp = str(opts['opp'])

  def __repr__(self):
    return "[{0: <2}] {1: <20}(${2}, {3}, {4})".format(self.position, \
                                    self.name, \
                                    self.salary,
                                    self.actual,
                                    self.proj
                                    )


class Roster:
  
    ''' '''

    def __init__(self, sport):
        self.players = []

        if sport == 'nhl':
        
            self.POSITION_ORDER = {
                "C": 0,
                "W": 1,
                "D": 2,
                "G": 3,
                }

        else:
        
            self.POSITION_ORDER = {
                "QB": 0,
                "RB": 1,
                "WR": 2,
                "TE": 3,
                "D": 4,
                }


    def add_player(self, player):
        self.players.append(player)

    def spent(self):
        return sum(map(lambda x: x.salary, self.players)) 

    def projected(self):
        return sum(map(lambda x: x.proj, self.players))
    
    def projected_std(self):
        return np.std(list(map(lambda x: x.proj, self.players)))
    
    def actual(self):
        return sum(map(lambda x: x.actual, self.players))

    def position_order(self, player):
        return self.POSITION_ORDER[player.position]

    def sorted_players(self):
        return sorted(self.players, key=self.position_order)

    def __repr__(self):
        s = '\n'.join(str(x) for x in self.sorted_players())
        s += "\n\nActual Score: %s" % self.actual()
        s += "\n\nProjected Score: %s" % self.projected()
        s += "\tCost: $%s" % self.spent()
        return s


class Optimize:
  
    ''''''

    def __init__(self, sport, site, historical):
        self.sport = sport
        self.site = site
        self.historical = historical
       
        position_limits = {
            'draftkings':
            {
                'nhl':[
                    ["C", 2, 3], 
                    ["W", 3, 4],
                    ["D", 2, 3],
                    ["G", 1, 1],
                    ],

                'nfl':[
                    ["QB", 1, 1], 
                    ["RB", 2, 3],
                    ["WR", 3, 4],
                    ["TE", 1, 2],
                    ["D", 1, 1]
                    ],
            },
            'fanduel':
            {
                'nhl':[
                    ["C", 2, 4], 
                    ["W", 2, 4],
                    ["D", 2, 4],
                    ["G", 1, 1],
                    ],

                'nfl':[
                    ["QB", 1, 1], 
                    ["RB", 2, 3],
                    ["WR", 3, 4],
                    ["TE", 1, 2],
                    ["D", 1, 1]
                    ],
            }
        }

        roster_sizes = {
            'draftkings':
            {
                'nhl':9,
                'nfl':9,
            },
            'fanduel':
            {
                'nhl':9,
                'nfl':9,
            }
        }

        salary_bands = {
            'draftkings':
            {
                'nhl':[49800,50000],
                'nfl':[49800,50000],
            },
            'fanduel':
            {
                'nhl':[54800,55000],
                'nfl':[59800, 60000],
            }
        }


        self.position_limit = position_limits[site][sport]
        self.roster_size = roster_sizes[site][sport]
        self.salary_band = salary_bands[site][sport]

        if historical==True:
            self.hist='historical'
        else:
            self.hist='live'
    
    # cur_week will either by a date or integer depending on live or historical
    def projected_optimal(self, cur_week):

        solver = pywraplp.Solver('FD', pywraplp.Solver.CBC_MIXED_INTEGER_PROGRAMMING)

        all_players = []
        with open(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            self.site,
            self.sport,
            self.hist,
            str(cur_week)),
            'r') as csvfile:
            
            csvdata = csv.DictReader(csvfile, skipinitialspace=True)

            for row in csvdata:
                plyr = Player(row, self.sport, self.historical)
                if (plyr.actual > 0):
                    all_players.append(plyr)
        
        variables = []
        all_players = np.random.choice(all_players, size=int(len(all_players))
            , replace=False)
        for player in all_players:     
                variables.append(solver.IntVar(0, 1, player.name))

        objective = solver.Objective()
        objective.SetMaximization()

        for i, player in enumerate(all_players):
            objective.SetCoefficient(variables[i], player.proj)

        salary_cap = solver.Constraint(self.salary_band[0], self.salary_band[1])
        for i, player in enumerate(all_players):
            salary_cap.SetCoefficient(variables[i], player.salary)

        for position, min_limit, max_limit in self.position_limit:
            position_cap = solver.Constraint(min_limit, max_limit)

            for i, player in enumerate(all_players):
                if position == player.position:
                    position_cap.SetCoefficient(variables[i], 1)

        size_cap = solver.Constraint(self.roster_size, self.roster_size)
        for variable in variables:
            size_cap.SetCoefficient(variable, 1)

        solution = solver.Solve()
        
        if solution == solver.OPTIMAL:
            roster = Roster(sport=self.sport)

            for i, player in enumerate(all_players):
                if variables[i].solution_value() == 1:
                    roster.add_player(player)

        else:
            print("No solution :(")
    
        return roster
        

    # cur_week will either by a date or integer depending on live or historical
    def run(self, cur_week, limlow, limhigh, projlimlow):

        solver = pywraplp.Solver('FD', pywraplp.Solver.CBC_MIXED_INTEGER_PROGRAMMING)

        all_players = []
        with open(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            self.site,
            self.sport,
            self.hist,
            str(cur_week)),
            'r') as csvfile:
            
            csvdata = csv.DictReader(csvfile, skipinitialspace=True)

            for row in csvdata:
                plyr = Player(row, self.sport, self.historical)
                if (plyr.actual > 0):
                    all_players.append(plyr)
        
        variables = []
        all_players = np.random.choice(all_players, size=int(len(all_players))
            , replace=False)
        for player in all_players:     
                variables.append(solver.IntVar(0, 1, player.name))

        objective = solver.Objective()
        objective.SetMaximization()

        for i, player in enumerate(all_players):
            objective.SetCoefficient(variables[i], player.theo_actual)

        salary_cap = solver.Constraint(self.salary_band[0], self.salary_band[1])
        for i, player in enumerate(all_players):
            salary_cap.SetCoefficient(variables[i], player.salary)
        
        limit = solver.Constraint(limlow, limhigh)
        for i, player in enumerate(all_players):
            limit.SetCoefficient(variables[i], player.actual)

        limit = solver.Constraint(projlimlow, 5000)
        for i, player in enumerate(all_players):
            limit.SetCoefficient(variables[i], player.proj)

        for position, min_limit, max_limit in self.position_limit:
            position_cap = solver.Constraint(min_limit, max_limit)

            for i, player in enumerate(all_players):
                if position == player.position:
                    position_cap.SetCoefficient(variables[i], 1)

        size_cap = solver.Constraint(self.roster_size, self.roster_size)
        for variable in variables:
            size_cap.SetCoefficient(variable, 1)

        solution = solver.Solve()
        
        if solution == solver.OPTIMAL:
            roster = Roster(sport=self.sport)

            for i, player in enumerate(all_players):
                if variables[i].solution_value() == 1:
                    roster.add_player(player)

        else:
            print("No solution :(")
    
        return roster



#count for historical == 1000, live == 40000
def balanced(strdates, sport, site, historical, count, live_tag='', sabersim=False):
    
    for date in strdates:
        
        #in order to optimize based on rules, inputs will be different historical vs live 
        #we define those here
        if historical==True:
            cur_week = slates[site][sport][date]['slate_id']
            score_reference = slates[site][sport][date]['winning_score']
            projected_floor = Optimize(sport,site, historical).projected_optimal(cur_week).projected()*.808
            hist='historical'
        else:
            cur_week = date
            score_reference = Optimize(sport,site, historical).projected_optimal(cur_week).projected()*.808
            projected_floor = Optimize(sport,site, historical).projected_optimal(cur_week).projected()*.808
            hist='live'
        
        if sport=='nhl':
            def_ticker = 'G'
        if sport=='nfl':
            def_ticker ='D'

    
        dfs = [] 

        i = 0
        while i < count:
            
            team = Optimize(sport,site,historical).run(cur_week, score_reference, 5000, projected_floor).players
            
            names = [i.name for i in team]
            actual = [i.actual for i in team]
            position = [i.position for i in team]
            salary = [i.salary for i in team]
            pm = [i.plusminus for i in team]
        
            

            team_exposures = [i.team for i in team]
            opps = [i.opp.replace('@','') for i in team]
            isteamstack = len([x for x in team_exposures if team_exposures.count(x) >= 2])
            
            df = pd.DataFrame([names, actual, position, salary, team_exposures], index = ['name',
                                'actual', 'position', 'salary', 'teamz']).T
            df['team_salary'] = sum(salary)
            df['lineup'] = str(i) + str(0) +'_959' + live_tag

            if (isteamstack > 0) & (((df[df['position']==def_ticker]['teamz'].iloc[0] in opps)==False)) & (sum(pm)>-20):
                print('{0}-{1}'.format(cur_week,i))  
                df.drop('teamz', inplace=True, axis=1)
                dfs.append(df)
                i+=1
        
        if (sabersim == True) & (site=='draftkings'):
            ssdf = pd.read_csv(r'C:\Users\rmathews\.unreal_fantasy\_live_projections\sabersim.csv')
            site_file = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            site,
            sport,
            hist,
            str(cur_week)))

            for i in np.arange(len(ssdf)):
                row = pd.DataFrame(ssdf.loc[i,:])
                ids = row.iloc[:9].astype(int).reset_index().set_index(i)
                ids = ids.join(site_file.set_index('Id'), how='left')

                names = [i for i in ids['RylandID_master']]
                actual = [i for i in ids['projections_proj']]
                position = [i for i in ids['pos']]
                salary = [i for i in ids['Salary']]

                df = pd.DataFrame([names, actual, position, salary, team_exposures], index = ['name',
                                'actual', 'position', 'salary', 'teamz']).T
                df['team_salary'] = sum(salary)
                df['lineup'] = str(i) + str(0) +'_959' + live_tag + '_ss'

                if len(df.dropna())==9:
                    dfs.append(df)

        if historical==True:

            i = 0
            score_reference_high = score_reference*.999
            score_reference_low = score_reference*.900
            while i < int((count/5)):

                team = Optimize(sport,site,historical).run(cur_week, score_reference_low, score_reference_high, projected_floor).players
                #######
                
                names = [i.name for i in team]
                actual = [i.actual for i in team]
                position = [i.position for i in team]
                salary = [i.salary for i in team]
                pm = [i.plusminus for i in team]

                
                team_exposures = [i.team for i in team]
                opps = [i.opp.replace('@','') for i in team]
                isteamstack = len([x for x in team_exposures if team_exposures.count(x) >= 2])
                
                df = pd.DataFrame([names, actual, position, salary, team_exposures], index = ['name',
                                    'actual', 'position', 'salary', 'teamz']).T
                df['team_salary'] = sum(salary)
                df['lineup'] = str(i) + str(0) +'_9'

                if (isteamstack > 0) & (((df[df['position']==def_ticker]['teamz'].iloc[0] in opps)==False)) & (sum(pm)>-20):
                    print('{0}-{1}'.format(cur_week,i))  
                    df.drop('teamz', inplace=True, axis=1)
                    dfs.append(df)
                    i+=1
            
            i = 0
            score_reference_high = score_reference*.900
            score_reference_low = score_reference*.800
            while i < int((count/5)):

                team = Optimize(sport,site,historical).run(cur_week, score_reference_low, score_reference_high, projected_floor).players
                #######
                
                names = [i.name for i in team]
                actual = [i.actual for i in team]
                position = [i.position for i in team]
                salary = [i.salary for i in team]
                pm = [i.plusminus for i in team]
 
                
                team_exposures = [i.team for i in team]
                opps = [i.opp.replace('@','') for i in team]
                isteamstack = len([x for x in team_exposures if team_exposures.count(x) >= 2])
                
                df = pd.DataFrame([names, actual, position, salary, team_exposures], index = ['name',
                                    'actual', 'position', 'salary', 'teamz']).T
                df['team_salary'] = sum(salary)
                df['lineup'] = str(i) + str(0) +'_8'

                if (isteamstack > 0) & (((df[df['position']==def_ticker]['teamz'].iloc[0] in opps)==False)) & (sum(pm)>-20):
                    print('{0}-{1}'.format(cur_week,i))  
                    df.drop('teamz', inplace=True, axis=1)
                    dfs.append(df)
                    i+=1

            i = 0
            score_reference_high = score_reference*.800
            score_reference_low = score_reference*.600
            while i < int((count/5)):

                team = Optimize(sport,site,historical).run(cur_week, score_reference_low, score_reference_high, projected_floor).players
                #######
                
                names = [i.name for i in team]
                actual = [i.actual for i in team]
                position = [i.position for i in team]
                salary = [i.salary for i in team]
                pm = [i.plusminus for i in team]
                
                team_exposures = [i.team for i in team]
                opps = [i.opp.replace('@','') for i in team]
                isteamstack = len([x for x in team_exposures if team_exposures.count(x) >= 2])
                
                df = pd.DataFrame([names, actual, position, salary, team_exposures], index = ['name',
                                    'actual', 'position', 'salary', 'teamz']).T
                df['team_salary'] = sum(salary)
                df['lineup'] = str(i) + str(0) +'_6'

                if (isteamstack > 0) & (((df[df['position']==def_ticker]['teamz'].iloc[0] in opps)==False)) & (sum(pm)>-20):
                    print('{0}-{1}'.format(cur_week,i))  
                    df.drop('teamz', inplace=True, axis=1)
                    dfs.append(df)
                    i+=1

            i = 0
            score_reference_high = score_reference*.600
            score_reference_low = score_reference*.400
            while i < int((count/5)):

                team = Optimize(sport,site,historical).run(cur_week, score_reference_low, score_reference_high, projected_floor).players
                #######
                
                names = [i.name for i in team]
                actual = [i.actual for i in team]
                position = [i.position for i in team]
                salary = [i.salary for i in team]
                pm = [i.plusminus for i in team]
                
                team_exposures = [i.team for i in team]
                opps = [i.opp.replace('@','') for i in team]
                isteamstack = len([x for x in team_exposures if team_exposures.count(x) >= 2])
                
                df = pd.DataFrame([names, actual, position, salary, team_exposures], index = ['name',
                                    'actual', 'position', 'salary', 'teamz']).T
                df['team_salary'] = sum(salary)
                df['lineup'] = str(i) + str(0) +'_4'

                if (isteamstack > 0) & (((df[df['position']==def_ticker]['teamz'].iloc[0] in opps)==False)) & (sum(pm)>-20):
                    print('{0}-{1}'.format(cur_week,i))  
                    df.drop('teamz', inplace=True, axis=1)
                    dfs.append(df)
                    i+=1

            i = 0
            score_reference_high = score_reference*.400
            score_reference_low = score_reference*.200
            while i < int((count/5)):

                team = Optimize(sport,site,historical).run(cur_week, score_reference_low, score_reference_high, projected_floor).players
                #######
                
                names = [i.name for i in team]
                actual = [i.actual for i in team]
                position = [i.position for i in team]
                salary = [i.salary for i in team]
                pm = [i.plusminus for i in team]
                
                team_exposures = [i.team for i in team]
                opps = [i.opp.replace('@','') for i in team]
                isteamstack = len([x for x in team_exposures if team_exposures.count(x) >= 2])
                
                df = pd.DataFrame([names, actual, position, salary, team_exposures], index = ['name',
                                    'actual', 'position', 'salary', 'teamz']).T
                df['team_salary'] = sum(salary)
                df['lineup'] = str(i) + str(0) +'_2'

                if (isteamstack > 0) & (((df[df['position']==def_ticker]['teamz'].iloc[0] in opps)==False)) & (sum(pm)>-20):
                    print('{0}-{1}'.format(cur_week,i))  
                    df.drop('teamz', inplace=True, axis=1)
                    dfs.append(df)
                    i+=1
           
        masterf = pd.concat(dfs)
        masterf = masterf.set_index('name')


        #join all stats because time not issue 
        stats = pd.read_csv(r"C:\Users\rmathews\.unreal_fantasy\fantasylabs\{0}\{1}\{2}\{3}.csv".format(
            site,
            sport,
            hist,
            str(cur_week))) 
        stats = stats.set_index('RylandID_master')
        masterf = masterf.join(stats, how='outer', lsuffix='_ot')

        
        #export to folder and ready for ML processing
        if historical==True:
            masterf.to_csv(r"C:\Users\rmathews\.unreal_fantasy\optimizations\{0}\{1}\{2}\{3}.csv.gz".format(
                site,
                sport,
                hist,
                str(cur_week)),
                compression='gzip', index=True) 

        else:
            masterf.to_csv(r"C:\Users\rmathews\.unreal_fantasy\optimizations\{0}\{1}\{2}\{3}.csv.gz".format(
                site,
                sport,
                hist,
                live_tag),
                compression='gzip', index=True)
            

        

    