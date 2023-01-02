from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
import os
import time
import datetime
import sys
import numpy as np
import math
from statistics import mode
from statistics import median_high as median
from statistics import mean
from statistics import covariance
import pandas as pd

from dotenv import load_dotenv
load_dotenv()
from datetime import date
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(os.environ["MONGO_URL"])
db=client[os.environ["DB"]]
SEASON=int(os.environ["SEASON"])
SEASON_GW=int(os.environ["SEASON_GW"])
GW = int(os.environ["GW"])
WEIGHTINGS = {
    1516:float(os.environ["1516"]),
    1617:float(os.environ["1617"]),
    1718:float(os.environ["1718"]),
    1819:float(os.environ["1819"]),
    1920:float(os.environ["1920"]),
    2021:float(os.environ["2021"]),
    2122:float(os.environ["2122"]),
    2223:float(os.environ["2223"]),
}
MODEL_VERSION=os.environ["MODEL_VERSION"]
SIMULATION_VERSION=os.environ["SIMULATION_VERSION"]
HOME_XG_FIELD = "home_xg"
HOME_G_FIELD = "home_goals"
AWAY_G_FIELD = "away_goals"
HOME_FIELD = "home"
AWAY_XG_FIELD = "away_xg"
HOME_XP_FIELD="home_xp"
AWAY_XP_FIELD="away_xp"
HOME_POSSESSION_FIELD="home_possession"
AWAY_POSSESSION_FIELD="away_possession"
AVERAGE_POSSESSION_FIELD="average_possession"
STD_POSSESSION_FIELD="std_possession"
POSSESSION_FIELD="possession"
AWAY_FIELD = "away"
HOME_POINTS_FIELD="home_points"
AWAY_POINTS_FIELD="away_points"
POINTS_FIELD="points"
ACTUAL_HOME_POINTS_FIELD="actual_home_points"
ACTUAL_AWAY_POINTS_FIELD="actual_away_points"
PREDICTED_HOME_POINTS_FIELD="predicted_home_points"
PREDICTED_AWAY_POINTS_FIELD="predicted_away_points"
GOALS_FIELD="goals"
GOALS_AGAINST_FIELD="goals_against"
GOAL_DIFFERENCE_FIELD="goal_difference"
HOME_OR_AWAY_FIELD="home_or_away"
STATUS_FIELD="status"
OPPONENT_FIELD="opponent"
OPPONENT_GROUP_FIELD="opponent_position_group"
XG_FIELD="xg"
WIN_FIELD="win"
DRAW_FIELD="draw"
XGA_FIELD="xga"
GW_FIELD="gw"
SEASON_FIELD="season"
XP_FIELD="xp"
XGD_FIELD="xgd"
TEAM_FIELD="team"
EOW_FIELD="end_of_week"
POSITION_FIELD="position"
XPOSITION_FIELD="xposition"
AVERAGE_FIELD="average"
AVERAGE_XG_FIELD="average_xg"
HOME_AVERAGE_XG_FIELD="average_home_xg"
AWAY_AVERAGE_XG_FIELD="average_away_xg"
HOME_AVERAGE_G_FIELD="average_home_g"
AWAY_AVERAGE_G_FIELD="average_away_g"
STD_XG_FIELD="std_xg"
HOME_STD_XG_FIELD="std_home_xg"
AWAY_STD_XG_FIELD="std_away_xg"
HOME_STD_G_FIELD="std_home_g"
AWAY_STD_G_FIELD="std_away_g"
MODEL_VERSION_FIELD="model_version"
SIMULATION_VERSION_FIELD="simulation_version"
HOME_POSITION_FIELD=MODEL_VERSION+".Home position"
AWAY_POSITION_FIELD=MODEL_VERSION+".Away position"
PREDICTED = "Actual"
PREDICTED = "Predicted"
SEASON_GW_FIELD="season_gw"

def weighted_average_xg_std(season=SEASON,gw=GW,model_version=MODEL_VERSION):

    weekly_stats = db.weekly_stats.find({})
    for team_stats in weekly_stats:
        plus_weightings = db.club_values.find({TEAM_FIELD:team_stats[TEAM_FIELD]})
        add_weight = 0
        for weight in plus_weightings:
            print("WEIGHT %s",weight)
            try:
                add_weight = abs(weight["value_change"])
            except:
                add_weight = 0
        query = {
            TEAM_FIELD:team_stats[TEAM_FIELD],
            SEASON_GW_FIELD:{"$lt":team_stats[SEASON_GW_FIELD]},
            HOME_OR_AWAY_FIELD:team_stats[HOME_OR_AWAY_FIELD]
        }
        print(query)
        stats = db.weekly_stats.find(query)
        xg_values = []
        weights = []
        average = team_stats[XG_FIELD]
        xg_std = 0
        for stat in stats:
            print("Team is: "+stat[TEAM_FIELD])
            xg_values.append(stat[XG_FIELD])
            weight_to_append = WEIGHTINGS[stat[SEASON_FIELD]]
            if(stat[SEASON_FIELD]==SEASON):
                weight_to_append = weight_to_append+add_weight
            weights.append(weight_to_append)
        
        if(len(xg_values)>0):
            average = np.average(xg_values, weights=weights)
            variance = np.average((xg_values-average)**2, weights=weights)
            xg_std = math.sqrt(variance)
        
        team_query = {
            TEAM_FIELD:team_stats[TEAM_FIELD],
            SEASON_GW_FIELD:team_stats[SEASON_GW_FIELD],
            MODEL_VERSION_FIELD:team_stats[MODEL_VERSION_FIELD]
        }
        if(team_stats[HOME_OR_AWAY_FIELD]==HOME_FIELD):

            team_update = {
                HOME_AVERAGE_XG_FIELD:average,
                HOME_STD_XG_FIELD:xg_std
            }
        else:
            team_update = {
                AWAY_AVERAGE_XG_FIELD:average,
                AWAY_STD_XG_FIELD:xg_std
            }

        db.weekly_stats.update_one(team_query,{"$set":team_update},True)


def weighted_average_g_std(season=SEASON,gw=GW,model_version=MODEL_VERSION):

    weekly_stats = db.weekly_stats.find({})
    for team_stats in weekly_stats:
        
        query = {
            TEAM_FIELD:team_stats[TEAM_FIELD],
            SEASON_GW_FIELD:{"$lt":team_stats[SEASON_GW_FIELD]},
            HOME_OR_AWAY_FIELD:team_stats[HOME_OR_AWAY_FIELD]
        }
        print(query)
        stats = db.weekly_stats.find(query)
        g_values = []
        weights = []
        average = team_stats[XG_FIELD]
        g_std = 0
        for stat in stats:
            print("Team is: "+stat[TEAM_FIELD])
            g_values.append(stat[GOALS_FIELD])
            weights.append(WEIGHTINGS[stat[SEASON_FIELD]])
        
        if(len(g_values)>0):
            average = np.average(g_values, weights=weights)
            variance = np.average((g_values-average)**2, weights=weights)
            g_std = math.sqrt(variance)
        
        team_query = {
            TEAM_FIELD:team_stats[TEAM_FIELD],
            SEASON_GW_FIELD:team_stats[SEASON_GW_FIELD],
            MODEL_VERSION_FIELD:team_stats[MODEL_VERSION_FIELD]
        }
        if(team_stats[HOME_OR_AWAY_FIELD]==HOME_FIELD):

            team_update = {
                HOME_AVERAGE_G_FIELD:average,
                HOME_STD_G_FIELD:g_std
            }
        else:
            team_update = {
                AWAY_AVERAGE_G_FIELD:average,
                AWAY_STD_G_FIELD:g_std
            }

        db.weekly_stats.update_one(team_query,{"$set":team_update},True)

def weighted_average_possession_std(season=SEASON,gw=GW,model_version=MODEL_VERSION):

    weekly_stats = db.weekly_stats.find({})
    for team_stats in weekly_stats:
        
        query = {
            TEAM_FIELD:team_stats[TEAM_FIELD],
            SEASON_GW_FIELD:{"$lt":team_stats[SEASON_GW_FIELD]},
            HOME_OR_AWAY_FIELD:team_stats[HOME_OR_AWAY_FIELD]
        }
        
        stats = db.weekly_stats.find(query)
        possession_values = []
        weights = []
        average = team_stats[XG_FIELD]
        g_std = 0
        for stat in stats:
            print("Team is: "+stat[TEAM_FIELD])
            possession_values.append(stat[POSSESSION_FIELD])
            weights.append(WEIGHTINGS[stat[SEASON_FIELD]])
        
        if(len(possession_values)>0):
            average = np.average(possession_values, weights=weights)
            variance = np.average((possession_values-average)**2, weights=weights)
            g_std = math.sqrt(variance)
        
        team_query = {
            TEAM_FIELD:team_stats[TEAM_FIELD],
            SEASON_GW_FIELD:team_stats[SEASON_GW_FIELD],
            MODEL_VERSION_FIELD:team_stats[MODEL_VERSION_FIELD]
        }
        if(team_stats[HOME_OR_AWAY_FIELD]==HOME_FIELD):

            team_update = {
                AVERAGE_POSSESSION_FIELD:average,
                STD_POSSESSION_FIELD:g_std
            }
        else:
            team_update = {
                AVERAGE_POSSESSION_FIELD:average,
                STD_POSSESSION_FIELD:g_std
            }

        db.weekly_stats.update_one(team_query,{"$set":team_update},True)

def create_match_stats():
    completed_matches = db.matches.find({"status":"complete"})
    for match in completed_matches:
        home_team = match[HOME_FIELD]
        away_team = match[AWAY_FIELD]
        home_xg = match[HOME_XG_FIELD]
        away_xg = match[AWAY_XG_FIELD]
        home_g = match[HOME_G_FIELD]
        away_g = match[AWAY_G_FIELD]
        home_points = match[HOME_POINTS_FIELD]
        away_points = match[AWAY_POINTS_FIELD]
        gw = match[GW_FIELD]
        home_possession=match[HOME_POSSESSION_FIELD]
        away_possession=match[AWAY_POSSESSION_FIELD]
        season = match[SEASON_FIELD]
        season_gw = match[SEASON_GW_FIELD]

        home_update = {
            XG_FIELD:home_xg,
            GOALS_FIELD:home_g,
            POINTS_FIELD:home_points,
            HOME_OR_AWAY_FIELD:"home",
            OPPONENT_FIELD:away_team,
            SEASON_GW_FIELD:season_gw,
            POSSESSION_FIELD:home_possession
        }

        away_update = {
            XG_FIELD:away_xg,
            GOALS_FIELD:away_g,
            POINTS_FIELD:away_points,
            HOME_OR_AWAY_FIELD:"away",
            OPPONENT_FIELD:home_team,
            SEASON_GW_FIELD:season_gw,
            POSSESSION_FIELD:away_possession
        }

        home_query={
            TEAM_FIELD:home_team,
            GW_FIELD:gw,
            SEASON_FIELD:season,
            SEASON_GW_FIELD:season_gw,
            MODEL_VERSION_FIELD:MODEL_VERSION
        }

        away_query={
            TEAM_FIELD:away_team,
            GW_FIELD:gw,
            SEASON_FIELD:season,
            SEASON_GW_FIELD:season_gw,
            MODEL_VERSION_FIELD:MODEL_VERSION
        }

        db.weekly_stats.update_one(home_query,{"$set":home_update},True)
        db.weekly_stats.update_one(away_query,{"$set":away_update},True)

def simulate_matches():
    remaining_matches = db.matches.find({SEASON_GW_FIELD:{"$gt":SEASON_GW}})

    total_matches = 0
    
    for match in remaining_matches:
        print(match)
        total_matches = total_matches+1
        home_team = match[HOME_FIELD]
        away_team = match[AWAY_FIELD]
        print(home_team+" vs "+away_team)
        # actual_home_points = match[HOME_POINTS_FIELD]
        # actual_away_points = match[AWAY_POINTS_FIELD]
        # actual_home_possession = match[HOME_POSSESSION_FIELD]
        # actual_away_possession = match[AWAY_POSSESSION_FIELD]
        home_query=[
        {
            '$match': {
                TEAM_FIELD: home_team, 
                SEASON_GW_FIELD: {
                    '$lt': SEASON_GW
                }, 
                HOME_OR_AWAY_FIELD: HOME_FIELD
            }
        }, {
            '$sort': {
                SEASON_GW_FIELD: -1
            }
        }
        , {
        "$limit": 1
        }
        ]
        
        home_weekly_stats = db.weekly_stats.aggregate(home_query)

        away_query = [
        {
            '$match': {
                TEAM_FIELD: away_team, 
                SEASON_GW_FIELD: {
                    '$lt': SEASON_GW
                }, 
                HOME_OR_AWAY_FIELD: AWAY_FIELD
            }
        }, {
            '$sort': {
                SEASON_GW_FIELD: -1
            }
        }, {
        "$limit": 1
        }
        ]
        away_weekly_stats = db.weekly_stats.aggregate(away_query)

        for home_weekly_stat in home_weekly_stats:
            home_average_xg = home_weekly_stat[HOME_AVERAGE_XG_FIELD]
            home_xg_std = home_weekly_stat[HOME_STD_XG_FIELD]
            home_average_g = home_weekly_stat[HOME_AVERAGE_G_FIELD]
            home_g_std = home_weekly_stat[HOME_STD_G_FIELD]
            home_average_possession=home_weekly_stat[AVERAGE_POSSESSION_FIELD]
            home_std_possession=home_weekly_stat[STD_POSSESSION_FIELD]
        for away_weekly_stat in away_weekly_stats:
            away_average_xg = away_weekly_stat[AWAY_AVERAGE_XG_FIELD]
            away_xg_std = away_weekly_stat[AWAY_STD_XG_FIELD]
            away_average_g = away_weekly_stat[AWAY_AVERAGE_G_FIELD]
            away_g_std = away_weekly_stat[AWAY_STD_G_FIELD]
            away_average_possession=away_weekly_stat[AVERAGE_POSSESSION_FIELD]
            away_std_possession=away_weekly_stat[STD_POSSESSION_FIELD]
        
        
        home_points_list = []
        away_points_list = []
        home_club = db.club_values.find_one({TEAM_FIELD:match[HOME_FIELD]})
        away_club = db.club_values.find_one({TEAM_FIELD:match[AWAY_FIELD]})
        home_regression = db.regression.find_one({TEAM_FIELD:match[HOME_FIELD]})
        away_regression = db.regression.find_one({TEAM_FIELD:match[AWAY_FIELD]})
        # home_value = home_club["value"]//10
        # away_value = away_club["value"]//10
       
        for i in range(0,100000,1):
            sim_home_xg = np.random.normal(home_average_xg,home_xg_std)
            home_goals = home_regression["beta"] + home_regression["alpha"]*sim_home_xg
            sim_home_possession = np.random.normal(home_average_possession,home_std_possession)
            sim_away_xg = np.random.normal(away_average_xg,away_xg_std)
            away_goals = away_regression["beta"] + away_regression["alpha"]*sim_away_xg
            sim_away_possession = np.random.normal(away_average_possession,away_std_possession)
            if home_goals>away_goals:
                home_points = 3
                away_points = 0
            elif away_goals>home_goals:
                home_points=0
                away_points=3
            else:
                home_points=1
                away_points=1
            home_points_list.append(home_points)
            away_points_list.append(away_points)
        
        home_points_array = np.array(home_points_list)
        home_counts_array = np.bincount(home_points_array)
        away_points_array = np.array(away_points_list)
        away_counts_array = np.bincount(away_points_array)

        home_points = mean(home_points_list)
        

        away_points = mean(away_points_list)
        
            
        
        # if(home_points==actual_home_points):
        #     correct_results=correct_results+1
        # else:
        #     print("HOME counts:%s"%home_counts_array)
        #     print("Away counts:%s"%away_counts_array)
        #     print("%s vs %s"%(home_team,away_team))
        #     print("Predicted possession %s vs %s"%(sim_home_possession,sim_away_possession))
        #     print("Actual possession %s vs %s"%(actual_home_possession,actual_away_possession))
        #     print("Predicted %s vs %s"%(home_points,away_points))
        #     print("Actual %s vs %s"%(actual_home_points,actual_away_points))

        simulated_query ={
            HOME_FIELD:home_team,
            AWAY_FIELD:away_team,
            SEASON_FIELD:match[SEASON_FIELD],
            SEASON_GW_FIELD:match[SEASON_GW_FIELD],
            GW_FIELD:match[GW_FIELD],
            SIMULATION_VERSION_FIELD:SIMULATION_VERSION
        }

        simulated_result = {
            HOME_POINTS_FIELD:home_points,
            AWAY_POINTS_FIELD:away_points,
        }
        
        
        db.simulated_results.update_one(simulated_query,{"$set":simulated_result},True)

    # print("Total Matches simulated: %s"%(total_matches))
    # print("Correct results simulated: %s"%(correct_results))
    # print("percentage correct : %s "%(round((correct_results/total_matches)*100)))
def home_actual_league_table():
    home_actual_query=[
        {
            '$match': {
                SEASON_FIELD: SEASON, 
                SEASON_GW_FIELD: {
                    '$lte': SEASON_GW
                },
                STATUS_FIELD:"complete"
            }
        }, {
            '$group': {
                '_id': '$home', 
                'points': {
                    '$sum': '$home_points'
                }
            }
        }
    ]
    print(home_actual_query)
    home_actual_points = db.matches.aggregate(home_actual_query)
    
    for points in home_actual_points:
        print("POINTS %s",points)
        team_points = points["points"]
        team_name = points["_id"]

        query = {
            TEAM_FIELD:team_name,
            SEASON_FIELD:SEASON
        }

        update={
            ACTUAL_HOME_POINTS_FIELD:team_points,

        }
        db.league_table.update_one(query,{"$set":update},True)

def away_actual_league_table():
    away_actual_query=[
        {
            '$match': {
                SEASON_FIELD: SEASON, 
                SEASON_GW_FIELD: {
                    '$lte': SEASON_GW
                },
                STATUS_FIELD:"complete"
            }
        }, {
            '$group': {
                '_id': '$away', 
                'points': {
                    '$sum': '$away_points'
                }
            }
        }
    ]
    
    away_actual_points = db.matches.aggregate(away_actual_query)
    
    for points in away_actual_points:
        team_points = points["points"]
        team_name = points["_id"]

        query = {
            TEAM_FIELD:team_name,
            SEASON_FIELD:SEASON
        }

        update={
            ACTUAL_AWAY_POINTS_FIELD:team_points,

        }
        db.league_table.update_one(query,{"$set":update},True)
        
def predicted_league_table():
    home_predicted_query=[
        {
            '$match': {
                SEASON_FIELD: SEASON, 
                SEASON_GW_FIELD: {
                    '$gt': SEASON_GW
                }
            }
        }, {
            '$group': {
                '_id': '$home', 
                'points': {
                    '$sum': '$home_points'
                }
            }
        }
    ]
    
    home_predicted_points = db.simulated_results.aggregate(home_predicted_query)

    for points in home_predicted_points:
        team_points = points["points"]
        team_name = points["_id"]

        query = {
            TEAM_FIELD:team_name,
            SEASON_FIELD:SEASON
        }

        update={
            PREDICTED_HOME_POINTS_FIELD:team_points,

        }
        db.league_table.update_one(query,{"$set":update},True)
    away_predicted_query=[
        {
            '$match': {
                SEASON_FIELD: SEASON, 
                SEASON_GW_FIELD: {
                    '$gt': SEASON_GW
                }
            }
        }, {
            '$group': {
                '_id': '$away', 
                'points': {
                    '$sum': '$away_points'
                }
            }
        }
    ]
    
    away_predicted_points = db.simulated_results.aggregate(away_predicted_query)

    for points in away_predicted_points:
        team_points = points["points"]
        team_name = points["_id"]

        query = {
            TEAM_FIELD:team_name,
            SEASON_FIELD:SEASON
        }

        update={
            PREDICTED_AWAY_POINTS_FIELD:team_points,

        }
        db.league_table.update_one(query,{"$set":update},True)

    league_table=[
    {
        '$match': {
            SEASON_FIELD: SEASON
        }
    }, {
        '$project': {
            '_id': '$team', 
            'points': {
                '$sum': [
                    '$actual_away_points', '$actual_home_points', '$predicted_away_points', '$predicted_home_points'
                ]
            }
        }
    }, {
        '$sort': {
            'points': -1
        }
    }
]

    positions = db.league_table.aggregate(league_table)
    print("|**Team**|**Predicted Position**|**Predicted Points**|")
    print("|-------------------|------------|------------|")
    i=0
    for position in positions:
         i=i+1
         print("|"+position["_id"]+"|"+str(i)+"|"+str(round(position["points"]))+"|")

def compare():
    home_actual_query=[
        {
            '$match': {
                SEASON_FIELD: SEASON, 
            }
        }, {
            '$group': {
                '_id': '$home', 
                'points': {
                    '$sum': '$homes'
                }
            }
        }
    ]
    
    home_actual_points = db.matches.aggregate(home_actual_query)
    for home_points in home_actual_points:
        points = home_points["points"]
        away_actual_query=[
            {
                '$match': {
                    AWAY_FIELD:home_points[HOME_FIELD],
                    SEASON_FIELD: SEASON, 
                }
            }, {
                '$group': {
                    '_id': '$away', 
                    'points': {
                        '$sum': '$away_points'
                    }
                }
            }
        ]
        away_actual_points = db.matches.aggregate(away_actual_query)
        for away_points in away_actual_points:
            points = points+away_points["points"]

def analysis():
    teams = db.club_values.find()
    for team in teams:
        print(team)
        team_stats = db.weekly_stats.find({TEAM_FIELD:team["team"]})
        xg_array = []
        g_array = []
        for stats in team_stats:
            xg_array.append(stats[XG_FIELD])
            g_array.append(stats[GOALS_FIELD])    

        df = pd.DataFrame(
            {
                'X':xg_array,
                'y':g_array
            }
        )

        xmean = np.mean(xg_array)
        ymean = np.mean(g_array)

        df['xycov'] = (df['X'] - xmean) * (df['y'] - ymean)
        df['xvar'] = (df['X'] - xmean)**2

        beta = df['xycov'].sum() / df['xvar'].sum()
        alpha = ymean - (beta * xmean)
        print(f'alpha = {alpha}')
        print(f'beta = {beta}')

        db.regression.update_one({TEAM_FIELD:team["team"]},{"$set":{"beta":beta,"alpha":alpha}},True)

if(len(sys.argv)>1 and sys.argv[1]=="create_stats"):
    start_time = time.time()
    create_match_stats()
    weighted_average_xg_std()
    weighted_average_g_std()
    print("---completed create_match_stats in %s seconds ---" % (time.time() - start_time))


if(len(sys.argv)==1):
    start_time = time.time()
    create_match_stats()
    weighted_average_xg_std()
    weighted_average_g_std()
    weighted_average_possession_std()
    analysis()
    simulate_matches()
    home_actual_league_table()
    away_actual_league_table()
    predicted_league_table()
    print("---completed all in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="simulate"):
    start_time = time.time()
    simulate_matches()
    print("---completed simulate_matches in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="analyse"):
    start_time = time.time()
    analysis()
    print("---completed analysis in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="create_league_table"):
    start_time = time.time()
    home_actual_league_table()
    away_actual_league_table()
    predicted_league_table()
    print("---completed create_league_table in %s seconds ---" % (time.time() - start_time))
