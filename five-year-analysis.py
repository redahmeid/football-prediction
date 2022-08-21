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
import pandas as pd

from dotenv import load_dotenv
load_dotenv()
from datetime import date
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(os.environ["MONGO_URL"])
db=client[os.environ["DB"]]
SEASON=os.environ["SEASON"]
SEASON_GW=int(os.environ["SEASON_GW"])
GW = int(os.environ["GW"])
WEIGHTINGS = {
    "17-18":int(os.environ["17-18"]),
    "18-19":int(os.environ["18-19"]),
    "19-20":int(os.environ["19-20"]),
    "20-21":int(os.environ["20-21"]),
    "21-22":int(os.environ["21-22"]),
    "22-23":int(os.environ["22-23"]),
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
AWAY_FIELD = "away"
HOME_POINTS_FIELD="home_points"
AWAY_POINTS_FIELD="away_points"
POINTS_FIELD="points"
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
ACTUAL = "Actual"
PREDICTED = "Predicted"
SEASON_GW_FIELD="season_gw"

def weighted_average_xg_std(season=SEASON,gw=GW,model_version=MODEL_VERSION):

    weekly_stats = db.weekly_stats.find({})
    for team_stats in weekly_stats:
        
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
            weights.append(WEIGHTINGS[stat[SEASON_FIELD]])
        
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
        season = match[SEASON_FIELD]
        season_gw = match[SEASON_GW_FIELD]

        home_update = {
            XG_FIELD:home_xg,
            GOALS_FIELD:home_g,
            POINTS_FIELD:home_points,
            HOME_OR_AWAY_FIELD:"home",
            OPPONENT_FIELD:away_team
        }

        away_update = {
            XG_FIELD:away_xg,
            GOALS_FIELD:away_g,
            POINTS_FIELD:away_points,
            HOME_OR_AWAY_FIELD:"away",
            OPPONENT_FIELD:home_team
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
    remaining_matches = db.matches.find({SEASON_GW_FIELD:{"$gt":SEASON_GW},SEASON_FIELD:SEASON})
    total_matches = 0
    correct_results = 0
    for match in remaining_matches:
        total_matches = total_matches+1
        home_team = match[HOME_FIELD]
        away_team = match[AWAY_FIELD]
        actual_home_points = match[HOME_POINTS_FIELD]
        actual_away_points = match[AWAY_POINTS_FIELD]
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
        print(home_query)
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
        for away_weekly_stat in away_weekly_stats:
            away_average_xg = away_weekly_stat[AWAY_AVERAGE_XG_FIELD]
            away_xg_std = away_weekly_stat[AWAY_STD_XG_FIELD]
            away_average_g = away_weekly_stat[AWAY_AVERAGE_G_FIELD]
            away_g_std = away_weekly_stat[AWAY_STD_G_FIELD]
        

        home_points_list = []
        away_points_list = []
        home_club = db.club_values.find_one({TEAM_FIELD:match[HOME_FIELD]})
        away_club = db.club_values.find_one({TEAM_FIELD:match[AWAY_FIELD]})
        home_regression = db.regression.find_one({TEAM_FIELD:match[HOME_FIELD]})
        away_regression = db.regression.find_one({TEAM_FIELD:match[AWAY_FIELD]})
        home_value = home_club["value"]//10
        away_value = away_club["value"]//10
        for i in range(0,100000,1):
            sim_home_xg = np.random.normal(home_average_xg,home_xg_std)
            home_goals = home_regression["alpha"] + home_regression["beta"]*sim_home_xg
            sim_away_xg = np.random.normal(away_average_xg,away_xg_std)
            away_goals = away_regression["alpha"] + away_regression["beta"]*sim_away_xg
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
        
        home_points = mode(home_points_list)
        away_points = mode(away_points_list)

        print("Mode home %s and away %s"%(home_points,away_points))

        if(home_points==actual_home_points):
            correct_results=correct_results+1

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

    print("Total Matches simulated: %s"%(total_matches))
    print("Correct results simulated: %s"%(correct_results))

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
    print("---completed create_match_stats in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="weighted"):
    start_time = time.time()
    weighted_average_xg_std()
    weighted_average_g_std()
    print("---completed weighted_average_std in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)==1):
    start_time = time.time()
    create_match_stats()
    weighted_average_xg_std()
    weighted_average_g_std()
    print("---completed all in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="simulate"):
    start_time = time.time()
    simulate_matches()
    print("---completed simulate_matches in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="analysis"):
    start_time = time.time()
    analysis()
    print("---completed analysis in %s seconds ---" % (time.time() - start_time))
