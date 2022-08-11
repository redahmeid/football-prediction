from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
import os
import time
import datetime
import sys

from dotenv import load_dotenv
load_dotenv()
from datetime import date
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(os.environ["MONGO_URL"])
db=client[os.environ["DB"]]
previous_db=client[os.environ["PREVIOUS_SEASON"]]

GW = int(os.environ["GW"])
MODEL_VERSION=os.environ["MODEL_VERSION"]
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
XGA_FIELD="xga"
GW_FIELD="gw"
XP_FIELD="xp"
XGD_FIELD="xgd"
TEAM_FIELD="team"
EOW_FIELD="end_of_week"
POSITION_FIELD="position"
XPOSITION_FIELD="xposition"
AVERAGE_FIELD="average"
MODEL_VERSION_FIELD="model_version"
HOME_POSITION_FIELD=MODEL_VERSION+".Home position"
AWAY_POSITION_FIELD=MODEL_VERSION+".Away position"
ACTUAL = "Actual"
PREDICTED = "Predicted"


# From the match stats work out how many points have been gain so far
def updateWeeklyResults(model_version=MODEL_VERSION,gw=GW):
    
    pointsSearch = [
      {
        '$match': {
          'gw': {
            '$lte': gw
          }
        }
      }
    ]
    matches = db.matches.aggregate(pointsSearch)
    for match in matches:
        print(match)
        # update home results
        gd = match[HOME_G_FIELD]-match[AWAY_G_FIELD]
        xgd = match[HOME_XG_FIELD]-match[AWAY_XG_FIELD]
        homeObj = {
          XGD_FIELD:xgd,
          HOME_OR_AWAY_FIELD:HOME_FIELD,
          STATUS_FIELD:ACTUAL,POINTS_FIELD:match[HOME_POINTS_FIELD],
          GOALS_FIELD:match[HOME_G_FIELD],
          GOALS_AGAINST_FIELD:match[AWAY_G_FIELD],
          GOAL_DIFFERENCE_FIELD:gd,
          XG_FIELD:match[HOME_XG_FIELD],
          XGA_FIELD:match[AWAY_XG_FIELD],
          XP_FIELD:match[HOME_XP_FIELD],
          OPPONENT_FIELD:match[AWAY_FIELD]
        }
        db.weekly_results.update_one({GW_FIELD:match[GW_FIELD],MODEL_VERSION_FIELD:model_version,TEAM_FIELD:match[HOME_FIELD]},{"$set":homeObj},True)


        gd = match[AWAY_G_FIELD]-match[HOME_G_FIELD]
        xgd = match[AWAY_XG_FIELD]-match[HOME_XG_FIELD]
        awayObj = {
          XGD_FIELD:xgd,
          HOME_OR_AWAY_FIELD:AWAY_FIELD,
          STATUS_FIELD:ACTUAL,POINTS_FIELD:match[AWAY_POINTS_FIELD],
          GOALS_FIELD:match[AWAY_G_FIELD],
          GOALS_AGAINST_FIELD:match[AWAY_G_FIELD],
          GOAL_DIFFERENCE_FIELD:gd,
          XG_FIELD:match[AWAY_XG_FIELD],
          XGA_FIELD:match[HOME_XG_FIELD],
          XP_FIELD:match[AWAY_XP_FIELD],
          OPPONENT_FIELD:match[HOME_FIELD]
        }
        db.weekly_results.update_one({GW_FIELD:match[GW_FIELD],MODEL_VERSION_FIELD:model_version,TEAM_FIELD:match[AWAY_FIELD]},{"$set":awayObj},True)

# From the match stats work out how many points have been gain so far
def updateWeeklyPositions(gw=GW, model_version=MODEL_VERSION):
    
    positionSearch = [
    {
        '$match': {
            'gw': {
                '$lte': gw
            }
        }
    }, {
        '$group': {
            '_id': '$team', 
            'points': {
                '$sum': '$points'
            }, 
            'goals': {
                '$sum': '$goals'
            }, 
            'gd': {
                '$sum': '$goal_difference'
            },
            'ga': {
                '$sum': '$goals_against'
            },
            'average_points': {
                '$avg': '$points'
            }, 
            'average_goals': {
                '$avg': '$goals'
            }, 
             'average_ga': {
                '$avg': '$goal_against'
            }, 
            'average_gd': {
                '$avg': '$goal_difference'
            }
        }
    }, {
        '$sort': {
            'points': -1, 
            'goals': -1, 
            'gd': -1
        }
    }
]
    positions = db.weekly_results.aggregate(positionSearch)
    
    i=1
    for position in positions:
        print("Position object %s",position)
        updateObj = {
          EOW_FIELD+"."+POSITION_FIELD:i,
          EOW_FIELD+"."+GOALS_FIELD:position[GOALS_FIELD],
          EOW_FIELD+"."+POINTS_FIELD:position[POINTS_FIELD],
          EOW_FIELD+"."+GOAL_DIFFERENCE_FIELD:position["gd"],
          EOW_FIELD+"."+GOALS_AGAINST_FIELD:position["ga"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+GOALS_FIELD:position["average_goals"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+POINTS_FIELD:position["average_points"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+GOAL_DIFFERENCE_FIELD:position["average_gd"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+GOALS_AGAINST_FIELD:position["average_ga"]
        }
        # update home results
        db.weekly_results.update_one({GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:position["_id"]},{"$set":updateObj},True)
        i=i+1


# From the match stats work out how many points have been gain so far
def updateWeeklyXPositions(gw=GW, model_version=MODEL_VERSION):
    
    positionSearch = [
    {
        '$match': {
            'gw': {
                '$lte': gw
            }
        }
    }, {
        '$group': {
            '_id': '$team', 
            'points': {
                '$sum': '$xp'
            }, 
            'goals': {
                '$sum': '$xg'
            },
            'ga': {
                '$sum': '$xga'
            }, 
            'gd': {
                '$sum': '$xgd'
            },
            'average_points': {
                '$avg': '$xp'
            }, 
            'average_goals': {
                '$avg': '$xg'
            }, 
             'average_ga': {
                '$avg': '$xga'
            }, 
            'average_gd': {
                '$avg': '$xgd'
            }
        }
    }, {
        '$sort': {
            'points': -1, 
            'goals': -1, 
            'gd': -1
        }
    }
]
    positions = db.weekly_results.aggregate(positionSearch)
    i=1
    for position in positions:
        print("Position object %s",position)
        updateObj = {
          EOW_FIELD+"."+XPOSITION_FIELD:i,
          EOW_FIELD+"."+XG_FIELD:position[GOALS_FIELD],
          EOW_FIELD+"."+XP_FIELD:position[POINTS_FIELD],
          EOW_FIELD+"."+XGD_FIELD:position["gd"],
          EOW_FIELD+"."+XGA_FIELD:position["ga"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+XG_FIELD:position["average_goals"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+XP_FIELD:position["average_points"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+XGD_FIELD:position["average_gd"],
          EOW_FIELD+"."+AVERAGE_FIELD+"."+XGA_FIELD:position["average_ga"]
        }
        # update home results
        db.weekly_results.update_one({GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:position["_id"]},{"$set":updateObj},True)
        i=i+1

def updateWeeklyOpponentPosition(gw=GW,model_version=MODEL_VERSION):
  positionSearch = [
    {
        '$match': {
            'gw': gw
        }
    }
  ]

  positions = db.weekly_results.aggregate(positionSearch)
  for position in positions:
    opponent_position_obj = db.weekly_results.find_one({TEAM_FIELD:position[OPPONENT_FIELD]})
    opponent_position = opponent_position_obj[EOW_FIELD][POSITION_FIELD]
    group = getGroup(opponent_position)
    query = {
      GW_FIELD:gw,
      MODEL_VERSION_FIELD:model_version,
      TEAM_FIELD:position[TEAM_FIELD]
    }
    updateObj = {
      OPPONENT_GROUP_FIELD:group
    }

    db.weekly_results.update_one(query,{"$set":updateObj})

def updateAverageStatsByGroup(gw=GW,model_version=MODEL_VERSION):
  statsSearch = [
    {
        '$match': {
            'gw': {
                '$lte': gw
            }
        }
    }, {
        '$group': {
            '_id': {
                'team': '$team', 
                'group': '$opponent_position_group',
                'home_or_away': '$home_or_away'
            }, 
            'xg': {
                '$avg': '$xg'
            }
            , 
            'xga': {
                '$avg': '$xga'
            },
            'xgd': {
                '$avg': '$xgd'
            },
            'xp': {
                '$avg': '$xp'
            }
        }
    }
  ]
 
  stats = db.weekly_results.aggregate(statsSearch)
  for stat in stats:
      print("Position object %s",stat)
      updateObj = {
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+"."+XG_FIELD:stat["xg"],
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+"."+XP_FIELD:stat["xp"],
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+"."+XGD_FIELD:stat["xgd"],
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+"."+XGA_FIELD:stat["xga"]
        
      }

      query = {GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:stat["_id"]["team"]}
      # update home results
      db.weekly_results.update_one(query,{"$set":updateObj},True)
     
def simulateMatch(gw=GW,model_version=MODEL_VERSION):
  matchesSearch=[
    {
        '$match': {
            'gw':  gw
        }
    }, {
        '$sort': {
            'gw': 1
        }
    }
  ]
  matches = db.matches.aggregate(matchesSearch)
  for match in matches:
    home_weekly_results = db.weekly_results.find_one({"team":match[HOME_FIELD],GW_FIELD:GW,MODEL_VERSION_FIELD:model_version})
    away_weekly_results = db.weekly_results.find_one({"team":match[AWAY_FIELD],GW_FIELD:GW,MODEL_VERSION_FIELD:model_version})

    last_home__weekly_results = db.weekly_results.find_one({"team":match[HOME_FIELD],GW_FIELD:gw-1,MODEL_VERSION_FIELD:model_version})
    last_away_weekly_results = db.weekly_results.find_one({"team":match[AWAY_FIELD],GW_FIELD:gw-1,MODEL_VERSION_FIELD:model_version})

    away_team_position = last_away_weekly_results[EOW_FIELD][POSITION_FIELD]
    home_team_position = last_home__weekly_results[EOW_FIELD][POSITION_FIELD]

    try:
      away_xg = away_weekly_results[EOW_FIELD][AVERAGE_FIELD][AWAY_FIELD][getGroup(home_team_position)][XG_FIELD]
    except:
      away_xg = away_weekly_results[EOW_FIELD][AVERAGE_FIELD][XG_FIELD]
    try:
      home_xg = home_weekly_results[EOW_FIELD][AVERAGE_FIELD][HOME_FIELD][getGroup(away_team_position)][XG_FIELD]
    except:
      home_xg = home_weekly_results[EOW_FIELD][AVERAGE_FIELD][XG_FIELD]

    home_value_obj = db.club_values.find_one({"team":match[HOME_FIELD]})
        
    home_value = home_value_obj["value"]
    away_value_obj = db.club_values.find_one({"team":match[AWAY_FIELD]})
    away_value = away_value_obj["value"]

    print("Home value %s vs %s Away value"%(home_value,away_value))
    home_value_diff = (home_value-away_value)//10
    away_value_diff = (away_value-home_value)//10
    print("Difference in value %s "%(home_value_diff))
    print("Home xG Before %s "%(home_xg))
    print("Away xG Before %s "%(away_xg))
    if(home_value_diff>0):
      home_xg = home_xg+(home_value_diff*0.05)
    if(away_value_diff>0):
      away_xg = away_xg+(away_value_diff*0.05)

    print("Home xG After %s "%(home_xg))
    print("Away xG After %s "%(away_xg))

    home_xg = round(home_xg*2)/2
    away_xg = round(away_xg*2)/2
    print("%s (%s) %s vs %s (%s) %s "%(home_xg,home_team_position,match[HOME_FIELD],match[AWAY_FIELD],away_team_position,away_xg))
    
    home_points = 1
    away_points = 1

    if(away_xg>home_xg):
      away_points = 3
      home_points = 0
    elif(away_xg<home_xg):
      away_points = 0
      home_points = 3
    
    updateObj = {
        POINTS_FIELD:home_points,
        STATUS_FIELD:PREDICTED,
        GOALS_FIELD:home_xg,
        GOALS_AGAINST_FIELD:away_xg,
        GOAL_DIFFERENCE_FIELD:(home_xg-away_xg),
        HOME_OR_AWAY_FIELD:HOME_FIELD,
        OPPONENT_FIELD:match[AWAY_FIELD]
      }
    # update home results
    db.weekly_results.update_one({GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:match[HOME_FIELD]},{"$set":updateObj},True)

    updateObj = {
        POINTS_FIELD:away_points,
        STATUS_FIELD:PREDICTED,
        GOALS_FIELD:away_xg,
        GOALS_AGAINST_FIELD:home_xg,
        GOAL_DIFFERENCE_FIELD:(away_xg-home_xg),
        HOME_OR_AWAY_FIELD:AWAY_FIELD,
        OPPONENT_FIELD:match[HOME_FIELD]
      }
    # update home results
    db.weekly_results.update_one({GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:match[AWAY_FIELD]},{"$set":updateObj},True)

def predict(gw=GW,model_version=MODEL_VERSION):
  predictionSearch = [
    {
        '$group': {
            '_id': '$team', 
            'points': {
                '$sum': '$points'
            }
        }
    }, {
        '$sort': {
            'points': -1
        }
    }
  ]

  final_positions = db.weekly_results.aggregate(predictionSearch)
  i=1
  print("|**Team**|**Predicted Position**|**Predicted Points**|")
  print("|-------------------|------------|------------|")
  for position in final_positions:
    print("|"+position["_id"]+"|"+str(i)+"|"+str(position["points"])+"|")
    updateObj = {
      POINTS_FIELD:position[POINTS_FIELD],
      POSITION_FIELD:i
    }

    queryObj = {
      TEAM_FIELD:position["_id"],
      GW_FIELD:gw,
      MODEL_VERSION_FIELD:model_version
    }

    db.prediction.update_one(queryObj,{"$set":updateObj},True)
    i=i+1

def getGroup(num,wider=False):
    group = ""
    if(wider):
        group="1-20"
     
    else:
        if num<=4 and num>=1:
            group="1-4"
        elif num<=7 and num>=5:
            group="5-7"
        elif num<=12 and num>=8:
            group="8-12"
        elif num<=17 and num>=13:
            group="13-17"
        elif num<=20 and num>=18:
            group="18-20"
    return group

if(len(sys.argv)>1 and sys.argv[1]=="drop_all"):
  print("dropping all")
  db.weekly_results.delete_many({MODEL_VERSION_FIELD:MODEL_VERSION})

if(len(sys.argv)>1 and sys.argv[1]=="update_stats"):
  updateWeeklyResults()
  for gw in range(GW,0,-1):
    updateWeeklyPositions(gw)
    updateWeeklyXPositions(gw)

  for gw in range(GW,0,-1):
    updateWeeklyOpponentPosition(gw)

  for gw in range(GW,0,-1):
    updateAverageStatsByGroup(gw)

if(len(sys.argv)>1 and sys.argv[1]=="simulate"):
  print("simulating....")
  for gw in range(GW+1,39,1):
    simulateMatch(gw)
    updateWeeklyPositions(gw)
    updateWeeklyXPositions(gw)
    updateWeeklyOpponentPosition(gw)
    updateAverageStatsByGroup(gw)

if(len(sys.argv)>1 and sys.argv[1]=="predict"):
  print("predicting....")
  predict()