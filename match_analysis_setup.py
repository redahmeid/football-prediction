from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
import os
import time
import datetime
import sys
import numpy as np

from dotenv import load_dotenv
load_dotenv()
from datetime import date
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(os.environ["MONGO_URL"])
db=client[os.environ["MATCH_ANALYSIS_DB"]]
# analysis_db=client[os.environ["MATCH_ANALYSIS_DB"]]
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
WIN_FIELD="win"
DRAW_FIELD="draw"
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
def updateWeeklyResults(gw=GW,model_version=MODEL_VERSION):
    
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
        win = 1 if match[HOME_G_FIELD]>match[AWAY_G_FIELD] else 0
        draw = 1 if match[HOME_G_FIELD]==match[AWAY_G_FIELD] else 0
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
          OPPONENT_FIELD:match[AWAY_FIELD],
          WIN_FIELD:win,
          DRAW_FIELD:draw
        }
        db.weekly_results.update_one({GW_FIELD:match[GW_FIELD],MODEL_VERSION_FIELD:model_version,TEAM_FIELD:match[HOME_FIELD]},{"$set":homeObj},True)


        win = 1 if match[AWAY_G_FIELD]>match[HOME_G_FIELD] else 0
        draw = 1 if match[AWAY_G_FIELD]==match[HOME_G_FIELD] else 0

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
          OPPONENT_FIELD:match[HOME_FIELD],
          WIN_FIELD:win,
          DRAW_FIELD:draw
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
            'number_of_wins':{
              '$sum': '$win'
            },
            'number_of_draws':{
              '$sum': '$draw'
            },
            'average_wins':{
              '$avg': '$win'
            },
            'average_draws':{
              '$avg': '$draw'
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
            'goals_std': {
                '$stdDevPop': '$xg'
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
            },
            'std_points': {
                '$stdDevPop': '$points'
            }, 
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
          EOW_FIELD+"."+AVERAGE_FIELD+"."+XGA_FIELD:position["average_ga"],
          EOW_FIELD+"."+AVERAGE_FIELD+".goals_std":position["goals_std"],
          EOW_FIELD+"."+AVERAGE_FIELD+".std_points":position["std_points"]
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
            },
            'xgstd': {
                '$stdDevPop': '$xg'
            }, 
            'xga': {
                '$avg': '$xga'
            },
            'xgd': {
                '$avg': '$xgd'
            },
            'xp': {
                '$avg': '$xp'
            },
            'number_of_wins':{
              '$sum': '$win'
            },
            'number_of_draws':{
              '$sum': '$draw'
            },
            'average_wins':{
              '$avg': '$win'
            },
            'average_draws':{
              '$avg': '$draw'
            },
            'average_points': {
                '$avg': '$points'
            },
            'std_points': {
                '$stdDevPop': '$points'
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
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+"."+XGA_FIELD:stat["xga"],
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+"."+POINTS_FIELD:stat["average_points"],
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+".goals_std":stat["xgstd"],
        EOW_FIELD+"."+AVERAGE_FIELD+"."+stat["_id"]["home_or_away"]+"."+stat["_id"]["group"]+".std_points":stat["std_points"]
      }

      query = {GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:stat["_id"]["team"]}
      # update home results
      db.weekly_results.update_one(query,{"$set":updateObj},True)
     


def updateWeightedGroupAverageStats(gw=GW,model_version=MODEL_VERSION):
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
            'xg_numerator': {
                '$sum': {
                    '$multiply': [
                        '$xg', {
                            '$multiply': [
                                {
                                    '$divide': [
                                        '$gw', gw
                                    ]
                                }, 5
                            ]
                        }
                    ]
                }
            }, 
            'xg_denominator': {
                '$sum': {
                    '$multiply': [
                        {
                            '$divide': [
                                '$gw', gw
                            ]
                        }, 5
                    ]
                }
            },
            'g_numerator': {
                '$sum': {
                    '$multiply': [
                        '$goals', {
                            '$multiply': [
                                {
                                    '$divide': [
                                        '$gw', gw
                                    ]
                                }, 5
                            ]
                        }
                    ]
                }
            }, 
            'g_denominator': {
                '$sum': {
                    '$multiply': [
                        {
                            '$divide': [
                                '$gw', gw
                            ]
                        }, 5
                    ]
                }
            }
        },
        
    }, {
        '$project': {
            'xg_average': {
                '$divide': [
                    '$xg_numerator', '$xg_denominator'
                ]
            },
            'g_average': {
                '$divide': [
                    '$g_numerator', '$g_denominator'
                ]
            },
            'g_xg_diff':{
              '$subtract':[
                '$g_numerator','$xg_numerator'
              ]
            }
        }
    }
]
  weighted_xg = db.weekly_results.aggregate(statsSearch)
  for xg in weighted_xg:
    print(xg)
    average_diff = xg["g_average"]-xg["xg_average"]
    updateObj = {
      EOW_FIELD+".weighted_"+AVERAGE_FIELD+"."+xg["_id"]["home_or_away"]+"."+xg["_id"]["group"]+"."+XG_FIELD:xg["xg_average"],
      EOW_FIELD+".weighted_"+AVERAGE_FIELD+"."+xg["_id"]["home_or_away"]+"."+xg["_id"]["group"]+"."+GOALS_FIELD:xg["g_average"],
      EOW_FIELD+".weighted_"+AVERAGE_FIELD+"."+xg["_id"]["home_or_away"]+"."+xg["_id"]["group"]+".g_xg_diff":average_diff
    }

    query = {GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:xg["_id"]["team"]}
    # update home results
    db.weekly_results.update_one(query,{"$set":updateObj},True)
     


def updateWeightedVenueAverageStats(gw=GW,model_version=MODEL_VERSION):
  statsSearch = [
    {
        '$match': {
            'gw': {
                '$lte': gw
            }
        }
    }, {
        '$group': {
            '_id':{"team":'$team',"home_or_away":"$home_or_away"},  
            'xg_numerator': {
                '$sum': {
                    '$multiply': [
                        '$xg', {
                            '$multiply': [
                                {
                                    '$divide': [
                                        '$gw', gw
                                    ]
                                }, 5
                            ]
                        }
                    ]
                }
            }, 
            'xga_numerator': {
                '$sum': {
                    '$multiply': [
                        '$xgq', {
                            '$multiply': [
                                {
                                    '$divide': [
                                        '$gw', gw
                                    ]
                                }, 5
                            ]
                        }
                    ]
                }
            }, 
            'xg_denominator': {
                '$sum': {
                    '$multiply': [
                        {
                            '$divide': [
                                '$gw', gw
                            ]
                        }, 5
                    ]
                }
            },
            'g_numerator': {
                '$sum': {
                    '$multiply': [
                        '$goals', {
                            '$multiply': [
                                {
                                    '$divide': [
                                        '$gw', gw
                                    ]
                                }, 5
                            ]
                        }
                    ]
                }
            }, 
            'g_denominator': {
                '$sum': {
                    '$multiply': [
                        {
                            '$divide': [
                                '$gw', gw
                            ]
                        }, 5
                    ]
                }
            }
        }
    }, {
        '$project': {
            'xg_average': {
                '$divide': [
                    '$xg_numerator', '$xg_denominator'
                ]
            },
            'g_average': {
                '$divide': [
                    '$g_numerator', '$g_denominator'
                ]
            },
            'xga_average': {
                '$divide': [
                    '$xga_numerator', '$g_denominator'
                ]
            }
        }
    }
]
  weighted_xg = db.weekly_results.aggregate(statsSearch)
  for xg in weighted_xg:
    print(xg)
    average_diff = xg["g_average"]-xg["xg_average"]
    updateObj = {
      EOW_FIELD+".weighted_"+AVERAGE_FIELD+"."+xg["_id"]["home_or_away"]+"."+XG_FIELD:xg["xg_average"],
      EOW_FIELD+".weighted_"+AVERAGE_FIELD+"."+xg["_id"]["home_or_away"]+"."+GOALS_FIELD:xg["g_average"],
      EOW_FIELD+".weighted_"+AVERAGE_FIELD+"."+xg["_id"]["home_or_away"]+"."+XGA_FIELD:xg["xga_average"],
      EOW_FIELD+".weighted_"+AVERAGE_FIELD+"."+xg["_id"]["home_or_away"]+".g_xg_diff":average_diff
    }

    query = {GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:xg["_id"]["team"]}
    # update home results
    db.weekly_results.update_one(query,{"$set":updateObj},True)
     



def simulateMatch(start_gw=GW,gw=GW,model_version=MODEL_VERSION):
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
  print(matchesSearch)
  matches = db.matches.aggregate(matchesSearch)
  for match in matches:
   
    print(match)
    home_weekly_results = db.weekly_results.find_one({TEAM_FIELD:match[HOME_FIELD],GW_FIELD:start_gw,MODEL_VERSION_FIELD:model_version})
    away_weekly_results = db.weekly_results.find_one({TEAM_FIELD:match[AWAY_FIELD],GW_FIELD:start_gw,MODEL_VERSION_FIELD:model_version})

    home_xg = round(home_weekly_results["end_of_week"]["weighted_average"]["home"]["xg"])
    away_xg = round(away_weekly_results["end_of_week"]["weighted_average"]["away"]["xg"])

    result = ""
    if(home_xg>away_xg):
      home_points=3
      away_points=0
      result = "Home win"
    elif away_xg>home_xg:
      home_points=0
      away_points=3
      result = "Away win"
    else:
      home_points=1
      away_points=1
      result = "Draw"

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
    # update away results
    db.weekly_results.update_one({GW_FIELD:gw,MODEL_VERSION_FIELD:model_version,TEAM_FIELD:match[AWAY_FIELD]},{"$set":updateObj},True)

def predict(gw=GW,model_version=MODEL_VERSION):
  predictionSearch =[
    {
        '$match': {
            'model_version': model_version
        }
    }, {
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
  start_time = time.time()
  print("dropping all")
  db.weekly_results.delete_many({MODEL_VERSION_FIELD:MODEL_VERSION})
  print("---completed  updating stats in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="update_stats"):
  start_time = time.time()
  start_gw = GW
  
  if(len(sys.argv)>2):
    start_gw = int(sys.argv[2])
  print(start_gw)
  updateWeeklyResults(start_gw)
  for gw in range(1,start_gw,1):
    updateWeeklyPositions(gw)
    updateWeeklyXPositions(gw)

  for gw in range(1,start_gw,1):
    updateWeeklyOpponentPosition(gw)

  for gw in range(1,start_gw,1):
    updateAverageStatsByGroup(gw)
  # updateWeightedVenueAverageStats()
  # updateWeightedGroupAverageStats()
  print("---completed  updating stats in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="simulate"):
  start_gw = GW
  if(len(sys.argv)>2):
    start_gw = int(sys.argv[2])
  start_time = time.time()
  print("simulating....")
  for gw in range(start_gw+1,39,1):
    simulateMatch(gw)
    updateWeeklyPositions(gw)
    updateWeeklyXPositions(gw)
    updateWeeklyOpponentPosition(gw)
    updateAverageStatsByGroup(gw)
  print("---completed  predicting in %s seconds ---" % (time.time() - start_time))
if(len(sys.argv)>1 and sys.argv[1]=="predict"):
  start_time = time.time()
  print("predicting....")
  predict()
  print("---completed  predicting in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="weighted"):
  print("In here")
  start_gw = GW
  if(len(sys.argv)>2):
    start_gw = int(sys.argv[2])
  print(start_gw)
  for gw in range(1,start_gw,1):
    print("In here")
    updateWeightedVenueAverageStats(gw)
    updateWeightedGroupAverageStats(gw)
