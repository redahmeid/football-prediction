from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
import os
import time
import datetime

from dotenv import load_dotenv
load_dotenv()
from datetime import date
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(os.environ["MONGO_URL"])
db=client[os.environ["DB"]]
previous_db=client[os.environ["PREVIOUS_SEASON"]]

GW = int(os.environ["GW"])
MODEL_VERSION="0.0.2"
HOME_XG_FIELD = "Home xG"
HOME_G_FIELD = "Home goals"
AWAY_G_FIELD = "Away goals"
HOME_FIELD = "Home"
AWAY_XG_FIELD = "Away xG"
AWAY_FIELD = "Away"
HOME_POINTS_FIELD="Home points"
AWAY_POINTS_FIELD="Away points"
HOME_POSITION_FIELD="Home position"
AWAY_POSITION_FIELD="Away position"
ACTUAL = "Actual"
PREDICTED = "Predicted"

class TeamStatsGroup:
    isHome = False
    positionGroup=""

class TeamStats:
    team = ""
    position_group= TeamStatsGroup()
    xG = 0

def updatexG(position_first,position_last,home_or_away,home_or_away_xg,home_or_away_g,model_version=MODEL_VERSION,predicted_GW=GW):
  pprint("In updatexG")
  oppositeHomeOrAway = AWAY_FIELD if home_or_away == HOME_FIELD else AWAY_FIELD
  oppositeHomeOrAwayPositionField = AWAY_POSITION_FIELD if home_or_away == HOME_FIELD else HOME_POSITION_FIELD
  xGSearch = [
    {
        '$match': {
            'GW': {
                '$lte': GW
            }
        }
    }, {
        '$group': {
            '_id': '$'+home_or_away, 
            'xG': {
                '$avg': '$'+home_or_away_xg
            },
            'G': {
                '$avg': '$'+home_or_away_g
            }
        }
    }
  ]

  xGSearch[0]['$match'][oppositeHomeOrAwayPositionField] = {'$gte': position_first,'$lte': position_last}

    
  pprint(xGSearch)
  matches = db.matches.aggregate(xGSearch)
  
  for match in matches:
    print(match)
    teamStat = TeamStats()
    teamStat.team = match["_id"]
    teamStat.xG = match["xG"]
    

    positionGroup=str(position_first)+"-"+str(position_last)
    
    previous_stat = db.previous_team_stats.find_one({"team":teamStat.team})
    
    weighting = 38-predicted_GW

    # try:
    #   teamStat.xG = ((previous_stat[home_or_away][positionGroup]["xG"]*weighting)+(teamStat.xG))/(predicted_GW+weighting)
    # except:
    #   teamStat.xG = teamStat.xG

    db.team_stats.update_one({"team":teamStat.team,"predicted_GW":predicted_GW,"model_version":model_version},{"$set":{"team":teamStat.team,home_or_away+"."+positionGroup+".xG":teamStat.xG,home_or_away+"."+positionGroup+".G":match["G"]}},True)
    
def updatexGFromPreviousYear(model_version=MODEL_VERSION):

  previous_stats = previous_db.team_stats.find({"predicted_GW":38})
  
  for team_stat in previous_stats:
    db.previous_team_stats.update_one({"team":team_stat["team"]},{"$set":{"Home":team_stat["Home"],"Away":team_stat["Away"]}},True)
    db.team_stats.update_one({"team":team_stat["team"],"predicted_GW":GW,"model_version":model_version},{"$set":{"Home":team_stat["Home"],"Away":team_stat["Away"]}},True)

def updatexGOverride(predicted_GW=GW,model_version=MODEL_VERSION):
  pprint("In updatexGOverride")
  
  xGSearch = [
    {
        '$match': {
            'predicted_GW': predicted_GW
        }
    }, {
        '$project': {
            '_id': '$team', 
            'xG': {
                '$avg': ['$Home.1-20.xG','$Away.1-20.xG']
            }
        }
    }
  ]

  
  stats = db.team_stats.aggregate(xGSearch)
  
  for stat in stats:
    db.team_stats.update_one({"team":stat["_id"],"predicted_GW":GW,"model_version":model_version},{"$set":{"override.xG":stat["xG"]}},True)
    

def updatePoints(homeOrAway):
    id = "$" + homeOrAway
    points = "$Points " + homeOrAway
    goals = "$"+homeOrAway+" goals"
    oppositeHomeOrAway = AWAY_FIELD if homeOrAway == HOME_FIELD else AWAY_FIELD
    goalsAgainst = "$"+oppositeHomeOrAway+" goals"
    pointsSearch = [
      {
        '$match': {
          'GW': {
            '$lte': GW
          }
        }
      }, {
        '$group': {
          '_id': id,
          'points': {
            '$sum': points
          },
          'goals': {
            '$sum': goals
          },
          'goalsAgainst': {
            '$sum': goalsAgainst
          }
        }
      }
    ]
    matches = db.matches.aggregate(pointsSearch)
    for match in matches:
        gd = match["goals"]-match["goalsAgainst"]
        print(match)
        db.predicted_points.update_one({"predicted_GW":GW,"model_version":MODEL_VERSION,"team":match["_id"]},{"$set":{"team":match["_id"],homeOrAway+"."+ACTUAL+".points":match["points"],homeOrAway+"."+ACTUAL+".goals":match["goals"],homeOrAway+"."+ACTUAL+".gd":gd}},True)

def updatePositions(homeOrAway,gw=GW,predicted_gw=GW,model_version=MODEL_VERSION):
      
      points = "$Points "+homeOrAway
      goals = "$"+homeOrAway+" goals"
      
      oppositeHomeOrAway = AWAY_FIELD if homeOrAway == HOME_FIELD else AWAY_FIELD
 
      goalsAgainst = "$"+oppositeHomeOrAway+" goals"
    #   console.log("insert for "+homeOrAwayPosition);
      for gw in range(gw,0,-1):
        
        pointsSearch =[
          {
            '$match': {
              'GW': {
                '$lte': gw
              }
            }
          }, {
            '$group': {
              '_id': "$"+homeOrAway, 
              'points': {
                '$sum': points
              },
              'goalsFor': {
                '$sum': goals
              },
              'goalsAgainst': {
                '$sum': goalsAgainst
              }
            }
          }, {
            '$sort': {
              'points': -1,
              'goalsFor':-1,
              'goalsAgainst':-1
            }
          }
        ]
        
        positions = db.matches.aggregate(pointsSearch)
        
        i=0
        for position in positions:
            i=i+1
            print("GW "+str(gw)+" team: "+position["_id"])
            db.positions.update_one({"predicted_GW":predicted_gw,"model_version":model_version,"GW":gw},{"$set":{homeOrAway+".rank."+position["_id"]+".position":i,homeOrAway+".rank."+position["_id"]+".points":position["points"],homeOrAway+".rank."+position["_id"]+".goalsFor":position["goalsFor"],homeOrAway+".rank."+position["_id"]+".goalsAgainst":position["goalsAgainst"]}},True)

def updatePositionsForSingleGW(homeOrAway,gw=GW,model_version=MODEL_VERSION):
      pprint("In updatePositionsForSingleGW(%s,%s)"%(gw,homeOrAway))
      points = "$Points "+homeOrAway
      goals = "$"+homeOrAway+" goals"
   
      oppositeHomeOrAway = AWAY_FIELD if homeOrAway == HOME_FIELD else AWAY_FIELD
 
      goalsAgainst = "$"+oppositeHomeOrAway+" goals"
    #   console.log("insert for "+homeOrAwayPosition);
    
      pointsSearch =[
        {
          '$match': {
            'GW':{
              "$lte":gw
            }
          }
        }, {
          '$group': {
            '_id': "$"+homeOrAway, 
            'points': {
              '$sum': points
            },
            'goalsFor': {
              '$sum': goals
            },
            'goalsAgainst': {
              '$sum': goalsAgainst
            }
          }
        }, {
          '$sort': {
            'points': -1,
            'goalsFor':-1,
            'goalsAgainst':-1
          }
        }
      ]
      
      positions = db.matches.aggregate(pointsSearch)
      
      i=0
      for position in positions:
          i=i+1
          print("GW "+str(gw)+" team: "+position["_id"])
          db.positions.update_one({"predicted_GW":GW,"model_version":model_version,"GW":gw},{"$set":{homeOrAway+".rank."+position["_id"]+".position":i,homeOrAway+".rank."+position["_id"]+".points":position["points"],homeOrAway+".rank."+position["_id"]+".goalsFor":position["goalsFor"],homeOrAway+".rank."+position["_id"]+".goalsAgainst":position["goalsAgainst"]}},True)


def updateMatchPosition(gw=GW,predicted_gw=GW,model_version=MODEL_VERSION):
  
  pprint("In updateMatchPosition(%s,%s,%s)"%(gw,predicted_gw,model_version))
  matches = db.matches.find({"GW":gw})

  gwpositions = db.positions.find_one({"GW":gw,"predicted_GW":predicted_gw,'model_version':model_version})
  
  for match in matches:
    hometeamposition = gwpositions["Home"]["rank"][match[HOME_FIELD]]["position"]
    awayteamposition = gwpositions["Away"]["rank"][match[AWAY_FIELD]]["position"]

    db.matches.update_one({HOME_FIELD:match[HOME_FIELD],AWAY_FIELD:match[AWAY_FIELD],"GW":gw},{"$set":{HOME_POSITION_FIELD:hometeamposition,AWAY_POSITION_FIELD:awayteamposition}})

def setup():
    
    # db.actualResult.drop()
    # db.predicted_points.drop()
    # db.prediction.drop()
    # db.positions.drop()
    # db.team_stats.drop()
    # db.simulated_matches.drop()
    start_time = time.time()
    updatePoints(HOME_FIELD)
    updatePoints(AWAY_FIELD)
    print("---completed  updatePoints in %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    updatePositions(HOME_FIELD)
    updatePositions(AWAY_FIELD)
    print("---completed  updatePositions in %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    for gw in range(GW,0,-1):
      updateMatchPosition(gw)
    print("---completed  updateMatchPosition in %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    updatexGFromPreviousYear()
    
    print("---completed  updatexGFromPreviousYear in %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    
    updatexG(1,4,HOME_FIELD,HOME_XG_FIELD,HOME_G_FIELD)
    updatexG(5,7,HOME_FIELD,HOME_XG_FIELD,HOME_G_FIELD)
    updatexG(8,12,HOME_FIELD,HOME_XG_FIELD,HOME_G_FIELD)
    updatexG(13,17,HOME_FIELD,HOME_XG_FIELD,HOME_G_FIELD)
    updatexG(18,20,HOME_FIELD,HOME_XG_FIELD,HOME_G_FIELD)
    updatexG(1,4,AWAY_FIELD,AWAY_XG_FIELD,AWAY_G_FIELD)
    updatexG(5,7,AWAY_FIELD,AWAY_XG_FIELD,AWAY_G_FIELD)
    updatexG(8,12,AWAY_FIELD,AWAY_XG_FIELD,AWAY_G_FIELD)
    updatexG(13,17,AWAY_FIELD,AWAY_XG_FIELD,AWAY_G_FIELD)
    updatexG(18,20,AWAY_FIELD,AWAY_XG_FIELD,AWAY_G_FIELD)
    updatexG(1,20,AWAY_FIELD,AWAY_XG_FIELD,AWAY_G_FIELD)
    updatexG(1,20,HOME_FIELD,HOME_XG_FIELD,HOME_G_FIELD)
    print("---completed  updatexG in %s seconds ---" % (time.time() - start_time))
    start_time = time.time()
    updatexGOverride()
    print("---completed  updatexGOverride in %s seconds ---" % (time.time() - start_time))
    start_time = time.time()
    simulateMatches()
    print("---completed  simulate_matches in %s seconds ---" % (time.time() - start_time))

def simulateMatches(model_version=MODEL_VERSION):
    print("In simulated matches")
    matchesSearch = [
      {
        '$match': {
          'GW': {
            '$gt': GW
          }
        }
      },
        {
            '$sort': {
                'GW': 1
            }
        }
    ]

    matches = db.matches.aggregate(matchesSearch)
   
     
    # db.predicted_points.delete_many({}) 
    nextGW = GW
    for match in matches:
        pprint("Looping round matches to simulate")
        if(match["GW"]!=nextGW):
          pprint("Gameweek should be different %s %s"%(match["GW"],nextGW))
          nextGW = match["GW"]
          updatePositionsForSingleGW(HOME_FIELD,match["GW"])
          updatePositionsForSingleGW(AWAY_FIELD,match["GW"])
          updateMatchPosition(match["GW"])
          
        gameWeekPositions = db.positions.find_one({'GW':nextGW}) 
        homeTeamPosition = gameWeekPositions[HOME_FIELD]["rank"][match[HOME_FIELD]]["position"]
        awayTeamPosition = gameWeekPositions[AWAY_FIELD]["rank"][match[AWAY_FIELD]]["position"]
        pprint("Simulating match %s (%s) vs %s (%s)"%(match[HOME_FIELD],homeTeamPosition,match[AWAY_FIELD],awayTeamPosition))

        homexGByLocAndPosition = db.team_stats.find_one({'team':match[HOME_FIELD],"predicted_GW":GW})
        print("homexGByLocAndPosition "+str(homexGByLocAndPosition))
        try:
         homexG = round((homexGByLocAndPosition[HOME_FIELD][getGroup(awayTeamPosition)]["xG"])*2)/2
        except KeyError:
            
            try:
              homexG = round((homexGByLocAndPosition[HOME_FIELD][getGroup(awayTeamPosition,True)]["xG"])*2)/2
            except:
              homexG = round((homexGByLocAndPosition["override"]["xG"])*2)/2


        awayxGByLocAndPosition = db.team_stats.find_one({'team':match[AWAY_FIELD],"predicted_GW":GW})
        try:
            awayxG = round((awayxGByLocAndPosition[AWAY_FIELD][getGroup(homeTeamPosition)]["xG"])*2)/2
        except KeyError:
            try:
              awayxG = round((awayxGByLocAndPosition[AWAY_FIELD][getGroup(homeTeamPosition,True)]["xG"])*2)/2
            except:
              awayxG = round((awayxGByLocAndPosition["override"]["xG"])*2)/2

        homePoints = 1
        awayPoints = 1
        if homexG > awayxG:
            homePoints=3
            awayPoints=0
        elif awayxG>homexG:
            homePoints=0
            awayPoints=3
        homegd = homexG-awayxG
        awaygd = awayxG-homexG
        db.simulated_matches.update_one({"Home":match[HOME_FIELD],"Away":match[AWAY_FIELD],"predicted_GW":GW,"model_version":model_version},{"$set":{HOME_POINTS_FIELD:homePoints,AWAY_POINTS_FIELD:awayPoints,HOME_XG_FIELD:homexG,AWAY_XG_FIELD:awayxG}},True)
        db.predicted_points.update_one({"predicted_GW":GW,"model_version":model_version,"team":match[HOME_FIELD]},{"$inc":{"Home."+PREDICTED+".points":homePoints,"Home."+PREDICTED+".goals":homexG,"Home."+PREDICTED+".gd":homegd}},True)
        db.predicted_points.update_one({"predicted_GW":GW,"model_version":model_version,"team":match[AWAY_FIELD]},{"$inc":{"Away."+PREDICTED+".points":awayPoints,"Away."+PREDICTED+".goals":awayxG,"Away."+PREDICTED+".gd":awaygd}},True)

def prediction(model_version=MODEL_VERSION):
    pointsSearch = [
    {
        '$match': {
            'model_version': model_version
        }
    }, {
        '$project': {
            'team': '$team', 
            'points': {
                '$sum': [
                    '$Home.Actual.points', '$Away.Actual.points', '$Home.Predicted.points', '$Away.Predicted.points'
                ]
            }, 
            'gd': {
                '$sum': [
                    '$Home.Actual.gd', '$Away.Actual.gd', '$Home.Predicted.gd', '$Away.Predicted.gd'
                ]
            }, 
            'goals': {
                '$sum': [
                    '$Home.Actual.goals', '$Away.Actual.goals', '$Home.Predicted.goals', '$Away.Predicted.goals'
                ]
            }
        }
    }, {
        '$sort': {
            'points': -1, 
            'gd': -1, 
            'goals': -1
        }
    }
]

    points= db.predicted_points.aggregate(pointsSearch)
    i = 1
    print("|**Team**|**Predicted Position**|**Predicted Points**|**Predicted Goals For**|**Predicted Goal Difference**")
    print("|-------------------|------------|------------|--------------|--------------|")
    
    for point in points:
        
        print("|"+point["team"]+"|"+str(i)+"|"+str(point["points"])+"|"+str(round(point["goals"]))+"|"+str(round(point["gd"])))
        db.prediction.update_one({"predicted_GW":GW,"team":point["team"],"model_version":model_version},{"$set":{"position":i,"points":point["points"],"gf":point["goals"],"gd":point["gd"],"date":str(date.today())}},True)
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

setup() 
prediction()



