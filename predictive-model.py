from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
import os
import time

from dotenv import load_dotenv
load_dotenv()
from datetime import date
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(os.environ["MONGO_URL"])
db=client[os.environ["DB"]]
previous_db=client[os.environ["PREVIOUS_SEASON"]]

GW = int(os.environ["GW"])

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

def updatexG(position_first,position_last,home_or_away,home_or_away_xg,home_or_away_g):
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
    previous_stats = previous_db.team_stats.find_one({"team":teamStat.team,"GW":38})
    try:
      teamStat.xG = (previous_stats[home_or_away+"."+positionGroup+".xG"]+(teamStat.xG*38))/(GW+1)
    except:
      teamStat.xG = teamStat.xG
    db.team_stats.update_one({"team":teamStat.team,"GW":GW},{"$set":{"team":teamStat.team,home_or_away+"."+positionGroup+".xG":teamStat.xG,home_or_away+"."+positionGroup+".G":match["G"]}},True)
    

def updatePoints(homeOrAway):
    id = "$" + homeOrAway
    points = "$Points " + homeOrAway
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
          }
        }
      }
    ]
    matches = db.matches.aggregate(pointsSearch)
    for match in matches:
        print(match)
        db.predicted_points.update_one({"team":match["_id"]},{"$set":{"team":match["_id"],homeOrAway+"."+ACTUAL:match["points"]}},True)

def updatePositions(homeOrAway):
      
      points = "$Points "+homeOrAway
      goals = "$"+homeOrAway+" goals"
      gw=GW
      oppositeHomeOrAway = AWAY_FIELD if homeOrAway == HOME_FIELD else AWAY_FIELD
 
      goalsAgainst = "$"+oppositeHomeOrAway+" goals"
    #   console.log("insert for "+homeOrAwayPosition);
      for gw in range(GW,0,-1):
        
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
            db.positions.update_one({"GW":gw},{"$set":{homeOrAway+".rank."+position["_id"]+".position":i,homeOrAway+".rank."+position["_id"]+".points":position["points"],homeOrAway+".rank."+position["_id"]+".goalsFor":position["goalsFor"],homeOrAway+".rank."+position["_id"]+".goalsAgainst":position["goalsAgainst"]}},True)

def updatePositionsForSingleGW(GW,homeOrAway):
      pprint("In updatePositionsForSingleGW(%s,%s)"%(GW,homeOrAway))
      points = "$Points "+homeOrAway
      goals = "$"+homeOrAway+" goals"
      gw=GW
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
          db.positions.update_one({"GW":gw},{"$set":{homeOrAway+".rank."+position["_id"]+".position":i,homeOrAway+".rank."+position["_id"]+".points":position["points"],homeOrAway+".rank."+position["_id"]+".goalsFor":position["goalsFor"],homeOrAway+".rank."+position["_id"]+".goalsAgainst":position["goalsAgainst"]}},True)


def updateMatchPosition(gw):
  

  matches = db.matches.find({"GW":gw})
  gwpositions = db.positions.find_one({"GW":gw})
  for match in matches:
    
    hometeamposition = gwpositions["Home"]["rank"][match[HOME_FIELD]]["position"]
    awayteamposition = gwpositions["Away"]["rank"][match[AWAY_FIELD]]["position"]

    db.matches.update_one({HOME_FIELD:match[HOME_FIELD],AWAY_FIELD:match[AWAY_FIELD],"GW":gw},{"$set":{HOME_POSITION_FIELD:hometeamposition,AWAY_POSITION_FIELD:awayteamposition}})

def setup():
    
    db.actualResult.drop()
    db.predicted_points.drop()
    db.prediction.drop()
    db.positions.drop()
    db.team_stats.drop()
    db.simulated_matches.drop()
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
    simulateMatches()
    print("---completed  simulate_matches in %s seconds ---" % (time.time() - start_time))

def simulateMatches():
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
          updatePositionsForSingleGW(match["GW"],HOME_FIELD)
          updatePositionsForSingleGW(match["GW"],AWAY_FIELD)
          updateMatchPosition(match["GW"])
          
        gameWeekPositions = db.positions.find_one({'GW':nextGW}) 
        homeTeamPosition = gameWeekPositions[HOME_FIELD]["rank"][match[HOME_FIELD]]["position"]
        awayTeamPosition = gameWeekPositions[AWAY_FIELD]["rank"][match[AWAY_FIELD]]["position"]
        pprint("Simulating match %s (%s) vs %s (%s)"%(match[HOME_FIELD],homeTeamPosition,match[AWAY_FIELD],awayTeamPosition))

        homexGByLocAndPosition = db.team_stats.find_one({'team':match[HOME_FIELD],"GW":GW})
        try:
         homexG = round((homexGByLocAndPosition[HOME_FIELD][getGroup(awayTeamPosition)]["xG"])*2)/2
        except KeyError:
            homexG = round((homexGByLocAndPosition[HOME_FIELD][getGroup(awayTeamPosition,True)]["xG"])*2)/2


        awayxGByLocAndPosition = db.team_stats.find_one({'team':match[AWAY_FIELD]})
        try:
            awayxG = round((awayxGByLocAndPosition[AWAY_FIELD][getGroup(homeTeamPosition)]["xG"])*2)/2
        except KeyError:
            awayxG = round((awayxGByLocAndPosition[AWAY_FIELD][getGroup(homeTeamPosition,True)]["xG"])*2)/2
        homePoints = 1
        awayPoints = 1
        if homexG > awayxG:
            homePoints=3
            awayPoints=0
        elif awayxG>homexG:
            homePoints=0
            awayPoints=3
        
        db.simulated_matches.update_one({"Home":match[HOME_FIELD],"Away":match[AWAY_FIELD]},{"$set":{HOME_POINTS_FIELD:homePoints,AWAY_POINTS_FIELD:awayPoints}},True)
        db.predicted_points.update_one({"team":match[HOME_FIELD]},{"$inc":{"Home."+PREDICTED:homePoints}},True)
        db.predicted_points.update_one({"team":match[AWAY_FIELD]},{"$inc":{"Away."+PREDICTED:awayPoints}},True)

def prediction():
    pointsSearch = [
        {
            '$project': {
                'team': '$team', 
                'points': {
                    '$add': [
                        '$Home.Actual', '$Away.Actual', '$Home.Predicted', '$Away.Predicted'
                    ]
                }
            }
        },
        {
            '$sort': {
                'points': -1
            }
        }
    ]

    points= db.predicted_points.aggregate(pointsSearch)
    i = 1
    for point in points:
        db.prediction.update_one({"date":str(date.today()),"team":point["team"]},{"$set":{"position":i,"points":point["points"]}},True)
        i=i+1
        


def trueActualResult(homeOrAway):

  id = "$" + homeOrAway
  points = "$Points " + homeOrAway
  pointsSearch = [
    {
      '$group': {
        '_id': id,
        'points': {
          '$sum': points
        }
      }
    },
        {
            '$sort': {
                'points': -1
            }
        }
  ]
  matches = db.matches.aggregate(pointsSearch)

  for match in matches:
      db.actualResult.update_one({"team":match["_id"]},{"$inc":{"points":match["points"]}},True)
      

def runResults():
  trueActualResult(HOME_FIELD)
  trueActualResult(AWAY_FIELD)
  
  pointsSearch = [
   
        {
            '$sort': {
                'points': -1
            }
        }
  ]
  results = db.actualResult.aggregate(pointsSearch)
  i=1
  for result in results:
      db.actualResult.update_one({"team":result["team"]},{"$set":{"position":i}},True)
      i=i+1

def comparison(date):
  predictions = db.prediction.find({"date":str(date)}).sort("position",1)
  correct = 0
  within_one = 0

  points_correct=0
  points_within_5=0
  points_within_10=0
  print("|**Team**|**Predicted Position**|**Actual Position**|**Difference**|**Predicted Points**|**Actual Points**|**Difference**|")
  print("|-------------------|------------|------------|--------------|--------------|----------|-----------|")
    
  for prediction in predictions:
    team = db.actualResult.find_one({"team":prediction["team"]})
    print("|"+prediction["team"]+"|"+str(prediction["position"])+"|"+str(team["position"])+"|"+str(prediction["position"]-team["position"])+"|"+str(prediction["points"])+"|"+str(team["points"])+"|"+str(prediction["points"]-team["points"])+"|")
    # print("|-------------------|------------|------------|--------------|--------------|----------|-----------|")
    correct = correct+1 if team["position"]==prediction["position"] else correct
    within_one = within_one+1 if (team["position"]-prediction["position"]==1) else within_one
    within_one = within_one+1 if (team["position"]-prediction["position"]==-1) else within_one
  
    points_correct = points_correct+1 if team["points"]==prediction["points"] else points_correct
    points_within_5 = points_within_5+1 if (team["points"]-prediction["points"]<5 & team["points"]-prediction["points"]>-5) else points_within_5
    points_within_10 = points_within_10+1 if (team["points"]-prediction["points"]<10 & team["points"]-prediction["points"]>-10) else points_within_10

  pprint("How many correct "+str(correct))
  pprint("How many within one "+str(within_one))

  pprint("How many points predicted correctly "+str(points_correct))
  pprint("How many predicted within 5 points "+str(points_within_5))
  pprint("How many predicted within 10 points "+str(points_within_10))

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
runResults()

comparison(date(2022,8,7))


