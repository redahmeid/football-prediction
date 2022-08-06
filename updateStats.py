from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
import os

from dotenv import load_dotenv
load_dotenv()
from datetime import date
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(os.environ["MONGO_URL"])
db=client[os.environ["DB"]]

GW = int(os.environ["GW"])

HOME_XG_FIELD = "Home xG"
HOME_FIELD = "Home"
AWAY_XG_FIELD = "Away xG"
AWAY_FIELD = "Away"
HOME_POINTS_FIELD="Home points"
AWAY_POINTS_FIELD="Away points"
ACTUAL = "Actual"
PREDICTED = "Predicted"

class TeamStatsGroup:
    isHome = False
    positionGroup=""

class TeamStats:
    team = ""
    position_group= TeamStatsGroup()
    xG = 0

def updatexG(position_first,position_last,home_or_away,home_or_away_xg):
  oppositeHomeOrAway = AWAY_FIELD if home_or_away == HOME_FIELD else AWAY_FIELD
  oppositeHomeOrAwayPositionField = oppositeHomeOrAway + " Position"
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
            }
        }
    }
  ]

  xGSearch[0]['$match'][oppositeHomeOrAwayPositionField] = {'$gte': position_first,'$lte': position_last}

  matches = db.matches.aggregate(xGSearch)
  for match in matches:
    print(match)
    teamStat = TeamStats()
    teamStat.team = match["_id"]
    teamStat.xG = match["xG"]
    
    positionGroup=str(position_first)+"-"+str(position_last)
    db.team_stats.update_one({"team":teamStat.team},{"$set":{"team":teamStat.team,home_or_away+"."+positionGroup+".xG":teamStat.xG}},True)
    

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

def setup():
    updatexG(1,4,HOME_FIELD,HOME_XG_FIELD)
    updatexG(5,7,HOME_FIELD,HOME_XG_FIELD)
    updatexG(8,12,HOME_FIELD,HOME_XG_FIELD)
    updatexG(13,17,HOME_FIELD,HOME_XG_FIELD)
    updatexG(18,20,HOME_FIELD,HOME_XG_FIELD)

    updatexG(1,4,AWAY_FIELD,AWAY_XG_FIELD)
    updatexG(5,7,AWAY_FIELD,AWAY_XG_FIELD)
    updatexG(8,12,AWAY_FIELD,AWAY_XG_FIELD)
    updatexG(13,17,AWAY_FIELD,AWAY_XG_FIELD)
    updatexG(18,20,AWAY_FIELD,AWAY_XG_FIELD)
    updatexG(1,20,AWAY_FIELD,AWAY_XG_FIELD)
    updatexG(1,20,HOME_FIELD,HOME_XG_FIELD)
    updatePoints(HOME_FIELD)
    updatePoints(AWAY_FIELD)
    updatePositions(HOME_FIELD)
    updatePositions(AWAY_FIELD)
    simulateMatches()

def simulateMatches():
    print("In simulated matches")
    matchesSearch = [
      {
        '$match': {
          'GW': {
            '$gt': GW
          }
        }
      }
    ]

    matches = db.matches.aggregate(matchesSearch)
   
    gameWeekPositions = db.positions.find_one({'GW':GW})  
    # db.predicted_points.delete_many({}) 
    for match in matches:
        
        homeTeamPosition = gameWeekPositions[HOME_FIELD]["rank"][match[HOME_FIELD]]["position"]
        awayTeamPosition = gameWeekPositions[AWAY_FIELD]["rank"][match[AWAY_FIELD]]["position"]
        pprint("Simulating match %s (%s) vs %s (%s)"%(match[HOME_FIELD],homeTeamPosition,match[AWAY_FIELD],awayTeamPosition))

        homexGByLocAndPosition = db.team_stats.find_one({'team':match[HOME_FIELD]})
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
    print("The predicted placings:")
    i = 1
    for point in points:
        print(point["team"])
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
  predictions = db.prediction.find({"date":str(date)})
  correct = 0
  within_one = 0
  for prediction in predictions:
    team = db.actualResult.find_one({"team":prediction["team"]})
    correct = correct+1 if team["position"]==prediction["position"] else correct
    within_one = within_one+1 if (team["position"]-prediction["position"]==1) else within_one
    within_one = within_one+1 if (team["position"]-prediction["position"]==-1) else within_one
  
  pprint("How many correct "+str(correct))
  pprint("How many within one "+str(within_one))


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

# setup() 
# prediction()
# runResults()

comparison(date(2022,8,6))


