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



def trueActualResult(homeOrAway,model_version=MODEL_VERSION):

  id = "$" + homeOrAway
  points = "$"+homeOrAway+"_points"
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
      db.actualResult.update_one({"team":match["_id"],"model_version":model_version},{"$inc":{"points":match["points"]}},True)
      

def runResults(model_version=MODEL_VERSION):
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
      db.actualResult.update_one({"team":result["team"],"model_version":model_version},{"$set":{"position":i}},True)
      i=i+1

def comparison(gw=GW,model_version=MODEL_VERSION):
  predictions = db.prediction.find({GW_FIELD:gw,MODEL_VERSION_FIELD:model_version}).sort(POSITION_FIELD,1)
  correct = 0
  within_one = 0

  points_correct=0
  points_within_5=0
  points_within_10=0
  
  print("|**Team**|**Predicted Position**|**Actual Position**|**Difference**|**Predicted Points**|**Actual Points**|**Difference**|")
  print("|-------------------|------------|------------|--------------|--------------|----------|-----------|")
    
  for prediction in predictions:
    # print("What was the prediction %s ",prediction)
    team = db.actualResult.find_one({"team":prediction["team"],MODEL_VERSION_FIELD:model_version})
    print("|"+prediction["team"]+"|"+str(prediction["position"])+"|"+str(team["position"])+"|"+str(prediction["position"]-team["position"])+"|"+str(prediction["points"])+"|"+str(team["points"])+"|"+str(prediction["points"]-team["points"])+"|")
    # print("|-------------------|------------|------------|--------------|--------------|----------|-----------|")
    correct = correct+1 if team["position"]==prediction["position"] else correct
    within_one = within_one+1 if (team["position"]-prediction["position"]==1) else within_one
    within_one = within_one+1 if (team["position"]-prediction["position"]==-1) else within_one
  
    points_correct = points_correct+1 if team["points"]==prediction["points"] else points_correct
    points_within_5 = points_within_5+1 if (team["points"]-prediction["points"]<5 and team["points"]-prediction["points"]>-5) else points_within_5
    points_within_10 = points_within_10+1 if (team["points"]-prediction["points"]<10 and team["points"]-prediction["points"]>-10) else points_within_10

  pprint("How many correct "+str(correct))
  pprint("How many within one "+str(within_one))

  pprint("How many points predicted correctly "+str(points_correct))
  pprint("How many predicted within 5 points "+str(points_within_5))
  pprint("How many predicted within 10 points "+str(points_within_10))



def compare_matches(gw=GW,model_version=MODEL_VERSION):
  query = [
    {
        '$match': {
            GW_FIELD: {
                '$gt': gw
            }
        }
    }
]

  print(query)
  matches = db.matches.aggregate(query)
  total_matches = 0
  correct_result = 0
  for match in matches:
    total_matches=total_matches+1
    query = {
    GW_FIELD:match[GW_FIELD],
    MODEL_VERSION_FIELD:MODEL_VERSION,
    TEAM_FIELD:match[HOME_FIELD]
    }
    print(query)
    home_prediction = db.weekly_results.find_one(query)
    print(home_prediction)
    correct = True if match[HOME_POINTS_FIELD]==home_prediction[POINTS_FIELD] else False
    correct_result = correct_result+1  if correct else correct_result
    if(not correct):
      print("Predicted GW "+str(match[GW_FIELD])+" "+match[HOME_FIELD]+" vs "+match[AWAY_FIELD]+" home result = "+str(home_prediction[POINTS_FIELD])+" but actual result was "+str(match[HOME_POINTS_FIELD]))

  pprint("How many correct "+str(correct_result)+" out of "+str(total_matches))

if(len(sys.argv)>1 and sys.argv[1]=="drop_all"):
  print("dropping all")
  
if(len(sys.argv)>1 and sys.argv[1]=="compare_positions"):
  db.actualResult.drop()
  runResults()
  comparison()

if(len(sys.argv)>1 and sys.argv[1]=="compare_matches"):
  print("comparing match results")
  compare_matches()


