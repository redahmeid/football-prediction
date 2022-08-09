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



def trueActualResult(homeOrAway,model_version=MODEL_VERSION):

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
  predictions = db.prediction.find({"predicted_GW":gw,"model_version":model_version}).sort("position",1)
  correct = 0
  within_one = 0

  points_correct=0
  points_within_5=0
  points_within_10=0
  
  print("|**Team**|**Predicted Position**|**Actual Position**|**Difference**|**Predicted Points**|**Actual Points**|**Difference**|")
  print("|-------------------|------------|------------|--------------|--------------|----------|-----------|")
    
  for prediction in predictions:
    # print("What was the prediction %s ",prediction)
    team = db.actualResult.find_one({"team":prediction["team"],"model_version":model_version})
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

if(len(sys.argv)>1 and sys.argv[1]=="drop_all"):
  print("dropping all")
  db.actualResult.drop()

runResults()

comparison()


