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
db=client[os.environ["MATCH_ANALYSIS_DB"]]
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
  home_win_incorrect = 0
  away_win_incorrect = 0
  draw_incorrect = 0
  home_win_correct = 0
  away_win_correct = 0
  draw_correct = 0
  draw_result = 0
  home_wins_were_draws = 0
  away_wins_were_draws = 0
  home_wins_incorrect_home_xg_lower_average = 0 
  away_wins_incorrect_away_xg_lower_average = 0
  away_wins_incorrect_home_xg_higher_average = 0
  home_wins_incorrect_away_xg_higher_average = 0
  home_wins_incorrect_home_xga_higher_average = 0
  away_wins_incorrect_away_xga_higher_average = 0

  home_wins_correct_home_xg_higher_average = 0 
  away_wins_correct_xg_lower_average = 0
  home_wins_correct_away_xg_lower_average = 0
  away_wins_correct_away_xg_higher_average = 0
  home_wins_correct_home_xga_lower_average = 0
  away_wins_correct_home_xga_higher_average = 0

  for match in matches:
    total_matches=total_matches+1
    query = {
    GW_FIELD:match[GW_FIELD],
    MODEL_VERSION_FIELD:MODEL_VERSION,
    TEAM_FIELD:match[HOME_FIELD]
    }
    print(query)
    home_prediction = db.weekly_results.find_one(query)
    query = {
    GW_FIELD:match[GW_FIELD],
    MODEL_VERSION_FIELD:MODEL_VERSION,
    TEAM_FIELD:match[AWAY_FIELD]
    }
    away_prediction = db.weekly_results.find_one(query)
    print(home_prediction)
    correct = True if match[HOME_POINTS_FIELD]==home_prediction[POINTS_FIELD] else False
    home_wins_were_draws = home_wins_were_draws+1 if home_prediction[POINTS_FIELD]==3 and match[HOME_POINTS_FIELD]==1 else home_wins_were_draws
    correct_result = correct_result+1  if correct else correct_result
    away_wins_were_draws = away_wins_were_draws+1 if home_prediction[POINTS_FIELD]==0 and match[HOME_POINTS_FIELD]==1 else away_wins_were_draws
    
    if(correct):
      if home_prediction[POINTS_FIELD]==3:
        home_wins_correct_home_xg_higher_average = home_wins_correct_home_xg_higher_average+1 if match[HOME_XG_FIELD]>home_prediction[GOALS_FIELD] else home_wins_correct_home_xg_higher_average
        home_wins_correct_away_xg_lower_average = home_wins_correct_away_xg_lower_average+1 if match[AWAY_XG_FIELD]<away_prediction[GOALS_FIELD] else home_wins_correct_away_xg_lower_average
        home_wins_correct_home_xga_lower_average = home_wins_correct_home_xga_lower_average+1 if match[AWAY_XG_FIELD]<home_prediction[GOALS_AGAINST_FIELD] else home_wins_correct_home_xga_lower_average
      home_win_correct = home_win_correct+1 if home_prediction[POINTS_FIELD]==3 else home_win_correct
      away_win_correct = away_win_correct+1 if home_prediction[POINTS_FIELD]==0 else away_win_correct
      draw_correct = draw_correct+1 if home_prediction[POINTS_FIELD]==1 else draw_correct
      # if away_prediction[POINTS_FIELD]==3:
      #   away_wins_incorrect_away_xg_lower_average = away_wins_incorrect_away_xg_lower_average+1 if match[AWAY_XG_FIELD]<away_prediction[GOALS_FIELD] else away_wins_incorrect_away_xg_lower_average
      #   away_wins_incorrect_home_xg_higher_average = away_wins_incorrect_home_xg_higher_average+1 if match[HOME_XG_FIELD]>home_prediction[GOALS_FIELD] else away_wins_incorrect_home_xg_higher_average
      #   away_wins_incorrect_away_xga_higher_average = away_wins_incorrect_away_xga_higher_average+1 if match[HOME_XG_FIELD]>away_prediction[GOALS_AGAINST_FIELD] else away_wins_incorrect_away_xga_higher_average

    if(not correct):
      if home_prediction[POINTS_FIELD]==3:
        home_wins_incorrect_home_xg_lower_average = home_wins_incorrect_home_xg_lower_average+1 if match[HOME_XG_FIELD]<home_prediction[GOALS_FIELD] else home_wins_incorrect_home_xg_lower_average
        home_wins_incorrect_away_xg_higher_average = home_wins_incorrect_away_xg_higher_average+1 if match[AWAY_XG_FIELD]>away_prediction[GOALS_FIELD] else home_wins_incorrect_away_xg_higher_average
        home_wins_incorrect_home_xga_higher_average = home_wins_incorrect_home_xga_higher_average+1 if match[AWAY_XG_FIELD]>home_prediction[GOALS_AGAINST_FIELD] else home_wins_incorrect_home_xga_higher_average
      if away_prediction[POINTS_FIELD]==3:
        away_wins_incorrect_away_xg_lower_average = away_wins_incorrect_away_xg_lower_average+1 if match[AWAY_XG_FIELD]<away_prediction[GOALS_FIELD] else away_wins_incorrect_away_xg_lower_average
        away_wins_incorrect_home_xg_higher_average = away_wins_incorrect_home_xg_higher_average+1 if match[HOME_XG_FIELD]>home_prediction[GOALS_FIELD] else away_wins_incorrect_home_xg_higher_average
        away_wins_incorrect_away_xga_higher_average = away_wins_incorrect_away_xga_higher_average+1 if match[HOME_XG_FIELD]>away_prediction[GOALS_AGAINST_FIELD] else away_wins_incorrect_away_xga_higher_average
      home_win_incorrect = home_win_incorrect+1 if home_prediction[POINTS_FIELD]==3 else home_win_incorrect
      away_win_incorrect = away_win_incorrect+1 if home_prediction[POINTS_FIELD]==0 else away_win_incorrect
      draw_incorrect = draw_incorrect+1 if home_prediction[POINTS_FIELD]==1 else draw_incorrect
      draw_result = draw_result+1 if match[HOME_POINTS_FIELD]==1 else draw_result
      print("Predicted GW "+str(match[GW_FIELD])+" actual (predicted) XG:"+str(match[HOME_XG_FIELD])+"("+str(home_prediction[GOALS_FIELD])+") "+match[HOME_FIELD]+" vs "+match[AWAY_FIELD]+" actual (predicted) XG:"+str(match[AWAY_XG_FIELD])+"("+str(away_prediction[GOALS_FIELD])+") home result = "+str(home_prediction[POINTS_FIELD])+" but actual result was "+str(match[HOME_POINTS_FIELD]))

  pprint("How many correct "+str(correct_result)+" out of "+str(total_matches))
  pprint("How many correct home wins "+str(home_win_correct))
  pprint("How many correct away wins "+str(away_win_correct))
  pprint("How many correct draws "+str(draw_correct))
  pprint("How many incorrect home wins "+str(home_win_incorrect))
  pprint("How many expected home wins were draws "+str(home_wins_were_draws))
  pprint("How many incorrect away wins "+str(away_win_incorrect))
  pprint("How many expected away wins were draws "+str(away_wins_were_draws))
  pprint("How many incorrect draws "+str(draw_incorrect))
  pprint("How many matches actually not expected to finish in a draw and did "+str(draw_result))
  pprint("How many incorrect home wins was home xg less than the average "+str(home_wins_incorrect_home_xg_lower_average))
  pprint("How many incorrect away wins was away xg less than the average "+str(away_wins_incorrect_away_xg_lower_average))
  pprint("How many incorrect home wins was away xg higher than the average "+str(home_wins_incorrect_away_xg_higher_average))
  pprint("How many incorrect away wins was home xg higher than the average "+str(away_wins_incorrect_home_xg_higher_average))
  pprint("How many incorrect home wins was home xga higher than the average "+str(home_wins_incorrect_home_xga_higher_average))
  pprint("How many incorrect away wins was away xga higher than the average "+str(away_wins_incorrect_away_xga_higher_average))
  pprint("How many correct home wins was home xg higher than the average "+str(home_wins_correct_home_xg_higher_average))
  pprint("How many correct home wins was away xg lower than the average "+str(home_wins_correct_away_xg_lower_average))
  pprint("How many correct home wins was home xga lower than the average "+str(home_wins_correct_home_xga_lower_average))

def compare_actual_with_expected(gw=GW,model_version=MODEL_VERSION):
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
  home_wins = 0
  away_wins=0
  draws=0

  expected_home_wins = 0
  expected_away_wins = 0
  expected_draws = 0
  home_xg_avg_correct = 0
  away_xg_avg_correct = 0
  home_predicted_comparison = 0
  away_predicted_comparison = 0
  for match in matches:
    home_goals = round(match[HOME_G_FIELD]*2)/2
    away_goals = round(match[AWAY_G_FIELD]*2)/2
    home_x_goals = round(match[HOME_XG_FIELD]*2)/2
    away_x_goals = round(match[AWAY_XG_FIELD]*2)/2
    home_predicted_result = db.weekly_results.find_one({"team":match[HOME_FIELD],GW_FIELD:match[GW_FIELD]})
    away_predicted_result = db.weekly_results.find_one({"team":match[AWAY_FIELD],GW_FIELD:match[GW_FIELD]})

    

    if(home_predicted_result[STATUS_FIELD]==PREDICTED):
      home_predicted_comparison = home_predicted_comparison+1
      away_predicted_comparison = away_predicted_comparison+1
      print("Predicted Home xg= %s vs actual xg= %s "%(home_predicted_result[GOALS_FIELD],home_x_goals))
      if home_predicted_result[GOALS_FIELD]==home_x_goals:
        
        home_xg_avg_correct = home_xg_avg_correct+1
      print("Predicted Away xg= %s vs actual xg= %s "%(away_predicted_result[GOALS_FIELD],away_x_goals))
      if away_predicted_result[GOALS_FIELD]==away_x_goals:
        
        away_xg_avg_correct = away_xg_avg_correct+1

    if(home_goals>away_goals):
      home_wins=home_wins+1
    
    if(away_goals>home_goals):
      away_wins=away_wins+1
    
    if(home_goals==away_goals):
      draws=draws+1
    
    if(home_x_goals>away_x_goals):
      expected_home_wins=expected_home_wins+1
    
    if(away_x_goals>home_x_goals):
      expected_away_wins=expected_away_wins+1
    
    if(home_x_goals==away_x_goals):
      expected_draws=expected_draws+1
    
  pprint("How many home wins vs how many expected: %s vs %s"%(home_wins,expected_home_wins) )
  pprint("How many away wins vs how many expected: %s vs %s"%(away_wins,expected_away_wins) )
  pprint("How many draws vs how many expected: %s vs %s"%(draws,expected_draws) )
  pprint("Number of times xG correct at Home %s out of %s games "%(home_xg_avg_correct,home_predicted_comparison))
  pprint("Number of times xG correct at Away %s out of %s games "%(away_xg_avg_correct,away_predicted_comparison))


def distrubition(gw=GW,model_version=MODEL_VERSION):
  query = [
    {
        '$match': {
            GW_FIELD: {
                '$lte': gw
            }
        }
    },{
      "$sort":{
        "home":1
      }
    }
  ]

  matches = db.matches.aggregate(query)
  number_xg_higher = 0
  number_xg_lower = 0
  number_g_higher = 0
  number_g_lower = 0
  new_team = ""
  team = {}
  team_array = []
  average_xg = 0
  average_g = 0
  for match in matches:
    home_team = match[HOME_FIELD]
    
    if(home_team!=new_team):
      team_array.append(team)
      weekly = db.weekly_results.find_one({"team":home_team,"gw":gw,MODEL_VERSION_FIELD:model_version})
      average_xg = weekly["end_of_week"]["weighted_average"]["home"]["xg"]
      average_g = weekly["end_of_week"]["weighted_average"]["home"]["goals"]
      number_xg_higher = 0
      number_xg_lower = 0
      number_xgd_higher = 0
      number_xgd_lower = 0
      number_g_higher = 0
      number_g_lower = 0
      new_team = home_team
      team={}

    team["name"] = home_team
    if match[HOME_XG_FIELD]>average_xg:
      number_xg_higher= number_xg_higher+1
    if match[HOME_XG_FIELD]<average_xg:
      number_xg_lower= number_xg_lower+1
    if match[HOME_G_FIELD]>average_xg:
      number_g_higher= number_g_higher+1
    if match[HOME_G_FIELD]<average_xg:
      number_g_lower= number_g_lower+1

    team["number_xg_higher"] = number_xg_higher
    team["number_g_higher"] = number_g_higher
    team["number_xg_lower"] = number_xg_lower
    team["number_g_lower"] = number_g_lower
  
  pprint(team_array)


def checkWeightedResultMatchedResult_based_on_XG_no_value(gw=GW,model_version=MODEL_VERSION):
  weighted_average_xg_correct = 0
  weighted_average_xg_incorrect = 0
  query = {GW_FIELD:{"$gt":gw}}
  matches = db.matches.find(query)
  for match in matches:
    home_team = db.club_values.find_one({TEAM_FIELD:match[HOME_FIELD]})
    away_team = db.club_values.find_one({TEAM_FIELD:match[AWAY_FIELD]})
    home_team_weekly = db.weekly_results.find_one({TEAM_FIELD:match[HOME_FIELD],GW_FIELD:gw,MODEL_VERSION_FIELD:model_version})
    away_team_weekly = db.weekly_results.find_one({TEAM_FIELD:match[AWAY_FIELD],GW_FIELD:gw,MODEL_VERSION_FIELD:model_version})
    weighted_average_home_xg = round(home_team_weekly["end_of_week"]["weighted_average"]["home"]["xg"])
    weighted_average_away_xg = round(away_team_weekly["end_of_week"]["weighted_average"]["away"]["xg"])
    home_goals = match[HOME_G_FIELD]
    away_goals = match[AWAY_G_FIELD]

    if(home_goals==away_goals and weighted_average_home_xg==weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1
    elif(home_goals>away_goals and weighted_average_home_xg>weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1  
    elif(home_goals<away_goals and weighted_average_home_xg<weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1
    else:
      weighted_average_xg_incorrect = weighted_average_xg_incorrect+1

  print("How often does the weighted average XG result match the actual result: "+str(weighted_average_xg_correct))
  print("How often does the weighted average XG result NOT match the actual result: "+str(weighted_average_xg_incorrect))

def checkWeightedResultMatchedResult_based_on_XG_with_value(gw=GW,model_version=MODEL_VERSION):
  weighted_average_xg_correct = 0
  weighted_average_xg_incorrect = 0
  query = {GW_FIELD:{"$gt":gw}}
  matches = db.matches.find(query)
  for match in matches:
    home_team = db.club_values.find_one({TEAM_FIELD:match[HOME_FIELD]})
    away_team = db.club_values.find_one({TEAM_FIELD:match[AWAY_FIELD]})
    home_team_weekly = db.weekly_results.find_one({TEAM_FIELD:match[HOME_FIELD],GW_FIELD:gw,MODEL_VERSION_FIELD:model_version})
    away_team_weekly = db.weekly_results.find_one({TEAM_FIELD:match[AWAY_FIELD],GW_FIELD:gw,MODEL_VERSION_FIELD:model_version})
    weighted_average_home_xg = round(home_team_weekly["end_of_week"]["weighted_average"]["home"]["xg"])
    weighted_average_away_xg = round(away_team_weekly["end_of_week"]["weighted_average"]["away"]["xg"])
    home_goals = match[HOME_G_FIELD]
    away_goals = match[AWAY_G_FIELD]

    home_value_rounded= home_team["value"]/10
    away_value_rounded = away_team["value"]//10
    if(home_team["value"]>away_team["value"]):
      # average_home_xg = average_home_xg+(home_value_rounded*0.05)
      weighted_average_home_xg = round(weighted_average_home_xg+(home_value_rounded*0.05))
      # average_away_xg = round(average_home_xga)
    if(away_team["value"]>home_team["value"]):
      # average_away_xg = average_away_xg+(away_value_rounded*0.05)
      weighted_average_away_xg = round(weighted_average_away_xg+(away_value_rounded*0.05))

    if(home_goals==away_goals and weighted_average_home_xg==weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1
    elif(home_goals>away_goals and weighted_average_home_xg>weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1  
    elif(home_goals<away_goals and weighted_average_home_xg<weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1
    else:
      weighted_average_xg_incorrect = weighted_average_xg_incorrect+1

  print("How often does the weighted average XG result (incl. value) match the actual result: "+str(weighted_average_xg_correct))
  print("How often does the weighted average XG result (incl. value) NOT match the actual result: "+str(weighted_average_xg_incorrect))

def matches_analysis(gw=GW,model_version=MODEL_VERSION):
  query = {GW_FIELD:{"$gt":gw}}

  matches = db.matches.find(query)
  xp_wrong = 0
  xg_correct = 0
  weighted_average_xg_correct = 0
  average_xg_vs_xg_correct = 0
  average_xg_value_correct = 0
  average_xga_correct = 0
  value_percentage_correct = 0
  team = {}
  team_array = []
  average_xg = 0
  average_g = 0
  value_correct = 0
  total_games = 0
  home_team_higher_pos_wins = 0
  away_team_higher_pos_wins = 0
  home_team_lower_pos_wins = 0
  away_team_lower_pos_wins = 0
  print("In here")
  for match in matches:
    total_games = total_games+1
    home_team = db.club_values.find_one({TEAM_FIELD:match[HOME_FIELD]})
    away_team = db.club_values.find_one({TEAM_FIELD:match[AWAY_FIELD]})
    home_team_weekly = db.weekly_results.find_one({TEAM_FIELD:match[HOME_FIELD],GW_FIELD:gw,MODEL_VERSION_FIELD:model_version})
    away_team_weekly = db.weekly_results.find_one({TEAM_FIELD:match[AWAY_FIELD],GW_FIELD:gw,MODEL_VERSION_FIELD:model_version})
    
    home_team_previous_weekly = db.weekly_results.find_one({TEAM_FIELD:match[HOME_FIELD],GW_FIELD:gw-1,MODEL_VERSION_FIELD:model_version})
    away_team_previous_weekly = db.weekly_results.find_one({TEAM_FIELD:match[AWAY_FIELD],GW_FIELD:gw-1,MODEL_VERSION_FIELD:model_version})
    
    home_position = home_team_previous_weekly["end_of_week"]["position"]
    away_position = away_team_previous_weekly["end_of_week"]["position"]
    try:
      previous_home = previous_db.weekly_results.find_one({"team":match[HOME_FIELD],GW_FIELD:38})
      previous_home_xg = round(previous_home["end_of_week"]["weighted_average"]["home"]["xg"])
      
    except:
      previous_home_xg = 0
    
    try:
      previous_away = previous_db.weekly_results.find_one({"team":match[AWAY_FIELD],GW_FIELD:38})
      previous_away_xg = round(previous_away["end_of_week"]["weighted_average"]["away"]["xg"])
    except:
      previous_away_xg = 0

    weighted_average_home_xg = round(home_team_weekly["end_of_week"]["weighted_average"]["home"]["xg"])
    average_home_xga = round(home_team_weekly["end_of_week"]["weighted_average"]["home"]["xga"])
    average_home_g = round(home_team_weekly["end_of_week"]["weighted_average"]["home"]["goals"])
    weighted_average_away_xg = round(away_team_weekly["end_of_week"]["weighted_average"]["away"]["xg"])
    average_home_g = round(away_team_weekly["end_of_week"]["weighted_average"]["away"]["goals"])
    average_away_xga = round(away_team_weekly["end_of_week"]["weighted_average"]["away"]["xga"])
    # average_home_xg = round(home_team_weekly["end_of_week"]["average"]["home"]["xg"])
    # average_away_xg = round(away_team_weekly["end_of_week"]["average"]["away"]["xg"])

    home_value_rounded= home_team["value"]//10
    away_value_rounded = away_team["value"]//10
    if(home_team["value"]>away_team["value"]):
      # average_home_xg = average_home_xg+(home_value_rounded*0.05)
      weighted_average_home_xg = round(weighted_average_home_xg+(home_value_rounded*0.05))
      # average_away_xg = round(average_home_xga)
    if(away_team["value"]>home_team["value"]):
      # average_away_xg = average_away_xg+(away_value_rounded*0.05)
      weighted_average_away_xg = round(weighted_average_away_xg+(away_value_rounded*0.05))
      # average_home_xg = round(average_away_xga)
    
   

    home_goals = match[HOME_G_FIELD]
    away_goals = match[AWAY_G_FIELD]
    home_xg = round(match[HOME_XG_FIELD])
    away_xg = round(match[AWAY_XG_FIELD])
    home_points = match[HOME_POINTS_FIELD]
    away_points = match[AWAY_POINTS_FIELD]
    home_xp = match[HOME_XP_FIELD]
    away_xp = match[AWAY_XP_FIELD]

    if(home_goals>away_goals and average_home_xga<average_away_xga):
      average_xga_correct = average_xga_correct+1
    
    if(home_goals<away_goals and average_home_xga>average_away_xga):
      average_xga_correct = average_xga_correct+1
    
    if(home_goals==away_goals and average_home_xga==average_away_xga):
      average_xga_correct = average_xga_correct+1
    
    if(home_goals==away_goals and weighted_average_home_xg==weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1

    if(home_goals>away_goals and weighted_average_home_xg>weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1
      

    if(home_goals<away_goals and weighted_average_home_xg<weighted_average_away_xg):
      weighted_average_xg_correct = weighted_average_xg_correct+1
      
    
    if(home_goals>away_goals):
      if(home_position>away_position):
        home_team_higher_pos_wins = home_team_higher_pos_wins+1
      if(home_position<away_position):
        home_team_lower_pos_wins = home_team_lower_pos_wins+1

    if(home_goals<away_goals):
      if(away_position>home_position):
        away_team_higher_pos_wins = away_team_higher_pos_wins+1
      if(away_position<home_position):
        away_team_lower_pos_wins = away_team_lower_pos_wins+1

    
    if(home_xg>away_xg and weighted_average_home_xg>weighted_average_away_xg):
      average_xg_vs_xg_correct = average_xg_vs_xg_correct+1
    
    if(home_xg>away_xg and weighted_average_home_xg>weighted_average_away_xg):
      average_xg_vs_xg_correct = average_xg_vs_xg_correct+1
    
    if(home_xg==away_xg and weighted_average_home_xg==weighted_average_away_xg):
      average_xg_vs_xg_correct = average_xg_vs_xg_correct+1


    if(home_team["value"]>away_team["value"] and home_points>away_points):
      value_correct=value_correct+1
      if(home_goals>away_goals and weighted_average_home_xg>weighted_average_away_xg):
        average_xg_value_correct = average_xg_value_correct+1
    
    if(home_team["value"]<away_team["value"] and home_points<away_points):
      value_correct=value_correct+1
      if(home_goals<away_goals and weighted_average_home_xg<weighted_average_away_xg):
        average_xg_value_correct = average_xg_value_correct+1
   
    
    if(home_goals>away_goals and home_xg>away_xg):
      xg_correct = xg_correct+1
    
    if(home_goals>away_goals and home_xg>away_xg):
      xg_correct = xg_correct+1
    
    if(home_goals==away_goals and home_xg==away_xg):
      xg_correct = xg_correct+1
    
    
  print("Total Games: "+str(total_games))
  print("Number of home wins where home position higher: "+str(home_team_higher_pos_wins))
  print("Number of away wins where away position higher: "+str(away_team_higher_pos_wins))
  print("Number of home wins where home position lower: "+str(home_team_lower_pos_wins))
  print("Number of away wins where away position lower: "+str(away_team_lower_pos_wins))
  print("How often did xga and points compare: "+str(average_xga_correct))
  print("How often did value and points compare: "+str(value_correct))
  print("How often did xg and goal difference compare: "+str(xg_correct))
  print("How often did average xg and goal difference compare: "+str(weighted_average_xg_correct))
  print("How often did value match xg & result: "+str(average_xg_value_correct))
  print("How often did average xg and xg difference compare: "+str(average_xg_vs_xg_correct))

if(len(sys.argv)>1 and sys.argv[1]=="drop_all"):
  print("dropping all")
  
if(len(sys.argv)>1 and sys.argv[1]=="compare_positions"):
  start_time = time.time()
  db.actualResult.drop()
  runResults()
  comparison()
  print("---completed compare positions in %s seconds ---" % (time.time() - start_time))
if(len(sys.argv)>1 and sys.argv[1]=="compare_matches"):
  start_time = time.time()
  print("comparing match results")
  compare_matches()
  print("---completed compare matches in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="compare_actual_with_expected"):
  start_time = time.time()
  print("comparing match results")
  compare_actual_with_expected()
  print("---completed compare matches in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="distribution"):
  start_time = time.time()
  print("distribution")
  distrubition()
  print("---completed distribution in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="match_analysis"):
  start_time = time.time()
  gw = GW
  if(len(sys.argv)>2):
    gw=int(sys.argv[2])
  matches_analysis(gw)
  print("---completed matches_analysis in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="xg_no_value"):
  start_time = time.time()
  gw = GW
  if(len(sys.argv)>2):
    gw=int(sys.argv[2])
  checkWeightedResultMatchedResult_based_on_XG_no_value(gw)
  print("---completed xg_no_value in %s seconds ---" % (time.time() - start_time))

if(len(sys.argv)>1 and sys.argv[1]=="xg_with_value"):
  start_time = time.time()
  gw = GW
  if(len(sys.argv)>2):
    gw=int(sys.argv[2])
  checkWeightedResultMatchedResult_based_on_XG_with_value(gw)
  print("---completed xg_with_value in %s seconds ---" % (time.time() - start_time))
