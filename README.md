# football-prediction

## What does it do

This is the code I use to predict the premier league. It is a command line program, backed by a Mongo instance. Currently, the code runs everything, including aggregating the raw stats, simulating the matches and printing the results. 

## Known Bugs


## How to use
There is some setup needed.

As mentioned above, you need a Mongo instance. 

You then need to import some data (would love to connect to an API at some point)

I use these csv files:
- [all-matches](all-matches.csv) **Import into a `matches` collection.** A list of matches for all seasons from 1516 season and their stats. Including xg and possession and other interesting things. I got this from footystats.com. It's not free and I don't use all the stats they provide.
- [clubs](clubs.csv) **Import into a `club_values` collection.** A list of all clubs and their relative value change since a specified time. I get this from Transfermarkt website and have got value change compared to end of last season. Reason for this described below


A .env file is needed. example values below

```
MONGO_URL=mongodb://localhost:27017/?retryWrites=true&w=majority
DB=football_analysis

# no longer used but leaving this in. It allows for different models to be tested side by side
MODEL_VERSION=1.0.3
SIMULATION_VERSION=0.0.4

## which game week do you want to analyse from - this matters if you want to predict from an earlier gameweek
GW=18
## which season are you running the predictions against
SEASON=2223

## season and gameweek - 2223 is the season and 18 is the gameweek (should be 01 etc... for less than 10) needs to match the above. Needs rewriting to not need all three
SEASON_GW=222318

## Weightings of past performance. Most recent has more weighting - 
## this can be and should be adjusted
1516=0.1
1617=0.25
1718=0.5
1819=1
1920=2
2021=5
2122=10
2223=100
```
After that, just run python five-year-analysis.py. This will run the setup of the data, analysis of performance, simulation of the matches and then a predicted table. When running locally (with a local Mongo) this will take around 230 seconds.

You can run these individually if you like, just add the following command when running the code:
- `create_stats` (sets up the data with working out xg and the like)
- `analyse` (analyses the data, developing coefficients for each team using the weightings)
- `simulate` (simulates all matches using a Monte Carlo simulation)
- `create_league_table` (creates the predicted final table for that season)

## How does the model work

It all revolves around the idea of Expected Goals(xG). The best description of xG can be found [here](https://theanalyst.com/eu/2021/07/what-are-expected-goals-xg/).

We start with the assumption that xG remains relatively stable as the season goes on. This is largely only true from the halfway stage and so the accuracy of the model applies best after 19 games, though from around 16 games it isn't bad. I have not run the new model early on in the season yet and now I take into account squad value this may be better. But will try that out at some other point.

The model then simulates the remaining matches, predicts the results of the those matches using a Monte Carlo simulation and then use the mean result from the simulation to give the most likely result. I did use the mode initially (this made sense at the time) but actually this felt too "binary" considering there were only 3 likely outcomes. So switched to mean and this does seem to work better.

When simulating I also take into account the relative value change of each squad compared to the end of last season. This is used to adjust this season's weighting. The idea is that the change in value will impact their relative quality to last season. Little change and you'd expect they would perform closer to last season than this. A lot of change and this season should have a greater impact than last, relatively. A squad's value can increase or decrease because of player purchases or sales, but also because a change in player's performance. e.g. Saka is more valuable this season than last, while Ronaldo (no longer with us...premier league wise) is less valuable

## How the model could be improved


## Other future features
1. An API that returns the predictions
2. A front end for the same reason
3. Currently the stats need to be uploaded manually. I will look to load them in automatically
4. In game predictions
5. A model for fantasy football suggestions

That's about all I can think of for now. Most focus will be on improving the model

## What is the prediction for the current season
[Look here](prediction.md)


## Enhancements to the code
- There are zero tests. This means I change code and test against Mongo directly. No biggie (as my volumes are so small), but I really want to be able to test this quickly
- This is the first time I have written Python. So I am sure there are many things to change in terms of style and minimising the amount of code.