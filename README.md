# football-prediction

## What does it do

This is the code I use to predict the premier league. It is a command line program, backed by a Mongo instance. Currently, the code runs everything, including aggregating the raw stats, simulating the matches and printing the results. This will change (as I don't to run against Mongo every time)

## Known Bugs
1. Currently, you cannot run the the code twice without deleting the predicted_points collection. Otherwise the predictions will increment. I'll get round to it at some point.

## How to use
There is some setup needed.

- A mongo instance. I am using Mongo Atlas, but of course local is fine. You will need to create a database within that and one collection called matches.
- Populate the matches collection. I do this using a csv like the one in this repo. 
- 3 environment variables are needed:
  - MONGO_URL - the fill Mongo connection string (including username and password)
  - DB - the name of the database you created above. I have created several to test the model over multiple seasons
  - GW - this is the gameweek that you are running the model from. For previous seasons you may want to run against different gameweeks

After that, just run python predictive-model.py

## How does the model work

It all revolves around the idea of Expected Goals(xG). The best description of xG can be found [here](https://theanalyst.com/eu/2021/07/what-are-expected-goals-xg/).

We start with the assumption that xG remains relatively stable as the season goes on. This is largely only true from the halfway stage and so the accuracy of the model applies best after 19 games.

From there we look at each teams' xG when playing:
- At home
- Against the ranks of the teams

The model then simulates the remaining matches, predicts the results of the those matches and then gets points. That's about it.

## How the model could be improved
There are a few areas that I would like to improve on which I think will make the accuracy of the predictions better
1. Monte Carlo simulation. Currently, the match simulation is based on the average xG per game. A Monte Carlo simulation will use the average and standard deviation and run through a few thousand simulations. The idea is that it will give the most likely result. I will try that out
2. ~~The simulation I run assumes that the position they were in when the prediction was made (e.g. Southampton were 15 in gameweek 19) is where they will be throughout. And therefore there positions are not taken into account as the games are simulated~~

## How well it works
After the half way stage, the model will predict 6/7 correctly with a further 5-8 within one space. 

## Enhancements to the code
- There are zero tests. This means I change code and test against Mongo directly. No biggie (as my volumes are so small), but I really want to be able to test this quickly
- This is the first time I have written Python. So I am sure there are many things to change in terms of style and minimising the amount of code.