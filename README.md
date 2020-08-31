## Data Mining

Box-scores are extracted from [FanGraphs' website](https://www.fangraphs.com/) and saved as CSV files via the following process:

```python
#1. Initialise a Baseball_Scrapper object with a provided path where the files will be stored. 
scrapper = Baseball_Scrapper("D:/MLB")

#2. Retrieve the box-scores URLs with respect to a date interval. 
scrapper.Get_FanGraphs_Game_URLs("2016-03-28", "2019-08-15")

#3. Scrape the box-scores off the URLs list previously acquired. 
scrapper.Extract_FanGraphs_Box_Scores()


#If you've already scrapped data for a previous time period and wish to simply update your box-scores list, use the following method:
scrapper.UPDATE_FanGraphs_Box_Scores()

#If you wish to process URLs with missing box-scores, use the following method:
scrapper.Extract_FanGraphs_Box_Scores_FROM_MISSING_MATCHES()
```


The game URLs are stored in **/Misc**, whereas the batters' csv file is saved in **/Bat** and the pitchers' in **/Pitch**. As an example, here's a screenshot of the raw **FanGraph_Box_Scores** csv file for the pitchers:

![](https://i.imgur.com/KX6K3AY.png)


## Data Pre-processing

**NOTE: You need to download the Abreviations_Dictionary.csv file, and save it under /Misc in order to be able to clean the data. Different websites use different abreviations to identify teams, thus a translation tool is sometimes needed to unify various files. This needs to be done only once, that is, upon the very first initiation of the Baseball_Scrapper object accross all instances of its usages.**

Box-scores csv files have to be cleaned before they can be used for statistical purposes. This includes formating team names, eliminating duplicated columns, converting strings to doubles, fixing duplicated match IDs (i.e.: when two games are played on the same day), adding a position variable to batters (1B, 2B, 3B, SS, ect.), replacing missing data with 0s, adding columns specifying if a 0 is in fact a case of missing data, and tagging starting pitchers. This can be done via the following method:

```python
scrapper.Clean_Data()
```

Historical money lines data are avaible in csv format at: https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/mlboddsarchives.htm. They can be extracted and processed via the following method:
```python
scrapper.Clean_Betting_Data()
```

Lastly, a database solely comprising relief pitches can be build via the method outlined below. (This is required in order to model the relief pitchers as some sort of random effect, since only the starting pitchers are announced in advance.)
```python
scrapper.Build_Filling_Pitchers_Database()
```


## Modeling

Modeling is done using a routine that computes the rolling averages over the last 30 days, over a selected subset of metrics judged to be relevant. The database is updated daily, and betting opportunities are taken from [Loto-Quebec's "Mise-O-Jeu" website](https://miseojeu.espacejeux.com/en/home) . Lineups predictions from [Rotowire](https://www.rotowire.com/baseball/daily-lineups.php) are used in conjunction with the database to construct the latest rolling averages, before feeding the data to the model. Bets are then made on options with over-evaluated returns.

Predictions are posted on [the mlbArbitrage subReddit](https://old.reddit.com/r/mlbArbitrage/) before matches start. The top frame contains the Cities, LotoQc Returns and Model Returns arranged in {Home, Visitor} order. The second frame contains the names of the teams that were betted on, the differences in returns and the number of batters that weren't found in the database. (Players are only evaluated with respect to their performance whilst playing for their current team, which means the player's stats will be unavaible after a trade has occured, up until he plays at least once for his new team.)

Implementation started on 2020-08-30, and the most recent performance are:

![](https://i.imgur.com/5Q2Z9cP.png)

















