## Data Mining

Box-scores are extracted from https://www.fangraphs.com/ and saved as CSV files via the following process:

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

**NOTE: You need to download the Abreviations_Dictionary.csv file at https://drive.google.com/file/d/19urLxfXN0JayHZbMYtme_CkKKfzA1N9e/view?usp=sharing, and save it under /Misc in order to be able to clean the data. Different websites use different abreviations to identify teams, thus a translation tool is sometimes needed to unify various files. This needs to be done only once, that is, upon the very first initiation of the Baseball_Scrapper object accross all instances of its usages.**

Box-scores csv files have to be cleaned before they can be used for statistical purposes. This includes formating team names, eliminating duplicated columns, converting strings to doubles, fixing duplicated match IDs (i.e.: when two games are played on the same day), adding a position variable to batters (1B, 2B, 3B, SS, ect.), and tagging starting pitchers. This can be done via the following method:

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


## Obtaining last-n-days team averages

In order to access a player's performance over the last **n** days, we obtain the sums of all game events that can logically be added. *(I.e.: we can add homeruns (HR) together for a given player, but not his homeruns over first base (HR/FB). To outline this fact, consider two games where a player scored 2, then 0 homeruns for 3, then 7 first base reached. While the average of his HR/FB is 1/3, he in fact scored 2 homeruns for a total of 8 first bases, which gives us a fraction of 1/10.)* Said sums are then used to compute the average sum of events per game for each individual, before being added up to compute the overall expected sum of events for a given roster. Once this is done, we can accurately compute the average team performance in terms of sabermetrics, ratios, ect.

Note that pitching statistics are a little bit harder to access, due to the uncertainty of who is going to act as a relief pitcher once the starting pitcher is worn out. We use a weighted average of the filling pitchers and the announced starting pitcher, based off the average innings completed by the starting pitcher. A time-window of **2n days** is used for the starting pitcher, whereas the usual **n days** is used for the filling pitchers.

In order to query a row for a specific set of players, use the following method:
```python
#pitcher_home: list of starting pitcher, i.e.: [Adrian Houser]
#batters_home: list of batters for the home team.
#...
#date: date, with string format "%Y-%m-%d"
#last_n_days: the number of days used to compute rolling averages
#at_location: boolean variable, if True then the home team's average will be assessed with respect to their matches player at home over the last_n_days,
#             whereas the away team's statistics will be computed with respect to their games player away from home.
#bat: the cleaned bat csv file
#pitch: the cleaned pitch csv file
#pitchSP: the csv file containing solely filling pitchers, obtained with the scrapper.Build_Filling_Pitchers_Database() method.

scrapper.Query_X_row(pitcher_home, batters_home, pitcher_away, batters_away, date, last_n_days, at_location, bat, pitch, pitchSP)
```



