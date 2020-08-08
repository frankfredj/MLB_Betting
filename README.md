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


## Obtaining last-n-days team averages

In order to access a player's performance over the last **n** days, we obtain the sums of all game events that can logically be added. (I.e.: we can add homeruns (HR) together for a given player, but not his homeruns over first base (HR/FB). To outline this fact, consider two games where a player scored 2, then 0 homeruns for 3, then 7 first base reached. While the average of his HR/FB is 1/3, he in fact scored 2 homeruns for a total of 8 first bases, which gives us a fraction of 1/10.)

In order to compute the batting statistics, the last-n-days averages of each player is first computed. Next, they are combined via a weighted average based on each batter's average number of plate appearances (PA). Consequently, batters with few PAs are less impactful, and all column statistics are now ratios of themselves versus total PAs. (I.e.: home runs (H) are now homeruns per plate appearance (H / PA) ).

Starting pitchers statistics are based of the last **2n** days averages, as they tend to participate in fewer games compared to batters. Additionnaly, a last **n** days averages of the relief pitcher are computed, then added to the starting pitcher's via a weighted average based off their respective innings pitched (IP). Consequently, starting pitchers with few IPs are less impactful, and all column statistics are now ratios of themselves versus total IPs. (I.e.: strikes (SO) are now strikes per inning pitcher (SO / IP) ).

Computing these statistics would take hours using R or Python. Thus, RCPP was used to run the computations using C++, alongside parallel processing if need be. The RCPP code is located in the [1.1_RCPP.cpp file](https://github.com/frankfredj/MLB_Betting/blob/master/1.1_RCPP.cpp), whereas the routine to compute the home-versus-away statistics dataframe is located in the [1.2_Fast_Averages_in_RCPP.R file](https://github.com/frankfredj/MLB_Betting/blob/master/1.2_Fast_Averages_in_RCPP.R).


## Outlier removal and Sabermetrics

###### Note: The data used throughout the remainder of this text was obtained with box-scores from 2010 to 2020, using last_n_days = 25. 

Vectors of average values computed with the functions above sometime contain gross outliers, due to being compiled with an insufficient amount of data or extracted tables having abnormal entries. To remove the problematic data points before computing sabermetric statistics, the following non-parametric procedure is recursively applied to every columns:<br/>

1. Compute the average knn distances with n = 20.
2. Eliminate the points **x<sub>k</sub>** such that knn(**x<sub>k</sub>**) is over 3.5 standard deviations away from its mean.
3. Update the remaining points' knn distances.<br/>

Here's an example of the before and after result of trimming with knn: <br/>

![](https://i.imgur.com/3ZvSxfM.png) <br/>


In total, 126 rows out of a total of 21 018 had to be removed due to the presence of gross outliers within their respective column, which translates to a 0.6% removal rate. Once the data has been cleaned, sabermetrics are computed via the code within the [3_Adding_Sabermetrics_to_CPP_data.R](https://github.com/frankfredj/MLB_Betting/blob/master/3_Adding_Sabermetrics_to_CPP_data.R) file.










