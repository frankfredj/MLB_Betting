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

In order to query a row given a specific match ID, use the following method:
```python
scrapper.Query_from_ID(ID, last_n_days, at_location, bat, pitch, pitchSP)
```

In order to query every single scrapped match (i.e.: if one wishes to build a completed regression frame based off his or her entire box-scores database), use the method outlined below. *Note that the same method is used to merely update an existing regression frame csv file.*
```python
#purge: boolean, if True then the bat, pitch and pitchSP frames will be purged every 100 iterations.
scrapper.Query_all_MLB_Odds_matches(last_n_days, at_location, purge)
```

###### Note: Computing rolling averages with Python is painfuly slow. A quick version is avaible in the file 1_Fast_Averages_in_RCPP.R. It is fully coded in C++, and uses parallel processing. 


## Gross outliers removal

###### Note: The data used throughout the remainder of this text was obtained with box-scores from 2010 to 2020, using last_n_days = 25 and at_location = True. 

Vectors of average values computed with the functions above sometime contain gross outliers, due to being compiled with an insufficient amount of data or extracted tables having abnormal entries. To remove the problematic data points before computing sabermetric statistics, the following non-parametric procedure is recursively applied to every columns:<br/>

1. Compute the average knn distances with n = 20.
2. Eliminate the points **x<sub>k</sub>** such that knn(**x<sub>k</sub>**) is over 3 standard deviations away from its mean.
3. Update the remaining points' knn distances.<br/>

Here's an example of the before and after result of trimming with knn: <br/>

![](https://i.imgur.com/cqibFfe.png) <br/>


In total, 180 rows out of a total of 21 018 had to be removed due to the presence of gross outliers within their respective column, which translates to a 0.86% removal rate.


## Simulating artificial data

In order to generate synthetic data, the following steps are first followed:<br/>
1. Add 4 extra columns to the data matrix **X** , so to include the opening moneylines and the final scores within our empirical joint distribution.
2. Remove colinearities and near-zero variance columns, so to produce an invertible variance-covariance matrix.
3. Obtain the Cholesky decomposition **L** of the variance-covariance matrix.
4. Obtain the principal components **Z** of **X** by multiplying **X** with the inverse of **L**.<br/> 

Once this is done, our data can be modeled as matrix multiplication of **m** independent random variables, that is: **X** = **Z % L** . For a single column vector **x** , this corresponds to **x = transpose(L) % z**. This, however, tends to produce data points which fall outside their usual range. (I.e.: scores below 0). Moreover, the convergence in terms of covariances and means is extremely slow, which can lead to synthetic samples that are downright statistically nonsensical.

To counter the serious downsides mentioned above, the simulated independent variables **z** are bounded by exploiting the fact that **transpose(L)** is a lower-triangular matrix. First, determine two bound vectors **lb** and **ub** based on the empirical dataset **X**. Next, apply the following standard MCMC algorithm: <br/>

**Iteration 1...**

**lb<sub>1</sub> <= L<sub>1,1</sub> z<sub>1</sub> <= ub<sub>1</sub>**

**lb<sub>1</sub> / L<sub>1,1</sub> <=  z<sub>1</sub> <= ub<sub>1</sub> / L<sub>1,1</sub>**

**Simulate z<sub>1</sub> from its bounded empirical CDF**<br/>

**Iteration 2...**

**lb<sub>2</sub> <= L<sub>2,1</sub> z<sub>1</sub> + L<sub>2,2</sub> z<sub>2</sub> <= ub<sub>2</sub>**

**(lb<sub>2</sub> - L<sub>2,1</sub> z<sub>1</sub>) / L<sub>2,2</sub> <=   z<sub>2</sub> <= (ub<sub>2</sub> - L<sub>2,1</sub> z<sub>1</sub>) / L<sub>2,2</sub>**

**Simulate z<sub>2</sub> from its bounded empirical CDF**<br/>

**...**<br/>

**Iteration m...**<br/>


As a way to reduce variance, vectors are simulated in pairs such that the first member uses **m** random variables **U** uniformly distributed on (0,1), whereas the second one uses **1-U** instead. It is trivial that **Cov(F<sup>-1</sup>(u) , F<sup>-1</sup>(1-u))** < 0.


To assess the MCMC convergence in distribution, a synthetic sample of 18 647 games was produced, then compared with the original data. Heatmaps of the correlations, alongside density plots of the normalized differences in column means and their associates t-test p-values, are show below.

![](https://i.imgur.com/7pK3Vqb.png)<br/>

![](https://i.imgur.com/l1W5bSJ.png)<br/>





