## Data Mining

Box-scores are extracted from https://www.fangraphs.com/ via the following process:

1. Initialise a Baseball_Scrapper object with a provided path where the files will be stored. 
```python
scrapper = Baseball_Scrapper("D:/MLB")
```
2. Retrieve the box-scores URLs with respect to a date interval. 
```python
scrapper.Get_FanGraphs_Game_URLs("2016-03-28", "2019-08-15")
```
3. Scrape the box-scores off the URLs list previously acquired. 
```python
scrapper.Extract_FanGraphs_Box_Scores()
```

If you've already scrapped data for a previous time period and wish to simply update your box-scores list, use the following method:
```python
scrapper.UPDATE_FanGraphs_Box_Scores()
```

If you wish to process URLs with missing box-scores, use the following method:
```python
scrapper.Extract_FanGraphs_Box_Scores_FROM_MISSING_MATCHESs()
```

