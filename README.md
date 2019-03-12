# Project

## Name

* Score Probability

## Description

* Score probability calculation model based on Poisson distribution

## Data source

* real-time crawling of QTW net

* QTW URL: http://zq.win007.com/info/index_cn.htm

## Instructions

* Generally, you only need to modify gauss_run_display.py, 
    and you can save the result csv to the output/ folder.

* The parameters that need to be modified include:

    1. league_name: representing a certain league or all league --> str
        1. "all": refer to the third rule of ## Note 
        2. "yc", "dj", "xj", "yj","fj", "yg", "dy", "fy" : certain league

    2. start_time: selected start time --> int(20181220)
    
    3. end_time: selected end time --> int(20181226)

## Note

* If the QTW interface does not capture data, you need to manually save the 
    data to a csv.
    
* Field names must be named as follows:
    Date, HomeTeam, AwayTeam, FTHG, FTAG, status, gameweek
    
* Currently supported league:
    "yc", "dj", "xj", "yj","fj", "yg", "dy", "fy", "bj", "sc", "hj", "pc", "ac"
    
* IF you want to support other league, you must modify gauss_crawl.py
    variable name: SPECIAL_SEASON_URL
    for example:
        add key: "ac", then go to QTW website to find ac and add the url of 
        display ac match result as the value

## Dependent python library

```
beautifulsoup4-version 4.6.3
fire-version 0.1.3
loguru-version 0.2.3
lxml-version 4.2.5
numpy-version 1.11.3
pandas-version 0.20.1
pymc-version 2.3.6
pymongo-version 3.7.2
requests-version 2.21.0
scipy-version 0.19.0
tornado-version 5.1.1
PyYAML-version 4.2b1
```

## Command

```shell
pip3 freeze > requirements.txt
---
python3 run_display.py --league_name dj --start_time 20181218 
--end_time 20181220 > gauss_predict_dj.txt
---
python3 run_display.py --league_name all --start_time 20181218 
--end_time 20181220 > gauss_predict.txt
```
