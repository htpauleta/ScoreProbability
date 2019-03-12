"""
@Project   : ScoreProbability
@Module    : base.py
@Author    : HjwGivenLyy [1752929469@qq.com]
@Created   : 12/16/18 11:55 AM
@Desc      : basic module of entire project
"""

import typing

import pandas as pd
import yaml
from pymongo import MongoClient

SUPPORT_LEAGUE_ID_NAME = {
    31: 'xj', 36: 'yc', 8: 'dj', 34: 'yj', 11: 'fj',
    9: 'dy', 37: 'yg', 12: 'fy', 33: 'xy', 40: 'yy',
    5: 'bj', 29: 'sc', 23: 'pc', 16: 'hj', 273: 'ac',
    17: 'hy'
}

SUPPORT_LEAGUE_NAME_ID = {
    'yc': 36, 'dj': 8, 'xj': 31, 'yj': 34, 'fj': 11,
    'dy': 9, 'yg': 37, 'fy': 12, 'xy': 33, 'yy': 40,
    'bj': 5, 'sc': 29, 'pc': 23, 'hj': 16, 'ac': 273,
    'hy': 17
}

SUPPORT_LEAGUE_ID_LIST = [
    8, 11, 31, 34, 36, 9, 37, 12, 33, 40,
    5, 29, 23, 16, 273, 17
]

LEAGUE_NAME_LST = [
    'all', 'dj', 'yc', 'xj', 'yj', 'fj', 'dy', 'fy', 'yg', 'bj', 'sc', 'pc',
    'hj', 'ac', 'xy', 'yy', 'hy'
]

SERVER_FILE_PATH = "/home/pauleta/pauleta-gauss/ScoreProbability/" \
                   "scoreprobability/server.yaml"


def connect_mongodb() -> MongoClient:
    """connect mongodb"""
    config = yaml.load(open(SERVER_FILE_PATH, encoding="utf-8"))
    config_dct = config.get("mongodb")
    client = MongoClient(
        "mongodb://{user}:{pwd}@{host}:{port}/{db}"
        "?readPreference=primary".format(
            user=config_dct.get("user"),
            pwd=config_dct.get("pwd"),
            host=config_dct.get("host"),
            port=config_dct.get("port"),
            db=config_dct.get("db")))

    return client


def team_id_cn_name_by_league_id(
        db_client: MongoClient, league_id: int) -> typing.Dict[int, str]:
    """get team id and cn name dict"""
    result_dct = {}

    tb = db_client["xscore"]["team_info"]
    result = tb.find(
        filter={"league_id": int(league_id)},
        projection={"team_cn_name": 1, "team_id": 1}
    )

    result_lst = list(result)

    for result_data in result_lst:
        result_dct[int(result_data["team_id"])] = result_data["team_cn_name"]

    return result_dct


def team_id_en_name_by_league_id(
        db_client: MongoClient, league_id: int) -> typing.Dict[int, str]:
    """get team id and en name dict"""
    result_dct = {}

    tb = db_client["xscore"]["team_info"]
    result = tb.find(
        filter={"league_id": league_id},
        projection={"team_en_name": 1, "team_id": 1}
    )

    result_lst = list(result)

    for result_data in result_lst:
        result_dct[int(result_data["team_id"])] = result_data["team_en_name"]

    return result_dct


def get_played_data(db_client: MongoClient,
                    qtw_league_id: int) -> pd.DataFrame:
    """get have finished match information by qtw_league_id"""

    Date_lst, HomeTeam_lst, AwayTeam_lst = [], [], []
    FTHG_lst, FTAG_lst, status_lst, gameweek_lst = [], [], [], []

    match_tb = db_client["xscore"]["match_info"]
    team_id_to_en_name = team_id_en_name_by_league_id(
        db_client=db_client, league_id=qtw_league_id)

    result = match_tb.find(
        filter={"qtw_league_id": int(qtw_league_id), "status": 2},
        projection={
            "home_id": 1, "away_id": 1, "match_time": 1, "home_score": 1,
            "away_score": 1, "status": 1, "game_week": 1
        }
    )

    result_lst = list(result)

    for result_data in result_lst:
        Date_lst.append(result_data["match_time"])
        HomeTeam_lst.append(
            team_id_to_en_name[result_data["home_id"]])
        AwayTeam_lst.append(
            team_id_to_en_name[result_data["away_id"]])
        FTHG_lst.append(int(result_data["home_score"]))
        FTAG_lst.append(int(result_data["away_score"]))
        status_lst.append(int(result_data["status"]))
        gameweek_lst.append(int(result_data["game_week"]))

    df = pd.DataFrame(
        {
            'Date': Date_lst, 'HomeTeam': HomeTeam_lst,
            'AwayTeam': AwayTeam_lst, 'FTHG': FTHG_lst, 'FTAG': FTAG_lst,
            'status': status_lst, 'gameweek': gameweek_lst
        },
        columns=[
            'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'status',
            'gameweek'
        ])

    return df


def get_fixture_data(db_client: MongoClient, qtw_league_id: int,
                     start_time: str, end_time: str) -> pd.DataFrame:
    """get fixture match information by qtw_league_id"""

    qtw_match_id_lst, match_time_lst, home_id_lst, away_id_lst = [], [], [], []

    match_tb = db_client["xscore"]["match_info"]

    result = match_tb.find(
        filter={
            "qtw_league_id": qtw_league_id,
            "match_time": {"$lt": end_time, "$gt": start_time},
            "status": {"$in": [1, 3]}
        },
        projection={
            "qtw_match_id": 1, "home_id": 1, "away_id": 1, "match_time": 1
        }
    )

    columns_lst = ['qtw_match_id', 'match_time', 'home_id', 'away_id']

    if result is not None:

        result_lst = list(result)
        for result_data in result_lst:
            qtw_match_id_lst.append(int(result_data["qtw_match_id"]))
            match_time_lst.append(result_data["match_time"])
            home_id_lst.append(int(result_data["home_id"]))
            away_id_lst.append(int(result_data["away_id"]))

        df = pd.DataFrame(
            {
                'qtw_match_id': qtw_match_id_lst, 'match_time': match_time_lst,
                'home_id': home_id_lst, 'away_id': away_id_lst
            },
            columns=columns_lst)

        return df

    else:

        return pd.DataFrame(columns=columns_lst)


def get_time(start_time, end_time):
    """
    transfer time type
    :param start_time: "20181218"
    :param end_time: "20181220"
    :return: "2018-12-18 00:00:01", "2018-12-20 23:59:59"
    """
    res1, res2 = "00:00:01", "23:59:59"
    start_time, end_time = str(start_time), str(end_time)
    time1 = "-".join([start_time[:4], start_time[4:6], start_time[6:]])
    time2 = "-".join([end_time[:4], end_time[4:6], end_time[6:]])
    return " ".join([time1, res1]), " ".join([time2, res2])


if __name__ == "__main__":
    db_cli = connect_mongodb()

    fixture_data = get_fixture_data(db_cli, 36, "2018-12-29 00:00:00",
                                    "2018-12-31 23:00:00")
    print(fixture_data)
