"""
@Project   : ScoreProbability
@Module    : crawl.py
@Author    : HjwGivenLyy [1752929469@qq.com]
@Created   : 12/17/18 4:54 PM
@Desc      : crawl data from qtw net
"""

import datetime
import re

import loguru
import requests
import yaml
from bs4 import BeautifulSoup
from pymongo import MongoClient

from base import SUPPORT_LEAGUE_NAME_ID, SERVER_FILE_PATH

# qtw company bet
QTW_COMPANY_BET = [8, 23, 24, 31]

YP_URL = "http://vip.win007.com/changeDetail/handicap.aspx?" \
         "id={qtw_match_id}&companyID={company_id}&l=0"

DXQ_URL = "http://vip.win007.com/changeDetail/overunder.aspx?" \
          "id={qtw_match_id}&companyID={company_id}&l=0"

LEAGUE_URL = "http://zq.win007.com/cn/SubLeague/{league_id}.html"

SEASON_URL = "http://zq.win007.com/jsData/matchResult/2018-2019/" \
             "s{id}.js?version={value}"

SPECIAL_SEASON_URL = {
    'fy': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s12_1778.js?version={value}",
    'dy': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s9_132.js?version={value}",
    'yg': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s37_87.js?version={value}",
    'bj': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s5_114.js?version={value}",
    'pc': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s23_1123.js?version={value}",
    'ac': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s273_462.js?version={value}",
    'xy': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s33_546.js?version={value}",
    'yy': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s40_261.js?version={value}",
    'hj': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s16_98.js?version={value}",
    'hy': "http://zq.win007.com/jsData/matchResult/2018-2019/"
          "s17_94.js?version={value}",
}

logger = loguru.logger


def get_latest_odds_by_qtw_match_id(qtw_match_id: int, odd_type: str):
    """
    according to qtw match id get latest odds
    :param qtw_match_id:
    :param odd_type: "yp" or "dxq"
    :return:
    """

    def get_latest_odd_by_company_id(match_id: int, company_id: int, odd: str):
        """get certain bet company odd"""

        if odd == "yp":
            url = YP_URL.format(qtw_match_id=match_id, company_id=company_id)
        elif odd == "dxq":
            url = DXQ_URL.format(qtw_match_id=match_id, company_id=company_id)
        else:
            return "odds type must be in ['yp', 'dxq']"

        headers1 = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; '
                          'rv:2.0.1) Gecko/20100101 Firefox/4.0.1'
        }

        try:
            content = requests.get(url, headers=headers1)
            page = BeautifulSoup(content.content, "lxml")
            odds2_lst = page.find_all(id="odds2")
            td_lst = odds2_lst[0].find_all("tr")[1].find_all("td")

            if td_lst[-1].text.encode("utf-8") != "滚":
                return td_lst[3].text
            else:
                return "match have begin!"

        except Exception as e:
            logger.exception(e)
            logger.error(
                "get qtw_match_id = {0} odds failure".format(qtw_match_id))
            return None

    for company_id_value in QTW_COMPANY_BET:
        rtn = get_latest_odd_by_company_id(
            match_id=qtw_match_id, company_id=company_id_value, odd=odd_type)
        if rtn is not None:
            return rtn
    else:
        return None


def save_match_info_to_mongodb(db_client: MongoClient, league_name: str):
    """
    save qtw match information data to mongodb
    :param db_client: mongodb client
    :param league_name: league name --> "yc", "dj", "fj", "xj", "yj"
    :return: run insert into
    """

    tb = db_client["xscore"]["match_info"]
    page_text = spare_url(league_name)

    pattern = re.compile('jh.* = .*]')
    match_lst = re.findall(pattern, page_text)

    game_week = 0
    for match_str in match_lst:
        game_week += 1
        match_info_str = match_str.replace(",,,", ",'','',")
        match_info_lst = eval(str(match_info_str).split(" = ")[1])
        for match_information in match_info_lst:
            qtw_match_id = int(match_information[0])
            result = tb.find_one(
                filter={"qtw_match_id": qtw_match_id},
                projection={'status': 1}
            )
            if result is not None and result["status"] == 2:
                continue
            elif result is not None and result["status"] != 2:
                if match_information[2] == -1:
                    new_status = 2
                    new_status_text = "Played"
                    score_lst = match_information[6].split("-")
                    tb.update(
                        {"qtw_match_id": qtw_match_id},
                        {
                            "$set": {
                                "match_time": match_information[3],
                                "status": new_status,
                                "status_text": new_status_text,
                                "home_score": int(score_lst[0]),
                                "away_score": int(score_lst[1])
                            }
                        }
                    )
                elif match_information[2] == -14:
                    new_status = 3
                    new_status_text = "Delay"
                    tb.update(
                        {"qtw_match_id": qtw_match_id},
                        {
                            "$set": {
                                "match_time": match_information[3],
                                "status": new_status,
                                "status_text": new_status_text
                            }
                        }
                    )
                else:
                    new_status = 3
                    new_status_text = "Fixture"
                    tb.update(
                        {"qtw_match_id": qtw_match_id},
                        {
                            "$set": {
                                "match_time": match_information[3],
                                "status": new_status,
                                "status_text": new_status_text
                            }
                        }
                    )
                logger.info(
                    "qtw_match_id = {0} have finished update!".format(
                        qtw_match_id))
            else:
                result_dict = dict()
                result_dict["qtw_match_id"] = qtw_match_id
                result_dict["qtw_league_id"] = int(match_information[1])
                result_dict["match_time"] = match_information[3]
                result_dict["home_id"] = int(match_information[4])
                result_dict["away_id"] = int(match_information[5])
                result_dict["game_week"] = int(game_week)
                if match_information[2] == -1:
                    score_lst = match_information[6].split("-")
                    result_dict["home_score"] = int(score_lst[0])
                    result_dict["away_score"] = int(score_lst[1])
                    result_dict["status"] = 2
                    result_dict["status_text"] = "Played"
                elif match_information[2] == -14:
                    result_dict["home_score"] = -1
                    result_dict["away_score"] = -1
                    result_dict["status"] = 3
                    result_dict["status_text"] = "Delay"
                else:
                    result_dict["home_score"] = -1
                    result_dict["away_score"] = -1
                    result_dict["status"] = 1
                    result_dict["status_text"] = "Fixture"
                logger.info("result_dict = {0}".format(result_dict))
                tb.insert_one(result_dict)


def save_team_info_to_mongodb(db_client: MongoClient, league_name: str):
    """
    save qtw team information data to mongodb
    :param db_client: mongodb client
    :param league_name: league name --> "yc", "dj", "fj", "xj", "yj"
    :return: run insert into
    """

    tb = db_client["xscore"]["team_info"]
    league_id = int(SUPPORT_LEAGUE_NAME_ID[league_name])
    page_text = spare_url(league_name)

    pattern = re.compile("arrTeam = .*]")
    match_str = re.search(pattern, page_text)
    match_group = match_str.group()
    team_information_lst = eval(str(match_group).split(" = ")[1])

    for team_information in team_information_lst:
        team_id = int(team_information[0])
        result = tb.find_one(
            filter={
                "league_id": league_id,
                "team_id": team_id
            },
            projection={'league_id': 1}
        )
        if result is not None:
            logger.info("team_id = {0} exist in mongodb".format(
                team_information[0]))
        else:
            result_dict = dict()

            result_dict["league_id"] = league_id
            result_dict["league_name"] = league_name
            result_dict['team_id'] = team_id
            result_dict["team_cn_name"] = team_information[1]
            result_dict["team_en_name"] = team_information[3]

            logger.info("result_dict = ", result_dict)
            tb.insert_one(result_dict)


def save_league_info_to_mongodb(db_client: MongoClient):
    """save qtw league information data to mongodb"""

    result_lst = []
    tb = db_client["xscore"]["league_info"]

    for key, value in SUPPORT_LEAGUE_NAME_ID.items():
        result = tb.find_one(
            filter={"league_id": int(value)},
            projection={'league_id': 1}
        )
        if result is not None:
            logger.info("league_id = {0} exist in mongodb".format(value))
        else:
            result_dct = dict()
            result_dct["league_name"] = key
            result_dct["league_id"] = int(value)
            result_lst.append(result_dct)

    if result_lst:
        tb.insert_many(result_lst)
    else:
        logger.info("no league info need update !!!")


def spare_url(league_name: str):
    """spare url"""

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    version_value = now.split(":")[0].replace("-", "").replace(" ", "")
    league_id = SUPPORT_LEAGUE_NAME_ID[league_name]

    headers1 = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) '
                      'Gecko/20100101 Firefox/4.0.1'
    }

    headers2 = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) '
                      'Gecko/20100101 Firefox/4.0.1',
        'Host': 'zq.win007.com',
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'Referer': 'http://zq.win007.com/cn/SubLeague/37.html',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9'
    }

    session = requests.Session()

    league_home_page_url = LEAGUE_URL.format(
        league_id=SUPPORT_LEAGUE_NAME_ID[league_name]
    )
    session.get(league_home_page_url, headers=headers1)

    if league_name in ['yc', 'dj', 'fj', 'xj', 'yj', 'sc']:
        season_home_page_url = SEASON_URL.format(
            id=league_id, value=version_value)
    else:
        season_home_page_url = SPECIAL_SEASON_URL[league_name].format(
            value=version_value)

    chi = session.get(season_home_page_url, headers=headers2)
    page = BeautifulSoup(chi.text, "lxml")
    page_text = page.p.text

    return page_text


# Another way of code implementation

class CrawlDataBase:

    def __init__(self, league_name: str):
        """
        Initialization parameters
        :param league_name: league name --> "yc", "dj", "fj", "xj", "yj"
        """
        self._db_client = self._get_db_client()
        self._league_name = league_name

    def __del__(self):
        self._db_client.close()

    @staticmethod
    def _get_db_client():
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

    def spare_url(self):
        """spare url"""

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        version_value = now.split(":")[0].replace("-", "").replace(" ", "")
        league_id = SUPPORT_LEAGUE_NAME_ID[self._league_name]

        headers1 = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; '
                          'rv:2.0.1) Gecko/20100101 Firefox/4.0.1'
        }

        headers2 = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; '
                          'rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
            'Host': 'zq.win007.com',
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'Referer': 'http://zq.win007.com/cn/SubLeague/37.html',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }

        session = requests.Session()

        league_home_page_url = LEAGUE_URL.format(
            league_id=SUPPORT_LEAGUE_NAME_ID[self._league_name]
        )
        session.get(league_home_page_url, headers=headers1)

        if self._league_name in ['yc', 'dj', 'fj', 'xj', 'yj', 'sc']:
            season_home_page_url = SEASON_URL.format(
                id=league_id, value=version_value)
        else:
            season_home_page_url = SPECIAL_SEASON_URL[
                self._league_name].format(
                value=version_value)

        chi = session.get(season_home_page_url, headers=headers2)
        page = BeautifulSoup(chi.text, "lxml")
        page_text = page.p.text

        return page_text

    def save_info_to_mongodb(self):
        raise NotImplementedError


class LeagueInfo(CrawlDataBase):

    def save_info_to_mongodb(self):
        """save qtw league information data to mongodb"""

        result_lst = []
        tb = self._db_client["xscore"]["league_info"]

        for key, value in SUPPORT_LEAGUE_NAME_ID.items():
            result_dct = dict()
            result_dct["league_name"] = key
            result_dct["league_id"] = int(value)
            result_lst.append(result_dct)

        tb.insert_many(result_lst)


class MatchInfo(CrawlDataBase):

    def save_info_to_mongodb(self):
        """save qtw match information data to mongodb"""

        tb = self._db_client["xscore"]["match_info"]
        page_text = self.spare_url()

        pattern = re.compile('jh.* = .*]')
        match_lst = re.findall(pattern, page_text)

        game_week = 0
        for match_str in match_lst:
            game_week += 1
            match_info_str = match_str.replace(",,,", ",'','',")
            match_info_lst = eval(str(match_info_str).split(" = ")[1])
            for match_information in match_info_lst:
                qtw_match_id = int(match_information[0])
                result = tb.find_one(
                    filter={"qtw_match_id": qtw_match_id},
                    projection={'status': 1}
                )
                if result is not None and result["status"] == 2:
                    continue
                elif result is not None and result["status"] != 2:
                    if match_information[2] == -1:
                        new_status = 2
                        new_status_text = "Played"
                        score_lst = match_information[6].split("-")
                        tb.update(
                            {"qtw_match_id": qtw_match_id},
                            {
                                "$set": {
                                    "match_time": match_information[3],
                                    "status": new_status,
                                    "status_text": new_status_text,
                                    "home_score": int(score_lst[0]),
                                    "away_score": int(score_lst[1])
                                }
                            }
                        )
                    elif match_information[2] == -14:
                        new_status = 3
                        new_status_text = "Delay"
                        tb.update(
                            {"qtw_match_id": qtw_match_id},
                            {
                                "$set": {
                                    "match_time": match_information[3],
                                    "status": new_status,
                                    "status_text": new_status_text
                                }
                            }
                        )
                    else:
                        new_status = 3
                        new_status_text = "Fixture"
                        tb.update(
                            {"qtw_match_id": qtw_match_id},
                            {
                                "$set": {
                                    "match_time": match_information[3],
                                    "status": new_status,
                                    "status_text": new_status_text
                                }
                            }
                        )
                    logger.info(
                        "qtw_match_id = {0} have finished update!".format(
                            qtw_match_id))
                else:
                    result_dict = dict()
                    result_dict["qtw_match_id"] = qtw_match_id
                    result_dict["qtw_league_id"] = int(match_information[1])
                    result_dict["match_time"] = match_information[3]
                    result_dict["home_id"] = int(match_information[4])
                    result_dict["away_id"] = int(match_information[5])
                    result_dict["game_week"] = int(game_week)
                    if match_information[2] == -1:
                        score_lst = match_information[6].split("-")
                        result_dict["home_score"] = int(score_lst[0])
                        result_dict["away_score"] = int(score_lst[1])
                        result_dict["status"] = 2
                        result_dict["status_text"] = "Played"
                    elif match_information[2] == -14:
                        result_dict["home_score"] = -1
                        result_dict["away_score"] = -1
                        result_dict["status"] = 3
                        result_dict["status_text"] = "Delay"
                    else:
                        result_dict["home_score"] = -1
                        result_dict["away_score"] = -1
                        result_dict["status"] = 1
                        result_dict["status_text"] = "Fixture"
                    logger.info("result_dict = {0}".format(result_dict))
                    tb.insert_one(result_dict)


class TeamInfo(CrawlDataBase):

    def save_info_to_mongodb(self):
        """save qtw team information data to mongodb"""

        tb = self._db_client["xscore"]["team_info"]
        league_id = int(SUPPORT_LEAGUE_NAME_ID[self._league_name])
        page_text = self.spare_url()

        pattern = re.compile("arrTeam = .*]")
        match_str = re.search(pattern, page_text)
        match_group = match_str.group()
        team_information_lst = eval(str(match_group).split(" = ")[1])

        for team_information in team_information_lst:
            result = tb.find_one(
                filter={
                    "league_id": league_id,
                    "team_id": int(team_information[0])
                },
                projection={'league_id': 1}
            )
            if result is not None:
                logger.info("team_id = {0} exist in mongodb".format(
                    team_information[0]))
            else:
                result_dict = dict()

                result_dict["league_id"] = league_id
                result_dict["league_name"] = self._league_name
                result_dict['team_id'] = int(team_information[0])
                result_dict["team_cn_name"] = team_information[1]
                result_dict["team_en_name"] = team_information[3]

                print("result_dict = ", result_dict)
                tb.insert_one(result_dict)


if __name__ == "__main__":

    yp_odd = get_latest_odds_by_qtw_match_id(1585238, "yp")
    print(yp_odd)
