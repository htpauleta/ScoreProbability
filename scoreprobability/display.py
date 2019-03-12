"""
@Project   : ScoreProbability
@Module    : display.py
@Author    : HjwGivenLyy [1752929469@qq.com]
@Created   : 12/17/18 4:54 PM
@Desc      : display the outcome of score probability calculation model
"""

import typing

import loguru
from pymongo import MongoClient

from base import SUPPORT_LEAGUE_NAME_ID, get_fixture_data
from base import team_id_cn_name_by_league_id
from crawl import get_latest_odds_by_qtw_match_id

O3_DICT = {
    '平手': 0, '平手/半球': -0.25, '半球': -0.5, '半球/一球': -0.75,
    '一球': -1, '一球/球半': -1.25, '球半': -1.5, '球半/两球': -1.75,
    '两球': -2, '两球/两球半': -2.25, '两球半': -2.5, '两球半/三球': -2.75,
    '三球': -3, '三球/三球半': -3.25, '三球半': -3.5, '三球半/四球': -3.75,
    '四球': -4, '受让平手/半球': 0.25, '受让半球': 0.5, '受让半球/一球': 0.75,
    '受让一球': 1, '受让一球/球半': 1.25, '受让球半': 1.5, '受让球半/两球': 1.75,
    '受让两球': 2, '受让两球/两球半': 2.25, '受让两球半': 2.5, '受让两球半/三球': 2.75,
    '受让三球': 3, '受让三球/三球半': 3.25, '受让三球半': 3.5, '受让三球半/四球': 3.75,
}

logger = loguru.logger


def get_gauss_model_result_from_mongodb(
        db_client: MongoClient, qtw_match_id: int) -> typing.Dict:
    """according the qtw_match_id to query score prob"""
    model_tb = db_client["xscore"]["model_gauss"]
    result = model_tb.find_one(
        filter={"qtw_match_id": qtw_match_id},
        projection={"score": 1, "home_id": 1, "away_id": 1}
    )
    return result


def get_score_pairs(odd_value: float) -> typing.Dict[str, list]:
    """
    according odd value to get the number of goals scored by the home and away
    team in the following situation, such as win, draw, lose
    :param odd_value:
        str = '-0.5' express 'half goal', that is home team let the half goal
    :return: dict
        result = {
                    'home_win': ["1:0", "2:0", "2:1", ...],
                    'draw': [],
                    'away_win': ["0:0", "1:1", "1:2", ...]
                 }
    """

    result = {}
    home_win, draw, away_win = [], [], []

    # i -> the number of goals scored by the home team
    # j -> the number of goals scored by the away team
    for i in range(11):
        for j in range(10):
            score_pair = ":".join([str(i), str(j)])
            if float(i) - float(j) + float(odd_value) > 0:
                home_win.append(score_pair)
            elif float(i) - float(j) + float(odd_value) == 0:
                draw.append(score_pair)
            else:
                away_win.append(score_pair)

    result['home_win'] = home_win
    result['draw'] = draw
    result['away_win'] = away_win

    return result


def get_one_prob(score_data: dict, score_pair_result: dict,
                 type_value: str) -> float:
    """
    according type value to get the prob
    :param score_data: score pairs prob dict
    :param score_pair_result: score pairs dict
    :param type_value: ['home_win', 'draw', 'away_win']
    :return:
    """
    prob = 0
    type_list = score_pair_result.get(type_value, None)
    if type_list is not None:
        for score_pair in type_list:
            prob += score_data.get(score_pair, 0.0)
    prob_value = round(prob, 2)
    return prob_value


def get_310_prob(score_prob_dct: dict) -> typing.Dict[str, float]:
    """get home win, draw, away win prob"""

    prob = {}

    result_dct = get_score_pairs(0)

    type_dict = ['home_win', 'draw', 'away_win']
    for i in type_dict:
        prob[i] = get_one_prob(score_prob_dct, result_dct, i)
    sum_value = float(sum(prob.values()))
    if sum_value != 1:
        avg_value = round((1 - sum_value) / 3, 2)
        prob['home_win'] += avg_value
        prob['draw'] += avg_value
        prob['away_win'] += avg_value

    return prob


def get_handicap_prob(qtw_match_id: int, score_prob_dct: dict):
    """get handicap prob by accordingly odd"""

    prob = {}

    odd_str = get_latest_odds_by_qtw_match_id(qtw_match_id=qtw_match_id,
                                              odd_type="yp")

    if odd_str is not None:
        odd_num = O3_DICT[odd_str]
        result_dct = get_score_pairs(odd_num)

        type_dict = ['home_win', 'draw', 'away_win']
        for i in type_dict:
            prob[i] = get_one_prob(score_prob_dct, result_dct, i)
        sum_value = float(sum(prob.values()))
        if sum_value != 1:
            avg_value = round((1 - sum_value) / 3, 2)
            prob['home_win'] += avg_value
            prob['draw'] += avg_value
            prob['away_win'] += avg_value
    else:
        return None, None

    return odd_str, prob


def get_dxq_score_pairs(odds: float) -> typing.Dict[str, list]:
    """
    according odd value to get the number of goals scored by the home and away
    team in the following situation, such as big, draw, small
    :param odds: 2.5
    :return: dict
             result = {'home_win': [(1, 0), (2, 0), ...],
                       'draw': [],
                       'away_win': []}
    """
    result = {}

    big_lst, draw_lst, small_lst = [], [], []
    for i in range(11):
        for j in range(11):
            score_pair = ":".join([str(i), str(j)])
            if float(i) + float(j) - float(odds) > 0:
                big_lst.append(score_pair)
            elif float(i) + float(j) - float(odds) == 0:
                draw_lst.append(score_pair)
            else:
                small_lst.append(score_pair)

    result['big'] = big_lst
    result['draw'] = draw_lst
    result['small'] = small_lst

    return result


def get_dxq_prob(qtw_match_id: int, score_prob_dct: dict):
    """get dxq prob by accordingly odd"""

    prob = {}

    odd_str = get_latest_odds_by_qtw_match_id(qtw_match_id=qtw_match_id,
                                              odd_type="dxq")
    if odd_str is not None:
        if "/" in odd_str:
            odd_lst = odd_str.split("/")
            odd_num = (float(odd_lst[0]) + float(odd_lst[1])) / 2
        else:
            odd_num = float(odd_str)
        result_dct = get_dxq_score_pairs(odd_num)

        type_dict = ['big', 'draw', 'small']
        for i in type_dict:
            prob[i] = get_one_prob(score_prob_dct, result_dct, i)
        sum_value = float(sum(prob.values()))
        if sum_value != 1:
            avg_value = round((1 - sum_value) / 3, 2)
            prob['big'] += avg_value
            prob['draw'] += avg_value
            prob['small'] += avg_value
    else:
        return None, None

    return odd_str, prob


def league_display(db_client: MongoClient, league_name: str, start_time: str,
                   end_time: str):
    """display league prob"""

    league_id = int(SUPPORT_LEAGUE_NAME_ID[league_name])
    team_id_to_cn_name = team_id_cn_name_by_league_id(
        db_client=db_client, league_id=league_id)
    fixture_data = get_fixture_data(db_client, league_id, start_time, end_time)

    if fixture_data.empty:
        return None

    total_qtw_match_id = fixture_data.qtw_match_id.unique()
    print("*************************************************")
    print("*********         {0}         *********".format(league_name))
    print("*************************************************")

    for match_id in total_qtw_match_id:
        logger.info("Start display qtw_match_id = {0}".format(match_id))

        gauss_result = get_gauss_model_result_from_mongodb(
            db_client=db_client, qtw_match_id=int(match_id))

        score_prob_dct = gauss_result["score"]
        home_id, away_id = gauss_result["home_id"], gauss_result["away_id"]
        home_away = '{home_team}  VS  {away_team}    '.format(
            home_team=team_id_to_cn_name[home_id],
            away_team=team_id_to_cn_name[away_id])

        # display match time
        match_time = fixture_data.loc[fixture_data['qtw_match_id'] == match_id,
                                      ['match_time']].values[0][0]
        print('{home_away}{match_time}'.format(
            home_away=home_away, match_time=match_time))

        # 310 result
        prob_310 = get_310_prob(score_prob_dct)
        print('{home_away}{odd}    {prob}'.format(
            home_away=home_away, odd="310", prob=prob_310))

        # handicap result
        yp_odd, prob_handicap = get_handicap_prob(match_id, score_prob_dct)
        if yp_odd is not None:
            print('{home_away}{odd}    {prob}'.format(
                home_away=home_away, odd=yp_odd,
                prob=prob_handicap))
        else:
            print("get qtw_match_id = {0} yp odds failure".format(match_id))

        # dxq result
        dxq_odd, prob_dxq = get_dxq_prob(match_id, score_prob_dct)
        if dxq_odd is not None:
            print('{home_away}{odd}    {prob}'.format(
                home_away=home_away, odd=dxq_odd, prob=prob_dxq))

        else:
            print("get qtw_match_id = {0} dxq odds failure".format(match_id))

        print("**********************************************")


if __name__ == "__main__":
    from base import connect_mongodb

    db_client1 = connect_mongodb()

    league_display(
        db_client=db_client1, league_name="pc",
        start_time="2019-01-10 00:00:01", end_time="2019-01-15 00:00:01")
