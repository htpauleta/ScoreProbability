"""
@Project   : ScoreProbability
@Module    : run_display.py
@Author    : HjwGivenLyy [1752929469@qq.com]
@Created   : 12/19/18 11:40 AM
@Desc      : project command line run entry
"""

import logging

import fire
import loguru
import warnings

from base import SUPPORT_LEAGUE_NAME_ID, get_time, connect_mongodb
from crawl import save_match_info_to_mongodb, save_team_info_to_mongodb
from display import league_display
from score_predict import run_predict

logger = loguru.logger
warnings.filterwarnings('ignore')


def main(league_name: str, start_time: str, end_time: str):
    """
    predict the prob of the number of goals scored by the home and away team by
    gauss model
    :param league_name:
        "all": express yc, dj, yj, fj, xj, yg, fy, dy
        for example: "dj", "yc", "xj", "yj", "fj"
    :param start_time: 20181218
    :param end_time: 20181220
    :return: 310, dxq, yp over and under odd result
    """

    db_client = connect_mongodb()
    start_time, end_time = get_time(start_time, end_time)

    if league_name == "all":
        league_name_lst = list(SUPPORT_LEAGUE_NAME_ID.keys())
    else:
        league_name_lst = [league_name]

    for name in league_name_lst:
        # first step
        # update team information
        logger.info(
            "Now start update {0} team info !!!".format(name))
        save_team_info_to_mongodb(db_client=db_client, league_name=name)
        logger.info(
            "{0} team info have update finished !!!".format(name))

        # second step
        # update match information
        logger.info(
            "Now start update {0} match info !!!".format(name))
        save_match_info_to_mongodb(db_client=db_client, league_name=name)
        logger.info(
            "{0} match info have update finished !!!".format(name))

        # third step
        # start model predict
        logger.info("Now start gauss model predict !!!")
        run_predict(db_client=db_client, league_name=name,
                    start_time=start_time, end_time=end_time)
        logger.info("Model predict have finished !!!")

        # fourth step
        # display model predict result
        logger.info("Now start model result display !!!")
        league_display(db_client=db_client, league_name=name,
                       start_time=start_time, end_time=end_time)
        logger.info("Model result display have finished !!!")

        # fifth step
        db_client.close()
        logger.info("Mongodb client have closed !!!")


if __name__ == "__main__":
    logging.basicConfig(
        filename="gauss_predict.log",
        format="%(asctime)-15s %(name)-5s %(message)s",
        level=logging.INFO)

    # python3 gauss_run_display.py --league_name dj
    # --start_time 20181218 --end_time 20181220 > gauss_predict_dj.txt
    fire.Fire(main)
