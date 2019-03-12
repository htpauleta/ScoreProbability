"""
@Project   : ScoreProbability
@Module    : score_predict.py
@Author    : HjwGivenLyy [1752929469@qq.com]
@Created   : 12/18/18 10:22 AM
@Desc      : predict the outcome by score probability calculation model
"""

import logging
import os
import typing

import loguru
import numpy as np
import pandas as pd
import pymc
from pymongo import MongoClient
from scipy.stats import poisson

from base import SUPPORT_LEAGUE_ID_LIST, team_id_en_name_by_league_id
from base import SUPPORT_LEAGUE_ID_NAME, SUPPORT_LEAGUE_NAME_ID
from base import get_fixture_data, get_played_data

logger = loguru.logger


class ScoreProbabilityModel:
    def __init__(self, db_client: MongoClient, data_source='database',
                 league_id=None, league_id2=None, csv=None, csv2=None,
                 lang='en'):
        """
        Initialization parameters
        :param db_client: mongodb client
        :param data_source: 'opta'
        :param league_id: league
        :param league_id2: league2
        :param csv: league match info (Played)
        :param csv2: league2 match info (Played)
        :param lang: "en" or "cn"
        """
        self.db_client = db_client
        self.data_source = data_source
        self.league_id = league_id
        self.league_id2 = league_id2
        self.csv = csv
        self.csv2 = csv2
        self.lang = lang

    def get_data(self):
        """
        get data from database or csv
        """
        if self.data_source == 'database':
            if self.league_id2 is None:
                self.data = get_played_data(self.db_client, self.league_id)
                logger.info('*' * 100)
                logger.info('team_A_name comes from: {0}'.format(
                    self.data.HomeTeam.unique()))
                logger.info('team_B_name comes from: {0}'.format(
                    self.data.AwayTeam.unique()))
                logger.info('*' * 100)
            elif self.league_id2 is not None:
                self.data = get_played_data(self.db_client, self.league_id)
                self.data2 = get_played_data(self.db_client, self.league_id2)
                logger.info('*' * 100)
                logger.info('team_A_name, team_B_name comes from: {0}'.format(
                    self.data.HomeTeam.unique()))
                logger.info('team_A_name, team_B_name comes from: {0}'.format(
                    self.data2.AwayTeam.unique()))
                logger.info('*' * 100)
        elif self.data_source == 'csv':
            if self.csv2 is None:
                self.data = pd.read_csv(self.csv)
                logger.info('*' * 100)
                logger.info('team_A_name comes from: {0}'.format(
                    self.data.HomeTeam.unique()))
                logger.info('team_B_name comes from: {0}'.format(
                    self.data.AwayTeam.unique()))
                logger.info('*' * 100)
            elif self.csv2 is not None:
                self.data = pd.read_csv(self.csv)
                self.data2 = pd.read_csv(self.csv2)
                logger.info('*' * 100)
                logger.info('team_A_name, team_B_name comes from: {0}'.format(
                    self.data.HomeTeam.unique()))
                logger.info('team_A_name, team_B_name comes from: {0}'.format(
                    self.data2.AwayTeam.unique()))
                logger.info('*' * 100)

    @staticmethod
    def build_model(data: pd.DataFrame):
        data['Date'] = [pd.to_datetime(date) for date in data['Date']]
        # setting hyper-parameters: a_i, b_i, c_i, d_i, g, h
        # N = 20 --> number of teams
        teams = sorted(data.HomeTeam.unique())
        n = len(teams)
        ab_hyper, cd_hyper = [(1, 1)] * n, [(1, 1)] * n
        g, h = 1, 1

        # prior for alpha_i, attack
        alpha_1 = pymc.Gamma(name='alpha_1', alpha=ab_hyper[0][0],
                             beta=ab_hyper[0][1], doc=teams[0] + '(attack)')
        alpha = np.empty(n, dtype=object)
        alpha[0] = alpha_1
        for i in range(1, n):
            alpha[i] = pymc.Gamma(
                name='alpha_%i' % (i + 1), alpha=ab_hyper[i][0],
                beta=ab_hyper[i][1], doc=teams[i] + '(attack)')

        # prior for beta_i, defence
        beta_1 = pymc.Gamma(
            name='beta_1', alpha=cd_hyper[0][0], beta=cd_hyper[0][1],
            doc=teams[0] + '(defence)')
        beta = np.empty(n, dtype=object)
        beta[0] = beta_1
        for i in range(1, n):
            beta[i] = pymc.Gamma(
                name='beta_%i' % (i + 1), alpha=cd_hyper[i][0],
                beta=cd_hyper[i][1], doc=teams[i] + '(defence)')

        # prior for lambda_value --> default: exists home advantage
        lambda_value = pymc.Gamma(
            name='lambda_value', alpha=g, beta=h, doc='home advantage')

        """
        alpha_i * beta_j * lambda_value, beta_i * alpha_j, 
        for each match in the dataset
        """
        # home team index
        i_s = [teams.index(t) for t in data.HomeTeam]
        # away team index
        j_s = [teams.index(t) for t in data.AwayTeam]

        # deterministic, determined by alpha_i, alpha_j, beta_i, beta_j,
        # lambda_value
        home_scoring_strength = np.array([alpha[i] for i in i_s]) * \
                                np.array([beta[j] for j in j_s]) * \
                                np.array(lambda_value)

        away_scoring_strength = np.array([beta[i] for i in i_s]) * \
                                np.array([alpha[j] for j in j_s])
        # params = zip(home_scoring_strength, away_scoring_strength)

        # likelihood
        home_score = pymc.Poisson('home_score', home_scoring_strength,
                                  value=data.FTHG, observed=True)
        away_score = pymc.Poisson('away_score', away_scoring_strength,
                                  value=data.FTAG, observed=True)

        t_now = data.Date[data.index[-1]] + pd.Timedelta('1 days 00:00:00')
        t_diff = np.array([item.days for item in (t_now - data.Date)])
        time_weighting = pymc.exp(-t_diff * 0.01)
        likelihood = (home_score * away_score) ** time_weighting

        # wrap the model
        model = pymc.MCMC([likelihood, alpha, beta, lambda_value])

        # run the simulation
        model.sample(iter=5000, burn=100, thin=10, verbose=False,
                     progress_bar=True)

        # estimated_params
        estimated_params = pd.DataFrame({
            'team': teams, 'alpha(attack)': [0.0] * n,
            'beta(defence)': [0.0] * n},
            columns=['team', 'alpha(attack)', 'beta(defence)'])

        for p in alpha:
            estimated_params.loc[
                estimated_params['team'] == p.__doc__.split('(')[0],
                'alpha(attack)'] = round(model.trace(p.__name__)[:].mean(), 2)
        for p in beta:
            estimated_params.loc[
                estimated_params['team'] == p.__doc__.split('(')[0],
                'beta(defence)'] = round(model.trace(p.__name__)[:].mean(), 2)

        estimated_gamma = lambda_value
        logger.info(estimated_params)

        return estimated_params, estimated_gamma

    def get_dir_file(self):
        if self.league_id in SUPPORT_LEAGUE_ID_LIST:
            dir_file = 'output/{0}/'.format(SUPPORT_LEAGUE_ID_NAME.get(
                self.league_id))
            return dir_file
        else:
            return None

    def predict(self, team_a_name: str, team_b_name: str) -> pd.DataFrame:

        estimated_params, estimated_gamma = self.build_model(self.data)

        team_a_attack = estimated_params.loc[
            estimated_params['team'] == team_a_name, 'alpha(attack)'].values[0]
        team_a_defence = estimated_params.loc[
            estimated_params['team'] == team_a_name, 'beta(defence)'].values[0]
        team_b_attack = estimated_params.loc[
            estimated_params['team'] == team_b_name, 'alpha(attack)'].values[0]
        team_b_defence = estimated_params.loc[
            estimated_params['team'] == team_b_name, 'beta(defence)'].values[0]

        home_strength = team_a_attack * team_b_defence * (
            estimated_gamma.value.reshape(1, 1)[0, 0])
        away_strength = team_b_attack * team_a_defence

        logger.info('home_strength = {0}, away_strength = {1}'.format(
            home_strength, away_strength))

        _home_rv, _away_rv = poisson(home_strength), poisson(away_strength)

        all_outcome = tuple(
            [tuple((j, i)) for i in range(11) for j in range(11)])

        goal_limit = range(11)
        mtr = pd.DataFrame(
            dict(zip(goal_limit, [np.zeros(11) for _ in range(11)])))

        for i, j in all_outcome:
            mtr.loc[i, j] = round(_home_rv.pmf(i) * _away_rv.pmf(j), 4)

        # save the score expectation
        dir_file = self.get_dir_file()

        if dir_file is not None:
            if not os.path.exists(dir_file):
                os.mkdir(dir_file)

            save_file = '{0}{1} vs {2}.csv'.format(
                dir_file, team_a_name, team_b_name)
            mtr.to_csv(save_file, index=True)

        return mtr


def total_score_matrix(data: pd.DataFrame) -> typing.Dict[str, float]:
    """
    according to a data frame to get a score matrix
    :param data: data frame
    :return:
    """
    result_dict = {}

    result_list = [(i, j) for i in range(11) for j in range(11)]
    for score_pairs in result_list:
        result_key = str(score_pairs[0]) + ':' + str(score_pairs[1])
        prob_value = round(data.ix[score_pairs[0], score_pairs[1]], 4)
        if prob_value != 0.0:
            result_dict[result_key] = prob_value

    return result_dict


def predict(db_client: MongoClient, qtw_league_id: int, home_name: str,
            away_name: str) -> pd.DataFrame:
    """
    when data_source = 'opta', csv and csv2 do not change
    when data_source = 'csv', csv --> data frame columns:
            Date, HomeTeam, AwayTeam, FTHG, FTAG, status, gameweek
            2016-08-13 11:30:00, Hull City, Leicester City, 2, 1, Played, 1
    """

    data_source, lang = "database", "cn"
    league_id, league_id2 = qtw_league_id, None
    csv, csv2 = None, None

    # run calculate
    model = ScoreProbabilityModel(db_client, data_source, league_id,
                                  league_id2, csv, csv2, lang)
    model.get_data()

    return model.predict(home_name, away_name)


def run_predict(db_client: MongoClient, league_name: str,
                start_time: str, end_time: str):
    """
    produce match score prob
    :return: csv file
    """

    model_tb = db_client['xscore']["model_gauss"]
    league_id = int(SUPPORT_LEAGUE_NAME_ID[league_name])
    team_id_to_en_name = team_id_en_name_by_league_id(
        db_client=db_client, league_id=league_id)

    fixture_data = get_fixture_data(db_client, league_id, start_time, end_time)
    if fixture_data.empty:
        logger.error("There is no match in {0} during {1} and {2} !!!".format(
            league_name, start_time, end_time))
        return None

    total_qtw_match_id = fixture_data.qtw_match_id.unique()

    for match_id in total_qtw_match_id:
        result = model_tb.find_one(filter={'qtw_match_id': int(match_id)},
                                   projection={'qtw_match_id': 1})
        if result is not None:
            logger.info("qtw_match_id = {0} have finished!".format(match_id))
        else:
            try:
                result_dict = dict()
                result_dict['qtw_match_id'] = int(match_id)
                match_data = fixture_data[
                    fixture_data['qtw_match_id'] == match_id]
                match_data = match_data.reset_index(drop=True)
                result_dict['match_time'] = match_data.ix[0, ['match_time']][
                    'match_time']
                result_dict['home_id'] = int(match_data.ix[0, ['home_id']][
                                                 'home_id'])
                result_dict['away_id'] = int(match_data.ix[0, ['away_id']][
                                                 'away_id'])

                # start calculate gauss model
                result = predict(
                    db_client=db_client,
                    qtw_league_id=league_id,
                    home_name=team_id_to_en_name[result_dict["home_id"]],
                    away_name=team_id_to_en_name[result_dict['away_id']])

                score_dict = total_score_matrix(result)
                result_dict["league_id"] = league_id
                result_dict["score"] = score_dict

                # save date to mongodb
                logging.info("result_dict = {0}".format(result_dict))
                model_tb.insert_one(result_dict)

            except Exception as e:
                logger.exception(e)
                logger.error("qtw_match_id = {0} failure".format(match_id))
                continue


if __name__ == "__main__":
    from base import connect_mongodb

    db_client1 = connect_mongodb()

    run_predict(
        db_client=db_client1, league_name="yc",
        start_time="2018-12-29 00:00:01", end_time="2018-12-31 23:00:01")
