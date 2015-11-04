from toolz import curry
import numpy as np
import pandas as pd


def team_stats(connection, include_preseason=False):
    sum_columns = [
        'rushing_att',
        'rushing_yds',
        'rushing_tds',
        'rushing_twoptm',
        'fumbles_lost',
        'passing_att',
        'passing_cmp',
        'passing_incmp',
        'passing_int',
        'passing_sk',
        'passing_yds',
        'passing_tds',
        'passing_twoptm',
        'defense_int',
        'defense_int_tds',
        'defense_frec',
        'defense_frec_tds',
        'defense_misc_tds',
        'defense_puntblk',
        'defense_fgblk',
        'kicking_xpa',
        'kicking_xpmade',
        'kicking_fga',
        'kicking_fgmissed',
        'kicking_fgm',
        'kickret_tds',
        'puntret_tds',
        'defense_safe',
        'defense_sk',
    ]
    sum_columns_sql = ', '.join('sum({0}) AS {0}'.format(column) for column in sum_columns)
    team_sums = pd.read_sql_query(
        """SELECT play_player.gsis_id, team, {}
            FROM play_player
            GROUP BY play_player.gsis_id, team
        """.format(sum_columns_sql),
        connection,
        index_col=['gsis_id', 'team'],
    ).sort_index()

    sum = _sum_cols(team_sums)
    team_sums['passing_plays'] = sum(['passing_att', 'passing_sk'])
    team_sums['offense_plays'] = sum(['passing_plays', 'rushing_att'])

    team_sums['defense_blk'] = sum(['defense_puntblk', 'defense_fgblk'], drop=True)
    team_sums['defense_ret_tds'] = sum(['kickret_tds', 'puntret_tds'], drop=True)
    team_sums['defense_tds'] = sum(['defense_misc_tds', 'defense_frec_tds', 'defense_int_tds'], drop=True)

    games = pd.melt(
        pd.read_sql_table(
            'game', connection,
            columns=['gsis_id', 'start_time', 'week', 'season_year', 'season_type', 'home_team', 'away_team'],
        ),
        id_vars=['gsis_id', 'start_time', 'season_type', 'season_year', 'week'],
        value_vars=['home_team', 'away_team'],
        value_name='team',
        var_name='home',
    )
    games['home'] = games['home'] == 'home_team'
    games = games.set_index(['gsis_id', 'team']).sort_index()

    game_data = (pd.concat([games, team_sums], axis=1)
                 .reset_index()
                 .set_index(['gsis_id', 'home'])
                 .sort_index()
                 )

    if not include_preseason:
        game_data.drop(game_data.index[game_data['season_type'] == 'Preseason'], axis=0, inplace=True)

    game_data['offense_pts'] = (
        game_data[
            ['passing_tds', 'rushing_tds', 'passing_twoptm', 'rushing_twoptm', 'kicking_fgm', 'kicking_xpmade']
        ] @ np.array([6, 6, 2, 2, 3, 1])
    )
    game_data['defense_ptsa'] = (
        game_data['offense_pts']
        .sortlevel('home', ascending=False)
        .sortlevel('gsis_id', sort_remaining=False)
        .values
    )

    return game_data


@curry
def _sum_cols(df, cols, drop=False):
    series = df[cols].sum(axis=1)
    if drop:
        df.drop(cols, axis=1, inplace=True)
    return series
