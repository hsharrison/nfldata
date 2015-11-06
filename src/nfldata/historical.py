from toolz import curry
import numpy as np
import pandas as pd

offense_team_stat_columns = [
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
    'kicking_xpa',
    'kicking_xpmade',
    'kicking_fga',
    'kicking_fgmissed',
    'kicking_fgm',
]
defense_team_stat_columns = [
    'defense_int',
    'defense_int_tds',
    'defense_frec',
    'defense_frec_tds',
    'defense_misc_tds',
    'defense_puntblk',
    'defense_fgblk',
    'defense_safe',
    'defense_sk',
]
special_team_stat_columns = [
    'kickret_tds',
    'puntret_tds',
]


def player_stats_by_game(connection, include_preseason=False):
    sum_columns = [
        'fumbles_lost',
        'kicking_fga',
        'kicking_fgm',
        'kicking_xpa',
        'kicking_xpmade',
        'kickret_tds',
        'passing_att',
        'passing_cmp',
        'passing_incmp',
        'passing_int',
        'passing_sk',
        'passing_tds',
        'passing_twoptm',
        'passing_yds',
        'puntret_tds',
        'receiving_rec',
        'receiving_tar',
        'receiving_tds',
        'receiving_twoptm',
        'receiving_yds',
        'rushing_att',
        'rushing_tds',
        'rushing_twoptm',
        'rushing_yds',
    ]
    positions = [
        'FB',
        'K',
        'QB',
        'RB',
        'TE',
        'WR',
        'UNK',
    ]
    query = """
      SELECT player_id, position, team, gsis_id, {}
      FROM play_player
      INNER JOIN player USING(team, player_id)
      {}
      WHERE position IN %(positions)s
      {}
      GROUP BY player_id, position, team, gsis_id
    """.format(
        ', '.join(_sum_query(col) for col in sum_columns),
        '' if include_preseason else 'INNER JOIN game USING(gsis_id)',
        '' if include_preseason else "AND season_type != 'Preseason'",
    )
    return pd.read_sql_query(
        query, connection,
        params=dict(positions=tuple(positions)),
        index_col=['gsis_id', 'player_id'],
    ).sort_index()


def team_stats_by_drive(connection, include_preseason=False):
    sum_columns_sql = ', '.join(_sum_query(col) for col in offense_team_stat_columns)
    return pd.read_sql_query(
        """SELECT gsis_id, pos_team AS team, drive_id, result, {}
            FROM drive
            INNER JOIN agg_play USING(gsis_id, drive_id)
            {}
            GROUP BY gsis_id, pos_team, drive_id, result
        """.format(
            sum_columns_sql,
            '' if include_preseason else """
            INNER JOIN game USING(gsis_id)
            WHERE season_type != 'Preseason'
            """
        ),
        connection,
        index_col=['gsis_id', 'team', 'drive_id'],
    ).sort_index()


def team_stats_by_game(connection, include_preseason=False):
    team_stat_columns = offense_team_stat_columns + defense_team_stat_columns + special_team_stat_columns
    sum_columns_sql = ', '.join(_sum_query(column) for column in team_stat_columns)
    team_sums = pd.read_sql_query(
        """SELECT gsis_id, team, {}
            FROM play_player
            GROUP BY gsis_id, team
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


def _sum_query(col):
    return 'sum({0}) AS {0}'.format(col)


@curry
def _sum_cols(df, cols, drop=False):
    series = df[cols].sum(axis=1)
    if drop:
        df.drop(cols, axis=1, inplace=True)
    return series
