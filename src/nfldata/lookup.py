from pkg_resources import resource_stream
from toolz import memoize
import yaml
import pandas as pd


@memoize
def gsis_id(connection, season_year, week, team, season_type='Regular', lookup_home=False, lookup_opp=False):
    query = """
        SELECT gsis_id{}{}
        FROM game
        WHERE season_type = %(season_type)s
          AND season_year = %(season_year)s
          AND week = %(week)s
          AND (home_team = %(team)s OR away_team = %(team)s)
    """.format(
        ', home_team = %(team)s AS home' if lookup_home else '',
        ', CASE WHEN home_team = %(team)s THEN away_team ELSE home_team END AS opp' if lookup_opp else '',
    )
    result = pd.read_sql_query(query, connection, params=dict(
        season_type=season_type,
        season_year=season_year,
        week=week,
        team=team,
    ))

    game_str = '{} in {} week {} ({})'.format(team, season_year, week, season_type)
    if result.shape[0] > 1:
        raise ValueError('Found more than one game for {}'.format(game_str))
    if not result.shape:
        raise ValueError('Could not find game for {}'.format(game_str))

    return result.iloc[0, :]


@memoize
def player_id(connection, name, pos, team=None):
    hardcoded_player_ids = _get_hardcoded_player_ids()
    if (name, pos) in hardcoded_player_ids:
        return hardcoded_player_ids[name, pos]

    if pos == 'RB':
        position_where = "(position = %(pos)s OR position = 'FB')"
    else:
        position_where = 'position = %(pos)s'

    result = pd.read_sql_query(
        """SELECT player_id
            FROM player
            WHERE full_name = %(name)s
                AND {}
        """.format(position_where), connection, params=dict(
            name=name,
            pos=pos,
        ),
    )

    player_str = '{} ({}{})'.format(name, pos, '-' + team if team else '')
    if not result.shape[0]:
        unk_pos_result = pd.read_sql_query(
            """SELECT player_id
                FROM player
                WHERE full_name = %(name)s
                    AND position = 'UNK'
            """, connection, params=dict(name=name))
        if unk_pos_result.shape[0] == 1:
            return unk_pos_result.iloc[0, 0]

        fuzzy_results = pd.read_sql_query(
            """SELECT player_id, levenshtein(%(name)s, full_name), full_name, position, team
                FROM player
                WHERE (levenshtein(%(name)s, full_name) < 7 AND {})
                    OR levenshtein(%(name)s, full_name) < 3
                ORDER BY levenshtein
            """.format(position_where), connection, params=dict(
                name=name,
                pos=pos,
            ), index_col='player_id',
        )

        if not fuzzy_results.shape[0]:
            fuzzy_results = pd.read_sql_query(
                """SELECT player_id, levenshtein(%(name)s, full_name), full_name, position, team
                    FROM player
                    WHERE levenshtein(%(name)s, full_name) < 7
                    ORDER BY levenshtein
                """, connection, params=dict(name=name), index_col='player_id')

        raise ValueError('No hits found for {}, {}'.format(
            player_str,
            'could be:\n\n{}'.format(fuzzy_results) if fuzzy_results.shape[0] else 'no similar names found',
        ))

    if result.shape[0] > 1:
        raise ValueError('Multiple hits found for {}:\n\n{}'.format(
            player_str, result,
        ))

    return result.iloc[0, 0]


@memoize
def _get_hardcoded_player_ids():
    hardcoded_ids = {
        tuple(k.split('; ')): v
        for k, v in yaml.load(resource_stream(__name__, 'data/hardcoded_player_ids.yaml')).items()
    }

    teams = yaml.load(resource_stream(__name__, 'data/teams.yaml'))
    for team_variations in teams:
        canonical = team_variations[0]
        for variation in team_variations:
            hardcoded_ids[variation, 'DST'] = canonical

    return hardcoded_ids
