from pkg_resources import resource_stream
from toolz import memoize, curry
import yaml
import numpy as np
import pandas as pd

from nfldata import lookup


def load_by_week(connection, week_loader, weeks_by_season, **kwargs):
    all_dfs = []
    for season_year, weeks in weeks_by_season.items():
        for week in weeks:
            if 'season_type' in kwargs:
                season_type = kwargs['season_type']
            elif week > 17:
                season_type = 'Postseason'
            else:
                season_type = 'Regular'

            week_df = week_loader(season_year=season_year, week=week)
            week_df['season_year'] = season_year
            week_df['week'] = week
            week_df['season_type'] = season_type
            all_dfs.append(week_df)

    df = pd.concat(all_dfs, ignore_index=True)
    return sanitize(connection, df, **kwargs)


def sanitize(connection, df, idp=False, source=''):
    df.columns = [get_column_renames().get(standardize_str(col), standardize_str(col)) for col in df.columns]
    df = df.drop(df.index[df['team'] == 'FA'], axis=0)
    df = df.drop(
        df.index[np.array(
            [(row['name'], row['pos']) in get_ignored_players()
             for _, row in df.iterrows()]),
        ], axis=0,
    )

    if 'bye' in df:
        df = df.drop(df.index[df['bye'] == df['week']], axis=0)

    if not idp:
        df = df.drop([col for col in df if col.startswith('idp')], axis=1)
        df = df.drop(df.index[df['pos'].isin({'DL', 'LB', 'DB'})], axis=0)

    df = df.drop(df.columns[df.apply(all_same_or_null)], axis=1)

    if source.lower() == 'fantasyfootballanalytics':
        df = df.drop([
            'passing_cmp_pct',
            'receiving_rec',
            'receiving_tds',
            'ret_tds',
            'ret_yds',
            '40_yd_passes',
            '40_yd_recs',
            '40_yd_rushes',
            'bye',
            'kicking_fgm_0_19',
            'kicking_fgm_20_29',
            'kicking_fgm_30_39',
            'kicking_fgm_40_49',
            'kicking_fgm_50p',
        ], axis=1)

    df = pd.concat([df, df.apply(_id_from_row(connection, lookup_home=True, lookup_opp=True), axis=1)], axis=1)
    df['player_id'] = [lookup.player_id(connection, row['name'], row['pos'], team=row['team'])
                       for _, row in df.iterrows()]

    return df.set_index(['gsis_id', 'player_id']).sort_index()


def standardize_str(str_):
    return str_.lower().replace(' ', '_')


def all_same_or_null(series):
    # Some columns we short-circuit in order to always keep.
    if series.name in {
        'season_year',
        'season_type',
        'week',
        'team',
        'gsis_id',
    }:
        return False
    return len(set(series.unique()) - {np.nan}) < 2


@curry
def _id_from_row(connection, row, **kwargs):
    return lookup.gsis_id(connection, row['season_year'], row['week'], row['team'],
                          season_type=row['season_type'], **kwargs)


@memoize
def get_ignored_players():
    return [tuple(player.split('; '))
            for player in yaml.load(resource_stream(__name__, 'data/ignored_players.yaml'))]


@memoize
def get_column_renames():
    return yaml.load(resource_stream(__name__, 'data/column_renames.yaml'))
