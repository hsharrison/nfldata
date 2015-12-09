from operator import itemgetter
from pkg_resources import resource_stream
from toolz import curry, compose
import yaml
import pandas as pd


class DFSSite:
    def __init__(self, roster_slots, name_fixes=None):
        self.roster_slots = roster_slots
        self.name_fixes = name_fixes or {}

    @classmethod
    def by_name(cls, name):
        return cls.from_yaml(resource_stream(__name__, 'data/dfs_sites.yaml'), name)

    @classmethod
    def from_yaml(cls, yaml_stream, name):
        raw = yaml.load(yaml_stream)[name]
        pos_order = [
            pos if isinstance(pos, str) else set(pos)
            for pos in raw['roster_slots']
        ]
        return cls(pos_order, raw.get('name_fixes'))

    def order_lineup(self, lineup, games=None):
        if games is None:
            team_order = [team for team, pos, name in lineup]
        else:
            team_order = list(pd.melt(games, id_vars='datetime', value_name='team')
                              .sort_values('datetime')
                              ['team']
                              )

        lineup_df = pd.DataFrame(dict(
            slot=self.roster_slots,
            cardinality=[len(slot) if is_flex(slot) else 1 for slot in self.roster_slots],
            team='',
            pos='',
            name='',
        ))

        remaining_lineup = sorted(lineup, key=compose(team_order.index, itemgetter(1)))
        for ix, row in lineup_df.sort_values('cardinality').iterrows():
            player = next(filter(matches_pos(row['slot']), remaining_lineup))
            remaining_lineup.remove(player)
            lineup_df.loc[ix, ['team', 'pos', 'name']] = player

        return [tuple(row[['team', 'pos', 'name']]) for _, row in lineup_df.iterrows()]

    def format_lineup(self, lineup):
        return [self.name_fixes.get(name, name) for team, pos, name in lineup]


def is_flex(roster_slot):
    return not isinstance(roster_slot, str)


@curry
def matches_pos(pos, player):
    player_pos = player[1]

    if isinstance(pos, str):
        return player_pos == pos
    return player_pos in pos
