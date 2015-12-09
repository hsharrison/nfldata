from pkg_resources import resource_stream
from toolz import curry
import yaml


class DFSSite:
    def __init__(self, pos_order, name_fixes=None):
        self.pos_order = pos_order
        self.name_fixes = name_fixes or {}

    @classmethod
    def by_name(cls, name):
        return cls.from_yaml(resource_stream(__name__, 'data/dfs_sites.yaml'), name)

    @classmethod
    def from_yaml(cls, yaml_stream, name):
        raw = yaml.load(yaml_stream)[name]
        pos_order = [
            pos if isinstance(pos, str) else set(pos)
            for pos in raw['position_order']
        ]
        return cls(pos_order, raw.get('name_fixes'))

    def format_lineup(self, lineup):
        lineup_set = set(lineup)
        final_lineup = []
        for pos_to_find in self.pos_order:
            team, pos, name = next(filter(matches_pos(pos_to_find), iter(lineup_set)))
            final_lineup.append(self.name_fixes.get(name, name))
        return final_lineup


@curry
def matches_pos(pos, player):
    player_pos = player[1]

    if isinstance(pos, str):
        return player_pos == pos
    return player_pos in pos
