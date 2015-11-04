import numpy as np
import pandas as pd


def same_team_corrs(data):
    return data.corr()


def cross_team_corrs(data):
    unstacked = data.unstack('home').reset_index(drop=True)
    unstacked.columns = pd.MultiIndex.from_tuples(
        [(stat, home) for stat, home in unstacked.columns],
        names=['stat', 'home'],
    )
    unstacked = unstacked.sort_index(axis='columns', level='home', sort_remaining=False)
    stats = unstacked.columns.get_level_values('stat')[:unstacked.shape[1]//2]

    switched = unstacked.copy()
    switched.columns = pd.MultiIndex.from_tuples(
        [(stat, not home) for stat, home in switched.columns],
        names=['stat', 'home'],
    )
    switched = switched.sort_index(axis='columns', level='home')

    return (
        pd.concat([unstacked, switched], axis=0, ignore_index=True)
        .corr()
        .sort_index(axis=0).sort_index(axis=1)
        .xs(True, axis=0, level='home').xs(False, axis=1, level='home')
        .reindex_axis(stats, axis=1).reindex_axis(stats, axis=0)
    )


def full_corrs(data):
    """Same- and cross-team correlations.
    Same-team correlations are above the diagonal;
    cross-team correlations are on and below the diagonal.

    """
    corr = same_team_corrs(data)
    tril_ixs = np.tril_indices_from(corr)
    corr.values[tril_ixs] = cross_team_corrs(data).values[tril_ixs]
    return corr
