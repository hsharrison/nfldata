from enum import IntEnum
import numpy as np


class Quarter(IntEnum):
    Q1 = 0
    Q2 = 1
    Half = 2
    Q3 = 3
    Q4 = 4
    OT = 5
    OT2 = 6
    Final = 7

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


def process_time_col(series):
    new_cols = series.str.strip('()').str.split(',', expand=True)
    return (
        new_cols[0],
        new_cols[1].astype(int),
    )


def is_before(quarters, times, quarter, time):
    quarters = np.array([Quarter[q] for q in quarters])
    times = np.asarray(times)
    quarter = Quarter[quarter]
    return (quarters < quarter) | ((quarters == quarter) & (times < time))
