from pathlib import Path

from misc import *
from performance_sources.mock_src import SeededSource
from performance_engine.perf import Performance
from fields import *
from block_stores.block_store_in_memory import InMemoryBlockStore
import pandas as pd
import numpy as np


def risk_free_rates(date,days):
    return np.round((2021 - date.year) * 0.005 + 0.003 * (days/365.0),6)


def test_sharpe_ratios(recording):
    fields = [DAY,AGE_DAYS,
            ROLL_YEAR,ROLL_3YR,ROLL_5YR,
            ANN_INC,ANN_1YR,ANN_3YR,ANN_5YR,
            VOL_INC,VOL_1YR,VOL_3YR,VOL_5YR,
            ANN_VOL_INC,ANN_VOL_1YR,ANN_VOL_3YR,ANN_VOL_5YR,
            RISK_FREE_1YR,RISK_FREE_3YR,RISK_FREE_5YR,
            SHARPE_1YR,SHARPE_3YR,SHARPE_5YR]

    src = SeededSource(rfr_func=risk_free_rates)
    src.add_seeded_perf_data("test", "sharpe_ratio", '2013-12-31', 24106)

    df = pd.DataFrame.from_records(
            Performance(
                entity_scope="test",
                entity_code="sharpe_ratio",
                src=src,
                block_store=InMemoryBlockStore()
            ).report(
                locked=False,
                start_date='2013-12-31',
                end_date='2020-03-05',
                asat='2020-03-05',
                fields=fields
            )
         )[['date', 'mv', 'inception'] + fields]

    # Test cases
    subset = df

    # To record tests, use pytest --recording=test_sharpe
    filename = Path(__file__).parent.joinpath('expected', 'sharpe_ratios.pk')

    if 'test_sharpe_ratios' in recording:
        # Record the result and save as the expectation
        subset.to_pickle(filename, protocol=0)

        # Also save a temporary csv version for manual review
        nicer(df).to_csv('sharpe_ratios.csv', index=False)

    else:
        # Load expected values and compare with the generated data
        target = pd.read_pickle(filename)

        # Compare with the expectation
        pd.testing.assert_frame_equal(subset,target)

