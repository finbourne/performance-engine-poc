from pathlib import Path
import pytest
import uuid

from misc import *
from perf import Performance
from returns import Returns
from return_sources.mock_src import ReturnSource
from block_stores.block_store_in_memory import InMemoryBlockStore
from fields import *
from tests.utilities.environment import test_scope

@pytest.mark.parametrize(
    ("fund","expected"),
   [("A",    1.094722),
    ("B",    -0.41831),
    ("C",    0.301223),
    ("D",    0.05),
    ("E",    0.710357)]
)
def test_cumulative_returns(fund,expected):
    bs = InMemoryBlockStore()

    portfolio_code = str(uuid.uuid4())

    Returns(bs).import_data(
        test_scope,
        portfolio_code,
        ReturnSource(dataset='Ret1', portfolio=fund, filename=Path(__file__).parent.parent.joinpath("test-data.xlsx")),
        '2020-01-01',
        '2020-01-11',
        '2020-01-11'
    )

    df=pd.DataFrame.from_records(
            Performance(
                test_scope,
                portfolio_code, None, bs).report(
                False,
                '2020-01-11',
                '2020-01-11',
                '2020-01-11'
            )
    )

    assert len(df)==1
    cum_ror = df['inception'].iloc[-1]
    assert cum_ror == pytest.approx(expected,abs=0.00005)


@pytest.mark.parametrize(
    ("fund","expected_cum_ror", "expected_correction"),
   [("A",    1.933077,          -0.136936),
    ("B",    -0.41831,          0.0),
    ("C",    0.704883,          -0.03844),
    ("D",    -1.00000,          0.0),
    ("E",    1.127013,          -0.04763)]
)
def test_two_periods(fund,expected_cum_ror,expected_correction):
    bs = InMemoryBlockStore()
    rs = ReturnSource(dataset='Ret1', portfolio=fund, filename=Path(__file__).parent.parent.joinpath("test-data.xlsx"))
    Returns(bs).import_data(
        test_scope,
        fund,
        rs,
        '2020-01-01',
        '2020-01-11',
        '2020-01-11'
    ).import_data(
        test_scope,
        fund,
        rs,
        '2020-01-08',
        '2020-01-15',
        '2020-01-15'
    )

    def check_returns(locked):
        df = pd.DataFrame.from_records(
                Performance(        test_scope,
        fund, None, bs).report(
                    locked,
                    '2020-01-01',
                    '2020-01-15',
                    '2020-01-15',
                    fields = [DAY,WTD]
                )
        )

        assert len(df)==15
        cum_ror = df['inception'].iloc[-1]
        correction = df['correction'].iloc[-4]
        assert cum_ror == pytest.approx(expected_cum_ror,abs=0.00005)
        assert correction == pytest.approx(expected_correction if locked else 0.0,abs=0.00005)

    # Check the returns in locked mode
    check_returns(True)
    # And, using the same block-store, run in 'true' mode
    check_returns(False)
