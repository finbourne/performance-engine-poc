from pathlib import Path

from misc import *
from performance_sources import mock_src
import pytest
from performance_engine.perf import Performance
from fields import *
from block_stores.block_store_in_memory import InMemoryBlockStore
from os import path
import pandas as pd
from performance_sources.mock_src import SeededSource


standard_fields = ['date','key','mv','flows','inception','correction']
field_set=[DAY,WTD,MTD,QTD,YTD,ROLL_WEEK,ROLL_MONTH,ROLL_YEAR,ROLL_QTR]

# Source for Set1 - used in env1 and env2
src1 = mock_src.MockSource('Set1')
src1.get_changes = lambda v, w, x, y, z: as_date('2020-01-08') # only needed once

# Set up environment #1 - no performance posted
env1 = Performance(entity_scope="test", entity_code="env1", src=src1, block_store=InMemoryBlockStore()) # No blocks
# Set up environment #2 - locked period, and back-dated changes
env2 = Performance(entity_scope="test", entity_code="env2", src=src1, block_store=InMemoryBlockStore())
# Set up environment #3 - 2 years data, and 1 b.p return/day (easy to verify)
env3 = Performance(entity_scope="test", entity_code="env3", src=mock_src.SimpleSource('2018-03-05'),
                   block_store=InMemoryBlockStore())
# Set up environment #4 - 2 years data, and 1 b.p return/day, with a weekly flow
env4 = Performance(entity_scope="test", entity_code="env4", src=mock_src.SimpleSource('2018-03-05', recurring_flow=300.0),
                   block_store=InMemoryBlockStore())

# Create a block in env2, locked on 2020-01-10
env2.get_performance(True,'2019-12-31','2020-01-10','2020-01-10',create=True)

test_cases = [
   ('env1-part-01-10',env1,False,'2020-01-07','2020-01-10','2020-01-10'),
   ('env1-full-01-15',env1,False,'2019-12-31','2020-01-15','2020-01-15'),
   ('env1-full-02-03',env1,False,'2019-12-31','2020-01-15','2020-02-03'),
   ('env2-lock-01-15',env2,True, '2019-12-31','2020-01-15','2020-01-15'),
   ('env2-open-01-15',env2,False,'2019-12-31','2020-01-15','2020-01-15'),
   ('env3-huge-set',env3,False,'2018-01-01','2020-03-19','2020-03-19'),
   ('env4-huge-set',env4,False,'2018-01-01','2020-03-19','2020-03-19')
]

@pytest.mark.parametrize("test_case",test_cases)
def test_performance(recording,test_case):

    name,environ,locked,start_date,end_date,asat = test_case

    prf = list(environ.report(
        locked, 
        start_date,
        end_date,
        asat,
        fields=field_set))

    # Convert to pandas dataframe for easy comparison
    df = pd.DataFrame.from_records(prf)[standard_fields + field_set]

    filename = Path(__file__).parent.joinpath('expected',f'results_for_{name}.pk')

    # To record tests, use pytest --recording=test_performance
    if 'test_performance' in recording:
       # Record the result and save as the expectation
       df.to_pickle(filename,protocol=0) 

       # Also save a temporary csv version for manual review
       nicer(df).to_csv(name + '.csv',index=False)
    else:
       # Load expected values and compare with the generated data
       target = pd.read_pickle(filename)

       # Compare with the expectation
       pd.testing.assert_frame_equal(df,target)

def test_volatility_example():
    # Test based upon the example here: 
    # http://invest-made-easy.blogspot.com/2013/03/understanding-volatility-and-sharpe.html
    # The calculated standard deviation of the monthly returns should be 2.25%

    vol_fields = [DAY,VOL_INC,ANN_VOL_INC]

    df = pd.DataFrame.from_records(
            Performance(
                entity_scope="test",
                entity_code="test_5yr_vol",
                src=mock_src.MockSource('VolEx1'),
                block_store=InMemoryBlockStore()
            ).report(
                False,
                '2019-12-31',
                '2020-01-11',
                '2020-01-11',
                fields=vol_fields
            )
         )[['date','mv'] + vol_fields]

    volatility = df[VOL_INC].values[-1]
    assert volatility == pytest.approx(0.0225,abs=0.00005)

def test_5yr_vol(recording):
    vol_fields = [DAY,VOL_INC,VOL_1YR,VOL_3YR,VOL_5YR,ANN_VOL_3YR,ANN_VOL_5YR,ANN_VOL_INC]

    src = SeededSource()
    src.add_seeded_perf_data("test", "test_5yr_vol", "2014-12-31", 24106)

    df = pd.DataFrame.from_records(
            Performance(
                entity_scope="test",
                entity_code="test_5yr_vol",
                src=src,
                block_store=InMemoryBlockStore()
            ).report(
                False,
                '2013-12-31',
                '2020-03-05',
                '2020-03-05',
                fields=vol_fields
            )
         )[['date','mv'] + vol_fields]

    # Test the boundaries of each year
    # This is where the different period volatilities
    # Will diverge from the inception volatility
    subset = df[df['date'].isin(set(dates('2014-12-31',
                      '2015-01-01','2015-12-31',
                      '2016-01-01','2016-12-31',
                      '2017-01-01','2017-12-31',
                      '2018-01-01','2018-12-31',
                      '2019-01-01','2019-12-31',
                      '2020-01-01','2020-03-05')))]

    # To record tests, use pytest --recording=test_5yr_vol
    filename = Path(__file__).parent.joinpath('expected', '5yr_vol.pk')

    if 'test_5yr_vol' in recording:
       # Record the result and save as the expectation
       subset.to_pickle(filename,protocol=0) 

       # Also save a temporary csv version for manual review
       nicer(df).to_csv('5yr_vol.csv',index=False)
    else:
       # Load expected values and compare with the generated data
       target = pd.read_pickle(filename)

       # Compare with the expectation
       pd.testing.assert_frame_equal(subset,target)

def test_perf_start_date_is_honoured():
    prf = list(Performance(entity_scope="test", entity_code="start_date", src=src1, block_store=InMemoryBlockStore(),
                           perf_start='2019-12-31').report(
        False, 
        '2020-01-03',
        '2020-01-05',
        '2020-01-10',
        fields=[DAY]))

    cum_ror = prf[-1]['inception']

    assert cum_ror == pytest.approx(0.187343)
