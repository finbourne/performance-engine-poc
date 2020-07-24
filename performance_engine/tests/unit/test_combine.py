import pds
from performance_sources import mock_src
import pytest
import uuid

from misc import *
from block_stores.block_store_in_memory import InMemoryBlockStore
from block_ops import combine
from performance_engine.tests.utilities.environment import test_scope

src = mock_src.MockSource('Set1')

def add_block(entity_scope, entity_code, bs,sd,ed,ad):
    b = pds.PerformanceDataSet(sd,ed,ad)

    for d,g in src.get_perf_data(None, None, b.from_date,b.to_date,b.asat).groupby('date'):
        b.add_values(d,g.apply(lambda r : (r['key'],r['mv'],r['net']),axis=1))
    bs.add_block(entity_scope, entity_code, b)

# Test the combine_blocks logic
@pytest.mark.parametrize("locked",[True,False])
def test_combinations(locked):
    bs = InMemoryBlockStore()
    entity_code = str(uuid.uuid4())
    # block 1
    add_block(test_scope, entity_code, bs,'2019-12-31','2020-01-05','2020-01-05')
    # block 2
    add_block(test_scope, entity_code, bs,'2020-01-06','2020-01-10','2020-01-10')
    # block 3
    add_block(test_scope, entity_code, bs,'2020-01-11','2020-01-11','2020-01-11')
    # block 4 - Includes back-dated activity into block 2
    add_block(test_scope, entity_code, bs,'2020-01-08','2020-01-15','2020-01-15')
    # block 5 Includes back-dated activity to first item of block 2
    add_block(test_scope, entity_code, bs,'2020-01-06','2020-01-15','2020-02-01')
    # block 6 Includes back-dated activity to last item of block 1
    add_block(test_scope, entity_code, bs,'2020-01-05','2020-01-15','2020-02-02')
    # block 7 Includes back-dated activity to exclude block 1 entirely
    add_block(test_scope, entity_code, bs,'2012-12-31','2020-01-15','2020-02-03')

    def run_scenario(entity_scope, entity_code, to_date,asat_date,expected_open_tmv,expected_locked_tmv):
        blocks = bs.find_blocks(entity_scope, entity_code, '2020-01-03',to_date,asat_date)
        df = pd.DataFrame.from_records(
                [(o.date,o.tmv) 
                    for o in 
                    combine(blocks,locked,'2020-01-03',to_date,asat_date)],
                columns=['date','tmv']
            )
        total = df['tmv'].sum()
        expected = expected_locked_tmv if locked else expected_open_tmv
        if debug:
           print(nicer(df))
           print(f"Expected: {expected}, Actual: {total}")
        assert (total == pytest.approx(expected,0.001))

    # View on 01/10 for 01/09
    run_scenario(test_scope, entity_code, '2020-01-09','2020-01-10',6061.34,6061.34)
    # View on 01/10 for 01/10
    run_scenario(test_scope, entity_code, '2020-01-10','2020-01-10',7082.17,7082.17)
    # View on 01/11
    run_scenario(test_scope, entity_code, '2020-01-11','2020-01-11',7963.82,7963.82)
    # View on 01/15 for 01/11
    run_scenario(test_scope, entity_code, '2020-01-11','2020-01-15',8295.58,7963.82)
    # View on 01/15 for 01/15
    run_scenario(test_scope, entity_code, '2020-01-15','2020-01-15',12158.41,11826.65)
    # View on 02/01
    run_scenario(test_scope, entity_code, '2020-01-15','2020-02-01',2425.39,11826.65)
    # View on 02/02
    run_scenario(test_scope, entity_code, '2020-01-15','2020-02-02',1606.25,11826.65)
    # View on 02/03
    run_scenario(test_scope, entity_code, '2020-01-15','2020-02-03',39.0,11826.65)
