from pathlib import Path

from misc import *
from performance_sources import mock_src
import pytest
from perf import Performance
from block_stores.block_store_in_memory import InMemoryBlockStore

src = mock_src.MockSource('Set1', filename=Path(__file__).parent.parent.joinpath("test-data.xlsx"))

# Mock the intended responses from the get_changes call
changes = { 
   dates('2020-01-05','2020-01-05','2020-01-10') 
        : as_date('2020-01-06'),
   dates('2020-01-10','2020-01-10','2020-01-15') 
        : as_date('2020-01-08'),
   dates('2020-01-12','2020-01-15','2020-01-16') 
        : as_date('2020-01-12'),
   dates('2020-01-15','2020-01-16','2020-02-03') 
        : as_date('2019-12-31'),
   dates('2020-01-15','2020-01-16','2020-02-02') 
        : as_date('2020-01-05'),
}

def mock_get_changes(*args):
    r = changes.get(tuple(args[2:]))
    return r

src.get_changes = mock_get_changes

def test_cumulative_ror():

    entity_scope="test"
    entity_code="cumulative_ror"

    prf = Performance(entity_scope=entity_scope, entity_code=entity_code, src=src, block_store=InMemoryBlockStore())

    def check_performance(expected_len, expected_ror, *args, **kwargs):
        perf_data = list(prf.get_performance(*args, **kwargs))
        df = pd.DataFrame.from_records(
                [(o.date,o.tmv,o.ror,o.cum_fctr) for o in perf_data],
                columns=['date','mv','ror','fctr'])
        #print(nicer(df))
        cum_ror = perf_data[-1].cum_fctr

        assert len(perf_data) == expected_len
        assert cum_ror == pytest.approx(expected_ror,0.00001)
        return df

    # Step 1 - Post performance up to 5th Jan - asat 5th Jan
    check_performance(6,1.187343,True,'2019-12-31','2020-01-05','2020-01-05',create=True)

    # Step 2 - Post performance up to 10th Jan - asat 10th Jan
    check_performance(11,1.458329,True,'2019-12-31','2020-01-10','2020-01-10',create=True)

    # Step 3 - Post performance up to 12th Jan - asat 15th Jan
    check_performance(13,1.293112,True,'2019-12-31','2020-01-12','2020-01-15',create=True)

    # Step 4 - Post performance up to 15th Jan - asat 16th Jan
    check_performance(16,1.646827,True,'2019-12-31','2020-01-15','2020-01-16',create=True)

    # Test 5 - Query actual performance up to 15th Jan - asat 16th Jan
    check_performance(16,1.646827,False,'2019-12-31','2020-01-15','2020-01-16')

    # Test 6 - Query actual performance from 3rd up to 9th Jan - asat 10th Jan
    check_performance(7,1.378071,False,'2020-01-03','2020-01-09','2020-01-10')

    # Test 7 - Query actual performance from 3rd up to 9th Jan - asat 15th Jan
    check_performance(7,1.370092,False,'2020-01-03','2020-01-09','2020-01-15')

    # Test 8 - Query locked performance from 3rd up to 9th Jan - asat 10th Jan
    check_performance(7,1.378071,True,'2020-01-03','2020-01-09','2020-01-10')

    # Test 9 - Query locked performance from 3rd up to 9th Jan - asat 15th Jan
    check_performance(7,1.378071,True,'2020-01-03','2020-01-09','2020-01-15')
    
    # Test 10 - Query locked performance up to 13th Jan - asat 3rd Feb
    check_performance(14,1.39204,True,'2019-12-15','2020-01-13','2020-02-03')
    
    # Test 11 - Query actual performance up to 13th Jan - asat 3rd Feb
    check_performance(14,1.0,False,'2019-12-15','2020-01-13','2020-02-03')

    # Test 11 - Query actual performance up to 13th Jan - asat 2nd Feb
    check_performance(14,0.00285714,False,'2019-12-15','2020-01-13','2020-02-02')

    # Test 11 - Query locked performance up to 13th Jan - asat 2nd Feb
    check_performance(14,1.39204,True,'2019-12-15','2020-01-13','2020-02-02')

    # The block store should only contain the 4 locked blocks
    assert len(prf.block_store.blocks[f"{entity_scope}_{entity_code}"]) == 4

