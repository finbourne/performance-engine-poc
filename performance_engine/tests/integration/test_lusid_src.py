import pytest
from performance_sources.lusid_src import LusidSource
from config.config import PerformanceConfiguration
from misc import *
from os import path
from fields import *
from block_stores.block_store_in_memory import InMemoryBlockStore
from perf import Performance

from tests.utilities import api_cacher

config = PerformanceConfiguration( ext_flow_types={'APPRCY','EXPRCY'})


@pytest.mark.skip("Setup of JLH Fund 1 or similar needs to be replicable before this test can be run")
def test_get_perf_data(recording):

    with api_cacher.CachingApi(filename='lusid_src') as api:
         l_src = LusidSource(api, config)
         df = l_src.get_perf_data('JLH','FUND1', '2019-07-10','2019-07-12',now())

         # To record tests, use pytest --recording=test_get_perf_data
         filename = path.join('expected','lusid_src_get_perf_data.pk')

         if 'test_get_perf_data' in recording:
            # Record the result and save as the expectation
            df.to_pickle(filename, protocol=0)
            
            # Also save a temporary csv version for manual review
            nicer(df).to_csv('lusid_src-perf_data_test.csv',index=False)
         else:
             target = pd.read_pickle(filename)

             # Compare with the expectation
             pd.testing.assert_frame_equal(df,target)

cases = [
        dates("2019-05-31","2020-03-25T08:45","2020-03-25T09:00","2019-05-30","chg-test1"),
        dates("2019-05-31","2020-03-25T08:45","2020-03-25T09:01","2019-05-30","chg-test2"),
        dates("2019-06-30","2020-03-25T09:30","2020-03-25T10:35","2019-06-24","chg-test3"),
        dates("2019-11-03","2020-03-26","2020-03-28",None,"chg-test4"),
        dates("2019-11-04","2020-03-26","2020-03-28",None,"chg-test5")
        ]


@pytest.mark.parametrize("test_case",cases)
@pytest.mark.skip("Setup of JLH Fund 1 or similar needs to be replicable before this test can be run")
def test_get_changes(test_case):
    last_date,last_asat,curr_asat,expected_result,cache_name = test_case
    with api_cacher.CachingApi(cache_name) as api:
         l_src = LusidSource(api,None)
         actual = l_src.get_changes('JLH','FUND1',last_date,last_asat,curr_asat)
         assert actual == expected_result

@pytest.mark.parametrize(["locked","expected"],[
    (False,0.0),
    (True,-0.009049)])
@pytest.mark.skip("Setup of JLH Fund 1 or similar needs to be replicable before this test can be run")
def test_scenario(locked,expected):
    with api_cacher.CachingApi("scenario1") as api:
         prf = Performance(
                  'JLH', 'FUND1',
                  LusidSource(api,config),
                  InMemoryBlockStore()
               )

         # Lock the May period
         prf.get_performance(True,"2019-05-01","2019-05-31","2020-03-25T09:00",create=True)
         # Lock the June period
         prf.get_performance(True,"2019-05-31","2019-06-30","2020-03-25T10:00",create=True)
         # Lock the July period
         prf.get_performance(True,"2019-06-30","2019-07-31","2020-03-25T11:00",create=True)

         fields = [DAY,WTD]
         df = pd.DataFrame.from_records(
                  prf.report(
                      locked,
                      '2019-05-01',
                      '2019-07-31',
                      '2020-03-25T11:00',
                      fields=fields
                  )
               )[['date','mv','inception','flows','correction','flow_correction'] + fields]

         assert len(df) == 92
         assert df.tail(1).iloc[0]['inception'] == pytest.approx(0.002099104)
         actual = df['correction'].sum()
         assert actual == pytest.approx(expected,abs=0.000005)

