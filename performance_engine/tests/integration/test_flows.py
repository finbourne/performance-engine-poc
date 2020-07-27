import pytest

import flows
from tests.utilities import api_cacher
from misc import *
from config.config import PerformanceConfiguration


@pytest.mark.skip("Setup of JLH Fund 1 or similar needs to be replicable before this test can be run")
def test_get_flows():
    config = PerformanceConfiguration(ext_flow_types = {'APPRCY','EXPRCY'})
    with api_cacher.CachingApi("flows") as api:
         v = flows.get_flows(
                 api,
                 'JLH',
                 'FUND1',
                 config,
                 '2019-07-05',
                 '2019-07-15',
                 now()
               )
         assert len(v) == 3
         assert v[as_date('2019-07-08')] == 306132.26
         assert v[as_date('2019-07-11')] == 261681.14
         assert v[as_date('2019-07-12')] == -1224380.23


@pytest.mark.skip("Setup of JLH Fund 1 or similar needs to be replicable before this test can be run")
def test_cursor():

    test_flows = {
            as_date('2018-03-05') :  1.0,
            as_date('2018-03-06') :  2.0,
            as_date('2018-04-10') :  4.0,
            as_date('2018-03-20') :  8.0,
            as_date('2018-04-11') : 16.0,
            as_date('2018-05-31') : 32.0
    }

    crs = flows.FlowCursor(test_flows)

    assert crs.upto('2018-03-01') == 0.0 # prior to first flow
    assert crs.upto('2018-03-06') == 3.0 # includes two flows
    assert crs.upto('2018-03-22') == 8.0 # Not a flow date, but includes one
    assert crs.upto('2018-04-09') == 0.0 # Prior to next flow, no flows occured
    assert crs.upto('2018-04-12') == 20.0 # two flows, sorting trial
