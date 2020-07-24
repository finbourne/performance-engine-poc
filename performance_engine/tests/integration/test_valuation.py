import valuation
from tests.utilities import api_cacher
import misc

def test_valuation():
    with api_cacher.CachingApi("valuation") as api:
         d,v = valuation.get_valuation(
                 api,
                 'JLH',
                 'FUND1',
                 api.models.ResourceId("JLH","JLH"),
                 '2019-05-31',
                 misc.now()
               )
         assert d == misc.as_date('2019-05-31')
         assert v == 114288432.78
         d,v = valuation.get_valuation(
                 api,
                 'JLH',
                 'FUND1',
                 api.models.ResourceId("JLH","JLH"),
                 '2019-09-04',
                 misc.now()
               )
         assert d == misc.as_date('2019-09-04')
         assert v == 120461267.95
