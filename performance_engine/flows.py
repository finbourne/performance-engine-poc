import pandas as pd
import numpy as np

from lusidtools.lpt import lpt
from misc import as_dates
from collections import defaultdict

@as_dates
def get_flows(api,scope,portfolio,config,from_date,to_date,asat):

    # Called if build_transactions() succeeds
    def success(result):
        accum = defaultdict(float)

        for txn in result.content.values:
            # Proof-of-concept flow logic
            # TOTAL level only. Flow amount is in the total_consideration field
            # White-list of valid codes provided in the config
            if txn.type in config.ext_flow_types:
               rate = txn.properties.get('Transaction/default/TradeToPortfolioRate')
               
               if rate:
                   rate = rate.value.metric_value.value / txn.exchange_rate
               else:
                   rate = txn.exchange_rate
               accum[txn.transaction_date] += np.round(txn.total_consideration.amount * rate,2)

        return accum

    def failure(error):
        lpt.display_error(error)
        exit()

    return api.call.build_transactions(
                 scope=scope,
                 code=portfolio,
                 transaction_query_parameters=api.models.TransactionQueryParameters(
                    start_date=from_date,
                    end_date=to_date,
                    query_mode='TradeDate',
                    show_cancelled_transactions=False
                 ),
                 as_at = asat
           ).match(failure,success)

class FlowCursor():
    def __init__(self,flows):
        self.iter = iter(sorted(flows.items()))
        self.curr = next(self.iter,None)

    @as_dates
    def upto(self,date):
        accum = 0.0

        while self.curr != None and self.curr[0] <= date:
            accum += self.curr[1]
            self.curr=next(self.iter,None)

        return accum
