from lusidtools.lpt import lpt

import pandas as pd
from pandas import DataFrame, Timestamp

import flows
from interfaces import IPerformanceSource
from misc import as_dates, ONE_DAY
from valuation import get_valuation


class LusidSource(IPerformanceSource):
    """
    The responsibility of this class is to source performance data from LUSID
    """
    def __init__(self, api, config, **kwargs):
        self.api = api
        self.config = config

    @as_dates
    def get_perf_data(self, entity_scope: str, entity_code: str, start_date: Timestamp, end_date: Timestamp,
                      asat: Timestamp, **kwargs) -> DataFrame:
        """
        The responsibility of this method is to get performance data from LUSID

        :param str entity_scope: The scope of the portfolio to get performance data for
        :param str entity_code: The code of the portfolio to get performance data for
        :param Timestamp start_date: The effectiveAt start date of the performance period
        :param Timestamp end_date: The effectiveAt end date of the performance period
        :param Timestamp asat: The asAt date of the performance period

        :return: DataFrame df: The DataFrame with performance data
        """
        # Transaction flows cursor. This stores the net in/out-flows. Merged with valuations to get performance.
        crs = flows.FlowCursor(
                flows.get_flows(
                  self.api,
                  entity_scope,
                  entity_code,
                  self.config,
                  start_date,
                  end_date,
                  asat
                )
              )

        if self.config.get('recipe_code') is not None:
            recipe = self.api.models.ResourceId(
                self.config.get('recipe_scope', self.scope),
                self.config.recipe_code
            )
        else:
            recipe = None  # Use the default recipe

        def reader(start_date):
            nonlocal end_date
            while start_date <= end_date:
                  date,value = get_valuation(
                                 self.api,
                                 entity_scope,
                                 entity_code,
                                 recipe,
                                 start_date,
                                 asat
                               )
                  flows = crs.upto(start_date)
                  yield (date,value,flows)

                  start_date += ONE_DAY # TODO: Use holiday calendar

        df = pd.DataFrame.from_records(reader(start_date),columns=['date','mv','net'])
        df['key']='all' # Not supporting multiple levels at this time.

        return df

    @as_dates
    def get_changes(self,entity_scope, entity_code, last_date,last_asat, curr_asat):
        # Called if get_aggregation_by_portfolio() succeeds
        def success(result):
            for chg in result.content.values:
                if chg.entity_id.code == entity_code:
                   # Found the record for our portfolio
                   # Note, result is by scope so we had to search
                   # TODO: change api to use scope + portfolio
                   # TODO: make use of curr_asat in the query 
                   # So that we can avoid unnecessary work
                   # when curr_asat != now
                   return chg.correction_effective_at
            return None

        def failure(error):
            lpt.display_error(error)
            exit()

        return self.api.call.get_portfolio_changes(
                  scope=entity_scope,
                  effective_at=last_date,
                  as_at=last_asat
               ).match(failure,success)
