from functools import reduce
from typing import Iterator, Dict

from pandas import Timestamp, DataFrame

from apis_performance.portfolio_performance_api import PortfolioPerformanceApi
import comp_method
from interfaces import IPerformanceSource, IComposite
from misc import *
from merge import Merger


class CompositeSource(IPerformanceSource):
    """
    This class is responsible for being the source of performance data for a composite
    """

    # The available modes for calculating the composite's returns from its members
    modes = {
        "asset": comp_method.AssWt,
        "equal": comp_method.EqWt,
        "agg": comp_method.Agg
    }

    def __init__(self, composite: IComposite, performance_api: PortfolioPerformanceApi, composite_mode: str = "asset"):
        """
        :param IComposite composite: The composite implementation to use
        :param PortfolioPerformanceApi performance_api: The performance api to use to get performance of the composite
        :param str composite_mode: The composite method to use e.g. asset, equal weighted etc.
        members
        """
        self.comp = composite
        self.performance_api = performance_api
        self.mode = self.modes[composite_mode]

    @as_dates
    def get_perf_data(self, entity_scope: str, entity_code: str, start_date: Timestamp, end_date: Timestamp,
                      asat: Timestamp, **kwargs) -> DataFrame:
        """
        The responsibility of this function is to get the performance data for the Composite by retrieving the composite's
        members, getting the performance for each of them and then merging the results together.

        :param str entity_scope: The scope of the composite to get performance data for
        :param str entity_code: The code of the composite to get performance data for
        :param Timestamp start_date: The effectiveAt start date of the performance period
        :param Timestamp end_date: The effectiveAt end date of the performance period
        :param Timestamp asat: The asAt date of the performance period

        :return: DataFrame: The DataFrame containing performance
        """
        # If passed, get the scope to use for getting performance from the block store
        performance_scope = kwargs.get("performance_scope")

        # Get the members of the composite
        composite_members = self.comp.get_composite_members(
            composite_scope=entity_scope,
            composite_code=entity_code,
            start_date=start_date,
            end_date=end_date,
            asat=asat
        )

        # Create a new merger class to merge the performance from all the Portfolios
        mrg = Merger(key_fn=lambda r: r.date)

        for member_id, member_date_ranges in composite_members.items():
            # Get the performance for each Portfolio and store the result to be merged
            [
                mrg.include(
                    member_id,
                    self.performance_api.prepare_portfolio_performance(
                        portfolio_scope=member_id.split("_")[0],
                        portfolio_code=member_id.split("_")[1]
                    ).get_performance(
                        locked=True,
                        start_date=max(start_range_date, start_date) + self.mode.start_date_offset,
                        end_date=min(end_range_date, end_date),
                        asat=asat,
                        performance_scope=performance_scope)
                # Do this for all date ranges that the Portfolio is a member inside the requested window
                ) for start_range_date, end_range_date in member_date_ranges
                if start_range_date <= end_date and end_range_date >= start_date
            ]

        def combine_composite() -> Iterator[Dict]:
            """
            Using the merger this function merges the performance together across all the members of the composite. It
            then calculates the performance for each day returning the daily performance via an Iterator.

            :return: Iterator[Dict]: An iterator of the performance for each day
            """
            nonlocal mrg
            nonlocal self

            for date, members in mrg.merge():
                yield reduce(self.mode.accumulate, members, self.mode(date)).result()

        # Convert the iterator of Dictionaries into a Pandas DataFrame
        return pd.DataFrame.from_records(combine_composite())
