from typing import List

import pandas as pd
from pandas import Timestamp

from config.config import global_config
from ext_fields import get_ext_fields
from fields import *
from interfaces import IPerformanceSource, IBlockStore
from lusid.utilities.api_client_factory import ApiClientFactory
from misc import as_dates, now
from perf import Performance
from performance_sources.lusid_src import LusidSource


class PortfolioPerformanceApi:
    """
    The responsibility of this class is to produce performance reports for a portfolio
    """
    def __init__(self, block_store: IBlockStore, portfolio_performance_source: IPerformanceSource,
                 api_factory: ApiClientFactory = None):
        """
        :param IBlockStore block_store: The block store to use to get performance to generate reports
        :param IPerformanceSource portfolio_performance_source: The source to use to get performance for a portfolio when
        there are missing blocks or working with an unlocked period
        :param ApiClientFactory api_factory: The API factory to use to interact with LUSID
        """
        self.block_store = block_store
        self.portfolio_performance_source = portfolio_performance_source
        self.api_factory = api_factory

    def prepare_portfolio_performance(self, portfolio_scope: str, portfolio_code: str):
        """
        The responsibility of this method is to prepare an instance of the Performance class for the specified
        portfolio which can be used to generate performance reports.

        :param str portfolio_scope: The scope of the portfolio.
        :param str portfolio_code: The code of the portfolio, together with the scope this uniquely identifies the
        portfolio.

        :return: Performance: The instance of the performance class which can be used to generate performance reports.
        """
        return Performance(
            entity_scope=portfolio_scope,
            entity_code=portfolio_code,
            src=self.portfolio_performance_source,
            block_store=self.block_store,
            perf_start=None,
        )

    @as_dates
    def get_portfolio_performance_report(self, portfolio_scope: str, portfolio_code: str, performance_scope: str,
                                         from_date: Timestamp, to_date: Timestamp, asat: Timestamp = None,
                                         locked: bool = True, fields: List[str] = None):
        """
        The responsibility of this method is to generate a performance report for the specified composite.

        :param str portfolio_scope: The scope of the portfolio.
        :param str portfolio_code: The code of the portfolio, together with the scope this uniquely identifies the
        portfolio.
        :param str performance_scope: The scope to use when fetching performance data to generate the report
        :param Timestamp from_date: The effectiveAt date to generate performance from
        :param Timestamp to_date: The effectiveAt date to generate performance until
        :param Timestamp asat: The asAt date to generate performance at
        :param bool locked: Whether or not the performance to use in generation the report is locked
        :param List[str] fields: The fields to have in the report e.g. WTD (week to date), Daily etc.

        :return: DataFrame: The Pandas DataFrame containing the performance report
        """

        # Default the fields to only provide the daily return
        fields = fields or [DAY]
        asat = asat or now()

        config = global_config

        if self.api_factory is not None:
            # Look for extension fields, e.g. arbitrary inception dates
            ext_fields = get_ext_fields(
                api_factory=self.api_factory,
                entity_type="portfolio",
                entity_scope=portfolio_scope,
                entity_code=portfolio_code,
                effective_date=from_date,
                asat=asat,
                fields=fields,
                config=config)
        else:
            ext_fields = {}

        # Prepare the portfolio performance which can be used to generate a report
        prf = self.prepare_portfolio_performance(
            portfolio_scope=portfolio_scope,
            portfolio_code=portfolio_code
        )

        # Generate the report and convert it into a DataFrame
        return pd.DataFrame.from_records(
                 prf.report(
                     locked=locked,
                     start_date=from_date,
                     end_date=to_date,
                     asat=now(),
                     performance_scope=performance_scope,
                     fields=fields,
                     ext_fields=ext_fields
                 )
        )[['date', 'mv', 'inception', 'flows'] + fields]
