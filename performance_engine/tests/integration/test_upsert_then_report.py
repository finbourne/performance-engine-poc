import pandas as pd

from apis_returns.upsert_returns import upsert_portfolio_returns
from apis_returns.upsert_returns_models import PerformanceDataSetRequest, PerformanceDataPointRequest
from block_stores.block_store_structured_results import BlockStoreStructuredResults
from performance_engine.perf import Performance
from fields import *
from performance_engine.tests.utilities.api_factory import api_factory
from performance_engine.tests.utilities.environment import test_scope

bs = BlockStoreStructuredResults(api_factory=api_factory)


def test_upsert_then_report():

    portfolio_code = "MyTestPort"

    response = upsert_portfolio_returns(
        performance_scope=test_scope,
        portfolio_scope=test_scope,
        portfolio_code=portfolio_code,
        request_body={
            "Test1": PerformanceDataSetRequest(
                data_points=[
                    PerformanceDataPointRequest(
                        date="2020-01-01",
                        ror=0.01,
                        weight=1560000
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-02",
                        ror=0.03,
                        weight=1590000
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-03",
                        ror=0.015,
                        weight=1640000
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-04",
                        ror=0.08,
                        weight=1690000
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-05",
                        ror=-0.10,
                        weight=1530000
                    )
                ]
            )
        },
        block_store=bs
    )

    perf = Performance(
        entity_scope=test_scope,
        entity_code=portfolio_code,
        src=None,
        block_store=bs,
        perf_start="2020-01-01"
    )

    report = perf.report(
        locked=True,
        start_date="2020-01-01",
        end_date="2020-01-05",
        asat="2020-08-10",
        fields=[DAY, WTD, QTD, MTD, ROLL_WEEK],
        performance_scope=test_scope
    )

    report_df = pd.DataFrame.from_records(report)
