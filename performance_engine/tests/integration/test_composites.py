import pytest
from datetime import datetime
import uuid

from lusid.api import PortfoliosApi, TransactionPortfoliosApi
from lusid.exceptions import ApiException
from lusid.models import CreateTransactionPortfolioRequest
import pytz
import pandas as pd

from apis_performance.portfolio_performance_api import PortfolioPerformanceApi
from block_stores.block_store_in_memory import InMemoryBlockStore
from performance_sources.mock_src import SeededSource
from performance_sources.comp_src import CompositeSource
from composites.in_memory_composite import InMemoryComposite
from composites.portfolio_groups_composite import PortfolioGroupComposite
from tests.utilities.api_factory import api_factory
from perf import Performance

test_scope = "PerformanceEngineCompositeTests"

single_member_codes = ["P1", "P2", "P3"]

for single_member_code in single_member_codes:
    try:
        single_member = api_factory.build(PortfoliosApi).get_portfolio(
            scope=test_scope, code=single_member_code
        )
    except ApiException as e:
        if e.status == 404:
            single_member = api_factory.build(TransactionPortfoliosApi).create_portfolio(
                scope=test_scope,
                create_transaction_portfolio_request=CreateTransactionPortfolioRequest(
                    display_name=single_member_code,
                    description=single_member_code,
                    code=single_member_code,
                    created=datetime(2000, 1, 1, tzinfo=pytz.UTC),
                    base_currency="AUD"
                )
            )
        else:
            raise e


# Simple composite test for the three methods
@pytest.mark.parametrize(
    "test_name, composite",
    [
        ("in_memory_composite", InMemoryComposite()),
        ("portfolio_groups_composite", PortfolioGroupComposite(api_factory=api_factory))
    ]
)
def test_composite_methods(test_name, composite):

    in_memory_block_store = InMemoryBlockStore()

    seeded_source = SeededSource()
    seeded_source.add_seeded_perf_data(entity_scope=test_scope, entity_code="P1", start_date='2018-03-05', seed=24106)
    seeded_source.add_seeded_perf_data(entity_scope=test_scope, entity_code="P2", start_date='2018-03-05', seed=12345)
    seeded_source.add_seeded_perf_data(entity_scope=test_scope, entity_code="P3", start_date='2018-10-16', seed=33333)

    performance_api = PortfolioPerformanceApi(
        block_store=InMemoryBlockStore(),
        portfolio_performance_source=seeded_source
    )

    # utility to create composite from the three portfolios
    def create_composite(composite_code, date='2018-03-19', **kwargs):

        nonlocal performance_api

        composite.create_composite(
            composite_scope=test_scope,
            composite_code=composite_code
        )

        composite.add_composite_member(
            composite_scope=test_scope,
            composite_code=composite_code,
            member_scope=test_scope,
            member_code="P1",
            from_date='2018-03-05',
            to_date=None)

        composite.add_composite_member(
            composite_scope=test_scope,
            composite_code=composite_code,
            member_scope=test_scope,
            member_code="P2",
            from_date='2018-03-05',
            to_date=None)

        composite.add_composite_member(
            composite_scope=test_scope,
            composite_code=composite_code,
            member_scope=test_scope,
            member_code="P3",
            from_date='2018-03-05',
            to_date=None)

        cs = CompositeSource(composite=composite, performance_api=performance_api, **kwargs)

        return Performance(test_scope, composite_code, cs, InMemoryBlockStore(), perf_start='2018-03-18')

    # AND Composites, created using the different methods
    c1 = create_composite(str(uuid.uuid4()))
    c2 = create_composite(str(uuid.uuid4()), composite_mode="equal")
    c3 = create_composite(str(uuid.uuid4()), composite_mode="agg")

    p1 = Performance(test_scope, "P1", src=seeded_source, block_store=in_memory_block_store, perf_start='2018-03-05')
    p2 = Performance(test_scope, "P2", src=seeded_source, block_store=in_memory_block_store, perf_start='2018-03-05')
    p3 = Performance(test_scope, "P3", src=seeded_source, block_store=in_memory_block_store, perf_start='2018-10-16')

    def run_performance(p):
        return pd.DataFrame.from_records(
                p.report(
                    locked=False,
                    start_date='2018-03-05',
                    end_date='2018-12-31',
                    asat='2019-01-05'
                    )
                )[['date','inception','mv','wt']]

    # WHEN we calculate the performance for all the portfolios and the
    #      composites
    # THEN the cumulative return on the final date should be as expected
    for df, exp_cum in zip(
            map(run_performance, [p1, p2, p3, c1, c2, c3]),
            [4.272095, 5.552946, 0.779682, 4.415115, 4.639544, 4.415115]
    ):
        last = df.tail(1).iloc[0]
        assert last['inception'] == pytest.approx(exp_cum)


@pytest.mark.parametrize(
    "test_name, date_range_1, date_range_2, date_range_3, expected_outcome",
    [
        (
            "complete_coverage",
            (datetime(2018, 3, 5, tzinfo=pytz.UTC), datetime(2018, 12, 31, tzinfo=pytz.UTC)),
            (datetime(2018, 3, 5, tzinfo=pytz.UTC), datetime(2018, 12, 31, tzinfo=pytz.UTC)),
            (datetime(2018, 3, 5, tzinfo=pytz.UTC), datetime(2018, 12, 31, tzinfo=pytz.UTC)),
            [4.272095, 5.552946, 0.779682, 4.415115]
        ),
        (
            "swap_2nd_for_3rd",
            (datetime(2018, 3, 5, tzinfo=pytz.UTC), datetime(2018, 12, 31, tzinfo=pytz.UTC)),
            (datetime(2018, 3, 5, tzinfo=pytz.UTC), datetime(2018, 10, 16, tzinfo=pytz.UTC)),
            (datetime(2018, 3, 5, tzinfo=pytz.UTC), datetime(2018, 12, 31, tzinfo=pytz.UTC)),
            [4.272095, 5.552946, 0.779682, 4.974371]
        ),
    ]
)
def test_composite_inclusion_dates(test_name, date_range_1, date_range_2, date_range_3, expected_outcome):

    in_memory_block_store = InMemoryBlockStore()

    composite = PortfolioGroupComposite(api_factory=api_factory)

    seeded_source = SeededSource()
    seeded_source.add_seeded_perf_data(entity_scope=test_scope, entity_code="P1", start_date='2018-03-05', seed=24106)
    seeded_source.add_seeded_perf_data(entity_scope=test_scope, entity_code="P2", start_date='2018-03-05', seed=12345)
    seeded_source.add_seeded_perf_data(entity_scope=test_scope, entity_code="P3", start_date='2018-10-16', seed=33333)

    performance_api = PortfolioPerformanceApi(
        block_store=InMemoryBlockStore(),
        portfolio_performance_source=seeded_source
    )

    # utility to create composite from the three portfolios
    def create_composite(composite_code, date_range_1, date_range_2, date_range_3, **kwargs):

        nonlocal performance_api

        composite.create_composite(
            composite_scope=test_scope,
            composite_code=composite_code
        )

        composite.add_composite_member(
            composite_scope=test_scope,
            composite_code=composite_code,
            member_scope=test_scope,
            member_code="P1",
            from_date=date_range_1[0],
            to_date=date_range_1[1]
        )

        composite.add_composite_member(
            composite_scope=test_scope,
            composite_code=composite_code,
            member_scope=test_scope,
            member_code="P2",
            from_date=date_range_2[0],
            to_date=date_range_2[1]
        )

        composite.add_composite_member(
            composite_scope=test_scope,
            composite_code=composite_code,
            member_scope=test_scope,
            member_code="P3",
            from_date=date_range_3[0],
            to_date=date_range_3[1]
        )

        cs = CompositeSource(composite=composite, performance_api=performance_api, **kwargs)

        return Performance(test_scope, composite_code, cs, InMemoryBlockStore(), perf_start='2018-03-18')

    # AND Composites, created using the different methods
    c1 = create_composite(str(uuid.uuid4()), date_range_1, date_range_2, date_range_3)

    p1 = Performance(test_scope, "P1", src=seeded_source, block_store=in_memory_block_store, perf_start='2018-03-05')
    p2 = Performance(test_scope, "P2", src=seeded_source, block_store=in_memory_block_store, perf_start='2018-03-05')
    p3 = Performance(test_scope, "P3", src=seeded_source, block_store=in_memory_block_store, perf_start='2018-10-16')

    def run_performance(p):
        return pd.DataFrame.from_records(
            p.report(
                locked=False,
                start_date='2018-03-05',
                end_date='2018-12-31',
                asat='2019-01-05'
            )
        )[['date', 'inception', 'mv', 'wt']]

    # WHEN we calculate the performance for all the portfolios and the
    #      composites
    # THEN the cumulative return on the final date should be as expected
    for df, exp_cum in zip(
            map(run_performance, [p1, p2, p3, c1]),
            expected_outcome
    ):
        last = df.tail(1).iloc[0]
        assert last['inception'] == pytest.approx(exp_cum)
