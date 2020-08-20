import pytest
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from lusid.api import PortfoliosApi, TransactionPortfoliosApi, PropertyDefinitionsApi
from lusid.models import (
    CreatePropertyDefinitionRequest,
    CreateTransactionPortfolioRequest,
    ModelProperty,
    PropertyValue,
    ResourceId
)
from lusid.exceptions import ApiException

from apis_returns.upsert_returns import upsert_portfolio_returns
from apis_returns.upsert_returns_models import PerformanceDataSetRequest, PerformanceDataPointRequest
from block_stores.block_store_structured_results import BlockStoreStructuredResults
from tests.utilities.api_factory import api_factory
from tests.utilities.environment import test_scope
from apis_performance.portfolio_performance_api import PortfolioPerformanceApi
from apis_performance.composite_performance_api import CompositePerformanceApi
from composites.portfolio_groups_composite import PortfolioGroupComposite
from fields import *
from performance_sources.comp_src import CompositeSource

block_store = BlockStoreStructuredResults(api_factory=api_factory)
portfolio_group_composite = PortfolioGroupComposite(api_factory=api_factory)

portfolio_performance_api = PortfolioPerformanceApi(
    block_store=block_store,
    portfolio_performance_source=None,
    api_factory=api_factory
)

asset_weighted_composite_source = CompositeSource(
    composite=portfolio_group_composite,
    performance_api=portfolio_performance_api,
    composite_mode="asset")

composite_performance_api = CompositePerformanceApi(
    block_store=block_store,
    composite_performance_source=asset_weighted_composite_source,
    api_factory=api_factory
)


set_1 = PerformanceDataSetRequest(
    data_points=[
        PerformanceDataPointRequest(
            date="2020-01-01",
            ror=0.05,
            weight=1000
        ),
        PerformanceDataPointRequest(
            date="2020-01-02",
            ror=0.02,
            weight=1050
        ),
        PerformanceDataPointRequest(
            date="2020-01-03",
            ror=-0.014,
            weight=987
        ),
        PerformanceDataPointRequest(
            date="2020-01-04",
            ror=0.05,
            weight=1050
        ),
        PerformanceDataPointRequest(
            date="2020-01-05",
            ror=0.07,
            weight=1150
        ),
        PerformanceDataPointRequest(
            date="2020-01-06",
            ror=-0.03,
            weight=1068
        )
    ]
)

set_2 = PerformanceDataSetRequest(
    data_points=[
        PerformanceDataPointRequest(
            date="2020-01-01",
            ror=0.03,
            weight=1000
        ),
        PerformanceDataPointRequest(
            date="2020-01-02",
            ror=0.08,
            weight=1033
        ),
        PerformanceDataPointRequest(
            date="2020-01-03",
            ror=-0.042,
            weight=896
        ),
        PerformanceDataPointRequest(
            date="2020-01-04",
            ror=0.009,
            weight=1250
        ),
        PerformanceDataPointRequest(
            date="2020-01-05",
            ror=-0.04,
            weight=1289
        ),
        PerformanceDataPointRequest(
            date="2020-01-06",
            ror=-0.02,
            weight=1198
        )
    ]
)


@pytest.mark.parametrize(
    "test_name, performance_scope, portfolio_scope, portfolio_code, request_body, expected_outcome",
    [
        (
            "standard_report",
            test_scope,
            test_scope,
            str(uuid.uuid4()),
            {
                "set1": set_1
            },
            "standard_report.gzip"
        )
    ]
)
def test_upsert_returns_produce_report(test_name, performance_scope, portfolio_scope, portfolio_code, request_body,
                                       expected_outcome):

    global block_store
    global portfolio_performance_api

    response = upsert_portfolio_returns(
        performance_scope=performance_scope,
        portfolio_scope=portfolio_scope,
        portfolio_code=portfolio_code,
        request_body=request_body,
        block_store=block_store
    )

    report = portfolio_performance_api.get_portfolio_performance_report(
        portfolio_scope=portfolio_scope,
        portfolio_code=portfolio_code,
        performance_scope=performance_scope,
        from_date="2020-01-02",
        to_date="2020-01-06",
        locked=True,
        fields=[DAY, WTD, YTD, ROLL_WEEK]
    )

    expected_file_path = Path(__file__).parent.joinpath(f"expected/{expected_outcome}")
    expected_outcome = pd.read_pickle(expected_file_path)
    assert(report.equals(expected_outcome))


@pytest.mark.parametrize(
    "test_name, performance_scope, composite_scope, composite_code, portfolio_returns, expected_outcome",
    [
        (
            "standard_composite",
            test_scope,
            test_scope,
            str(uuid.uuid4()),
            [
                (
                    test_scope,
                    str(uuid.uuid4()),
                    {
                        "set1": set_1
                    },
                    ("2020-01-01", None)
                ),
                (
                    test_scope,
                    str(uuid.uuid4()),
                    {
                        "set2": set_2
                    },
                    ("2020-01-01", None)
                )
            ],
            "standard_composite.gzip"
        )
    ]
)
def test_upsert_returns_create_composite_produce_report(test_name, performance_scope, composite_scope, composite_code,
                                                        portfolio_returns, expected_outcome):

    global block_store
    global portfolio_group_composite
    global composite_performance_api

    responses = [upsert_portfolio_returns(
        performance_scope=performance_scope,
        portfolio_scope=portfolio_scope,
        portfolio_code=portfolio_code,
        request_body=request_body,
        block_store=block_store
    ) for portfolio_scope, portfolio_code, request_body, date_ranges in portfolio_returns]

    portfolio_group_composite.create_composite(
        composite_scope=composite_scope,
        composite_code=composite_code,
    )

    for portfolio_scope, portfolio_code, request_body, date_ranges in portfolio_returns:
        try:
            response = api_factory.build(PortfoliosApi).get_portfolio(
                scope=portfolio_scope, code=portfolio_code
            )
        except ApiException as e:
            if e.status == 404:
                single_member = api_factory.build(TransactionPortfoliosApi).create_portfolio(
                    scope=portfolio_scope,
                    create_transaction_portfolio_request=CreateTransactionPortfolioRequest(
                        display_name=portfolio_code,
                        description=portfolio_code,
                        code=portfolio_code,
                        created=datetime(2000, 1, 1, tzinfo=pytz.UTC),
                        base_currency="AUD"
                    )
                )
            else:
                raise e

    [portfolio_group_composite.add_composite_member(
        composite_scope=composite_scope,
        composite_code=composite_code,
        member_scope=portfolio_scope,
        member_code=portfolio_code,
        from_date=date_ranges[0],
        to_date=date_ranges[1]
    ) for portfolio_scope, portfolio_code, request_body, date_ranges in portfolio_returns]

    report = composite_performance_api.get_composite_performance_report(
        composite_scope=composite_scope,
        composite_code=composite_code,
        performance_scope=performance_scope,
        from_date="2020-01-02",
        to_date="2020-01-06",
        locked=True,
        fields=[DAY, WTD, YTD, ROLL_WEEK]
    )

    expected_file_path = Path(__file__).parent.joinpath(f"expected/{expected_outcome}")
    expected_outcome = pd.read_pickle(expected_file_path)
    assert(report.equals(expected_outcome))



@pytest.mark.parametrize(
    "test_name, performance_scope, portfolio_scope, portfolio_code, request_body, expected_outcome",
    [
        (
            "standard_report_arbitrary_inception",
            test_scope,
            test_scope,
            str(uuid.uuid4()),
            {
                "set1": set_1
            },
            "standard_report_arbitrary_inception.gzip"
        )
    ]
)
def test_upsert_returns_produce_report_arbitrary_inception(test_name, performance_scope, portfolio_scope,
                                                           portfolio_code, request_body, expected_outcome):

    global block_store
    global portfolio_group_composite
    global composite_performance_api

    response = upsert_portfolio_returns(
        performance_scope=performance_scope,
        portfolio_scope=portfolio_scope,
        portfolio_code=portfolio_code,
        request_body=request_body,
        block_store=block_store
    )

    try:
        response = api_factory.build(PortfoliosApi).get_portfolio(
            scope=portfolio_scope, code=portfolio_code
        )
    except ApiException as e:
        if e.status == 404:
            single_member = api_factory.build(TransactionPortfoliosApi).create_portfolio(
                scope=portfolio_scope,
                create_transaction_portfolio_request=CreateTransactionPortfolioRequest(
                    display_name=portfolio_code,
                    description=portfolio_code,
                    code=portfolio_code,
                    created=datetime(2000, 1, 1, tzinfo=pytz.UTC),
                    base_currency="AUD"
                )
            )
        else:
            raise e

    try:
        response = api_factory.build(PropertyDefinitionsApi).create_property_definition(
            create_property_definition_request=CreatePropertyDefinitionRequest(
                domain="Portfolio",
                scope="Managers",
                code="ManagerStart",
                value_required=False,
                display_name="ManagerStartDate",
                data_type_id=ResourceId(
                    scope="system",
                    code="string"
                ),
                life_time="TimeVariant",
                constraint_style="Property"
            )
        )
    except ApiException as e:
        pass

    api_factory.build(PortfoliosApi).upsert_portfolio_properties(
        scope=portfolio_scope,
        code=portfolio_code,
        request_body={
            "Portfolio/Managers/ManagerStart": ModelProperty(
                key="Portfolio/Managers/ManagerStart",
                value=PropertyValue(
                    label_value="2020-01-03"
                ),
                effective_from=datetime(2000, 1, 1, tzinfo=pytz.UTC)
            )
        }
    )

    report = portfolio_performance_api.get_portfolio_performance_report(
        portfolio_scope=portfolio_scope,
        portfolio_code=portfolio_code,
        performance_scope=performance_scope,
        from_date="2020-01-02",
        to_date="2020-01-06",
        locked=True,
        fields=[DAY, WTD, YTD, ROLL_WEEK, "ManagerStart"],
    )

    expected_file_path = Path(__file__).parent.joinpath(f"expected/{expected_outcome}")
    expected_outcome = pd.read_pickle(expected_file_path)
    assert(report.equals(expected_outcome))
