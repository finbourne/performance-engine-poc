import pytest
from typing import List, Dict

from pds import PerformanceDataSet, PerformanceDataPoint
from apis_returns.upsert_returns import upsert_portfolio_returns

from apis_returns.upsert_returns_models import (
    UpsertReturnsResponse,
    PerformanceDataSetRequest,
    PerformanceDataPointRequest,
    PerformanceDataSetResponse,
    PerformanceDataPointResponse,
)
from block_stores.block_store_structured_results import BlockStoreStructuredResults
from tests.utilities.api_factory import api_factory

pdp_ror_only_2020_01_01 = {
    "date": "2020-01-01",
    "ror": 0.05
}

pdp_ror_only_2020_01_01_persisted = {
    "date": "2020-01-01",
    "ror": 0.05,
    "cnt": 0,
    "cum_fctr": 1.05,
    "cum_flow": 0.0,
    "flows": 0,
    "pnl": None,
    "sum_ror": 0.05,
    "sum_ror_sqr": 0.05**2,
    "weight": 0
}

pdp_ror_only_2020_01_02 = {
    "date": "2020-01-02",
    "ror": 0.03
}

pdp_ror_only_2020_01_03 = {
    "date": "2020-01-03",
    "ror": -0.02
}

pdp_ror_only_2020_01_01_to_02_persisted = {
    "date": "2020-01-02",
    "ror": 0.03,
    "cnt": 1,
    "cum_fctr": 1.0815,
    "cum_flow": 0.0,
    "flows": 0,
    "pnl": None,
    "sum_ror": 0.08,
    "sum_ror_sqr": 0.03 ** 2 + 0.05 ** 2,
    "weight": 0
}

pdp_ror_only_2020_01_01_to_03_persisted = {
    "date": "2020-01-03",
    "ror": -0.02,
    "cnt": 2,
    "cum_fctr": 1.05987,
    "cum_flow": 0.0,
    "flows": 0,
    "pnl": None,
    "sum_ror": 0.06,
    "sum_ror_sqr": 0.03 ** 2 + 0.05 ** 2 + (-0.02) ** 2,
    "weight": 0
}

pdp_ror_mv_2020_01_01 = {
    "date": "2020-01-01",
    "ror": 0.05,
    "weight": 15000000
}

pdp_ror_mv_2020_01_02 = {
    "date": "2020-01-02",
    "ror": 0.08,
    "weight": 15300000
}

pdp_ror_mv_2020_01_03 = {
    "date": "2020-01-03",
    "ror": -0.03,
    "weight": 15400000
}

pdp_ror_mv_2020_01_01_persisted = {
    "date": "2020-01-01",
    "ror": 0.05,
    "cnt": 0,
    "cum_fctr": 1.05,
    "cum_flow": 0.0,
    "flows": 0,
    "pnl": None,
    "sum_ror": 0.05,
    "sum_ror_sqr": 0.05**2,
    "weight": 15000000
}

pdp_ror_mv_2020_01_01_to_02_persisted = {
    "date": "2020-01-02",
    "ror": 0.08,
    "cnt": 1,
    "cum_fctr": 1.134,
    "cum_flow": 0.0,
    "flows": 0,
    "pnl": None,
    "sum_ror": 0.13,
    "sum_ror_sqr": 0.05 ** 2 + 0.08 ** 2,
    "weight": 15300000
}

pdp_ror_mv_2020_01_01_to_03_persisted = {
    "date": "2020-01-03",
    "ror": -0.03,
    "cnt": 2,
    "cum_fctr": 1.09998,
    "cum_flow": 0.0,
    "flows": 0,
    "pnl": None,
    "sum_ror": 0.10,
    "sum_ror_sqr": 0.05 ** 2 + 0.08 ** 2 + (-0.03)**2,
    "weight": 15400000
}


def create_test_case(performance_scope: str, entity_scope: str, entity_code: str, start_date, end_date,
                     upserted_pdps: List[Dict], persisted_pdps: List[Dict]):
    """
    Creates a test case for testing the upsert of returns

    :param str performance_scope: The scope of the block store to use to store performance returns
    :param str entity_scope: The scope of the entity to store returns against
    :param str entity_code: The code of the entity to store returns against
    :param start_date: The start date of the returns
    :param end_date: The end date of the returns
    :param List[Dict] upserted_pdps: The keyword arguments to create the PerformanceDataPoints to upsert
    :param List[Dict] persisted_pdps: The keyword arguments for the PerformanceDataPoints expected to be Persisted
    in the BlockStore

    :return: List: The created test case to use
    """
    return [
        performance_scope,
        entity_scope,
        entity_code,
        {
            "set1": PerformanceDataSetRequest(
                data_points=[
                    PerformanceDataPointRequest(
                        **pdp
                    ) for pdp in upserted_pdps
                ]
            )
        },
        UpsertReturnsResponse(
            successes={
                "set1": PerformanceDataSetResponse(
                    from_date=start_date,
                    to_date=end_date,
                    previous=PerformanceDataPointResponse(
                        **upserted_pdps[-1]
                    ),
                    data_points=[
                        PerformanceDataPointResponse(
                            **pdp
                        ) for pdp in upserted_pdps
                    ]
                )
            },
            failures={}
        ),
        [
            PerformanceDataSet(
                from_date=start_date,
                to_date=end_date,
                data_points=[
                    PerformanceDataPoint(
                            **pdp
                        ) for pdp in persisted_pdps
                ],
                previous=PerformanceDataPoint(
                        **persisted_pdps[-1]
                )
            )
        ]
    ]


testcases = {
    "Standard Upsert return only with single PerformanceDataPoint": create_test_case(
        performance_scope="PerformanceDataSetPersistence",
        entity_scope="test",
        entity_code="port",
        start_date="2020-01-01",
        end_date="2020-01-01",
        upserted_pdps=[pdp_ror_only_2020_01_01],
        persisted_pdps=[pdp_ror_only_2020_01_01_persisted]
    ),
    "Standard Upsert return only with multiple PerformanceDataPoint": create_test_case(
        performance_scope="PerformanceDataSetPersistence",
        entity_scope="test",
        entity_code="port",
        start_date="2020-01-01",
        end_date="2020-01-03",
        upserted_pdps=[pdp_ror_only_2020_01_01, pdp_ror_only_2020_01_02, pdp_ror_only_2020_01_03],
        persisted_pdps=[
            pdp_ror_only_2020_01_01_persisted,
            pdp_ror_only_2020_01_01_to_02_persisted,
            pdp_ror_only_2020_01_01_to_03_persisted]
    ),
    "Standard Upsert return, market value with single PerformanceDataPoint": create_test_case(
        performance_scope="PerformanceDataSetPersistence",
        entity_scope="test",
        entity_code="port",
        start_date="2020-01-01",
        end_date="2020-01-01",
        upserted_pdps=[pdp_ror_mv_2020_01_01],
        persisted_pdps=[pdp_ror_mv_2020_01_01_persisted]
    ),
    "Standard Upsert return, market value with multiple PerformanceDataPoint": create_test_case(
        performance_scope="PerformanceDataSetPersistence",
        entity_scope="test",
        entity_code="port",
        start_date="2020-01-01",
        end_date="2020-01-03",
        upserted_pdps=[pdp_ror_mv_2020_01_01, pdp_ror_mv_2020_01_02, pdp_ror_mv_2020_01_03],
        persisted_pdps=[
            pdp_ror_mv_2020_01_01_persisted,
            pdp_ror_mv_2020_01_01_to_02_persisted,
            pdp_ror_mv_2020_01_01_to_03_persisted
        ]
    )
}


@pytest.mark.parametrize(
    "scenario", list(testcases.keys())
)
def test_upsert_returns(scenario):

    global testcases
    global api_factory
    block_store = BlockStoreStructuredResults(api_factory)
    scenario = testcases[scenario]

    # Upsert the returns
    response = upsert_portfolio_returns(
        performance_scope=scenario[0],
        portfolio_scope=scenario[1],
        portfolio_code=scenario[2],
        request_body=scenario[3],
        block_store=block_store
    )

    # Can't compare asAt as generated by system
    for result in response.values.values():
        result.asat = None

    # Ensure that response is as expected
    assert response == scenario[4]

    # Check that blocks were persisted in the block store as expected
    persisted_blocks = block_store.find_blocks(
        entity_scope=scenario[1],
        entity_code=scenario[2],
        from_date="2020-01-01",
        to_date="2020-01-03",
        asat="2025-07-02",
        performance_scope=scenario[0]
    )

    # Can't compare asAt as generated by system
    for result in persisted_blocks:
        result.asat = None

    assert persisted_blocks == scenario[5]
