from typing import Dict

from apis_returns.upsert_returns_models import (
    PerformanceDataPointResponse,
    PerformanceDataSetRequest,
    PerformanceDataSetResponse,
    UpsertReturnsResponse,
)
from interfaces import IBlockStore
from pds import PerformanceDataSet


def upsert_portfolio_returns(performance_scope: str, portfolio_scope: str, portfolio_code: str,
                             request_body: Dict[str, PerformanceDataSetRequest], block_store: IBlockStore):
    """
    Upsert returns into a block store for a given portfolio.

    :param str performance_scope: The scope of the BlockStore to use to store the performance returns
    :param str portfolio_scope: The scope of the Portfolio the returns are associated with
    :param str portfolio_code: The code of the Portfolio the returns are associated with, together with the scope
    this uniquely identifies the Portfolio
    :param Dict[str, PerformanceDataSetRequest] request_body: The body of the request containing the PerformanceDataSets
    to persist as blocks
    :param IBlockStore block_store: The block store to use to persist the blocks

    :return: UpsertReturnsResponse: The response to the Upsert request
    """

    # 2) Do Validation
    # Validate no duplication on dates of PerformanceDataPoint inside PerformanceDataSet
    # Validate no duplicates of PerformanceDataSets (based on unique code using from_date, to_date, entity_scope, entity_code)

    # 3) Cast to PerformanceDataSets
    pds = {}

    # For each PerformanceDataSet
    for correlation_id, performance_data_set_request in request_body.items():

        start_date = performance_data_set_request.start_date
        end_date = performance_data_set_request.end_date

        # Sort all the PerformanceDataPoint into chronological order
        performance_data_point_chronological = sorted(
            performance_data_set_request.data_points, key=lambda x: x.date
        )

        # Ensure that the start_date and end_date are available
        if start_date is None:
            start_date = performance_data_point_chronological[0].date

        if end_date is None:
            end_date = performance_data_point_chronological[-1].date

        # Create a new PerformanceDataSet
        pd = PerformanceDataSet(
            from_date=start_date,
            to_date=end_date
        )

        # Add the returns to the PerformanceDataSet in the form of a number of PerformanceDataPoint
        [
            pd.add_returns(
                date=pdp.date, weight=getattr(pdp, "weight", 0), ror=pdp.ror
            ) for pdp in performance_data_point_chronological
         ]

        pds[correlation_id] = pd

    # 4) Persist each PerformanceDataSet in the BlockStore
    pds = {
        correlation_id: block_store.add_block(
            entity_scope=portfolio_scope,
            entity_code=portfolio_code,
            block=pd,
            performance_scope=performance_scope) for correlation_id, pd in pds.items()
    }

    # 5) Cast to Response objects
    pd_responses = {}
    # For each PerformanceDataSet
    for correlation_id, performance_data_set in pds.items():

        # Create the response object
        pd = PerformanceDataSetResponse(
            from_date=performance_data_set.from_date,
            to_date=performance_data_set.to_date,
            asat=performance_data_set.asat,
            data_points=[
                PerformanceDataPointResponse(**performance_data_point.__dict__)
                for performance_data_point in performance_data_set.data_points
            ],
            previous=PerformanceDataPointResponse(**performance_data_set.latest_data_point.__dict__)
        )

        pd_responses[correlation_id] = pd

    return UpsertReturnsResponse(pd_responses, {})
