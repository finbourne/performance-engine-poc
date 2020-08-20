import pytest
from datetime import datetime, timedelta
import logging
import uuid

import pytz
from lusid import ApiException
from lusid.api import TransactionPortfoliosApi, PortfoliosApi, PortfolioGroupsApi
from lusid.models import CreateTransactionPortfolioRequest, CreatePortfolioGroupRequest, ResourceId
from tests.utilities.api_factory import api_factory
from composites.portfolio_groups_composite import PortfolioGroupComposite

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

test_scope = "PerformanceEngineCompositeTests"

single_member_single_period_composite_code = str(uuid.uuid4())

single_member_codes = ["TestSingleMember", "TestSingleMember2", "TestSingleMember3"]

single_member_start_date = datetime(2020, 1, 15, tzinfo=pytz.UTC)
single_member_end_date = datetime(2020, 1, 20, tzinfo=pytz.UTC)
single_member_range = (single_member_end_date - single_member_start_date).days

prepopulated_portfolio_group_code = str(uuid.uuid4())
prepopulated_portfolio_group_created_date = datetime(2020, 1, 1, tzinfo=pytz.UTC)

prepopulated_deleted_portfolio_group_code = str(uuid.uuid4())
prepopulated_deleted_portfolio_group_created_date = datetime(2020, 1, 1, tzinfo=pytz.UTC)

portfolio_groups_composite = PortfolioGroupComposite(
    api_factory=api_factory
)

portfolio_groups_composite.create_composite(
    composite_scope=test_scope,
    composite_code=single_member_single_period_composite_code
)


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

try:
    prepopulated_portfolio_group = api_factory.build(PortfolioGroupsApi).get_portfolio_group(
        scope=test_scope, code=prepopulated_portfolio_group_code
    )
except ApiException as e:
    if e.status == 404:
        prepopulated_portfolio_group = api_factory.build(PortfolioGroupsApi).create_portfolio_group(
            scope=test_scope,
            create_portfolio_group_request=CreatePortfolioGroupRequest(
                display_name=prepopulated_portfolio_group_code,
                description=prepopulated_portfolio_group_code,
                code=prepopulated_portfolio_group_code,
                created=prepopulated_portfolio_group_created_date,
                values=[ResourceId(scope=test_scope, code=code) for code in single_member_codes]
            )
        )
    else:
        raise e

try:
    prepopulated_deleted_portfolio_group = api_factory.build(PortfolioGroupsApi).get_portfolio_group(
        scope=test_scope, code=prepopulated_deleted_portfolio_group_code
    )
except ApiException as e:
    if e.status == 404:
        prepopulated_deleted_portfolio_group = api_factory.build(PortfolioGroupsApi).create_portfolio_group(
            scope=test_scope,
            create_portfolio_group_request=CreatePortfolioGroupRequest(
                display_name=prepopulated_deleted_portfolio_group_code,
                description=prepopulated_deleted_portfolio_group_code,
                code=prepopulated_deleted_portfolio_group_code,
                created=prepopulated_deleted_portfolio_group_created_date,
                values=[ResourceId(scope=test_scope, code=code) for code in single_member_codes]
            )
        )
    else:
        raise e

api_factory.build(PortfolioGroupsApi).delete_portfolio_group(
        scope=test_scope, code=prepopulated_deleted_portfolio_group_code
    )

portfolio_groups_composite.create_composite(
    composite_scope=test_scope,
    composite_code=prepopulated_deleted_portfolio_group_code
)

portfolio_groups_composite.add_composite_member(
    composite_scope=test_scope,
    composite_code=single_member_single_period_composite_code,
    member_scope=test_scope,
    member_code=single_member_codes[0],
    from_date=single_member_start_date,
    to_date=single_member_end_date,
)


@pytest.mark.parametrize(
    "test_name, other_composite_member_code_1_dates, other_composite_member_code_2_dates",
    [
        (
            "no_other_members",
            [],
            [],
        ),
        (
            "same_dates",
            [
                (single_member_start_date, single_member_end_date)
            ],
            [
                (single_member_start_date, single_member_end_date)
            ],
        ),
        (
            "different_dates",
            [
                (single_member_start_date - timedelta(days=10), single_member_end_date + timedelta(days=4))
            ],
            [
                (single_member_end_date + timedelta(days=3), single_member_end_date + timedelta(days=10))
            ],
        )
    ]
)
def test_members_are_independent(test_name, other_composite_member_code_1_dates, other_composite_member_code_2_dates):
    """
    The responsibility of this test is to ensure that regardless of how many members there are in a composite,
    the membership of one member in the composite is not affected by the overall composition of the composite. This
    allows for other tests to be conducted using just a single member with the confidence that the findings will
    extend to a composite with many members.

    :return: None
    """
    global test_scope
    global portfolio_groups_composite

    global single_member_codes
    global single_member_start_date
    global single_member_end_date

    independent_member_code = single_member_codes[0]
    other_composite_member_code_1 = single_member_codes[1]
    other_composite_member_code_2 = single_member_codes[2]

    composite_code = str(uuid.uuid4())

    portfolio_groups_composite.create_composite(
        composite_scope=test_scope,
        composite_code=composite_code
    )

    # Add the member to test for independence to the composite
    portfolio_groups_composite.add_composite_member(
        composite_scope=test_scope,
        composite_code=composite_code,
        member_scope=test_scope,
        member_code=independent_member_code,
        from_date=single_member_start_date,
        to_date=single_member_end_date
    )

    # Add other members
    [portfolio_groups_composite.add_composite_member(
        composite_scope=test_scope,
        composite_code=composite_code,
        member_scope=test_scope,
        member_code=other_composite_member_code_1,
        from_date=from_date,
        to_date=to_date
    ) for from_date, to_date in other_composite_member_code_1_dates]

    [portfolio_groups_composite.add_composite_member(
        composite_scope=test_scope,
        composite_code=composite_code,
        member_scope=test_scope,
        member_code=other_composite_member_code_2,
        from_date=from_date,
        to_date=to_date
    ) for from_date, to_date in other_composite_member_code_2_dates]

    # Get the membership
    response = portfolio_groups_composite.get_composite_members(
        composite_scope=test_scope,
        composite_code=composite_code,
        start_date=single_member_start_date,
        end_date=single_member_end_date,
        asat=datetime.now(pytz.UTC)
    )

    # Ensure that regardless of membership of other members, the independent member remains unaffected
    expected_outcome = [(single_member_start_date, single_member_end_date)]
    assert (response[f"{test_scope}_{independent_member_code}"] == expected_outcome)


@pytest.mark.parametrize(
    "test_name, start_date, end_date, expected_outcome",
    [
        (
            "inside_window",
            single_member_start_date-timedelta(days=3),
            single_member_end_date+timedelta(days=3),
            {f"{test_scope}_{single_member_codes[0]}": [
                (
                    single_member_start_date,
                    single_member_end_date
                )
            ]}
        ),
        (
            "before_window",
            single_member_end_date + timedelta(days=3),
            single_member_end_date + timedelta(days=10),
            {}
        ),
        (
            "after_window",
            single_member_start_date - timedelta(days=10),
            single_member_start_date - timedelta(days=3),
            {}
        ),
        (
            "overlap_before_window",
            single_member_start_date + timedelta(days=int(single_member_range/2)),
            single_member_end_date + timedelta(days=3),
            {f"{test_scope}_{single_member_codes[0]}": [
                (
                    single_member_start_date + timedelta(days=int(single_member_range/2)),
                    single_member_end_date
                )
            ]}
        ),
        (
            "overlap_after_window",
            single_member_start_date - timedelta(days=3),
            single_member_start_date + timedelta(days=int(single_member_range / 2)),
            {f"{test_scope}_{single_member_codes[0]}": [
                (
                    single_member_start_date,
                    single_member_start_date + timedelta(days=int(single_member_range / 2))
                )
            ]}
        ),
        (
            "on_start",
            single_member_start_date,
            single_member_start_date,
            {f"{test_scope}_{single_member_codes[0]}": [
                (
                    single_member_start_date,
                    single_member_start_date
                )
            ]}
        ),
        (
            "on_end",
            single_member_end_date,
            single_member_end_date,
            {f"{test_scope}_{single_member_codes[0]}": [
                (
                    single_member_end_date,
                    single_member_end_date
                )
            ]}
        ),
    ]
)
def test_single_period_window_for_retrieval_single_member(test_name, start_date, end_date, expected_outcome):
    """
    The responsibility of this test is to demonstrate that given a composite with a single member, retrieval and
    construction of the date ranges that the member exists in the composite inside a given effectiveAt window
    are as expected for all possible combinations.

    :return:
    """
    global portfolio_groups_composite
    global test_scope
    global single_member_single_period_composite_code

    membership = portfolio_groups_composite.get_composite_members(
        composite_scope=test_scope,
        composite_code=single_member_single_period_composite_code,
        start_date=start_date,
        end_date=end_date,
        asat=datetime.now(pytz.UTC)
    )

    assert (membership == expected_outcome)


@pytest.mark.parametrize(
    "test_name, date_ranges, expected_outcome",
    [
        (
            "mutually_exclusive_chronological",
            [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 18, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 18, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "overlapping_chronological",
            [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 13, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "overlapping_chronological_on_boundary",
            [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 15, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "mutually_exclusive_reverse_chronological",
            [
                (datetime(2020, 1, 18, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 18, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "overlapping_reverse_chronological",
            [
                (datetime(2020, 1, 13, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "overlapping_reverse_chronological_on_boundary",
            [
                (datetime(2020, 1, 15, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "mutually_exclusive_sporadic",
            [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 18, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC))

            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 18, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "overlapping_sporadic_on_boundary",
            [
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 15, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "neighbours",
            [
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 16, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 6, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 26, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "no_end_date_single",
            [
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), None),
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "no_end_date_multi",
            [
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), None),
                (datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 9, tzinfo=pytz.UTC),  datetime(2020, 1, 18, tzinfo=pytz.UTC)),
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 5, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 19, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "duplicates",
            [
                (datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
            ]}
        )
    ]
)
def test_multiple_periods_for_single_member_add_only(test_name, date_ranges, expected_outcome):
    """
    The responsibility of this test is to test that all possible ways of constructing and editing the membership
    of a composite's member behave as expected.

    :return:
    """
    global test_scope
    global single_member_codes
    global portfolio_groups_composite

    composite_code = str(uuid.uuid4())

    portfolio_groups_composite.create_composite(
        composite_scope=test_scope,
        composite_code=composite_code
    )

    [portfolio_groups_composite.add_composite_member(
        composite_scope=test_scope,
        composite_code=composite_code,
        member_scope=test_scope,
        member_code=single_member_codes[0],
        from_date=from_date,
        to_date=to_date
    ) for from_date, to_date in date_ranges]

    min_date = min([date_range[0] for date_range in date_ranges])

    try:
        max_date = max([date_range[1] for date_range in date_ranges if date_range[1] is not None])
    except ValueError:
        max_date = datetime(2020, 2, 27, tzinfo=pytz.UTC)

    response = portfolio_groups_composite.get_composite_members(
        composite_scope=test_scope,
        composite_code=composite_code,
        start_date=min_date-timedelta(days=1),
        end_date=max_date+timedelta(days=1),
        asat=datetime.now(pytz.UTC)
    )

    assert (response == expected_outcome)


@pytest.mark.parametrize(
    "test_name, date_ranges, expected_outcome",
    [
        (
            "remove_period_inside_add_period",
            [
                ("add", datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
                ("remove", datetime(2020, 1, 10, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 9, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 16, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "add_period_inside_remove_period",
            [
                ("remove", datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
                ("add", datetime(2020, 1, 10, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 10, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "add_remove_add",
            [
                ("add", datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
                ("remove", datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
                ("add", datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC))
            ]}
        ),
        (
            "add_remove_pattern_decreasing_range",
            [
                ("add", datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
                ("remove", datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 23, tzinfo=pytz.UTC)),
                ("add", datetime(2020, 1, 5, tzinfo=pytz.UTC), datetime(2020, 1, 21, tzinfo=pytz.UTC)),
                ("remove", datetime(2020, 1, 7, tzinfo=pytz.UTC), datetime(2020, 1, 19, tzinfo=pytz.UTC)),
                ("add", datetime(2020, 1, 9, tzinfo=pytz.UTC), datetime(2020, 1, 17, tzinfo=pytz.UTC)),
                ("remove", datetime(2020, 1, 11, tzinfo=pytz.UTC), datetime(2020, 1, 15, tzinfo=pytz.UTC)),
                ("add", datetime(2020, 1, 13, tzinfo=pytz.UTC), datetime(2020, 1, 13, tzinfo=pytz.UTC))
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 2, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 5, tzinfo=pytz.UTC), datetime(2020, 1, 6, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 9, tzinfo=pytz.UTC), datetime(2020, 1, 10, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 13, tzinfo=pytz.UTC), datetime(2020, 1, 13, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 16, tzinfo=pytz.UTC), datetime(2020, 1, 17, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 20, tzinfo=pytz.UTC), datetime(2020, 1, 21, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 24, tzinfo=pytz.UTC), datetime(2020, 1, 25, tzinfo=pytz.UTC)),
            ]}
        ),
        (
            "no_end_dates",
            [
                ("add", datetime(2020, 1, 1, tzinfo=pytz.UTC), None),
                ("remove", datetime(2020, 1, 3, tzinfo=pytz.UTC), None),
                ("add", datetime(2020, 1, 28, tzinfo=pytz.UTC), None)
            ],
            {f"{test_scope}_{single_member_codes[0]}": [
                (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 2, tzinfo=pytz.UTC)),
                (datetime(2020, 1, 28, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC))
            ]}
        )
    ]
)
def test_multiple_periods_for_single_member_add_and_remove(test_name, date_ranges, expected_outcome):
    """
    The responsibility of this test is to test that all possible ways of constructing and editing the membership
    of a composite's member behave as expected.

    :return:
    """
    
    global test_scope
    global single_member_codes
    global portfolio_groups_composite

    composite_code = str(uuid.uuid4())

    portfolio_groups_composite.create_composite(
        composite_scope=test_scope,
        composite_code=composite_code
    )

    [getattr(portfolio_groups_composite, f"{method}_composite_member")(
        composite_scope=test_scope,
        composite_code=composite_code,
        member_scope=test_scope,
        member_code=single_member_codes[0],
        from_date=from_date,
        to_date=to_date
    ) for method, from_date, to_date in date_ranges]

    min_date = min([date_range[1] for date_range in date_ranges])

    try:
        max_date = max([date_range[2] for date_range in date_ranges if date_range[2] is not None])
    except ValueError:
        max_date = datetime(2020, 2, 27, tzinfo=pytz.UTC)

    response = portfolio_groups_composite.get_composite_members(
        composite_scope=test_scope,
        composite_code=composite_code,
        start_date=min_date-timedelta(days=1),
        end_date=max_date+timedelta(days=1),
        asat=datetime.now(pytz.UTC)
    )

    assert (response == expected_outcome)

@pytest.mark.parametrize(
    "test_name, portfolio_group_code, date_ranges, expected_outcome",
    [
        (
            "prepopulated_no_changes",
            prepopulated_portfolio_group_code,
            [],
            {
                f"{test_scope}_{single_member_codes[0]}": [
                    (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC)),
                ],
                f"{test_scope}_{single_member_codes[1]}": [
                    (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC)),
                ],
                f"{test_scope}_{single_member_codes[2]}": [
                    (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC)),
                ]
            }
        ),
        (
            "prepopulated_deleted_no_changes",
            prepopulated_deleted_portfolio_group_code,
            [],
            {}
        ),
        (
            "prepopulated_with_subsequent_removal_of_members",
            prepopulated_portfolio_group_code,
            [
                ("remove", datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 28, tzinfo=pytz.UTC))
            ],
            {
                f"{test_scope}_{single_member_codes[0]}": [
                    (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 1, 2, tzinfo=pytz.UTC)),
                    (datetime(2020, 1, 29, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC))
                ],
                f"{test_scope}_{single_member_codes[1]}": [
                    (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC)),
                ],
                f"{test_scope}_{single_member_codes[2]}": [
                    (datetime(2020, 1, 1, tzinfo=pytz.UTC), datetime(2020, 2, 28, tzinfo=pytz.UTC)),
                ]
            }
        ),
        (
            "prepopulated_deleted_subsequent_addition_of_portfolio",
            prepopulated_deleted_portfolio_group_code,
            [
                ("add", datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 28, tzinfo=pytz.UTC))
            ],
            {
                f"{test_scope}_{single_member_codes[0]}": [
                    (datetime(2020, 1, 3, tzinfo=pytz.UTC), datetime(2020, 1, 28, tzinfo=pytz.UTC))                ]
            }
        ),
    ]
)
def test_prepopulated_group_as_composite(test_name, portfolio_group_code, date_ranges, expected_outcome):
    """
    The responsibility of this test is to ensure that a composite can be created with a pre-populated Portfolio Group,
    even one that has been created and then deleted again.

    :return:
    """
    global portfolio_groups_composite

    [getattr(portfolio_groups_composite, f"{method}_composite_member")(
        composite_scope=test_scope,
        composite_code=portfolio_group_code,
        member_scope=test_scope,
        member_code=single_member_codes[0],
        from_date=from_date,
        to_date=to_date
    ) for method, from_date, to_date in date_ranges]

    response = portfolio_groups_composite.get_composite_members(
        composite_scope=test_scope,
        composite_code=portfolio_group_code,
        start_date=datetime(2020, 1, 1, tzinfo=pytz.UTC),
        end_date=datetime(2020, 2, 28, tzinfo=pytz.UTC),
        asat=datetime.now(pytz.UTC)
    )

    assert (response == expected_outcome)
