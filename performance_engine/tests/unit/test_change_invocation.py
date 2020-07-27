import pytest

from misc import *
from block_stores.block_store_in_memory import InMemoryBlockStore
from performance_engine.perf import Performance
from performance_sources import mock_src

src = mock_src.MockSource('Set1')


@as_dates
def dates(*args):
    """
    Helper function to convert the provided arguments into a list of dates

    :return: list: The list of dates
    """
    return list(args)


class ChangeCalled(Exception):
    """
    Testing exception - will be thrown by a call to get_changes
    """
    def __init__(self, args):
        self.args = args


def change_thrower(*args) -> None:
    """
    A function that captures its arguments and throws a ChangeCalled Exception

    :return: None
    """
    raise ChangeCalled(args)


# Make any calls to get_changes throw a ChangeCalled exception
src.get_changes = change_thrower

# These tests are to make sure that get_performance() will call get_changes() with the 
# correct parameters.

# We mock get_changes() to throw an exception
# We will catch that exception and check that it is as expected


def test_get_changes_not_called_if_block_exists():

    global src
    # Create an instance of the Performance class for generating reports
    prf = Performance(entity_scope="test", entity_code="get_changes", src=src, block_store=InMemoryBlockStore())

    # Create a block - as an initial block, get_changes will not be called. No exception raised.
    p = list(prf.get_performance(
        locked=True,
        start_date='2019-12-31',
        end_date='2020-01-05',
        asat='2020-01-05',
        create=True)
    )
    assert len(p) == 6

    # Subsequent calls can reuse the original block - no exception should be thrown

    # Get performance for the exact same period as the original block
    p = list(prf.get_performance(
        locked=True,
        start_date='2019-12-31',
        end_date='2020-01-05',
        asat='2020-01-05')
    )
    assert len(p) == 6

    # Get performance for a subset of the original block with the start trimmed
    p = list(prf.get_performance(
        locked=True,
        start_date='2020-01-03',
        end_date='2020-01-05',
        asat='2020-01-05')
    )
    assert len(p) == 3

    # Get performance for a subset of the original block with the end trimmed
    p = list(prf.get_performance(
        locked=True,
        start_date='2019-12-31',
        end_date='2020-01-04',
        asat='2020-01-05'))
    assert len(p) == 5

    # Get performance for a subset of the original block with both the ends trimmed
    p = list(prf.get_performance(
        locked=True,
        start_date='2020-01-03',
        end_date='2020-01-04',
        asat='2020-01-05')
    )
    assert len(p) == 2


testcases = {
    "Simple-follow-on": [
        ('2019-12-31', '2020-01-10', '2020-01-10'),
        dates('2020-01-05', '2020-01-06', '2020-01-10')
    ],
    "Repeat-later-asat": [
        ('2019-12-31', '2020-01-05', '2020-01-10'),
        dates('2020-01-05', '2020-01-06', '2020-01-10')
    ],
    "Narrow-later-asat": [
        ('2020-01-02', '2020-01-03', '2020-01-10'),
        dates('2020-01-05', '2020-01-06', '2020-01-10')
    ],
    "Wider-later-asat": [
        ('2019-12-31', '2020-01-10', '2020-01-10'),
        dates('2020-01-05', '2020-01-06', '2020-01-10')
    ]
}


@pytest.mark.parametrize(
    "scenario", [
        "Simple-follow-on",
        "Repeat-later-asat",
        "Narrow-later-asat",
        "Wider-later-asat"
    ]
)
def test_get_change_parameters_are_as_expected(scenario):
    # These will test that the logic for unlocked queries is correct
    global testcases
    global src

    scenario = testcases[scenario]
    prf = Performance(entity_scope="Test", entity_code="GetChangesParams", src=src, block_store=InMemoryBlockStore())

    # Create an initial block
    prf.get_performance(
        locked=True,
        start_date='2019-12-31',
        end_date='2020-01-05',
        asat='2020-01-06',
        create=True
    )

    with pytest.raises(ChangeCalled) as cc:
        prf.get_performance(False, *scenario[0])

    for actual, expected, field in zip(
        cc.value.args[2:],
        scenario[1],
        ['last_eff_date', 'last_asat_date', 'required_asat_date']
    ):
        assert actual == expected, field


def test_change_parameters_called_when_outside_locked_period():

    global src
    prf = Performance(entity_scope="Test", entity_code="GetChangesParams", src=src, block_store=InMemoryBlockStore())

    # Create an initial block
    prf.get_performance(True,'2019-12-31','2020-01-05','2020-01-06',create=True)

    # Query locked performance, crossing the boundary
    with pytest.raises(ChangeCalled) as cc:
        prf.get_performance(True,'2020-01-02','2020-01-10','2020-01-10')

    assert cc.value.args[2] == as_date('2020-01-05')
    assert cc.value.args[3] == as_date('2020-01-06')
    assert cc.value.args[4] == as_date('2020-01-10')


def test_change_parameters_not_called_when_inside_locked_period():
    prf = Performance(entity_scope="Test", entity_code="GetChangesParams", src=src, block_store=InMemoryBlockStore())

    # Create an initial block
    prf.get_performance(True,'2019-12-31','2020-01-05','2020-01-06',create=True)

    # Query locked performance within the boundary
    r = prf.get_performance(True,'2020-01-02','2020-01-03','2020-01-10')

    # No exception thrown means we will get here
    assert r is not None


def test_change_parameters_called_when_creating_locked_period():
    prf = Performance(entity_scope="Test", entity_code="GetChangesParams", src=src, block_store=InMemoryBlockStore())

    # Create an initial block
    prf.get_performance(True,'2019-12-31','2020-01-05','2020-01-06',create=True)

    # Query locked performance, within the boundary, but with create=True
    with pytest.raises(ChangeCalled) as cc:
        prf.get_performance(True,'2020-01-02','2020-01-05','2020-01-10',create=True)

    assert cc.value.args[2] == as_date('2020-01-05')
    assert cc.value.args[3] == as_date('2020-01-06')
    assert cc.value.args[4] == as_date('2020-01-10')
