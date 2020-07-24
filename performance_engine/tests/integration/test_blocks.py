import pytest
import uuid

from pds import PerformanceDataSet
from performance_sources import mock_src
from misc import *
from block_stores.block_store_in_memory import InMemoryBlockStore
from block_stores.block_store_structured_results import BlockStoreStructuredResults

from performance_engine.tests.utilities.environment import test_scope
from performance_engine.tests.utilities.api_factory import api_factory
from interfaces import IBlockStore

src = mock_src.MockSource('Set1')


def add_block(entity_scope: str, entity_code: str, bs: InMemoryBlockStore, sd, ed, ad) -> None:
    """
    Adds a block to a block store

    :param str entity_scope: The scope of the entity to add
    :param str entity_code: The code of the entity to add
    :param InMemoryBlockStore bs: The block store to add the block to
    :param sd: The effectiveAt start date
    :param ed: The effectiveAt end date
    :param ad: The asAt date

    :return: None
    """

    # Create a block using the provided dates
    b = PerformanceDataSet(from_date=sd, to_date=ed, asat=ad)

    global src

    # For each date and group (DataFrame)
    for d, g in src.get_perf_data(
            entity_scope=entity_scope,
            entity_code=entity_code,
            from_date=b.from_date,
            to_date=b.to_date,
            asat=b.asat
    ).groupby('date'):

        # Populate the block with each PerformanceDataPoint in chronological order
        b.add_values(date=d, data_source=g.apply(lambda r: (r['key'], r['mv'], r['net']), axis=1))

    # Add the populated block to the block store
    bs.add_block(entity_scope=entity_scope, entity_code=entity_code, block=b)

@pytest.mark.parametrize(
    "test_name, bs",
    [
        ("InMemoryBlockStore", InMemoryBlockStore()),
        ("StructuredResultsBlockStore", BlockStoreStructuredResults(api_factory=api_factory))
    ]
)
def test_single_block(test_name: str, bs: IBlockStore) -> None:
    """
    Test that find_blocks works as expected when there is a single block in the store

    :return: None
    """
    entity_code = str(uuid.uuid4())

    # Add a block to the block store
    add_block(test_scope, entity_code, bs, sd='2020-01-05', ed='2020-01-10', ad='2020-01-10')

    def pb(b: PerformanceDataSet) -> None:
        """
        Prints the salient dates of the block

        :param PerformanceDataPoint b: The block to print dates for

        :return: None
        """
        print(b.from_date, b.to_date, b.asat)

    # Outer range envelopes block |---[----]---------------------|
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-01',
        to_date='2020-02-01',
        asat='2020-01-10'
    )) == 1

    # Inner range within block [|--|]
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-06',
        to_date='2020-01-09',
        asat='2020-01-10'
    )) == 1

    # Partial coverage of start |---[|---]
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-01', to_date='2020-01-06', asat='2020-01-10')) == 1

    # Partial coverage includes start only |---([|)----]
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-01',
        to_date='2020-01-05',
        asat='2020-01-10'
    )) == 1

    # Partial coverage of end [--|-]--------------------|
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-08',
        to_date='2020-02-01',
        asat='2020-01-10'
    )) == 1

    # Partial coverage includes end only [----(|])--------------------|
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-10',
        to_date='2020-02-01',
        asat='2020-01-10'
    )) == 1

    # Before block |--|[----]
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-01',
        to_date='2020-01-04',
        asat='2020-01-10'
    )) == 0

    # After block [----]|----------------------|
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-11',
        to_date='2020-02-04',
        asat='2020-01-10'
    )) == 0

    # Asat time exception
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-01',
        to_date='2020-02-01',
        asat='2020-01-04'
    )) == 0


@pytest.mark.parametrize(
    "test_name, bs",
    [
            ("InMemoryBlockStore", InMemoryBlockStore()),
            ("StructuredResultsBlockStore", BlockStoreStructuredResults(api_factory=api_factory))
    ]
)
def test_multiple_blocks(test_name: str, bs: IBlockStore) -> None:
    """
    Test that find_blocks works as expected when there are multiple blocks in the store

    :return: None
    """
    entity_code = str(uuid.uuid4())

    # Add 3 mutually exclusive blocks to the block store [---][---][---]
    add_block(test_scope, entity_code, bs, sd='2020-01-01', ed='2020-01-05', ad='2020-01-05')
    add_block(test_scope, entity_code, bs, sd='2020-01-06', ed='2020-01-10', ad='2020-01-10')
    add_block(test_scope, entity_code, bs, sd='2020-01-11', ed='2020-01-15', ad='2020-01-15')

    # Range envelopes 3 blocks |[---][---][---]-------------|
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2019-12-31', to_date='2020-02-01', asat='2020-01-15')) == 3

    # Range is 3 blocks (|[)---][---][---(]|)
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-01', to_date='2020-01-15', asat='2020-01-15')) == 3

    # Range is 3 blocks with asat exclusion
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-01', to_date='2020-01-15', asat='2020-01-14')) == 2

    # Range covers 3 blocks partially [-|-][---][|--]
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-03', to_date='2020-01-12', asat='2020-01-15')) == 3

    # Range covers first 2 blocks partially [-|-][|--][---]
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-03', to_date='2020-01-07', asat='2020-01-15')) == 2

    # Range covers last 2 blocks partially [---][|--][-|-]
    assert len(bs.find_blocks(
        entity_scope=test_scope,
        entity_code=entity_code,
        from_date='2020-01-07', to_date='2020-01-13', asat='2020-01-15')) == 2


@pytest.mark.parametrize(
    "test_name, bs",
    [
            ("InMemoryBlockStore", InMemoryBlockStore()),
            ("StructuredResultsBlockStore", BlockStoreStructuredResults(api_factory=api_factory))
    ]
)
def test_get_previous_record_single(test_name: str, bs: IBlockStore) -> None:
    """
    Tests that get_previous_record on the block store works as expected with a single block

    :return: None
    """
    entity_code = str(uuid.uuid4())

    # Add a single block
    add_block(test_scope, entity_code, bs, sd='2020-01-01', ed='2020-01-05', ad='2020-01-05')

    # Can find first record in block with asat of block
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-02', asat='2020-01-05')
    assert o.date == as_date('2020-01-01')

    # Can find first record in block with asat later than block
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-02', asat='2020-01-06')
    assert o.date == as_date('2020-01-01')

    # Can't find record in the middle of a block with asat before
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-04', asat='2020-01-04')
    assert o is None
    
    # Can find record in the middle of a block with asat the same
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-04', asat='2020-01-05')
    assert o.date == as_date('2020-01-03')
    
    # Can find record in the middle of a block with asat after
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-04', asat='2020-01-06')
    assert o.date == as_date('2020-01-03')
    
    # Will find last record when searching after block
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-10', asat='2020-01-06')
    assert o.date == as_date('2020-01-05')

    # Cant find record when searching with block start on asat
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-01', asat='2020-01-05')
    assert o is None
    
    # Cant find record when searching with block start after asat
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2020-01-01', asat='2020-01-06')
    assert o is None
    
    # Cant find record when searching prior to block start on asat
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2019-01-01', asat='2020-01-05')
    assert o is None
    
    # Cant find record when searching prior to block start after asat
    o = bs.get_previous_record(
        entity_scope=test_scope,
        entity_code=entity_code,
        date='2019-01-01', asat='2020-01-06')
    assert o is None


@pytest.mark.parametrize(
    "test_name, bs",
    [
            ("InMemoryBlockStore", InMemoryBlockStore()),
            ("StructuredResultsBlockStore", BlockStoreStructuredResults(api_factory=api_factory))
    ]
)
def test_get_previous_record_multi(test_name: str, bs: IBlockStore):
    """
    Tests that get_previous_record on the block store works as expected with multiple blocks

    :return: None
    """
    entity_code = str(uuid.uuid4())

    # Add 6 blocks to the block store
    add_block(test_scope, entity_code, bs, sd='2020-01-01', ed='2020-01-05', ad='2020-01-05') # -[---]----------  Block #1
    add_block(test_scope, entity_code, bs, sd='2020-01-08', ed='2020-01-11', ad='2020-01-11') # --------[--]----  Gap between this and block #1
    add_block(test_scope, entity_code, bs, sd='2020-01-09', ed='2020-01-15', ad='2020-01-15') # ---------[-----]  Overlap with block #2
    add_block(test_scope, entity_code, bs, sd='2020-01-05', ed='2020-01-15', ad='2020-02-02') # -----[---------]  Overlap with block #1, #2, #3
    add_block(test_scope, entity_code, bs, sd='2019-12-31', ed='2020-01-15', ad='2020-02-03') # [--------------]  Envelope all earlier blocks

    # Find block #1 record with asat between blocks
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-04', asat='2020-01-16')
    assert o.date == as_date('2020-01-03')
    assert o.tmv == pytest.approx(781.95, rel=0.001)
    
    # Find block #5 record with asat after last block
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-04', asat='2020-02-16')
    assert o.date == as_date('2020-01-03')
    assert o.tmv == pytest.approx(3, rel=0.001)
    
    # Find block #1 last record when searching in the gap
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-07', asat='2020-01-12')
    assert o.date == as_date('2020-01-05')
    assert o.tmv == pytest.approx(831.14, rel=0.001)
    
    # Find block #1 last record when searching at left edge of the gap
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-06', asat='2020-01-12')
    assert o.date == as_date('2020-01-05')
    assert o.tmv == pytest.approx(831.14, rel=0.001)
    
    # Find block #1 last record when searching at right edge of the gap
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-08', asat='2020-01-12')
    assert o.date == as_date('2020-01-05')
    assert o.tmv == pytest.approx(831.14, rel=0.001)
    
    # asat 16th - pick up first block
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-04', asat='2020-01-16')
    assert o.date == as_date('2020-01-03')
    assert o.tmv == pytest.approx(781.95, rel=0.001)
    
    # asat 16th - pick up second block
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-09', asat='2020-01-16')
    assert o.date == as_date('2020-01-08')
    assert o.tmv == pytest.approx(925.79, rel=0.001)
    
    # asat 16th - pick up third block
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-10', asat='2020-01-16')
    assert o.date == as_date('2020-01-09')
    assert o.tmv == pytest.approx(1011.29, rel=0.001)
    
    # asat 2-Feb - pick up first block
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-03', asat='2020-02-02')
    assert o.date == as_date('2020-01-02')
    assert o.tmv == pytest.approx(752.67, rel=0.001)
    
    # asat 2-Feb - pick up first block edge case
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-05', asat='2020-02-02')
    assert o.date == as_date('2020-01-04')
    assert o.tmv == pytest.approx(802.3, rel=0.001)
    
    # asat 2-Feb - pick up fourth block edge case
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-06', asat='2020-02-02')
    assert o.date == as_date('2020-01-05')
    assert o.tmv == pytest.approx(2.0, rel=0.001)

    # asat 6-Feb - pick up fifth block lhs edge case
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-01', asat='2020-02-03')
    assert o.date == as_date('2019-12-31')
    assert o.tmv == pytest.approx(3.0, rel=0.001)

    # asat 6-Feb - pick up fifth block middle case
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-10', asat='2020-02-03')
    assert o.date == as_date('2020-01-09')
    assert o.tmv == pytest.approx(3.0, rel=0.001)

    # asat 6-Feb - pick up fifth block rhs edge case
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-15', asat='2020-02-03')
    assert o.date == as_date('2020-01-14')
    assert o.tmv == pytest.approx(3.0, rel=0.001)

    # asat 6-Feb - pick up fifth block after rhs
    o = bs.get_previous_record(test_scope, entity_code, date='2020-01-16', asat='2020-02-03')
    assert o.date == as_date('2020-01-15')
    assert o.tmv == pytest.approx(3.0, rel=0.001)
