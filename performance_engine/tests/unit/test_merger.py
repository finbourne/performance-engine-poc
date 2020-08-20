from typing import Dict

from merge import Merger


def v(idx, value) -> Dict:
    """
    Takes two arguments and joins them as values for different keys in a dictionary

    :param idx: The value for the 'idx' key
    :param value: The value for the 'val' key

    :return: Dict: Dictionary containing the arguments as values
    """
    return {
        'idx': idx,
        'val': value
    }


s1 = [v(1, 10), v(2, 20), v(3, 30), v(4, 40)]  # uniform range
s2 = [v(1, 11), v(2, 22), v(3, 33), v(4, 44)]  # uniform range
s3 = [v(1, 12), v(2, 24), v(3, 36), v(4, 48)]  # uniform range

s4 = [v(0, 100)]  # prior
s5 = [v(5, 500)]  # after

s6 = [v(3, -99), v(4, -131)]  # inside


def test_empty_merge() -> None:
    """
    Test that calling a merge on a Merger with no iterators returns an empty iterator

    :return: None
    """
    m = Merger(key_fn=lambda v: v['idx'])
    assert len(list(m.merge())) == 0


def test_uniform_merge() -> None:
    """
    Tests that a set of uniformly increasing iterables can be merged as expected

    :return: None
    """
    m = Merger(key_fn=lambda v: v['idx'])

    # Add 3 iterables to the Merger
    m.include('S1', s1)
    m.include('S2', s2)
    m.include('S3', s3)

    results = list(m.merge())

    # Ensure that there are 4 results corresponding to universe of keys 1 through 4
    assert len(results) == 4

    for i in range(4):
        # Handle test cases using 1 as first index instead of 0
        expected = i + 1
        # Check that the index is correct
        assert results[i][0] == expected
        # Check that there are the correct number of values for the index
        got = results[i][1]
        assert len(got) == 3
        # The total should match the formula
        assert sum([v['val'] for v in got]) == expected * 30 + expected * 3


def test_sparse_merge():
    m = Merger(lambda v : v['idx'])
    m.include('S1',s1)
    m.include('S2',s2)
    m.include('S3',s3)
    m.include('S4',s4)
    m.include('S5',s5)
    m.include('S6',s6)

    i = iter(m.merge())

    for idx,cnt,tot in [
            (0,1,100),
            (1,3,33),
            (2,3,66),
            (3,4,0),
            (4,4,1),
            (5,1,500)]:
        merged = next(i)

        assert idx == merged[0]
        assert len(merged[1]) == cnt
        assert sum([v['val'] for v in merged[1]]) == tot

    assert next(i,24106) == 24106 
