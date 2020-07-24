from pds import PerformanceDataSet


def test_lazy_loading():
    # Keep track of how many times the loader is called.
    counter = 0

    def dummy_loader():
        nonlocal counter
        counter += 1
        return []

    pds = PerformanceDataSet('2018-03-05','2018-03-19','2018-03-19',loader=dummy_loader)

    # Before we get the data points, counter should be 0
    assert counter == 0
    r = pds.get_data_points()

    # After  get the data points, counter should be 1
    assert counter == 1

    r2 = pds.get_data_points()

    # If we get the points again, the counter is still 1
    assert counter == 1

    # and it is the same object we got earlier
    assert r is r2

