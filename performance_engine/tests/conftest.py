import pytest


def pytest_addoption(parser):
    parser.addoption(
       "--recording",nargs='+', default=[], metavar='test-name', help = "Record results for listed tests")


@pytest.fixture
def recording(request):
    """
    This fixture is available to all tests in directories at or below the location of this file.
    """
    return request.config.getoption('--recording')
