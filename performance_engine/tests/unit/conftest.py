import pytest

def pytest_addoption(parser):
    parser.addoption(
       "--recording",nargs='+', default=[], metavar='test-name', help = "Record results for listed tests")

@pytest.fixture
def recording(request):
    return request.config.getoption('--recording')
