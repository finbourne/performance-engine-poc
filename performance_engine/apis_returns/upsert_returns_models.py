from typing import List, Dict

from misc import as_dates


class AttributionDataPointRequest:
    """
    This is the model used when upserting an AttributionDataPoint into a block store
    """
    @as_dates
    def __init__(self, date, key, mv, flows):
        self.date = date,
        self.key = key,
        self.mv = mv
        self.flows = flows


class PerformanceDataPointRequest:
    """
    This is the model used when upserting a PerformanceDataPoint into a block store
    """
    @as_dates
    def __init__(self, date, ror: float, weight: float = 0):

        self.date = date
        self.weight = weight
        self.ror = ror


class PerformanceDataPointResponse:
    """
    This class is the response when retrieving a PerformanceDataPoint
    """
    @as_dates
    def __init__(self, date, weight: float = 0, flows: float = 0, pnl: float = None, ror: float = None,
                 data: List[AttributionDataPointRequest] = None, **kwargs):

        self.date = date
        self.weight = weight
        self.flows = flows
        self.pnl = pnl
        self.ror = ror
        self.data = data

    def __eq__(self, other):
        if not isinstance(other, PerformanceDataPointResponse):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.__dict__ == other.__dict__


class PerformanceDataSetRequest:
    """
    This is the model used when upserting a PerformanceDataSet into a block store
    """
    @as_dates
    def __init__(self, data_points: List[PerformanceDataPointRequest], start_date=None, end_date=None):
        self.data_points = data_points
        self.start_date = start_date
        self.end_date = end_date


class PerformanceDataSetResponse:
    """
    This class is what is returned when querying a PerformanceDataSet
    """
    @as_dates
    def __init__(self, from_date, to_date, asat=None, data_points=None, previous: PerformanceDataPointResponse = None,
                 **kwargs):
        """
        :param from_date: The beginning of the block in effectiveAt time
        :param to_date: The end of the block in effectiveAt time
        :param asat: The asAt time of the block
        :param PerformanceDataPoint previous: The most recent PerformanceDataPoint in effectiveAt time
        :param Callable loader: A loader to load the PerformanceDataSet
        """
        self.from_date = from_date
        self.to_date = to_date
        self.asat = asat
        self.data_points = data_points
        self.latest_data_point = previous

    def __eq__(self, other):
        if not isinstance(other, PerformanceDataSetResponse):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.__dict__ == other.__dict__


class UpsertReturnsResponse:
    """
    This is the class returned by when retrieving returns
    """
    @as_dates
    def __init__(self, successes: Dict[str, PerformanceDataSetResponse], failures):
        self.values = successes
        self.failures = failures
        self.links = None

    def __eq__(self, other):
        if not isinstance(other, UpsertReturnsResponse):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.__dict__ == other.__dict__
