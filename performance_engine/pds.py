from typing import Iterator, Callable, Dict

import numpy as np
import pandas as pd

from misc import as_dates


class AttributionDataPoint:
    """
    This class represents a single data point of attribution data, this is the lowest level data point.
    """
    @as_dates
    def __init__(self, date, key, bod, eod, flows):
        """
        :param date: The date of the data point
        :param str key: The key for the data point
        :param float bod: The beginning of day market value
        :param float eod: The end of day market value
        :param float flows: The flows for the day
        """
        self.date = date
        self.key = key
        self.mv = eod
        self.flows = flows
        self.pnl = eod - bod - flows

    def __eq__(self, other):
        if not isinstance(other, AttributionDataPoint):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.__dict__ == other.__dict__


class PerformanceDataPoint:
    """
    This class represents a single data point of performance data. This is made up of one or more AttributionDataPoint
    """
    @as_dates
    def __init__(self, date, tmv=0, flows=0, weight=0, data=None, pnl=None, ror=None, cum_fctr = None, cum_flow=None,
                 cnt=None, sum_ror=None, sum_ror_sqr=None):
        """
        :param date: The effectiveAt date of the data point
        """
        self.date = date
        self.tmv = tmv
        self.flows = flows
        self.weight = weight
        # A dictionary of AttributionDataPoint
        self.data = data
        self.pnl = pnl
        self.ror = ror
        self.cum_fctr = cum_fctr
        self.cum_flow = cum_flow
        self.cnt = cnt
        self.sum_ror = sum_ror
        self.sum_ror_sqr = sum_ror_sqr

    def from_values(self, data_source: Iterator, previous=None):
        """
        From a data source containing one or more AttributionDataPoint and an optional previous PerformanceDataPoint
        construct the current PerformanceDataPoint

        :param Iterator data_source: The iterable containing data for one or more AttributionDataPoint
        :param PerformanceDataPoint previous: The previous PerformanceDataPoint

        :return: PerformanceDataPoint self: The instance of the PerformanceDataPoint
        """

        def make_dp():
            """
            From a data source create a generator which yields all the AttributionDataPoint for a PerformanceDataPoint

            :return: Iterator[str, AttributionDataPoint]: A generator returning each AttributionDataPoint and its key for
            a PerformanceDataPoint
            """
            nonlocal data_source
            for r in data_source:
                # Create an AttributionDataPoint
                adp = AttributionDataPoint(date=self.date, key=r[0], bod=r[1], eod=r[2], flows=r[3])
                # Increase the market value of the PerformanceDataPoint by the market value of the AttributionDataPoint
                self.tmv += adp.mv
                # Same for flows
                self.flows += adp.flows
                yield adp.key, adp

        # Cast the generator to a dictionary of AttributionDataPoint
        self.data = dict(make_dp())
        # Sum the profit and loss over all the AttributionDataPoint
        self.pnl = sum([i.pnl for i in self.data.values()])
        # If possible set the beginning of day market value from the previous PerformanceDataPoint
        bod = 0.0 if previous is None else previous.tmv
        # Get an alternative profit and loss number using the beginning of day market value
        pnl = (self.tmv - bod - self.flows)
        # If the two profit and loss numbers do not match throw
        if np.round(pnl-self.pnl, 2)+0.0 != 0.0:
           # Should never get here. If so, let's take a look ...
           import pdb; pdb.set_trace()
           print("Flow calculation unexpected error")

        # Calculate the rate of return using 0 if there is no starting point
        self.ror = pnl/bod if bod != 0 else 0
        self.weight = bod

        # Construct the geometrically linked metrics based on whether or not there is a previous PerformanceDataPoint
        if previous:
            # Cumulative factor for multiplying a consecutive string of returns
            self.cum_fctr = (1 + self.ror) * previous.cum_fctr
            # Cumulative flow (i.e. inflows and outflows)
            self.cum_flow = self.flows + previous.cum_flow
            # Count of number of consecutive returns
            self.cnt = previous.cnt + 1
            # A sum of the rate of return
            self.sum_ror = self.ror + previous.sum_ror
            # The squared sum of the rate of return
            self.sum_ror_sqr = self.ror * self.ror + previous.sum_ror_sqr
        else:
            self.cum_fctr = (1 + self.ror)
            self.cum_flow = self.flows
            self.cnt = 0
            self.sum_ror = self.ror
            self.sum_ror_sqr = self.ror * self.ror

        return self

    def get_mv(self,key):
        """
        Returns the market value of an AttributionDataPoint based on its key

        :param str key: The key of the AttributionDataPoint

        :return: float: The market value of the requested AttributionDataPoint
        """
        adp = self.data.get(key)
        return 0.0 if adp is None else adp.mv

    def from_returns(self, weight, ror, previous=None):
        """
        From the returns and an optional previous PerformanceDataPoint construct the current PerformanceDataPoint

        :param float tmv: The total market value of the PerformanceDataPoint
        :param float ror: The rate of return
        :param PerformanceDataPoint previous: The previous PerformanceDataPoint

        :return: PerformanceDataPoint self: The instance of the PerformanceDataPoint
        """

        self.weight = weight
        self.ror = ror
        self.cum_flow = 0.0

        if previous:
           self.cum_fctr = (1 + self.ror) * previous.cum_fctr
           self.cnt = previous.cnt + 1
           self.sum_ror = self.ror + previous.sum_ror
           self.sum_ror_sqr = self.ror * self.ror + previous.sum_ror_sqr
        else:
           self.cum_fctr = (1 + self.ror)
           self.cnt = 0
           self.sum_ror = self.ror
           self.sum_ror_sqr = self.ror * self.ror
        return self

    def __eq__(self, other):
        if not isinstance(other, PerformanceDataPoint):
            # don't attempt to compare against unrelated types
            return NotImplemented

        def round_dict_values(dict_to_round: Dict):
            """
            Rounds the floats to 6 decimal places for comparison of two dictionaries

            :param Dict dict_to_round: The dictionary to round down the values of

            :return: Dict: The dictionary with the values rounded
            """
            for key, value in dict_to_round.items():
                if type(value) == float:
                    dict_to_round[key] = round(value, 6)

            return dict_to_round

        return round_dict_values(self.__dict__) == round_dict_values(other.__dict__)


class PerformanceDataSet:
    """
    This class is represents a block of performance data.
    """
    version = "0.0.1"

    @as_dates
    def __init__(self, from_date, to_date, asat=None, data_points=None, previous: PerformanceDataPoint = None, loader: Callable = None):
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
        if data_points is None:
            self.data_points = []
        else:
            self.data_points = data_points
        self.latest_data_point = previous

        if loader != None:
           org_data_points=self.get_data_points
           def lazy_loader():
               self.data_points = loader()
               self.get_data_points=org_data_points
               return self.get_data_points()

           self.get_data_points=lazy_loader

    @as_dates
    def add_values(self, date, data_source: pd.Series):
        """
        Adds a single PerformanceDataPoint to the block store from the market value and flows. This function
        MUST be called in chronological order along the effectiveAt axis, otherwise the PerformanceDataPoints will
        not be geometrically linked.

        :param date: The date of the PerformanceDataPoint
        :param pd.Series data_source: The series containing the market value and flow data used to
        construct the PerformanceDataPoint from a set of AttributionDataPoint. Each value in the Series
        should be a tuple containing (unique_key_for_attribution_data_point, market_value, flows).

        :return: PerformanceDataSet self: The instance of the PerformanceDataSet class which has had
        the PerformanceDataPoint added to it
        """

        # Defines a lambda function to use to get the market value of an AttributionDataPoint
        if self.latest_data_point is None:
            get_bod = lambda r: 0.0
        else:
            get_bod = lambda r: self.latest_data_point.get_mv(r)

        def knit():
            """
            Converts the Pandas Series into a an iterable, enriching each row with the beginning of day market
            value

            :return: iterable(float, float, float, float): The data to populate an AttributionDataPoint
            """
            for row in data_source:
                yield row[0], get_bod(row[0]), row[1], row[2]

        # Using the latest PerformanceDataPoint construct the new latest PerformanceDataPoint and override this value
        self.latest_data_point = PerformanceDataPoint(date).from_values(
            data_source=knit(),
            previous=self.latest_data_point)

        # Add the new PerformanceDataPoint to the data points for the PerformanceDataSet
        self.data_points.append(self.latest_data_point)
        return self

    @as_dates
    def add_returns(self, date, weight, ror):
        self.latest_data_point = PerformanceDataPoint(date).from_returns(weight, ror, self.latest_data_point)
        self.data_points.append(self.latest_data_point)
        return self

    def get_data_points(self):
        return self.data_points

    def __eq__(self, other):
        if not isinstance(other, PerformanceDataSet):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.__dict__ == other.__dict__
