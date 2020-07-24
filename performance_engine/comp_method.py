from __future__ import annotations
from typing import Dict

from pandas import Timestamp

from interfaces import ICompositeMethod
from pds import PerformanceDataPoint
from misc import *


class AssWt(ICompositeMethod):
    """
    This class is responsible for the logic of creating a composite using the asset weighted method. Each instance
    of this class is responsible for a single date. The asset weighted method involves computing a weighted average
    return from the member's of the composite.
    """
    start_date_offset = ONE_DAY

    def __init__(self, date: Timestamp):
        """
        :param Timestamp date: The date at which to calculate the composite
        """
        self.date = date
        self.wt_accum = 0.0
        self.accum = 0.0

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, date: Timestamp):
        self._date = date

    def result(self) -> Dict:
        """
        Using the accumulated values, return the performance for the date

        :return: Dict: The performance for the date
        """
        return {
            "date": self.date,
            "wt": self.wt_accum,
            "ror": self.accum/self.wt_accum}

    def accumulate(self, item: PerformanceDataPoint) -> AssWt:
        """
        Takes a PerformanceDataPoint and adds its weight and return to the accumulated totals

        :param PerformanceDataPoint item: The PerformanceDataPoint to accumulate

        :return: AssWt self: The instance of the class
        """
        self.wt_accum += item.weight
        self.accum += item.weight * item.ror
        return self


class EqWt(ICompositeMethod):
    """
    This class is responsible for the logic of creating a composite using the equal weighted method. Each instance
    of this class is responsible for a single date. The equal weighted method involves computing an average return
    from the members of the composite.
    """
    start_date_offset = ONE_DAY

    def __init__(self, date: Timestamp):
        """
        :param Timestamp date: The date at which to calculate the composite
        """
        self.date = date
        self.wt_accum = 0.0
        self.accum = 0.0

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, date: Timestamp):
        self._date = date

    def result(self):
        """
        Using the accumulated values, return the performance for the date

        :return: Dict: The performance for the date
        """
        return {
            "date": self.date,
            "wt": self.wt_accum,
            "ror": self.accum/self.wt_accum}

    def accumulate(self, item: PerformanceDataPoint) -> EqWt:
        """
        Takes a PerformanceDataPoint and adds its return and a constant weight to the accumulated totals

        :param PerformanceDataPoint item: The PerformanceDataPoint to accumulate

        :return: EqWt self: The instance of the class
        """
        self.wt_accum += 1.0
        self.accum += item.ror
        return self


class Agg(ICompositeMethod):
    """
    This class is responsible for the logic of producing daily performance fora  composite using the aggregation method.
    Each instance of this class is responsible for a single date.
    """
    start_date_offset = NO_DAYS

    def __init__(self, date: Timestamp):
        """
        :param Timestamp date: The date at which to calculate the composite
        """
        self._date = date
        self.mv_accum = 0.0
        self.net_accum = 0.0

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, date: Timestamp):
        self._date = date

    def result(self) -> Dict:
        """
        Using the accumulated values, return the performance for the date

        :return: Dict: The performance for the date
        """
        return {
            "key": "TOTAL",
            "date": self.date,
            "mv": self.mv_accum,
            "net": self.net_accum}

    def accumulate(self, item: PerformanceDataPoint) -> Agg:
        """
        Takes a PerformanceDataPoint and adds its total market value and flows to the accumulated totals

        :param PerformanceDataPoint item: The PerformanceDataPoint to accumulate

        :return: Agg self: The instance of the class
        """
        self.mv_accum += item.tmv
        # If the weight (typically beginning of day market value is 0, then this is all flows)
        self.net_accum += item.tmv if item.weight == 0 else item.flows
        return self
