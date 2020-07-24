from __future__ import annotations
import abc
from typing import List, Dict, Tuple

from pandas import Timestamp, DataFrame

from misc import as_dates
from pds import PerformanceDataSet, PerformanceDataPoint


class IBlockStore(metaclass=abc.ABCMeta):
    """
    This acts as the interface for the Block Store. The block store is responsible for storing and finding
    each PerformanceDataSet block.
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'blocks') and
                hasattr(subclass, 'get_blocks') and
                callable(subclass.get_blocks) and
                hasattr(subclass, 'add_block') and
                callable(subclass.add_block) and
                hasattr(subclass, 'find_blocks') and
                callable(subclass.find_blocks) and
                hasattr(subclass, 'get_previous_record') and
                callable(subclass.get_previous_record) and
                hasattr(subclass, 'get_first_date') and
                callable(subclass.get_first_date)
                )

    @property
    @abc.abstractmethod
    def blocks(self):
        """
        Information on the blocks to be used by get_blocks in returning the blocks in their entirity
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_blocks(self, entity_scope: str, entity_code: str, performance_scope: str = None) -> List[PerformanceDataSet]:
        """
        This is used to get all blocks from the BlockStore.

        :param str entity_scope: The scope of the entity to get blocks for. The meaning of this is dependent upon
        the implementation
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param str performance_scope: The scope to use in the BlockStore, the meaning of this is dependent upon the
        implementation

        :return: List[PerformanceDataSet]: The blocks contained in the BlockStore
        """
        raise NotImplementedError

    @as_dates
    @abc.abstractmethod
    def add_block(self, entity_scope: str, entity_code: str, block: PerformanceDataSet,
                  performance_scope: str = None) -> PerformanceDataSet:
        """
        This adds a block to the BlockStore.

        :param str entity_scope: The scope of the entity to get blocks for. The meaning of this is dependent upon
        the implementation
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param PerformanceDataSet block: The block to add to the BlockStore
        :param str performance_scope: The scope of the BlockStore to use, the meaning of this depends on the implementation

        :return: PerformanceDataSet block: The block that was added to the BlockStore along with the asAt time of
        the operation
        """
        raise NotImplementedError

    @as_dates
    @abc.abstractmethod
    def find_blocks(self, entity_scope: str, entity_code: str, from_date: Timestamp, to_date: Timestamp, asat: Timestamp,
                    performance_scope: str = None) -> List[PerformanceDataSet]:
        """
        Returns all the PerformanceDataSet blocks in a given bi-temporal period

        :param str entity_scope: The scope of the entity to get blocks for. The meaning of this is dependent upon
        the implementation
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param Timestamp from_date: The effectiveAt start date of the period
        :param Timestamp to_date: The effectiveAt end date of the period
        :param Timestamp asat: The asAt date of the period
        :param str performance_scope: The scope of the block store to use, its meaning is dependent on the block store implementation

        :return: List[PerformanceDataSet]: The list of relevant PerformanceDataSet blocks
        """
        raise NotImplementedError

    @as_dates
    def get_previous_record(self, entity_scope: str, entity_code: str, date: Timestamp,
                            asat: Timestamp, performance_scope: str = None) -> PerformanceDataPoint:
        """
        Find the block that precedes the given bi-temporal date.

        :param str entity_scope: The scope of the entity to get blocks for. The meaning of this is dependent upon
        the implementation
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param Timestamp date: The effectiveAt date
        :param Timestamp asat: The asAt date
        :param str performance_scope: The scope of the block store to use, its meaning is dependent on the block store implementation

        :return: PerformanceDataPoint latest: The latest performance data point in the preceding block
        """

        raise NotImplementedError

    def get_first_date(self, entity_scope: str, entity_code: str, performance_scope: str = None) -> Timestamp:
        """
        Finds the earliest state in a set of blocks

        :param str entity_scope: The scope of the entity to get blocks for. The meaning of this is dependent upon
        the implementation
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param str performance_scope: The scope of the block store to use, its meaning is dependent on the block store implementation

        :return: Timestamp: The earliest date or None of this can not be determined
        """
        raise NotImplementedError


class IPerformanceSource(metaclass=abc.ABCMeta):
    """
    This acts as the interface for the Performance Source classes. A source is a source of performance data i.e. market
    value and flows.
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'get_perf_data') and
                callable(subclass.get_perf_data)
                )

    @abc.abstractmethod
    def get_perf_data(self, entity_scope: str, entity_code: str, start_date: Timestamp, end_date: Timestamp, asat: Timestamp, **kwargs) -> DataFrame:
        """
        Gets performance data and returns it in a DataFrame

        :param str entity_scope: The scope of the entity to get performance data for, the meanings of this is dependent
        upon the implementation
        :param str entity_code: The code of the entity to get performance data for, the meaning of this is dependent
        upon the implementation. Together with the entity_scope it uniquely identifies the entity
        :param Timestamp start_date: The start datetime of the performance data
        :param Timestamp end_date: The end datetime of the performance data
        :param Timestamp asat: The asAt datetime of the performance data

        :return: DataFrame: The dataframe containing the Performance data
        """
        raise NotImplementedError


class ICompositeMethod(metaclass=abc.ABCMeta):
    """
    This acts as the interface for the Performance Source classes. A source is a source of performance data i.e. market
    value and flows.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'result') and
                callable(subclass.result) and
                hasattr(subclass, 'accumulate') and
                callable(subclass.accumulate) and
                hasattr(subclass, 'date')
                )

    @property
    @abc.abstractmethod
    def date(self):
        """
        The date for which to calculate performance for the composite
        """
        raise NotImplementedError

    @abc.abstractmethod
    def result(self) -> Dict:
        """
        Using the accumulated values, return the performance for the date

        :return: Dict: The performance for the date
        """
        raise NotImplementedError

    @abc.abstractmethod
    def accumulate(self, item: PerformanceDataPoint) -> ICompositeMethod:
        """
        Takes a PerformanceDataPoint and adds its weight and return to the accumulated totals

        :param PerformanceDataPoint item: The PerformanceDataPoint to accumulate

        :return: ICompositeMethod self: The instance of the class
        """
        raise NotImplementedError


class IComposite(metaclass=abc.ABCMeta):
    """
    The responsibility of this class is to generate date ranges for inclusing in composites
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'add_composite_member') and
                callable(subclass.add_composite_member) and
                hasattr(subclass, 'remove_composite_member') and
                callable(subclass.remove_composite_member) and
                hasattr(subclass, 'get_composite_members') and
                callable(subclass.get_composite_members) and
                hasattr(subclass, 'create_composite') and
                callable(subclass.create_composite)
                )

    @abc.abstractmethod
    def add_composite_member(self, composite_scope: str, composite_code: str, member_scope: str, member_code: str,
                             from_date: Timestamp, to_date: Timestamp) -> Timestamp:
        """
        This function is responsible for adding a member to a composite over an effectiveAt range

        :param str composite_scope: The scope of the composite. This is specific to the implementation
        :param str composite_code: The code of the composite. This is specific to the implementation. Along with the
        composite_scope it should uniquely identify the composite.
        :param str member_scope: The scope of the member. This is specific to the implementation.
        :param str member_code: The code of the member. This is specific to the implementation. Along with the
        member_scope it should uniquely identify the member.
        :param Timestamp from_date: The start date (inclusive) from which this member is part of the composite
        :param Timestamp to_date: The end date (inclusive) from which this member is part of the composite

        :return: Timestamp: The asAt date at which the member was added to the composite.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def remove_composite_member(self, composite_scope: str, composite_code: str, member_scope: str, member_code: str,
                                from_date: Timestamp, to_date: Timestamp) -> Timestamp:
        """
        This function is responsible for removing a member from a composite over an effectiveAt range

        :param str composite_scope: The scope of the composite. This is specific to the implementation
        :param str composite_code: The code of the composite. This is specific to the implementation. Along with the
        composite_scope it should uniquely identify the composite.
        :param str member_scope: The scope of the member. This is specific to the implementation.
        :param str member_code: The code of the member. This is specific to the implementation. Along with the
        member_scope it should uniquely identify the member.
        :param Timestamp from_date: The start date (inclusive) from which this member is not part of the composite
        :param Timestamp to_date: The end date (inclusive) from which this member is not part of the composite

        :return: Timestamp: The asAt date at which the member was removed from the composite.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_composite_members(self, composite_scope: str, composite_code: str, start_date: Timestamp,
                              end_date: Timestamp, asat: Timestamp) -> Dict[str, List[Tuple[Timestamp, Timestamp]]]:
        """
        This function is responsible for returning the composite members and the effectiveAt date ranges
        for which they are members of the composite for a given bi-temporal period.

        It should return a dictionary keyed by the id for each member of the composite and a list of effectiveAt
        date ranges (inclusive) for which this member is part of the composite.

        :param str composite_scope: The scope of the composite. This is specific to the implementation
        :param str composite_code: The code of the composite. This is specific to the implementation. Along with the
        composite_scope it should uniquely identify the composite.
        :param Timestamp start_date: The start date (inclusive) from which this member is not part of the composite
        :param Timestamp end_date: The end date (inclusive) from which this member is not part of the composite
        :param Timestamp asat: The asAt date at which to retrieve the members of the composite

        :return: Dict[str, List[Tuple[Timestamp, Timestamp]]]: The members of the composite and the date ranges that
        they were members of the composite
        """
        raise NotImplementedError

    @abc.abstractmethod
    def create_composite(self, composite_scope: str, composite_code: str) -> Timestamp:
        """
        This function is responsible for creating a composite

        :param str composite_scope: The scope of the composite. This is specific to the implementation
        :param str composite_code: The code of the composite. This is specific to the implementation. Along with the
        composite_scope it should uniquely identify the composite.

        :return: Timestamp: The asAt datetime at which the composite was created
        """
        raise NotImplementedError
