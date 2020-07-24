from collections import defaultdict
from datetime import datetime
from typing import Dict, List

from pandas import Timestamp
import pytz

from interfaces import IBlockStore
from misc import as_dates
from pds import PerformanceDataPoint, PerformanceDataSet


class InMemoryBlockStore(IBlockStore):
    """
    This acts an in memory Block Store. The block store is responsible for storing and finding
    each PerformanceDataSet block.
    """
    def __init__(self, blocks: Dict[str, List] = None):
        """
        :param blocks: The blocks contained in the InMemoryBlockStore
        """
        self._blocks = defaultdict(list)

        if blocks is not None:
            self._blocks.update(blocks)

    @property
    def blocks(self):
        return self._blocks

    @blocks.setter
    def blocks(self, blocks):
        self._blocks = blocks

    @staticmethod
    def _create_id_from_scope_code(scope: str, code: str) -> str:
        """
        Creates an id from a scope and code

        :param scope: The scope
        :param code: The code

        :return: str: The id created from the scope and code.
        """
        return f"{scope}_{code}"

    def get_blocks(self, entity_scope: str, entity_code: str, performance_scope: str = None) -> List[PerformanceDataSet]:
        """
        This is used to get all blocks from the BlockStore for the specified entity.

        :param str entity_scope: The scope of the entity to get blocks for.
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param str performance_scope: The scope to use in the BlockStore. This has no meaning and is not implemented in
        the InMemory implementation.

        :return: List[PerformanceDataSet]: The blocks contained in the BlockStore
        """
        entity_id = self._create_id_from_scope_code(entity_scope, entity_code)
        return self.blocks[entity_id]

    @as_dates
    def add_block(self, entity_scope: str, entity_code: str, block: PerformanceDataSet,
                  performance_scope: str = None) -> PerformanceDataSet:
        """
        This adds a block to the BlockStore for the specified entity.

        :param str entity_scope: The scope of the entity to get blocks for.
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param PerformanceDataSet block: The block to add to the BlockStore
        :param str performance_scope: The scope to use in the BlockStore. This has no meaning and is not implemented in
        the InMemory implementation.
        """
        entity_id = self._create_id_from_scope_code(entity_scope, entity_code)
        self.blocks[entity_id].append(block)
        if block.asat is None:
            # If the block has no asAt time, add one
            block.asat = datetime.now(pytz.UTC)
        return block

    @as_dates
    def find_blocks(self, entity_scope: str, entity_code: str, from_date: Timestamp, to_date: Timestamp, asat: Timestamp,
                    performance_scope: str = None) -> List[PerformanceDataSet]:
        """
        Returns all the PerformanceDataSet blocks in a given bi-temporal period for the specified entity.

        :param str entity_scope: The scope of the entity to get blocks for.
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param Timestamp from_date: The effectiveAt start date of the period
        :param Timestamp to_date: The effectiveAt end date of the period
        :param Timestamp asat: The asAt date of the period
        :param str performance_scope: The scope to use in the BlockStore. This has no meaning and is not implemented in
        the InMemory implementation.

        :return: List[PerformanceDataSet]: The list of relevant PerformanceDataSet blocks
        """

        def in_scope(block: PerformanceDataSet):
            """
            Checks whether or not a block exists inside a relevant bi-temporal window

            :param PerformanceDataSet block: The block to check if it exists inside the window

            :return: bool: Whether or not a block exists inside the relevant bi-temporal window
            """

            return (block.to_date >= from_date and 
                    block.from_date <= to_date and 
                    block.asat <= asat)

        return [b for b in self.get_blocks(entity_scope, entity_code, performance_scope) if in_scope(b)]

    # Find the record that precedes the given date point
    @as_dates
    def get_previous_record(self, entity_scope: str, entity_code: str, date: Timestamp,
                            asat: Timestamp, performance_scope: str = None) -> PerformanceDataPoint:
        """
        Find the block that precedes the given bi-temporal date for the specified entity.

        :param str entity_scope: The scope of the entity to get blocks for.
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param Timestamp date: The effectiveAt date
        :param Timestamp asat: The asAt date
        :param str performance_scope: The scope to use in the BlockStore. This has no meaning and is not implemented in
        the InMemory implementation.

        :return: PerformanceDataPoint latest: The latest performance data point in the preceding block
        """

        match = None

        # Loop over all possible blocks
        for candidate in self.get_blocks(entity_scope, entity_code, performance_scope):
            # Check if block is viable
            if candidate.asat <= asat and candidate.from_date < date:
               # If so, choose the best match
               if match is None or candidate.asat > match.asat:
                  match = candidate

        latest = None
        # If we have a matching block ...
        if match:
           # Find the last data_point prior to our date, assuming data points are in chronological order
           for o in match.get_data_points():
               # Have gone past date we are interested in finding the previous data point for
               if o.date >= date:
                  break
               latest = o

        return latest

    def get_first_date(self, entity_scope: str, entity_code: str, performance_scope: str = None) -> Timestamp:
        """
        Finds the earliest state in a set of blocks for the specified entity.

        :param str entity_scope: The scope of the entity to get blocks for.
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param str performance_scope: The scope to use in the BlockStore. This has no meaning and is not implemented in
        the InMemory implementation.

        :return: Timestamp: The earliest date or None of this can not be determined
        """
        blocks = self.get_blocks(entity_scope, entity_code, performance_scope)

        if len(blocks) == 0:
            return None

        return min([b.from_date for b in blocks])
