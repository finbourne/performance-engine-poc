from __future__ import annotations
from pandas import Timestamp

import pds
from return_sources.mock_src import ReturnSource
from misc import *
from interfaces import IBlockStore


class Returns:

    def __init__(self, block_store: IBlockStore):
        """
        :param IBlockStore block_store: The block store to populate
        """
        self.block_store = block_store

    @as_dates
    def import_data(self, entity_scope: str, entity_code: str, src: ReturnSource, start_date: Timestamp, end_date: Timestamp,
                    asat: Timestamp) -> Returns:
        """
        This function is responsible for importing data from a ReturnSource and populating the
        block store with this data

        :param str entity_scope: The scope of the entity to populate the block store with data
        :param str entity_code: The code of the entity to populate the block store with data
        :param ReturnSource src: The source of the data
        :param Timestamp start_date: The effectiveAt start date of the data to import
        :param Timestamp end_date: The effectiveAt end date of the data to import
        :param Timestamp asat: The asAt date of the data

        :return: Returns: The instance of the class
        """
        prev = self.block_store.get_previous_record(entity_scope, entity_code, start_date, asat)
        b = pds.PerformanceDataSet(start_date, end_date, asat, previous=prev)

        for rec in src.get_return_data(entity_scope, entity_code, b.from_date, b.to_date, b.asat):
            b.add_returns(rec['date'], rec['wt'], rec['ror'])
        self.block_store.add_block(entity_scope, entity_code, b)

        return self
