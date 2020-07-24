from typing import List

from lusid.api import StructuredResultDataApi
from lusid.models import (
    StructuredResultData,
    StructuredResultDataId,
    UpsertStructuredResultDataRequest
)
from lusid.utilities import ApiClientFactory
from lusidtools.cocoon.utilities import make_code_lusid_friendly
from pandas import Timestamp

from block_stores.block_store_in_memory import InMemoryBlockStore
from pds import PerformanceDataSet
from misc import as_dates
from serialiser import deserialise, serialise


class BlockStoreStructuredResults(InMemoryBlockStore):
    """
    The structured results block store is responsible for persisting performance blocks in the LUSID Structured Result
    Data store (https://www.lusid.com/docs/api/#tag/Structured-Result-Data).

    Currently there is no validation to check that the Portfolio blocks are being stored against actually exists inside
    LUSID.
    """
    def __init__(self, api_factory: ApiClientFactory):
        """
        :param ApiClientFactory api_factory: The api factory to use to connect to the Structured Result Data API
        """
        super().__init__()
        self.api_factory = api_factory
        # These are used in the id of documents stored in the structured result data store
        self.source = "client"
        self.result_type = "PerformanceDataSet"

    @staticmethod
    def _create_result_id(entity_scope: str, entity_code: str, from_date: Timestamp, to_date: Timestamp) -> str:
        """
        Create a unique result id from the entity and block details

        :param str entity_scope: The scope of the entity
        :param str entity_code: The code of the entity
        :param Timestamp from_date: The from date of the performance data
        :param Timestamp to_date: The to date of the performance data

        :return: str: The result id created from the entity scope and code and date range
        """
        if "_" in entity_scope or "_" in entity_code:
            raise ValueError("Can not have '_' in entity scope or code")

        code = f"{from_date.strftime('%Y-%m-%d')}_{to_date.strftime('%Y-%m-%d')}_{entity_scope}_{entity_code}"
        code = make_code_lusid_friendly(code)
        return code

    @staticmethod
    def _split_result_id(result_id: str):
        """
        Split out each element of a result_id

        :param str result_id: The result id to split up

        :return: List[str]: The split id
        """
        return result_id.split("_")

    def get_blocks(self, entity_scope: str, entity_code: str, performance_scope: str = None) -> List[PerformanceDataSet]:
        """
        This is used to get all blocks from the BlockStore for the specified entity.

        :param str entity_scope: The scope of the entity to get blocks for. The meaning of this is dependent upon
        the implementation
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param str performance_scope: The scope of the BlockStore to use, this is the scope in LUSID to use when adding
        the block to the Structured Result Store

        :return: List[PerformanceDataSet]: The blocks contained in the BlockStore
        """
        structured_results_api = StructuredResultDataApi(self.api_factory.build(StructuredResultDataApi))

        if performance_scope is None:
            performance_scope = "PerformanceBlockStore"

        entity_id = self._create_id_from_scope_code(entity_scope, entity_code)

        eligible_blocks = [code for code in self.blocks[entity_id] if code[2] == performance_scope]

        if len(eligible_blocks) == 0:
            return []

        # Retrieve the blocks from the Structured Result Data Store
        response = structured_results_api.get_structured_result_data(
            scope=performance_scope,
            request_body={
                code[0]: StructuredResultDataId(
                    source=self.source,
                    code=code[0],
                    effective_at=self._split_result_id(code[0])[1],
                    result_type=self.result_type)
                for code in eligible_blocks
            }
        )

        # Ensure that there were no failures
        if len(response.failed) > 0:
            raise ValueError("Some blocks could not be retrieved")

        # De-serialise each block into a PerformanceDataSet
        blocks = {
            code: deserialise(block.document, block.version) for code, block in response.values.items()
        }

        for code in self.blocks[entity_id]:
            if blocks[code[0]].asat is None:
                blocks[code[0]].asat = code[1]

        return list(blocks.values())

    @as_dates
    def add_block(self, entity_scope: str, entity_code: str, block: PerformanceDataSet,
                  performance_scope: str = None) -> PerformanceDataSet:
        """
        This adds a block to the BlockStore for the specified entity.

        :param str entity_scope: The scope of the entity to add the block for.
        :param str entity_code: The code of the entity to get blocks for. Together with the entity_scope this uniquely
        identifies the entity.
        :param PerformanceDataSet block: The block to add to the BlockStore
        :param str performance_scope: The scope of the BlockStore to use, this is the scope in LUSID to use when adding
        the block to the Structured Result Store

        :return: PerformanceDataSet block: The block that was added to the BlockStore along with the asAt time of
        the operation
        """
        if performance_scope is None:
            performance_scope = "PerformanceBlockStore"

        serialised_block = serialise(block)
        code = self._create_result_id(entity_scope, entity_code, block.from_date, block.to_date)
        effective_at = block.to_date

        structured_results_api = StructuredResultDataApi(self.api_factory.build(StructuredResultDataApi))

        response = structured_results_api.upsert_structured_result_data(
            scope=performance_scope,
            request_body={
                code: UpsertStructuredResultDataRequest(
                    id=StructuredResultDataId(
                        source=self.source,
                        code=code,
                        effective_at=effective_at,
                        result_type=self.result_type),
                    data=StructuredResultData(
                        document_format="Json",
                        version=block.version,
                        name="PerformanceDataSet",
                        document=serialised_block
                    )
                )
            }
        )

        as_at_time = list(response.values.values())[0]

        entity_id = self._create_id_from_scope_code(entity_scope, entity_code)
        self.blocks[entity_id].append((code, as_at_time, performance_scope))

        if block.asat is None:
            block.asat = as_at_time

        return block
