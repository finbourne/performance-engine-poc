import pandas as pd
import pickle
import os

from block_stores.block_store_in_memory import InMemoryBlockStore
from pds import PerformanceDataSet
from config.config import PerformanceConfiguration


class LocalBlockStore(InMemoryBlockStore):
    """
    This class is responsible for persisting performance blocks in the local file system
    """
    def __init__(self, scope, portfolio):
        super().__init__()
        self.scope = scope
        self.portfolio = portfolio
        self.path = os.path.join(PerformanceConfiguration.item('LocalStorePath', 'blocks'), scope, portfolio)
        # Load existing blocks (if any)

        # Create a loader function for a block
        def wrap(idx):
            def loader():
                return pd.read_pickle(f'{self.path}.block-{idx+1}')
            return loader

        try:
            df = pd.read_csv(f'{self.path}.idx', parse_dates=['from_date', 'to_date', 'asat'])
        except:
            return # File doesn't exist. Not a problem at this stage

        for i, r in df.iterrows():
            block = PerformanceDataSet(
                       r['from_date'],
                       r['to_date'],
                       r['asat'],loader = wrap(i)
                    )
            super().add_block(self.scope, self.portfolio, block)

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
        super().add_block(entity_scope, entity_code, block)
        entity_id = self._create_id_from_scope_code(self.scope, self.portfolio)
        # Save block to the file-system
        fn = f'{self.path}.block-{len(self.blocks[entity_id])}'

        def save():
            with open(fn,'wb') as fp:
                pickle.dump(block.get_data_points(),fp)
        try:
           save()
        except:
            # If there is an error try creating the folder and resubmitting
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            save()

        # Block save succeeded, now save the index
        df = pd.DataFrame.from_records([
                (b.from_date,b.to_date,b.asat) for b in self.blocks[entity_id]],
                columns=['from_date','to_date','asat'])

        df.to_csv(f'{self.path}.idx',index=False)

        return block
