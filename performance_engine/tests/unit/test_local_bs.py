import os
from block_stores.block_store_local import LocalBlockStore
from config.config import PerformanceConfiguration
from pds import PerformanceDataSet

def test_local_block(fs):
    # NOTE : Using the fake file-system
    # Set global config file paths
    block_path = os.path.join('folder','sub-folder')
    PerformanceConfiguration.set_global_config(LocalStorePath=block_path)
    
    # Create a block store
    bs = LocalBlockStore('SCOPE', 'NAME')
    bs.add_block('SCOPE', 'NAME', PerformanceDataSet('2018-03-05', '2018-03-19', '2020-03-19'))
    bs.add_block('SCOPE', 'NAME', PerformanceDataSet('2018-03-20', '2018-05-31', '2020-03-19'))

    # Make sure folder have been created
    assert os.listdir(block_path) == ['SCOPE']

    # Make sure files are created
    contents = os.listdir(os.path.join(block_path, 'SCOPE'))

    assert 'NAME.idx' in contents
    assert 'NAME.block-1' in contents
    assert 'NAME.block-2' in contents
