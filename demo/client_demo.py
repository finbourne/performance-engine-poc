from misc import *
from performance_sources import mock_src
from performance_engine.perf import Performance
from performance_engine.block_stores.block_store_in_memory import InMemoryBlockStore
import pandas as pd

@as_dates
def get_performance(resultset,from_date,to_date):
    df = pd.DataFrame.from_records(
            Performance(
                mock_src.MockSource(resultset, filename='client-demo.xlsx'),
                InMemoryBlockStore()
            ).report(
                False,
                from_date,
                to_date,
                to_date
            )
         )[['date','mv','flows','inception']]

    return nicer(df)


if __name__ == "__main__":
    import sys
    print(get_performance(sys.argv[1],sys.argv[2],sys.argv[3]))
