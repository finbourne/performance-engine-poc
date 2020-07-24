from misc import *
from performance_sources import mock_src
from performance_engine.perf import Performance
from fields import *
from block_stores.block_store_in_memory import InMemoryBlockStore
import pandas as pd
import numpy as np

def risk_free_rates(date,days):
    return np.round((2021 - date.year) * 0.005 + 0.003 * (days/365.0),6)

def create_demo_sheet():
    fields = [AGE_DAYS,DAY,WTD,MTD,QTD,YTD,
            ROLL_WEEK,ROLL_MONTH,ROLL_QTR,
            ROLL_YEAR,ROLL_3YR,ROLL_5YR,
            ANN_INC,ANN_1YR,ANN_3YR,ANN_5YR,
            VOL_INC,VOL_1YR,VOL_3YR,VOL_5YR,
            ANN_VOL_INC,ANN_VOL_1YR,ANN_VOL_3YR,ANN_VOL_5YR,
            RISK_FREE_1YR,RISK_FREE_3YR,RISK_FREE_5YR,
            SHARPE_1YR,SHARPE_3YR,SHARPE_5YR]

    df = pd.DataFrame.from_records(
            Performance(
                mock_src.SeededSource(
                    '2013-12-31',
                    24106,
                    rfr_func=risk_free_rates,
                    max = 0.02,
                    trend = 0.0005
                ),
                InMemoryBlockStore()
            ).report(
                False,
                '2013-12-31',
                '2020-03-05',
                '2020-03-05',
                fields=fields
            )
         )[['date',AGE_DAYS,'mv','inception'] + fields[1:]]

    nicer(df).to_csv('demo.csv',index=False)

create_demo_sheet()
