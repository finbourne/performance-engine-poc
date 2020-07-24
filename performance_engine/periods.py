from misc import as_dates

from datetime import *
from dateutil.relativedelta import *

from fields import *

ONE_LESS_DAY=relativedelta(days=-1)

quarters = sorted([1,4,7,10] * 3)


def args(**kwargs):
    return kwargs


deltas = {
    DAY: args(days=-1),
    ROLL_WEEK: args(weeks=-1),
    ROLL_MONTH: args(months=-1),
    ROLL_YEAR: args(years=-1),
    ROLL_3YR: args(years=-3),
    ROLL_5YR: args(years=-5),
    ANN_1YR: args(years=-1),
    ANN_3YR: args(years=-3),
    ANN_5YR: args(years=-5),
    ROLL_QTR: args(months=-3),
    WTD: args(weeks=-1,weekday=FR),
    MTD: args(months=-1,day=31),
    YTD: args(years=-1,month=12,day=31),
    VOL_1YR: args(years=-1),
    VOL_3YR: args(years=-3),
    VOL_5YR: args(years=-5),
    ANN_VOL_1YR: args(years=-1),
    ANN_VOL_3YR: args(years=-3),
    ANN_VOL_5YR: args(years=-5),
    SHARPE_1YR: args(years=-1),
    SHARPE_3YR: args(years=-3),
    SHARPE_5YR: args(years=-5),
    RISK_FREE_1YR: args(years=-1),
    RISK_FREE_3YR: args(years=-3),
    RISK_FREE_5YR: args(years=-5),
} 


@as_dates
def start_date(code, date, compare=None, extensions={}):

    delta = deltas.get(code)

    if delta:
       d = date + relativedelta(**delta)
    elif code == QTD:
       d = (date + relativedelta(month=quarters[date.month-1],day=1)) + ONE_LESS_DAY
    elif code in NO_RANGE_REQUIRED:
       d = date
    elif code in extensions:
       d = extensions[code]
    else:
       raise Exception(f"Invalid code: '{code}'")

    return d if compare is None else min(d,compare)
