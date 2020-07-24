import re
import pandas as pd
import os
import datetime

ONE_DAY=datetime.timedelta(days=1)
NO_DAYS=datetime.timedelta(days=0)

# YYYY-MM-DD matching expression
rexp = re.compile(r"\d{4}-\d{2}-\d{2}")

debug = os.getenv('JLH_DEBUG')

# Safe division function
def safe_divide(num,denom,ifzero=0.0):
    """
    Performs a safe division, returning the provided value if the denominator is 0 to avoid a divide by 0 error

    :param num:
    :param denom:
    :param ifzero:

    :return:
    """

    return ifzero if denom==0 else num/denom

# Convert string date to LUSID standard dates
def as_date(s):
    return pd.to_datetime(s, utc=True)

# return current date/time in UTC
def now():
    return as_date(datetime.datetime.now())

# Decorator to support string format dates
def as_dates(func):
    def wrap(*args,**kwargs):
        def convert(v):
            """
            If the provided variable is a string in a date format it is returned as a timezone aware datetime

            :param v: The variable to convert

            :return v: The variable returned converted if it was a string otherwise left as is
            """

            if type(v) is str and rexp.match(v):
                return as_date(v)
            return v

        # Run all arguments and keyword arguments through the converter
        args = [convert(a) for a in args]
        kwargs = {k: convert(v) for k, v in kwargs.items()}
        return func(*args, **kwargs)
    return wrap

# Helper function to create a list of dates
@as_dates
def dates(*args):
    return tuple(args)

# Make a dataframe nicer for displaying
def nicer(df):
    def tweak(col):
        return df[col].dt.date if str(df[col].dtype) == 'datetime64[ns, UTC]' else df[col]

    return pd.DataFrame({ col : tweak(col) for col in df.columns })[df.columns]
