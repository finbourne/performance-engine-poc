from datetime import timedelta
from typing import Any, Callable, Dict, Tuple

import numpy as np
import numpy.random as rnd
import pandas as pd
from pandas import DataFrame, Timestamp
import pytz

from interfaces import IPerformanceSource
from misc import as_date, as_dates


class MockSource(IPerformanceSource):
    """
    This class mocks a source of data
    """

    def __init__(self, dataset: Any, **kwargs):
        """
        :param Any dataset: The DataFrame or sheet name to use
        """
        if type(dataset) is pd.DataFrame:
            self.data = dataset
        else:
            filename = kwargs.get('filename', 'test-data.xlsx')
            self.data = pd.read_excel(filename, sheet_name=dataset)

        if len(self.data.columns) == 4:
            self.data.columns = ['asat', 'date', 'mv.all', 'net.all']

        self.data['date'] = as_date(self.data['date'])
        self.data['asat'] = as_date(self.data['asat'])

    @as_dates
    def get_perf_data(self, entity_scope, entity_code, from_date, to_date, asat, **kwargs):
        """
        Gets the relevant performance data from the mock source relevant to the requested bi-temporal window

        :param str entity_scope: The scope of the entity to get performance data for
        :param str entity_code: The code of the entity to get performance data for
        :param from_date: The effectiveAt from date to retrieve performance data for
        :param to_date: The effectiveAt to date to retrieve performance data for
        :param asat: The asAt date at which to retrieve performance data

        :return: pd.DataFrame df: The DataFrame with the appropriate performance data
        """

        def check(r):
            """
            Checks a row from a Pandas DataFrame which is represented as a Pandas Series to
            see if it is relevant to the bi-temporal window requested

            :param pd.Series r: The row of the DataFrame to check

            :return: bool: True if relevant to the requested bi-temporal window, False if not
            """
            return from_date <= r['date'] <= to_date and r['asat'] <= asat

        # Apply a check to each row in the DataFrame
        df = self.data[self.data.apply(check,axis=1)]

        # Remove the asAt column and drop records for the same day in the effectiveAt column keeping the last record
        df = df.drop('asat',axis=1).drop_duplicates('date',keep='last').sort_values('date')

        # Takes all columns starting with "mv" or "net" and turns it into just two columns called
        # "mv" and "net" along with a "key" column which contains the suffix of all the columns which start
        # with "mv" or "net" and is identified by splitting the column name on a ".".
        # The date is also copied down for each row
        df = pd.wide_to_long(df, ['mv', 'net'], i='date', j='key', sep='.', suffix=r'\w+').reset_index()
        return df


class SimpleSource(IPerformanceSource):
    """
    The responsibility of this class is to provide a simple source of performance data in which there is a recurring
    flow for the same amount at a specified frequency and a consistent daily return.
    """
    @as_dates
    def __init__(self, start_date: Timestamp, recurring_flow: float = 0.0, recurring_freq: int = 7, ror: float = 0.0001):
        """
        :param Timestamp start_date: The start date of performance
        :param float recurring_flow: The value of a recurring flow
        :param int recurring_freq: The frequency at which the flow occurs in days
        :param float ror: The daily rate of return which is experienced each day from the start date
        """
        self.start_date = start_date
        self.rec_flow = recurring_flow
        self.rec_freq = recurring_freq
        self.daily_ror = 1.0 + ror

    @as_dates
    def get_perf_data(self, entity_scope, entity_code, from_date, to_date, asat, **kwargs) -> DataFrame:
        """
        Gets the relevant performance data from the mock source relevant to the requested bi-temporal window

        :param str entity_scope: The scope of the entity to get performance data for
        :param str entity_code: The code of the entity to get performance data for
        :param from_date: The effectiveAt from date to retrieve performance data for
        :param to_date: The effectiveAt to date to retrieve performance data for
        :param asat: The asAt date at which to retrieve performance data

        :return: pd.DataFrame df: The DataFrame with the appropriate performance data
        """
        from_date = max(from_date, self.start_date)
        increase_base = (from_date - self.start_date).days

        def make_perf(i) -> Tuple[Timestamp, float, float]:
            """
            This function is responsible for constructing performance for a particular day

            :param int i: The number of days into making performance

            :return: Tuple[Timestamp, float, float]: Data to use to construct a series of performance
            """
            nonlocal increase_base
            increase = increase_base + i  # Represents the number of days since the start date

            return (
                from_date + timedelta(days=i),  # Date: Produce the correct date
                np.round(1000000.0 * pow(self.daily_ror, increase), 2),  # Market Value: Return is consistently applied
                self.rec_flow if increase % self.rec_freq == 0 else 0  # Flows: Log a flow at the recurring frequency
            )

        df = pd.DataFrame.from_records(
                [
                    make_perf(i) for i in range((to_date-from_date).days+1)
                ],
                columns=['date', 'mv', 'net']
             )

        df['key'] = 'all'
        return df


class SeededSource(IPerformanceSource):

    @as_dates
    def __init__(self, entities: Dict = None, rfr_func: Callable = None):
        """
        :param Dict entities: The entities and their seed values to initialise the seeded source with
        :param Callable rfr_func: The risk free rate function
        """
        if entities is None:
            self.entities = {}
        else:
            self.entities = entities

        self.risk_free_rate = rfr_func

    @staticmethod
    def _create_id_from_scope_code(scope: str, code: str) -> str:
        """
        Creates an id from a scope and code

        :param scope: The scope
        :param code: The code

        :return: str: The id created from the scope and code.
        """
        return f"{scope}_{code}"

    @as_dates
    def add_seeded_perf_data(self, entity_scope, entity_code, start_date, seed, **kwargs) -> None:
        """
        Add a new entity to the source with its parameters for the seeded data

        :param str entity_scope: The scope of the entity to get performance data for
        :param str entity_code: The code of the entity to get performance data for
        :param start_date: The start date of the peformance
        :param seed: The seed to use for generating the random performance

        :return: None
        """
        entity_id = self._create_id_from_scope_code(entity_scope, entity_code)

        self.entities[entity_id] = {
            "start_date": start_date,
            "seed": seed,
            "max": kwargs.get('max', 0.05),
            "amt": kwargs.get('amt', 1000000.0),
            "trend_adj": 1.0 + kwargs.get("trend", 0.005)
        }

    @as_dates
    def get_perf_data(self, entity_scope, entity_code, from_date: Timestamp, to_date: Timestamp, asat: Timestamp,
                      **kwargs) -> DataFrame:
        """
        Gets the relevant performance data from the mock source relevant to the requested bi-temporal window

        :param str entity_scope: The scope of the entity to get performance data for
        :param str entity_code: The code of the entity to get performance data for
        :param from_date: The effectiveAt from date to retrieve performance data for
        :param to_date: The effectiveAt to date to retrieve performance data for
        :param asat: The asAt date at which to retrieve performance data

        :return: pd.DataFrame df: The DataFrame with the appropriate performance data
        """

        entity_id = self._create_id_from_scope_code(entity_scope, entity_code)

        if entity_id not in self.entities:
            raise KeyError(f"No perf data for entity with scope {entity_scope} and code {entity_code}")

        keyword_arguments = self.entities[entity_id]
        keyword_arguments["end_date"] = to_date

        return self._produce_perf_data(**keyword_arguments)

    @staticmethod
    def _produce_perf_data(seed: int, start_date: Timestamp, end_date: Timestamp, max: float, trend_adj: float,
                           amt: float) -> DataFrame:
        """
        Produce randomised performance data based on the provided parameters

        :param int seed: The seed to use to initialise the random number generator
        :param Timestamp start_date: The start date of the performance
        :param Timestamp end_date: The end date of the performance
        :param float max: The maximum daily return possible
        :param float trend_adj: An adjustment to make on each daily return
        :param float amt: The starting market value

        :return: DataFrame df: The DataFrame containing the random performance data
        """
        start_date = start_date.replace(tzinfo=pytz.UTC)
        end_date = end_date.replace(tzinfo=pytz.UTC)

        # Create a shell DataFrame
        df = pd.DataFrame(
            data=pd.date_range(start_date, end_date),
            columns=['date']
        )

        # Get the number of days in the DataFrame
        num = len(df)

        # Seed the random number generator
        rnd.seed(seed)

        '''
        1) Create a series of random returns with size of the DataFrame + 4
        2) Smooth out the returns by taking the rolling 5 day mean
        3) Shift back 3 places to fill in missing data where window was less than 5 days, leaving the first day NaN
        4) Ensure that the series is the same length as the DataFrame
        5) Fill the missing first day with 1
        6) Turn the series into a cumulative product and multiple this by the initial market value to get a series
            of market values
        7) Round to 2 decimal places
        '''
        df['mv'] = np.round(pd.Series(
                data=(rnd.random(num + 4) * max * 2.0) + trend_adj - max
            ).rolling(5).mean().shift(-3).head(num).fillna(1.0).cumprod() * amt, 2)

        # No flows
        df['net'] = 0.0
        df['key'] = 'all'
        return df
