import calendar
from dateutil.relativedelta import *
from typing import Iterator, List, Dict

import numpy as np
from pandas import Timestamp

from interfaces import IBlockStore, IPerformanceSource
from pds import PerformanceDataPoint, PerformanceDataSet
import block_ops
import periods
from misc import *
from fields import *


def date_diffs(d1,d2):

    # Calculate the number of years between the dates
    rd = relativedelta(d2,d1)
    years = rd.years

    if rd.months == 0 and rd.days <= 1:
        # Shortcut for common occurence (e.g. rolling years)
        if rd.days==0:
           return years,0,0
        # Special case for leap years
        if (calendar.isleap(d2.year) 
                and d1.day   == 28
                and d1.month ==  2 
                and d2.month ==  2 
                and d2.day   == 29):
           return years,0,0

    # Get the first date of the 'daily' period
    start_date = d1 + relativedelta(years=years)
    
    total_days = (d2 - start_date).days

    if calendar.isleap(d2.year):
       if d2.year == start_date.year:
          return years,0,total_days # Every day is in a leap year

       # Calculate how many days are from the leap year
       lyr_days = (d2 - (d2 - relativedelta(month=1,day=1))).days + 1
       return years,total_days-lyr_days,lyr_days
    elif calendar.isleap(start_date.year):
       # Calculate how many days are in the regular year
       reg_days = (d2 - (d2 - relativedelta(month=1,day=1))).days + 1
       return years,reg_days,total_days-reg_days

    # Every day is in a regular year
    return years,total_days,0


def annualise(d1,d2,r):
    if (d2 - d1).days < 1:
       return 0

    years,reg_days,lyr_days = date_diffs(d1,d2)

    exponent = 1/(years + reg_days/365 + lyr_days/366)

    return pow(1.0 + r,exponent) - 1 


class Performance:
    """
    This class controls getting performance and building reports
    """

    @as_dates
    def __init__(self, entity_scope: str, entity_code: str, src: IPerformanceSource, block_store: IBlockStore,
                 perf_start: Timestamp=None):
        """
        :param str entity_scope: The scope of the entity that the Performance is for
        :param str entity_code: The code of the entity that the Performance is for, together with the code
        this uniquely identifies the entity
        :param IPerformanceSource src: The source of the data
        :param IBlockStore block_store: The block store where the performance data is to be stored
        :param Timestamp  perf_start: The start date of the performance calculations
        """
        self.entity_scope = entity_scope
        self.entity_code = entity_code
        self.src = src
        self.block_store = block_store

        # If no perf_start is provided and the block_store is empty this resolves to None
        self.perf_start=perf_start or block_store.get_first_date(entity_scope, entity_code)

    @as_dates
    def get_performance(self, locked: bool, start_date: Timestamp, end_date: Timestamp, asat: Timestamp,
                        performance_scope: str = None, **kwargs) -> Iterator[PerformanceDataPoint]:
        """
        Retrieves the performance for a given bi-temporal period

        :param bool locked: Whether or not this is a locked performance period
        :param Timestamp start_date: The effectiveAt start date of the period
        :param Timestamp end_date: The effectiveAt end date of the period
        :param Timestamp asat: The asAt date of the period
        :param str performance_scope: The scope to use to get the performance data

        :return: Iterator[PerformanceDataPoint]: The set of PerformanceDataPoint which make up performance
        """

        # get the blocks required to cover the date range
        blocks = self.block_store.find_blocks(
            entity_scope=self.entity_scope,
            entity_code=self.entity_code,
            from_date=start_date,
            to_date=end_date,
            asat=asat,
            performance_scope=performance_scope)

        if len(blocks) > 0:
           # See if there are any recent updates that
           # must be added to the data set

           # find our top block
           top = max(blocks, key=lambda b: b.asat)

           # See if the required data set is covered.
           # append any extra blocks to the list
           asat_matters = locked == False or kwargs.get('create', False)
           if (asat_matters and top.asat < asat) or top.to_date < end_date:
              blocks.extend(
                  self.addendum(
                      last_date=top.to_date,
                      last_asat=top.asat,
                      end_date=end_date,
                      asat=asat, **kwargs))
        else:
           # No blocks found, read from the source
           blocks = [self.read_block(self.perf_start or start_date,end_date,asat,performance_scope,**kwargs)]

        return block_ops.combine(blocks,locked,start_date,end_date,asat)
        
    @as_dates
    def addendum(self, last_date: Timestamp, last_asat: Timestamp,
                 end_date: Timestamp, asat: Timestamp, **kwargs) -> List[PerformanceDataSet]:
        """
        This function is responsible for adding additional blocks onto blocks which have already been read.

        :param Timestamp last_date: The last (most recent) effectiveAt date of the blocks which have been read already
        :param Timestamp last_asat: The last (most recent) asAt date of the blocks which have been read already
        :param Timestamp end_date: The effectiveAt end date of the performance period of interest
        :param Timestamp asat: The asAT date of the performance period of interest

        :return: List[PerformanceDataSet]: A list of blocks
        """
        # Find the effectiveAt date at which there have been changes from
        from_date = self.src.get_changes(self.entity_scope, self.entity_code, last_date, last_asat, asat) or (last_date + ONE_DAY)
        # If nothing has changed, and the date range is already covered
        # We can return nothing
        if from_date > end_date:
           return []
        # Find the record that precedes the updated data
        follow_from = self.block_store.get_previous_record(self.entity_scope, self.entity_code, from_date,asat)
        return [self.read_block(from_date, end_date, asat, previous=follow_from, **kwargs)]

    @as_dates
    def read_block(self, start_date: Timestamp, end_date: Timestamp, asat: Timestamp, performance_scope: str = None,
                   **kwargs) -> PerformanceDataSet:
        """
        This function is responsible for reading a single PerformanceDataSet (block) from the Performance source

        :param Timestamp start_date: The effectiveAt start date of the block to read
        :param Timestamp end_date: The effectiveAt end date of the block to read
        :param Timestamp asat: The asAt date of the block to read
        :param str performance_scope: The scope to use to write the performance data to

        :return: PerformanceDataSet b: The block which has been read from the source
        """
        b = PerformanceDataSet(from_date=start_date, to_date=end_date, asat=asat, previous=kwargs.get('previous'))

        for d, g in self.src.get_perf_data(
                self.entity_scope,
                self.entity_code,
                b.from_date,
                b.to_date,
                b.asat,
                performance_scope=performance_scope
        ).groupby('date'):
            if 'ror' in g.columns:
                row = g.iloc[0]
                b.add_returns(date=d, weight=row['wt'], ror=row['ror'])
            else:
                b.add_values(date=d, data_source=g.apply(
                    lambda r: (r['key'], r['mv'], r['net']), axis=1))

        if kwargs.get('create', False):
            self.block_store.add_block(
                entity_scope=self.entity_scope,
                entity_code=self.entity_code,
                block=b,
                performance_scope=performance_scope)
            self.perf_start = min(self.perf_start or start_date, start_date)

        return b

    @as_dates
    def report(self, locked, start_date: Timestamp, end_date: Timestamp, asat: Timestamp, performance_scope: str = None,
               **kwargs) -> List[Dict]:
        """
        Generates a performance report

        :param bool locked: Whether or not this is for a locked period
        :param Timestamp start_date: The effectiveAt start date of the report
        :param Timestamp end_date: The effectiveAt end date of the report
        :param Timestamp asat: The asAt date at which to run the report
        :param str performance_scope: The scope to use to get performance

        :return: List[Dict]: A list of results which form the performance report
        """
        # Gets the performance start date, defaulting to the start date if self.perf_start is None
        perf_start_date = min(self.perf_start or start_date, start_date)
        min_date = start_date
        
        # Get list of additional fields
        fields = kwargs.get('fields',[])

        # Identify any extension fields
        ext_fields = kwargs.get('ext_fields',{})

        # Make sure we cover the full range of dates based upon the required columns
        # This ensures that the min_date is the minimum required to generate data for all required columns
        # For example of the start_date of the report is 30/01/20 and you ask for a quarter to date (QTD) return
        # then you need to actually go back as far as 31/10/19 to generate the QTD return for 30/01/20
        for f in fields:
            min_date = periods.start_date(f,start_date,min_date,extensions=ext_fields)

        # ... but don't go earlier than the start date
        min_date=max(min_date,perf_start_date)

        # Get the basic performance for the range Dict[str, PerformanceDataPoint]
        lookup = { p.date : p for p in self.get_performance(locked,min_date,end_date,asat, performance_scope)}

        # Convert the dictionary into a list sorted by date and filter out PerformanceDataPoints before the start date
        perf = sorted(
                [p for p in lookup.values() if p.date >= start_date],
                key=lambda p : p.date
               )

        # Calculate how many days old the portfolio is
        def age_days(fld,o):
            """
            Determines how many days there are between the start of a performance period and a
            PerformanceDataPoint

            :param PerformanceDataPoint o: The performance data point to compare to the performance start date

            :return: Timedelta: The number of days since the start that this data point is for
            """
            nonlocal perf_start_date
            return (o.date - perf_start_date).days

        # Calculate return for a period. Default method
        def period_return(fld,o):
            """
            Calculate the return for a period using the default method.

            :param str fld: The field to calculate the return for
            :param PerformanceDataPoint o: The performance data point to compare to the performance start date

            :return: The return for the period
            """
            # Check to see if there is a PerformanceDataPoint for the start date required for this calculation
            nonlocal lookup
            start_rec = lookup.get(periods.start_date(fld,o.date,extensions=ext_fields))
            if start_rec:
               if start_rec.date >= o.date:
                  # If there is a data point for the start date safely divide the cumulative factors
                  return 0.0
               return safe_divide(o.cum_fctr,start_rec.cum_fctr,1.0) - 1
            else:
                # Otherwise take just use the cumulative factor on the current data point
                return o.cum_fctr - 1

        # Calculate annualised return since inception
        def annualised_inc_return(fld,o):
            if o.cum_fctr == 0.0:
                return 0.0
            return annualise(perf_start_date,o.date,o.cum_fctr - 1)

        # Calculate annualised return for a period.
        def annualised_return(fld,o):
            ror = period_return(fld,o)
            if ror == 0.0:
                return 0.0

            start_rec = lookup.get(periods.start_date(fld,o.date,extensions=ext_fields))
            start_date = start_rec.date if start_rec else perf_start_date

            return annualise(start_date,o.date,ror)

        # Calculate the volatility (sample standard deviation of daily returns)
        # Use the cumulative 'sum_ror' and 'sum_ror_sqr' fields on the performance
        # record.
        def volatility(fld,o,start_rec = None):        
            if start_rec:
               c1 = (o.sum_ror_sqr - start_rec.sum_ror_sqr)
               c2 = (o.sum_ror - start_rec.sum_ror)
               n = o.cnt - start_rec.cnt
            else:
               c1 = o.sum_ror_sqr
               c2 = o.sum_ror
               n = o.cnt
          
            if n < 2:
               return 0.0

            c1 /= n
            c2 /= n

            stddev = pow(abs(c1 - c2 * c2) * n / (n-1),0.5)

            return stddev * ANN_VOL_FCTR if fld.startswith('ann') else stddev
            
        def period_volatility(fld,o):        
            start_rec = lookup.get(periods.start_date(fld,o.date))
            return volatility(fld,o,start_rec)

        def risk_free_rate(fld,o):
            start_rec = lookup.get(periods.start_date(fld,o.date))
            start_date = start_rec.date if start_rec else perf_start_date

            days = (o.date - start_date).days
            return self.src.risk_free_rate(start_date,days) 

        # Calculate the Sharpe ratio
        def sharpe_ratio(fld,o):        
            rfr = risk_free_rate(fld,o)

            if rfr is None:
               return 0

            start_rec = lookup.get(periods.start_date(fld,o.date))
            vol = volatility("ann",o,start_rec)

            return 0.0 if vol == 0.0 else (annualised_return(fld,o) - rfr) / vol

        def calculated_flows(o):
            start_rec = lookup.get(periods.start_date(DAY,o.date))
            if start_rec:
               return np.round(o.cum_flow - start_rec.cum_flow,2)
            return o.flows

        # If a field is not in this mapping, then it will call period_return
        evaluation_method = {
            AGE_DAYS : age_days,
            VOL_1YR : period_volatility,
            VOL_3YR : period_volatility,
            VOL_5YR : period_volatility,
            VOL_INC : volatility,
            ANN_VOL_INC : volatility,
            ANN_VOL_1YR : period_volatility,
            ANN_VOL_3YR : period_volatility,
            ANN_VOL_5YR : period_volatility,
            ANN_INC : annualised_inc_return,
            ANN_1YR : annualised_return,
            ANN_3YR : annualised_return,
            ANN_5YR : annualised_return,
            RISK_FREE_1YR : risk_free_rate,
            RISK_FREE_3YR : risk_free_rate,
            RISK_FREE_5YR : risk_free_rate,
            SHARPE_1YR : sharpe_ratio,
            SHARPE_3YR : sharpe_ratio,
            SHARPE_5YR : sharpe_ratio }
        
        def as_dict(o) -> Dict:
            """
            Convert a PerformanceDataPoint into a dictionary of fields.

            :param PerformanceDataPoint o: The performance data point to convert

            :return: Dict d: A dictionary containing the results from the PerformanceDataPoint
            """

            # Create the default fields
            d = { 'date' : o.date,
                  'mv' : o.tmv,
                  'flows' : calculated_flows(o),
                  'key' : 'TOTAL',
                  'wt' : o.weight,
                  'inception' : o.cum_fctr - 1}

            # Add additional request fields using the appropriate evaluation method
            for f in fields:
                d[f] = evaluation_method.get(f,period_return)(f,o)

            # Calculate the correction amount
            d['correction'] = np.round(safe_divide(d.get(DAY,o.ror) + 1,1 + o.ror,1.0) - 1,6)
            d['flow_correction'] = np.round(d['flows'] - o.flows,2)
            return d

        # Apply as_dict to every item in perf and return the outcomes as a list of results
        return map(as_dict, perf)
