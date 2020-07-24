from pandas import Timestamp
from typing import List, Iterator

from pds import PerformanceDataPoint, PerformanceDataSet
from misc import *
from datetime import timedelta

msec = timedelta(microseconds=1)


@as_dates
def combine(blocks: List[PerformanceDataSet], locked: bool, from_date: Timestamp, to_date: Timestamp,
            asat: Timestamp) -> Iterator[PerformanceDataPoint]:
    """
    Takes a list of blocks which are combined together before returning each PerformanceDataPoint


    :param List[PerformanceDataSet] blocks: The blocks to combine
    :param bool locked: Whether or not the period is locked
    :param Timestamp from_date: The effectiveAt from date of the performance period
    :param Timestamp to_date: The effectiveAt to date of the performance period
    :param Timestamp asat: The asAt date for the performance period

    :return: Iterator[PerformanceDataPoint] o:
    """

    # Sort by asat date to put the blocks in order
    # Chronological order if locked, reverse chronological order if not locked
    blocks.sort(reverse=not locked, key=lambda b: b.asat)

    if debug:
       import pandas as pd
       df = pd.DataFrame.from_records([
               (b.from_date,b.to_date,b.asat,b) for b in blocks],
               columns=['from_date','to_date','asat','block'])
       print(nicer(df[['from_date','to_date','asat']]))

    if locked:
        # Combine blocks at their lock-down points

        # Blocks are sorted in forward direction
        # Iterate over blocks returning pruned records
        limit_date = from_date-msec

        for b in blocks:
            for o in b.get_data_points():
                if o.date > to_date:
                   return # found every record
                if o.date > limit_date:
                   yield o
            limit_date = b.to_date
    else:
        # combine blocks to give a continuous up-to-date return

        # Blocks are in reverse asat order
        # 'later' over-riding blocks appear first
        # Go through the blocks and figure out the slices required from each block
        slices = []
        limit_date = as_date('2050-12-31')

        for b in blocks:
            slice_start_date = max(from_date,b.from_date)
            slice_end_date = min(to_date,b.to_date,limit_date)
            if slice_start_date <= slice_end_date:
               limit_date=slice_start_date - msec
               slices.append((slice_start_date,slice_end_date,b))
            if slice_start_date == from_date:
               #We have found all the blocks that cover our required range
               #We can terminate the loop here
               break

        # Return valid items from each slice
        # slices are in reverse order so process from the back
        for slice_start_date,slice_end_date,b in slices[::-1]:
            for o in b.get_data_points():
                if o.date > slice_end_date:
                   # We have reported all the items for this slice
                   # So we can terminate the loop here
                   break
                if o.date >= slice_start_date:
                   yield o
