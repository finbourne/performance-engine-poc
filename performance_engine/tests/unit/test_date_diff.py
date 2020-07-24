import pytest
import perf
from misc import *

@pytest.mark.parametrize("scenario",[
    # Whole year tests
   (dates('2017-01-19','2019-01-19'),(2,0,0)), # no leap year
   (dates('2017-10-16','2021-10-16'),(4,0,0)), # include a leap year
   (dates('2017-01-19','2020-01-19'),(3,0,0)), # end in leap leap year before l.day
   (dates('2017-03-19','2020-03-19'),(3,0,0)), # end in leap leap year after l.day
   (dates('2020-01-19','2021-01-19'),(1,0,0)), # start in leap leap year before l.day
   (dates('2020-03-19','2022-03-19'),(2,0,0)), # start in leap leap year after l.day
   (dates('2019-02-28','2020-02-29'),(1,0,0)), # feb-28 to feb-29
   (dates('2020-02-29','2021-02-28'),(1,0,0)), # feb-29 to feb-28

   # Less than 1 year tests
   (dates('2018-03-05','2018-03-19'),(0,14,0)),  # within a regular year < 1 month
   (dates('2018-03-05','2018-11-30'),(0,270,0)),  # within a regular year > 1 month
   (dates('2018-01-01','2018-12-31'),(0,364,0)), # 364 days in a regular year
   (dates('2020-01-04','2020-05-31'),(0,0,148)), # Across leap day
   (dates('2020-01-04','2020-02-28'),(0,0,55)), # Before leap day
   (dates('2020-01-04','2020-02-29'),(0,0,56)), # Upto leap day
   (dates('2020-01-04','2020-03-01'),(0,0,57)), # after leap day
   
   # Cross the year-end, less than a year tests
   (dates('2018-01-04','2019-01-03'),(0,364,0)), # Across year end regular year max
   (dates('2018-12-31','2019-01-01'),(0,1,0)), # Across year end regular year min
   (dates('2019-01-04','2020-01-03'),(0,361,3)), # Across year end into leap year ex l.d
   (dates('2019-05-01','2020-04-30'),(0,244,121)), # Across year end into leap year inc l.d
   (dates('2020-02-25','2021-01-05'),(0,5,310)), # Across year end out of leap year inc l.d
   (dates('2020-06-01','2021-05-31'),(0,151,213)), # Across year end out of leap year ex l.d

   # Years and days
   (dates('2018-03-05','2020-03-19'),(2,0,14)), # Days in leap year
   (dates('2017-03-05','2019-03-19'),(2,14,0)), # Days in regular year
   (dates('2014-10-16','2020-03-19'),(5,76,79)) # Days in both types of year
   ])
def test_dates(scenario):

    dates,expectation = scenario

    assert perf.date_diffs(*dates) == expectation
