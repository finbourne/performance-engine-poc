from periods import *
import pytest

from misc import as_date

mar05 = as_date('2020-03-05') # Arbitrary date
dec11 = as_date('2020-12-11') # A Friday
jun01 = as_date('2020-06-01') # A Monday and 1st of month
feb29 = as_date('2020-02-29') # Leap day and Saturday

def test_precedence():
    # calculates and returns earlier date
    assert start_date(DAY,mar05,jun01) == as_date('2020-03-04')
    # calculates later date, and returns the earlier, given date
    assert start_date(MTD,jun01,mar05) == mar05

def test_exception():
    # Should throw an exception when unrecognised code is given
    with pytest.raises(Exception):
        start_date("Sausage",mar05)

def test_mar05():
    assert start_date(DAY,mar05) == as_date('2020-03-04')
    assert start_date(ROLL_WEEK,mar05) == as_date('2020-02-27')
    assert start_date(ROLL_MONTH,mar05) == as_date('2020-02-05')
    assert start_date(ROLL_YEAR,mar05) == as_date('2019-03-05')
    assert start_date(ROLL_QTR,mar05) == as_date('2019-12-05')
    assert start_date(WTD,mar05) == as_date('2020-02-28')
    assert start_date(MTD,mar05) == as_date('2020-02-29')
    assert start_date(YTD,mar05) == as_date('2019-12-31')
    assert start_date(QTD,mar05) == as_date('2019-12-31')
    # Volatility tests - these are all 'year' variants so 
    # We only need to test on one regular day, and leap year
    # This is the regular day
    assert start_date(VOL_1YR,mar05) == as_date('2019-03-05')
    assert start_date(VOL_3YR,mar05) == as_date('2017-03-05')
    assert start_date(VOL_5YR,mar05) == as_date('2015-03-05')
    assert start_date(VOL_INC,mar05) == mar05
    assert start_date(ANN_VOL_3YR,mar05) == as_date('2017-03-05')
    assert start_date(ANN_VOL_5YR,mar05) == as_date('2015-03-05')
    assert start_date(ANN_VOL_INC,mar05) == mar05

def test_dec11():
    assert start_date(DAY,dec11) == as_date('2020-12-10')
    assert start_date(ROLL_WEEK,dec11) == as_date('2020-12-04')
    assert start_date(ROLL_MONTH,dec11) == as_date('2020-11-11')
    assert start_date(ROLL_YEAR,dec11) == as_date('2019-12-11')
    assert start_date(ROLL_QTR,dec11) == as_date('2020-09-11')
    assert start_date(WTD,dec11) == as_date('2020-12-04')
    assert start_date(MTD,dec11) == as_date('2020-11-30')
    assert start_date(YTD,dec11) == as_date('2019-12-31')
    assert start_date(QTD,dec11) == as_date('2020-09-30')

def test_jun01():
    assert start_date(DAY,jun01) == as_date('2020-05-31')
    assert start_date(ROLL_WEEK,jun01) == as_date('2020-05-25')
    assert start_date(ROLL_MONTH,jun01) == as_date('2020-05-01')
    assert start_date(ROLL_YEAR,jun01) == as_date('2019-06-01')
    assert start_date(ROLL_QTR,jun01) == as_date('2020-03-01')
    assert start_date(WTD,jun01) == as_date('2020-05-29')
    assert start_date(MTD,jun01) == as_date('2020-05-31')
    assert start_date(YTD,jun01) == as_date('2019-12-31')
    assert start_date(QTD,jun01) == as_date('2020-03-31')

def test_feb29():
    assert start_date(DAY,feb29) == as_date('2020-02-28')
    assert start_date(ROLL_WEEK,feb29) == as_date('2020-02-22')
    assert start_date(ROLL_MONTH,feb29) == as_date('2020-01-29')
    assert start_date(ROLL_YEAR,feb29) == as_date('2019-02-28')
    assert start_date(ROLL_QTR,feb29) == as_date('2019-11-29')
    assert start_date(WTD,feb29) == as_date('2020-02-28')
    assert start_date(MTD,feb29) == as_date('2020-01-31')
    assert start_date(YTD,feb29) == as_date('2019-12-31')
    assert start_date(QTD,feb29) == as_date('2019-12-31')
    # Volatility tests - these are all 'year' variants so 
    # We only need to test on one regular day, and leap year
    # This is the leap year
    assert start_date(VOL_1YR,feb29) == as_date('2019-02-28')
    assert start_date(VOL_3YR,feb29) == as_date('2017-02-28')
    assert start_date(VOL_5YR,feb29) == as_date('2015-02-28')
    assert start_date(VOL_INC,feb29) == feb29
    assert start_date(ANN_VOL_3YR,feb29) == as_date('2017-02-28')
    assert start_date(ANN_VOL_5YR,feb29) == as_date('2015-02-28')
    assert start_date(ANN_VOL_INC,feb29) == feb29
