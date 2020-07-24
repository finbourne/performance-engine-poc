# Returns
DAY="day"
WTD="wtd"
MTD="mtd"
QTD="qtd"
YTD="ytd"
ROLL_WEEK="roll-w"
ROLL_MONTH="roll-m"
ROLL_QTR="roll-q"
ROLL_YEAR="roll-y"
ROLL_3YR="roll-3y"
ROLL_5YR="roll-5y"
ANN_1YR="ann-1y"
ANN_3YR="ann-3y"
ANN_5YR="ann-5y"
ANN_INC="ann-inc"

# Volatility
VOL_1YR="1yr_vol"
VOL_3YR="3yr_vol"
VOL_5YR="5yr_vol"
VOL_INC="inc_vol"

ANN_VOL_1YR="ann_1yr_vol"
ANN_VOL_3YR="ann_3yr_vol"
ANN_VOL_5YR="ann_5yr_vol"
ANN_VOL_INC="ann_inc_vol"

# Sharpe Ratios
SHARPE_1YR="1yr_sharpe"
SHARPE_3YR="3yr_sharpe"
SHARPE_5YR="5yr_sharpe"

# Misc
AGE_DAYS="days_old"
RISK_FREE_1YR="1yr_risk_free"
RISK_FREE_3YR="3yr_risk_free"
RISK_FREE_5YR="5yr_risk_free"

# For annualising volatility from the daily value
# Assume 252 trading days/year 
ANN_VOL_FCTR = pow(252,0.5)

# Fields that do not require a range for calculation
NO_RANGE_REQUIRED = {ANN_INC,VOL_INC,ANN_VOL_INC,AGE_DAYS}
