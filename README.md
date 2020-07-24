# Proof of Concept Performance Engine

This repository contains a proof of concept performance engine built on top of LUSID. It
explores the idea of using LUSID's bi-temporal data store to persist performance returns for Portfolios.

Furthermore it also explores the possibility of creating performance composites using LUSID's Portfolio Groups.

The scope of this project is to solicit feedback on the functionality of the performance engine
so that this feedback can be incorporated into the design and implementation of LUSID's performance engine APIs. 

This repository is not guaranteed to be maintained and should not be used outside the scope of testing against
functional requirements. 

FINBOURNE welcomes all feedback on this project. 


## Portfolio Performance

This proof of concept contains a reference implementation on how you might use LUSID to store Portfolio
level returns. 

These returns come in the form of a geometrically linked daily returns.

Constraints:

- All returns are daily
- There is no integration with a holiday calendar
- Missing returns are NOT treated as a break in the geometrically linked series
- Each return also contains a weight which is the beginning of day market value, in the case
that there is no beginning of day market value, this can be substituted by the end of day 
market value


### Block Store Backed by the Structured Results Data Store

Portfolio performance is stored as a set of blocks. 

Each block has a start date and an end date and thus contains daily 
performance across a range of dates. The size of each block is arbitrary. 

A single block is modelled by an instance of the 
[PerformanceDataSet](performance_engine/pds.py) class.

To persist a block the PerformanceDataSet class is lazily serialised to Javascript Object Notation (JSON) via the 
Python library [`jsonpickle`](https://jsonpickle.readthedocs.io/en/latest/).

The serialised block is then upserted into LUSID's [Structured Result Data](https://www.lusid.com/docs/api/#operation/UpsertStructuredResultData) 
store. This is a bi-temporal store.


### Upserting Portfolio Returns to the Block Store

This proof of concept provides a method for upserting Performance blocks into the Structured Result Data Store as 
described above. You can find an example of this below, as well as in the [upsert returns tests](performance_engine/tests/integration/test_upsert_returns.py) 
and [end to end tests](performance_engine/tests/integration/test_end_to_end.py).

```python
from apis_returns.upsert_returns import upsert_portfolio_returns
from apis_returns.upsert_returns_models import (
    PerformanceDataSetRequest,
    PerformanceDataPointRequest,
)
from block_stores.block_store_structured_results import BlockStoreStructuredResults
from tests.utilities.api_factory import api_factory

block_store = BlockStoreStructuredResults(api_factory)

response = upsert_portfolio_returns(
        performance_scope="MyBlockStore",  # This is the scope of the block store to use
        portfolio_scope="MyPortfolios",  # This is the scope of the Portfolio in LUSID
        portfolio_code="MyFixedIncomePortfolio",  # This is the code of the Portfolio in LUSID
        request_body={
            "block_1": PerformanceDataSetRequest(  # The block to persist
                data_points=[
                    PerformanceDataPointRequest(
                        date="2020-01-01", # The date of the return
                        ror=0.03,  # The daily rate of return
                        weight=1000  # The beginning of day market value
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-02",
                        ror=0.08,
                        weight=1033
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-03",
                        ror=-0.042,
                        weight=896
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-04",
                        ror=0.009,
                        weight=1250
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-05",
                        ror=-0.04,
                        weight=1289
                    ),
                    PerformanceDataPointRequest(
                        date="2020-01-06",
                        ror=-0.02,
                        weight=1198
                    )
                ]
            )
        },
        block_store=block_store
    )
```

This block is now persisted in LUSID.


### Block combinations

Depending on the reporting window a single block may be sufficient to generate a performance report. Where a single block
does not contain all the daily returns required to construct a sufficiently long geometrically linked series of daily 
returns to produce the requested performance report it may be combined with other blocks.

The basic logic currently implemented for combining blocks together can be found in the [block_ops.py file](performance_engine/block_ops.py).


### Generating Portfolio Performance Reports

With one or more blocks (each containing a set of geometrically linked daily returns) persisted in the block store, you
can generate performance reports for your Portfolio. The currently supported metrics are listed below:

#### Returns
- DAY
- WTD
- MTD
- QTD
- YTD
- ROLL_WEEK
- ROLL_MONTH
- ROLL_QTR
- ROLL_YEAR
- ROLL_3YR
- ROLL_5YR
- ANN_1YR
- ANN_3YR
- ANN_5YR
- ANN_INC

#### Volatility

- VOL_1YR
- VOL_3YR
- VOL_5YR
- VOL_INC
- ANN_VOL_1YR
- ANN_VOL_3YR
- ANN_VOL_5YR
- ANN_VOL_INC

#### Sharpe Ratios
- SHARPE_1YR
- SHARPE_3YR
- SHARPE_5YR

These can also be found in the [fields.py file](performance_engine/fields.py).

A performance report is generated as a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html). 
You can find an example of generating a performance report for a Portfolio below. There are also examples in the 
[end to end tests](performance_engine/tests/integration/test_end_to_end.py)

```python
from apis_performance.portfolio_performance_api import PortfolioPerformanceApi
from block_stores.block_store_structured_results import BlockStoreStructuredResults
from fields import DAY, WTD, YTD, ROLL_WEEK
from tests.utilities.api_factory import api_factory


block_store = BlockStoreStructuredResults(api_factory=api_factory)

portfolio_performance_api = PortfolioPerformanceApi(
    block_store=block_store,
    portfolio_performance_source=None,
    api_factory=api_factory
)

report = portfolio_performance_api.get_portfolio_performance_report(
    portfolio_scope="MyPortfolios",
    portfolio_code="MyFixedIncomePortfolio",
    performance_scope="MyBlockStore",
    from_date="2020-01-02",
    to_date="2020-01-06",
    locked=True,
    fields=[DAY, WTD, YTD, ROLL_WEEK]
)

report.head()
```

The Portfolio performance report is shown below. 

|index|date|mv|inception|flows|day|wtd|ytd|roll-w|
|---|---|---|---|---|----|-----|----|----|
|0|2020-01-02 00:00:00+00:00|0|0.0710|0.0|0.0710|0.0710|0.0710|0.0710|
|1|2020-01-03 00:00:00+00:00|0|0.0560|0.0|-0.0140|0.0560|0.0560|0.0560|
|2|2020-01-04 00:00:00+00:00|0|0.1088|0.0|0.0500|0.0500|0.1088|0.10886|
|3|2020-01-05 00:00:00+00:00|0|0.1864|0.0|0.0700|0.1235|0.1864|0.1864|
|4|2020-01-06 00:00:00+00:00|0|0.1508|0.0|-0.0300|0.0898|0.1508|0.1508|


### Using Arbitrary Inception Dates in your Reports

In addition to the default supported fields, there is also the capability to generate performance from arbitrary start
dates. This can be achieved using a [Property](https://support.finbourne.com/what-is-a-property) on the Portfolio in
LUSID.

The first thing you need to do is create (if it does not already exist) a [Property Definition](https://www.lusid.com/docs/api/#operation/CreatePropertyDefinition) 
for the property in LUSID. The data type should be "string". Once you have done this you can [populate](https://www.lusid.com/docs/api/#operation/UpsertPortfolioProperties) 
the value of the Property on the Portfolio with the arbitrary inception date in the format YYYY-MM-DD e.g. "2020-01-03". 

To make the field available for use you must also add it to the [configuration](performance_engine/config/config.json) in the
fields section. For example if you wanted to add an inception date for when a portfolio manager began managing
the Portfolio. You would add the entry "ManagerStart": "Portfolio/Managers/ManagerStart" to the fields section 
as shown below. 

"ManagerStart" is the name of the field that you can now use and "Portfolio/Managers/ManagerStart" is the fully
qualified property key in LUSID.

```json
{
	"fields" : {
		"ClientInc" : "Portfolio/JLH/ClientInc",
		"ManagerStart": "Portfolio/Managers/ManagerStart" 
	},
	"ext_flow_types" : ["APPRCY","EXPRCY"]
}
```

You can then specify your custom field when generating a Performance report. 

```python
from apis_performance.portfolio_performance_api import PortfolioPerformanceApi
from block_stores.block_store_structured_results import BlockStoreStructuredResults
from fields import DAY, WTD, YTD, ROLL_WEEK
from tests.utilities.api_factory import api_factory


block_store = BlockStoreStructuredResults(api_factory=api_factory)

portfolio_performance_api = PortfolioPerformanceApi(
    block_store=block_store,
    portfolio_performance_source=None,
    api_factory=api_factory
)

report = portfolio_performance_api.get_portfolio_performance_report(
    portfolio_scope="MyPortfolios",
    portfolio_code="MyFixedIncomePortfolio",
    performance_scope="MyBlockStore",
    from_date="2020-01-02",
    to_date="2020-01-06",
    locked=True,
    fields=[DAY, WTD, YTD, ROLL_WEEK, "ManagerStart"]
)

report.head()
```

The Portfolio performance report is shown below. 

|index|date|mv|inception|flows|day|wtd|ytd|roll-w| **ManagerStart** |
|---|---|---|---|---|----|-----|----|----|----|
|0|2020-01-02 00:00:00+00:00|0|0.0710|0.0|0.0710|0.0710|0.0710|0.0710|**0.0000**|
|1|2020-01-03 00:00:00+00:00|0|0.0560|0.0|-0.0140|0.0560|0.0560|0.0560|**0.0000**|
|2|2020-01-04 00:00:00+00:00|0|0.1088|0.0|0.0500|0.0500|0.1088|0.10886|**0.0500**|
|3|2020-01-05 00:00:00+00:00|0|0.1864|0.0|0.0700|0.1235|0.1864|0.1864|**0.1235**|
|4|2020-01-06 00:00:00+00:00|0|0.1508|0.0|-0.0300|0.0898|0.1508|0.1508|**0.0898**|

## Compositing 

In addition to generating performance reports for a Portfolio, this proof of concept also contains a reference
implementation modelling a composite using [Portfolio Groups](https://support.finbourne.com/how-do-you-group-and-aggregate-portfolios) 
in LUSID.
 

### Creating a Composite

A composite can be modelled on top of an existing Portfolio Group. Alternatively if a Portfolio Group does not
already exist it can be created as shown in the example below.

```python
from composites.portfolio_groups_composite import PortfolioGroupComposite
from tests.utilities.api_factory import api_factory

portfolio_group_compositing = PortfolioGroupComposite(api_factory=api_factory)

portfolio_group_compositing.create_composite(
    composite_scope="MyStrategyComposites",
    composite_code="MyFixedIncomeStrategy"
)
```


### Updating the membership of a Composite

Once a composite has been created or using a Portfolio Group which already exists, members can be added
or removed from the composite.

Taking advantage of the bi-temporal nature of LUSID, each member can be added/removed for a provided date
range. This allows members to come and go from the composite over time. If no end date is specified then the composite
is added/removed from the from_date until the end of time. 

#### Adding a member to a composite

In the example below you are adding your `MyFixedIncomePortfolio` and `MyGovernmentDebtPortfolio` to the composite from the 1st of January 2020
until the end of time i.e. forever. 

```python
from composites.portfolio_groups_composite import PortfolioGroupComposite
from tests.utilities.api_factory import api_factory

portfolio_group_compositing = PortfolioGroupComposite(api_factory=api_factory)

portfolio_group_compositing.add_composite_member(
    composite_scope="MyStrategyComposites",
    composite_code="MyFixedIncomeStrategy",
    member_scope="MyPortfolios",
    member_code="MyFixedIncomePortfolio",
    from_date='2020-01-01',
    to_date=None)
    
portfolio_group_compositing.add_composite_member(
    composite_scope="MyStrategyComposites",
    composite_code="MyFixedIncomeStrategy",
    member_scope="MyPortfolios",
    member_code="MyGovernmentDebtPortfolio",
    from_date='2020-01-01',
    to_date=None)
```

#### Removing a member from a composite

In the example below you are removing your `MyFixedIncomePortfolio` from the composite from the 4th of January 2020
until the 6th of January 2020. When adding or removing members all dates are inclusive. Therefore after this operation
the `MyFixedIncomePortfolio` portfolio will be a member of the `MyFixedIncomeStrategy` composite for the following date
ranges:
- 1st Jan 2020 to 3rd Jan 2020
- 7th Jan 2020 to End of Time

```python
from composites.portfolio_groups_composite import PortfolioGroupComposite
from tests.utilities.api_factory import api_factory

portfolio_group_compositing = PortfolioGroupComposite(api_factory=api_factory)

portfolio_group_compositing.remove_composite_member(
    composite_scope="MyStrategyComposites",
    composite_code="MyFixedIncomeStrategy",
    member_scope="MyPortfolios",
    member_code="MyFixedIncomePortfolio",
    from_date='2020-01-04',
    to_date='2020-01-06')
```


#### Getting the members of a composite

You can see the membership of all members of the `MyFixedIncomeStrategy` composite over time as shown by the example below.

```python
from datetime import datetime
import pytz

from composites.portfolio_groups_composite import PortfolioGroupComposite
from tests.utilities.api_factory import api_factory

portfolio_group_compositing = PortfolioGroupComposite(api_factory=api_factory)

response = portfolio_group_compositing.get_composite_members(
    composite_scope="MyStrategyComposites",
    composite_code="MyFixedIncomeStrategy",
    start_date='2020-01-01',
    end_date='2020-01-10',
    asat=datetime.now(pytz.UTC)
)

print (response)
```

This produces the output shown below.

```python
{
    "MyPortfolios_MyFixedIncomePortfolio": [
        ("2020-01-01", "2020-01-03")
        ("2020-01-07", "2020-01-10")
    ],
    "MyPortfolios_MyGovernmentDebtPortfolio": [
        ("2020-01-01", "2020-01-10")
    ]
}
```


### Generating the performance for a Composite

A performance report can be generated for a composite in a similar way to a portfolio. 

```python
from apis_performance.composite_performance_api import CompositePerformanceApi
from apis_performance.portfolio_performance_api import PortfolioPerformanceApi
from block_stores.block_store_structured_results import BlockStoreStructuredResults
from composites.portfolio_groups_composite import PortfolioGroupComposite
from fields import DAY, WTD, YTD, ROLL_WEEK
from performance_sources.comp_src import CompositeSource
from tests.utilities.api_factory import api_factory

block_store = BlockStoreStructuredResults(api_factory=api_factory)
portfolio_group_compositing = PortfolioGroupComposite(api_factory=api_factory)

portfolio_performance_api = PortfolioPerformanceApi(
    block_store=block_store,
    portfolio_performance_source=None,
    api_factory=api_factory
)

asset_weighted_composite_source = CompositeSource(
    composite=portfolio_group_compositing,
    performance_api=portfolio_performance_api,  # The composite source uses the Portfolio Performance API
    composite_mode="asset")  # The compositing method is set here, other options are "equal" and "agg"

composite_performance_api = CompositePerformanceApi(
    block_store=block_store,
    composite_performance_source=asset_weighted_composite_source,
    api_factory=api_factory
)

report = composite_performance_api.get_composite_performance_report(
    composite_scope="MyStrategyComposites",
    composite_code="MyFixedIncomeStrategy",
    performance_scope="MyBlockStore",
    from_date="2020-01-02",
    to_date="2020-01-06",
    locked=True,
    fields=[DAY, WTD, YTD, ROLL_WEEK]
)
```

## Locked and Unlocked Periods

All performance that is persisted in the block store is considered to be "locked". When generating a performance
report for a Portfolio or a Composite, you have the ability to pass `locked=False`. In this case the returns for the
Portfolio or Composite can be sourced from outside the block store. 

An example of this would be to source the returns from LUSID by valuating the Portfolio and capturing the flows. This
is implemented in [lusid_src.py](performance_engine/performance_sources/lusid_src.py).

Working with locked and unlocked periods is still a work in progress.

