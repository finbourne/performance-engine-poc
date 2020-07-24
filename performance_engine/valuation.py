import numpy as np
from typing import Tuple

from lusid.exceptions import ApiException
from lusid.models import (
    AggregationRequest,
    ConfigurationRecipe,
    MarketContext,
    MarketContextSuppliers,
    MarketDataKeyRule,
    MarketOptions,
    ResourceId,
    ResourceSupplier
)
from lusidtools.lpt import lpt
from lusidtools.lpt.lpt import Rec
from lusidtools.lpt.lse import ExtendedAPI
from misc import as_dates
from pandas import Timestamp

PV = 'Holding/default/PV'
AGG_PV = f'Sum({PV})'


def default_recipe(scope: str):
    """
    The responsibility of this function is to produce a default inline recipe for the provided scope

    :param str scope: The scope to use for the recipe

    :return: ConfigurationRecipe: The created recipe
    """
    return ConfigurationRecipe(
             scope=scope,
             code=scope,
             market=MarketContext(
                 market_rules=[
                     MarketDataKeyRule(
                        key='Equity.LusidInstrumentId.*',
                        supplier='DataScope',
                        data_scope=scope,
                        quote_type='Price',
                        quote_interval='3M',
                        field='Mid'),
                     MarketDataKeyRule(
                        key='Fx.CurrencyPair.*',
                        supplier='DataScope',
                        data_scope=scope,
                        quote_type='Rate',
                        quote_interval='3M',
                        field='Mid')
                 ],
                 suppliers=MarketContextSuppliers(
                     commodity=ResourceSupplier.DATASCOPE,
                     credit=ResourceSupplier.DATASCOPE,
                     equity=ResourceSupplier.DATASCOPE,
                     fx=ResourceSupplier.DATASCOPE,
                     rates=ResourceSupplier.DATASCOPE),
                 options=MarketOptions(
                     attempt_to_infer_missing_fx=True,
                     default_supplier='DataScope',
                     default_instrument_code_type='LusidInstrumentId',
                     default_scope=scope)
             )
    )


@as_dates
def get_valuation(api: ExtendedAPI, scope, portfolio, recipe_id: ResourceId, date: Timestamp,
                  asat: Timestamp) -> Tuple[Timestamp, float]:
    """
    The responsibility of this function is to value a Portfolio inside LUSID for a given date.

    :param ExtendedAPI api: The extended API to use to call LUSID
    :param str scope: The scope of the Portfolio in LUSID
    :param str portfolio: The code of the Portfolio in LUSID. Together with the scope this uniquely identifies the
    Portfolio
    :param ResourceId recipe_id: The scope and code of the persisted recipe to use
    :param Timestamp date: The effectiveAt date of the valuation
    :param Timestamp asat: The asAt date of the valuation

    :return: Tuple[Timestamp, float]: The date of the request and the value of the Portfolio
    """
    if recipe_id:
        args = {'recipe_id': recipe_id}
    else:
        args = {'inline_recipe':  default_recipe(scope)}

    request = AggregationRequest(
      effective_at=date,
      as_at=asat,
      metrics=[
         api.models.AggregateSpec(PV, 'Sum')
      ],
      **args  # The appropriate recipe parameter
    )

    def success(result: Rec) -> Tuple[Timestamp, float]:
        """
        The responsibility of this function is to handle a successful API call

        :param Rec result: The result which contains a ListAggregationResponse in its .content attribute

        :return: Tuple[Timestamp, float]
        """
        nonlocal date
        return date, np.round(result.content.data[0][AGG_PV], 2)

    def failure(error: ApiException) -> None:
        """
        The responsibility of this function is to handle a failed API call

        :param ApiException error: The error from the API call

        :return: None
        """
        lpt.display_error(error)
        exit()

    return api.call.get_aggregation(
             scope=scope,
             code=portfolio,
             aggregation_request=request
    ).match(failure, success)
