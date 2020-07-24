from typing import Dict, List

from lusid.api import PortfoliosApi, PortfolioGroupsApi
from lusid.models import PortfolioProperties
from lusid.utilities.api_client_factory import ApiClientFactory
from lusidtools.lpt import lpt
from pandas import Timestamp

from config.config import PerformanceConfiguration


def get_ext_fields(api_factory: ApiClientFactory, entity_type: str, entity_scope: str, entity_code: str,
                   effective_date: Timestamp, asat: Timestamp, fields: List[str],
                   config: PerformanceConfiguration) -> Dict[str, Timestamp]:
    """
    The responsibility of this function is to get extended fields for performance reporting from LUSID

    :param ApiClientFactory api_factory: The api factory to use to connect to LUSID
    :param str entity_type: Whether the entity is a portfolio or a composite
    :param str entity_scope: The scope of the entity to fetch properties for
    :param str entity_code: The code of the entity to fetch properties for
    :param Timestamp effective_date: The effectiveAt to fetch properties for
    :param Timestamp asat: The asAt date to fetch properties for
    :param List[str] fields: The fields from which to extract available extended fields
    :param PerformanceConfiguration config: The configuration containing the available extended fields

    :return: Dict[str, Timestamp]: The extended fields and their values retrieved from LUSID
    """
    ext_fields = config.get("fields", {})

    api_mapping = {
        "portfolio": PortfoliosApi,
        "composite": PortfolioGroupsApi
    }

    api_call_mapping = {
        "portfolio": "get_portfolio_properties",
        "composite": "get_group_properties"
    }

    entity_type = entity_type.lower()

    if entity_type not in api_mapping:
        raise KeyError(
            f"The entity type of {entity_type} is unsupported. The supported types are {str(list(api_mapping.keys()))}")

    # Get shortlist of extension fields requested, reverse mapping
    shortlist = {ext_fields[f]: f for f in fields if f in set(ext_fields.keys())}

    if len(shortlist) == 0:
        return {}

    def get_props(result: PortfolioProperties) -> Dict[str, Timestamp]:
        """
        The responsibility of this function

        :param PortfolioProperties result: The result from the API call to get properties for the Portfolio

        :return: Dict[str, Timestamp]: The extended fields and their values retrieved from LUSID
        """
        nonlocal shortlist
        return {
            shortlist[pk]: lpt.to_date(pv.value.label_value)
            for pk, pv in result.properties.items()
            if pk in shortlist
        }

    response = getattr(api_factory.build(api_mapping[entity_type]), api_call_mapping[entity_type])(
        scope=entity_scope,
        code=entity_code,
        effective_at=effective_date,
        as_at=asat)

    return get_props(response)
