from datetime import datetime
from typing import Dict, List, Tuple

from pandas import Timestamp
import pytz

from interfaces import IComposite


class InMemoryComposite(IComposite):
    """
    This class is responsible for an in memory implementation of a Composite
    """
    def __init__(self, composites: Dict = None, **kwargs):
        """
        :param Dict composites: The composites to initialise the in memory composite with
        """
        if composites is None:
            self.composites = {}
        else:
            self.composites = composites

    @staticmethod
    def _create_id_from_scope_code(scope: str, code: str) -> str:
        """
        Creates an id from a scope and code

        :param scope: The scope
        :param code: The code

        :return: str: The id created from the scope and code.
        """
        return f"{scope}_{code}"

    def add_composite_member(self, composite_scope: str, composite_code: str, member_scope: str, member_code: str,
                             from_date: Timestamp, to_date: Timestamp) -> Timestamp:
        """
        This function is responsible for adding a member to the composite over an effectiveAt range. In the case
        of the in memory composite, the member is added for the entire effectiveAt range.

        :param str composite_scope: The scope of the composite.
        :param str composite_code: The code of the composite. Along with the scope this is used to construct the
        id of the composite.
        :param str member_scope: The scope of the member.
        :param str member_code: The code of the member. Along with the scope this is used to construct the id of
        the member
        :param Timestamp from_date: The start date (inclusive) from which this member is part of the composite
        :param Timestamp to_date: The end date (inclusive) from which this member is part of the composite

        :return: Timestamp: The asAt date at which the member was added to the composite.
        """
        composite_id = self._create_id_from_scope_code(composite_scope, composite_code)
        member_id = self._create_id_from_scope_code(member_scope, member_code)

        if composite_id not in self.composites:
            raise ValueError("Composite does not exist, please create it and try again")

        if member_id not in self.composites[composite_id]:
            self.composites[composite_id].append(member_id)

        return datetime.now(pytz.UTC)

    def remove_composite_member(self, composite_scope: str, composite_code: str, member_scope: str, member_code: str,
                                from_date: Timestamp, to_date: Timestamp) -> Timestamp:
        """
        This function is responsible for removing a member from a composite over an effectiveAt range. In the case
        of the in memory composite, the member is removed for the entire effectiveAt range.

        :param str composite_scope: The scope of the composite.
        :param str composite_code: The code of the composite. Along with the scope this is used to construct the
        id of the composite.
        :param str member_scope: The scope of the member.
        :param str member_code: The code of the member. Along with the scope this is used to construct the id of
        the member
        :param Timestamp from_date: The start date (inclusive) from which this member is not part of the composite
        :param Timestamp to_date: The end date (inclusive) from which this member is not part of the composite

        :return: Timestamp: The asAt date at which the member was removed from the composite.
        """
        composite_id = self._create_id_from_scope_code(composite_scope, composite_code)
        member_id = self._create_id_from_scope_code(member_scope, member_code)

        if composite_id not in self.composites:
            raise ValueError("Composite does not exist, please create it and try again")

        if member_id in self.composites[composite_id]:
            self.composites[composite_id].remove(member_id)

        return datetime.now(pytz.UTC)

    def get_composite_members(self, composite_scope: str, composite_code: str, start_date: Timestamp,
                              end_date: Timestamp, asat: Timestamp) -> Dict[str, List[Tuple[Timestamp, Timestamp]]]:
        """
        This function is responsible for returning the composite members and the effectiveAt date ranges
        for which they are members of the composite for a given bi-temporal period.

        It should return a dictionary keyed by the id for each member of the composite and a list of effectiveAt
        date ranges (inclusive) for which this member is part of the composite.

        In the case of the in memory composite, a member is either part of the composite for the entire effectiveAt
        range or not at all.

        :param str composite_scope: The scope of the composite.
        :param str composite_code: The code of the composite. Along with the scope this is used to construct the
        id of the composite.
        :param Timestamp start_date: The start date (inclusive) from which this member is not part of the composite
        :param Timestamp end_date: The end date (inclusive) from which this member is not part of the composite
        :param Timestamp asat: The asAt date at which to retrieve the members of the composite

        :return: Dict[str, List[Tuple[Timestamp, Timestamp]]]: The members of the composite and the date ranges that
        they were members of the composite
        """
        composite_id = self._create_id_from_scope_code(composite_scope, composite_code)

        if composite_id not in self.composites:
            raise ValueError("Composite does not exist, please create it and try again")

        return {
            member: [(start_date, end_date)] for member in self.composites[composite_id]
        }

    def create_composite(self, composite_scope: str, composite_code: str) -> Timestamp:
        """
        This function is responsible for creating a composite

        :param str composite_scope: The scope of the composite.
        :param str composite_code: The code of the composite. Along with the scope this is used to construct the
        id of the composite.

        :return: Timestamp: The asAt datetime at which the composite was created
        """
        composite_id = self._create_id_from_scope_code(composite_scope, composite_code)

        if composite_id in self.composites:
            raise ValueError("Composite already exists")

        self.composites[composite_id] = []

        return datetime.now(pytz.UTC)
