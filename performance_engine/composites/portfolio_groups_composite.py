import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce
import json
import logging
import requests
import time
from typing import Dict, Tuple, List
import urllib.parse

import dateutil.parser
from lusid.api import PortfolioGroupsApi
from lusid.exceptions import ApiException
from lusid.models import ResourceId, CreatePortfolioGroupRequest, ProcessedCommand, PortfolioGroup
from lusid.utilities import ApiClientFactory
from lusidtools.cocoon.async_tools import (
    run_in_executor, start_event_loop_new_thread, stop_event_loop_new_thread, ThreadPool
)
from pandas import Timestamp
import pytz

from interfaces import IComposite
from misc import as_dates


class CommandDescriptions:
    """
    The responsibility of this class is to hold variables which resolve to the command descriptions for working
    with Portfolio Group Commands
    """
    add_command = "Add portfolio to group".lower()
    remove_command = "Delete portfolio from group".lower()
    create_command = "Create portfolio group".lower()
    delete_command = "Delete portfolio group".lower()


class PortfolioGroupComposite(IComposite):
    """
    The responsibility of this class is to model a composite using Portfolio Groups in LUSID. The following constraints
    exist:

    1) There are no nested composites

    2) If it doesn't already exist the creation date of the Portfolio Group is an arbitrarily early date of 1st January
    1980 to ensure that it is always less then any of its members

    3) Members are always added/removed from the group for a date range starting after the creation date of the
    Portfolio Group
    """

    def __init__(self, api_factory: ApiClientFactory):
        """
        :param ApiClientFactory api_factory: The API factory to use to connect to LUSID
        """
        self.api_factory = api_factory

    async def _enrich_commands_using_insights(self, commands: List[ProcessedCommand],
                                              **kwargs) -> List[Tuple[str, str, str, Timestamp, Timestamp]]:
        """
        The responsibility of this function is to enrich a list of ProcessedCommand with details about the command.

        :param List[ProcessedCommand] commands: The commands to enrich

        :return: List[Tuple[str, str, str, Timestamp, Timestamp]] enriched_commands: The enriched commands
        """

        # The set of commands which can be enriched
        enrichable_commands = {
            CommandDescriptions.add_command,
            CommandDescriptions.remove_command,
            CommandDescriptions.create_command
        }

        # Log a warning if a command is provided which can not be enriched
        [
            logging.warning(
                f"Command of type {command.description} with requestId {command.path} will not be enriched as it"
                f" is not in the enrichable commands of {str(enrichable_commands)}")
            for command in commands if command.description not in enrichable_commands
        ]

        # Enrich the commands
        enriched_commands = await asyncio.gather(
            *[
                self._enrich_single_command_using_insights(
                    request_id=":".join(command.path.split("-")[:2]),
                    command_type=command.description,
                    asat=command.processed_time,
                    **kwargs,
                )
                for command in commands if command.description.lower() in enrichable_commands
            ],
            return_exceptions=False,
        )

        # Some commands can return multiple results, this flattens the enriched commands
        enriched_commands = list(reduce(lambda a, b: a + b, enriched_commands))

        return enriched_commands

    @run_in_executor
    def _enrich_single_command_using_insights(self, request_id: str, command_type: str, asat: Timestamp,
                                              **kwargs) -> List[Tuple[str, str, str, Timestamp, Timestamp]]:
        """
        The responsibility of this function is that given a request_id and command_type it determines
        which portfolio was added or removed from a group and at what effectiveAt time.

        :param str request_id: The request_id for the command
        :param str command_type: The type of command issued
        :param Timestamp asat: The asAt date of the command

        :return: List[Tuple[str, str, str, Timestamp, Timestamp]] portfolio_scope, portfolio_code, command_type,
        asat, effective_date: The identifier for the Portfolio and command type, asAt and effectiveAt date of the command
        """
        start = time.time()
        # Get the API URL for the Insights API from the LUSID API URL
        api_url = self.api_factory.api_client.configuration.host
        request_url = api_url.rstrip('api') + f"insights/api/requests/{request_id}/request"

        # Get the request payload, it might not be available straight away if the command was just issued
        status_code = 404
        retries = 0
        while status_code == 404:

            raw_response = requests.get(
                url=request_url,
                headers={"Authorization": f"Bearer {self.api_factory.api_client.configuration.access_token}"}
            )
            status_code = raw_response.status_code
            logging.debug(f"Status code {raw_response.status_code} and response of {raw_response} with headers "
                          f"{raw_response.headers} trying to enrich Log for {request_id}")

            if status_code == 404 and retries < 5:
                retries += 1
                time.sleep(2)
            elif 200 <= status_code <= 299:
                break
            else:
                raise ApiException(status=status_code)

        response = json.loads(raw_response.text)
        request_url = response["url"]
        url_split = request_url.split("/")
        command_type = command_type.lower()
        # Depending on the command type, parse out the relevant information from the request
        if command_type == CommandDescriptions.add_command:
            effective_date = dateutil.parser.parse(urllib.parse.unquote(url_split[-1].split("=")[1]))
            request_body = json.loads(response["body"])
            portfolio_scopes = [request_body["scope"]]
            portfolio_codes = [request_body["code"]]

        elif command_type == CommandDescriptions.remove_command:
            portfolio_info_start_index = url_split.index("portfolios")
            portfolio_info = url_split[portfolio_info_start_index:]
            portfolio_scopes = [portfolio_info[1]]
            portfolio_codes = [portfolio_info[2].split("?")[0]]
            effective_date = dateutil.parser.parse(urllib.parse.unquote(portfolio_info[2].split("=")[1]))

        elif command_type == CommandDescriptions.create_command:
            # This command is converted into 0 or more Add portfolio to group commands which have the same impact
            request_body = json.loads(response["body"])
            effective_date = dateutil.parser.parse(request_body["created"])
            if "values" in request_body:
                portfolio_scopes = [resource_id["scope"] for resource_id in request_body["values"]]
                portfolio_codes = [resource_id["code"] for resource_id in request_body["values"]]
            else:
                portfolio_scopes = []
                portfolio_codes = []
            command_type = CommandDescriptions.add_command

        logging.debug(f"Getting Insight Log for {request_id} Took: {time.time() - start}")

        return [
            (portfolio_scope, portfolio_code, command_type, asat, effective_date)
            for portfolio_scope, portfolio_code in zip(portfolio_scopes, portfolio_codes)
        ]

    def _get_portfolio_group_membership_history(self, composite_scope: str, composite_code: str,
                                                asat: Timestamp = None) -> Dict[str, List[Tuple[
                                                str, Timestamp, Timestamp]]]:
        """
        The responsibility of this function is to retrieve the membership history of all Portfolios associated with the
        Portfolio Group

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group
        :param Timestamp asat: The asAt datetime at which to look at the group membership

        :return: Dict[str, List[Tuple[str, Timestamp, Timestamp]]] membership_history: The membership history of for
        each Portfolio
        """
        keyword_arguments = {}

        if asat is not None:
            keyword_arguments["to_as_at"] = asat

        start = time.time()

        # Get the commands issued to the Portfolio Group
        commands = self.api_factory.build(PortfolioGroupsApi).get_portfolio_group_commands(
            scope=composite_scope,
            code=composite_code,
            **keyword_arguments
        ).values

        logging.debug(f"Getting Portfolio Commands took: {time.time() - start}")

        # Filter out all those before the most recent creation event, this handles deletion and re-creation of the group
        most_recent_creation = max([
            command.processed_time for command in commands if command.description.lower() == CommandDescriptions.create_command
        ])
        commands = list(filter(lambda x: x.processed_time >= most_recent_creation, commands))

        # If there is a delete event in here still then the Portfolio does not exist
        if CommandDescriptions.delete_command in set([command.description.lower() for command in commands]):
            raise ValueError("Portfolio Group does not exist")

        # Enrich the commands using Insights
        loop = start_event_loop_new_thread()
        start = time.time()
        enriched_commands = asyncio.run_coroutine_threadsafe(
            self._enrich_commands_using_insights(
                commands=commands,
                thread_pool=ThreadPool(25).thread_pool,
            ),
            loop,
        ).result()

        # Construct the membership history from the enriched commands
        membership_history = defaultdict(list)

        for enriched_command in enriched_commands:
            membership_history[f"{enriched_command[0]}_{enriched_command[1]}"].append(
                (enriched_command[2], enriched_command[3], enriched_command[4]))

        logging.debug(f"Getting all Insights for {len(commands)} commands took: {time.time() - start}")
        stop_event_loop_new_thread(loop)

        return membership_history

    @staticmethod
    def _calculate_effective_at_date_ranges_from_membership_history(history: List[Tuple[str, Timestamp,
                                                                    Timestamp]]) -> List[Tuple[str, Timestamp,
                                                                    Timestamp, Timestamp]]:
        """
        The responsibility of this function is for a single Portfolio to construct the date ranges that each command
        to add or remove the Portfolio from the Group is effective for. For example a single add Portfolio to
        Group command effectiveAt 2020-01-05 is effective for the range 2020-01-05 to datetime.max, whereas a subsequent
        command to add the Portfolio to the group effectiveAt 2019-12-15 is only effective for the range 2019-12-15 to
        2020-01-04. The effectiveAt range is inclusive.

        Assumptions:
        - Precision is not greater than daily

        :param List[Tuple[str, Timestamp, Timestamp]] history: The membership history for the Portfolio based on commands
        to add or remove the Portfolio from the Portfolio Group

        :return: List[Tuple[str, Timestamp, Timestamp, Timestamp]] date_ranges: The date ranges that each command is
        effective for.
        """

        # Make a copy of the list so as to not affect the original
        history_chronological = history.copy()

        # Sort by the as_at date to give the events in chronological order along the asAt axis
        history_chronological.sort(key=lambda x: x[1])

        '''
        A command will have an effective for range from its effectiveAt date up until the effectiveAt date of the next
        command (which has already been issued i.e. has an asAt date less than the current command) moving into the 
        future along the effectiveAt axis. 
        
        Therefore by going through the events in chronological order along the asAt axis, you can determine the 
        effectiveAt range for each subsequent command by finding its nearest neighbour in the future in effectiveAt 
        time.
        
        As the effective for range is inclusive, you then walk back the minimum amount of time (in this case a single
        day) to produce the inclusive end date for the effectiveAt range. 
        '''
        date_ranges = []

        # Walk through the commands in chronological order along the asAt axis
        for command in history_chronological:

            # Get the details of the command which are useful
            event_type = command[0]
            event_as_at = command[1]
            event_effective_at = command[2]

            # Start of the range is always the effectiveAt date of the event
            start = event_effective_at

            # Determine the possible candidates for the end date which is the nearest effectiveAt date in the future
            # of an event which already exists i.e. has an asAt time less than the current event
            possible_next_commands = list(
                filter(
                    lambda x: x[1] < event_as_at and x[2] > event_effective_at, history_chronological
                )
            )

            # If there are candidates, determine the closest neighbour and take a day off to give the inclusive end date
            if len(possible_next_commands) > 0:
                end = min(possible_next_command[2] for possible_next_command in possible_next_commands) - timedelta(days=1)
            # Otherwise it will apply forever which for comparison is represented by datetime.max
            else:
                end = datetime.max
                # Must give it a timezone to compare with other timezone aware datetimes
                end = end.replace(tzinfo=pytz.UTC)

            # With the inclusive start and end dates determined, add the date range to the list
            date_ranges.append((event_type, event_as_at, start, end))

        return date_ranges

    @staticmethod
    def _calculate_current_membership_from_effective_at_date_ranges(date_ranges: List[Tuple[str, Timestamp,
                                                                    Timestamp, Timestamp]]) -> List[Tuple[Timestamp,
                                                                    Timestamp]]:
        """
        The responsibility of this function, is given the effectiveAt ranges that each command in a Portfolio's
        membership history to produce the current membership of the Portfolio in the group as a list
        of datetime ranges when it is a member of the Group.

        This function essentially flattens the list of commands across the asAt axis to produce the current
        membership.

        This function also discards explicit information about when a Portfolio is not a member of group as this is
        implied by the missing date ranges in the response containing the date ranges in which the Portfolio is a
        member of a group.

        :param List[Tuple[str, Timestamp, Timestamp, Timestamp]] date_ranges: The effectiveAt date ranges of each
        command

        :return: List[Tuple[Timestamp, Timestamp]] membership_joined: The date ranges of which the Portfolio is a 
        member of the Portfolio Group
        """

        '''
        With the effectiveAt ranges discovered, you can now go in reverse chronological order along the asAt axis with
        the more recent commands taking precedence. For example if there was a command issued asAt 2020-02-10 to add the 
        Portfolio to the  Portfolio Group for the effectiveAt range 2019-12-15 to 2020-01-05 in addition to to a command
        issued asAt 2020-01-10 to delete the Portfolio from the Portfolio Group from 2019-12-01 to 2020-01-3, then 
        the more recent add command takes precedence leading to the effectiveAt range of the delete event being trimmed
        to become 2019-12-01 to 2019-12-14. 
        
        By traversing backwards along the asAt axis, this allows you to construct the current membership. 
        '''
        date_ranges_flattened = []
        date_ranges_copy = date_ranges.copy()
        
        # Sort in reverse chronological order
        date_ranges_copy.sort(key=lambda x: x[1], reverse=True)

        for date_range in date_ranges_copy:
            
            # Get the useful information from the date range
            date_range_type = date_range[0]
            date_range_start = date_range[2]
            date_range_end = date_range[3]
            
            # Base case, if no date range has been added to the flattened membership then add the date range
            if len(date_ranges_flattened) == 0:
                date_ranges_flattened.append((date_range_type, date_range_start, date_range_end))
                continue
            
            # For each flattened date range, find where the current date range intersects with it, trimming down
            # or completely removing the date range based on what already exists
            date_ranges_flattened.sort(key=lambda x: x[2], reverse=True)
            for date_range_flattened in date_ranges_flattened:

                date_range_flattened_start = date_range_flattened[1]
                date_range_flattened_end = date_range_flattened[2]

                # No overlap case, no trimming required
                if date_range_start > date_range_flattened_end or date_range_end < date_range_flattened_start:
                    continue

                # Encapsulated, complete removal
                elif date_range_start >= date_range_flattened_start and date_range_end <= date_range_flattened_end:
                    date_range_start, date_range_end = None, None
                    break

                # Border End, complete removal
                elif date_range_start == date_range_flattened_end and date_range_end == date_range_flattened_end:
                    date_range_start, date_range_end = None, None
                    break

                # Border Start, complete removal
                elif date_range_start == date_range_flattened_start and date_range_end == date_range_flattened_start:
                    date_range_start, date_range_end = None, None
                    break

                # Cross Start, update end date
                elif date_range_start < date_range_flattened_start <= date_range_end <= date_range_flattened_end:
                    date_range_end = date_range_flattened_start - timedelta(days=1)

                # Cross End, update start date
                elif date_range_end > date_range_flattened_end >= date_range_start >= date_range_flattened_start:
                    date_range_start = date_range_flattened_end + timedelta(days=1)

                # Encapsulates - Need to confirm that this is impossible
                else:
                    continue

            # If the effectiveAt range is completely supersededed by a more recent event in effectiveAt time, it is
            # not included at all
            if date_range_start is not None and date_range_end is not None:
                date_ranges_flattened.append((date_range_type, date_range_start, date_range_end))

        # Drop the "Remove portfolio points"
        add_ranges = list(filter(lambda x: x[0].lower() == CommandDescriptions.add_command, date_ranges_flattened))
        # Drop the asAt date as it is no longer required
        membership = [(add_range[1], add_range[2]) for add_range in add_ranges]
        # Sort in chronological order based on effectiveAt start date of range
        membership.sort(key=lambda x: x[0])

        # If there is only one date range, return it
        if len(membership) <= 1:
            return membership

        # Otherwise look to see if any of the date ranges can be joined to form a single continuous range, e.g. if you
        # have the range 2020-01-05 to 2020-01-08 and the range 2020-01-09 to 2020-01-15 these can be combined to form
        # a single range of 2020-01-05 to 2020-01-15.
        membership_joined = []

        previous_start_date = membership[0][0]
        previous_end_date = membership[0][1]

        for member_date_range in membership[1:]:
            current_start_date = member_date_range[0]
            current_end_date = member_date_range[1]

            if (current_start_date - previous_end_date).days != 1:
                membership_joined.append((previous_start_date, previous_end_date))
                previous_start_date = current_start_date
                previous_end_date = current_end_date
                continue

            previous_end_date = current_end_date

        membership_joined.append((previous_start_date, previous_end_date))

        return membership_joined

    def _add_portfolio(self, composite_scope: str, composite_code: str, from_date: Timestamp, member_scope: str,
                       member_code: str) -> Timestamp:
        """
        The responsibility of this function is to add a Portfolio to a Group at the provided effectiveAt date.

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group
        :param str member_scope: The scope of the Portfolio to add to the Portfolio Group as a member
        :param str member_code: The code of the Portfolio to add to the Portfolio Group as a member, together with
        the member_scope this uniquely identifies the Portfolio in LUSID
        :param Timestamp from_date: The date (inclusive) at which the Portfolio becomes a member of the Portfolio Group

        :return: Timestamp: The asAt date of the version of the Portfolio Group
        """
        portfolio_groups_api = self.api_factory.build(PortfolioGroupsApi)

        add_response = portfolio_groups_api.add_portfolio_to_group(
            scope=composite_scope,
            code=composite_code,
            effective_at=from_date,
            resource_id=ResourceId(
                scope=member_scope,
                code=member_code
            )
        )

        return add_response.version.as_at_date

    def _remove_portfolio(self, composite_scope: str, composite_code: str, from_date: Timestamp, member_scope: str,
                          member_code: str) -> Timestamp:
        """
        The responsibility of this function is to remove a Portfolio from a Portfolio Group at the provided effectiveAt
        date

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group
        :param str member_scope: The scope of the Portfolio to remove to the Portfolio Group as a member
        :param str member_code: The code of the Portfolio to renive to the Portfolio Group as a member, together with
        the member_scope this uniquely identifies the Portfolio in LUSID
        :param Timestamp from_date: The date (inclusive) at which the Portfolio is no longer a member of the Portfolio
        Group

        :return: Timestamp: The asAt date of the version of the Portfolio Group
        """
        portfolio_groups_api = self.api_factory.build(PortfolioGroupsApi)

        remove_response = portfolio_groups_api.delete_portfolio_from_group(
            scope=composite_scope,
            code=composite_code,
            portfolio_scope=member_scope,
            portfolio_code=member_code,
            effective_at=from_date
        )

        return remove_response.version.as_at_date

    @as_dates
    def _update_composite_membership(self, composite_scope: str, composite_code: str, method: str, member_scope: str,
                                     member_code: str, from_date: Timestamp, to_date: Timestamp = None) -> Timestamp:
        """
        The responsibility of this function is to add/remove a Portfolio to/from the Portfolio Group for the provided
        range of effectiveAt time. If no to_date is specified the range is open ended. Regardless of the state of the
        group before this method is executed the Portfolio is guaranteed to either exist/not exist for the range
        specified when calling this function.

        Assumptions

        1) A Portfolio will be a member of a group for an entire day or none of the day.

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group
        :param str method: The method to update the composite membership, can be 'add' or 'remove'
        :param str member_scope: The scope of the Portfolio to add to the Portfolio Group as a member
        :param str member_code: The code of the Portfolio to add to the Portfolio Group as a member, together with
        the member_scope this uniquely identifies the Portfolio in LUSID
        :param Timestamp from_date: The date (inclusive) at which the Portfolio is included/excluded as a member of the
        Portfolio Group
        :param Timestamp to_date: The date (inclusive) to which the Portfolio is included/excluded as a member of the
        Portfolio Group

        :return: Timestamp as_at: The as_at date of the most recent command affecting the Portfolio Group
        """

        method = method.lower()

        if method not in ["add", "remove"]:
            raise ValueError(f"Allowed methods are 'add' or 'remove'. You specified {method}.")

        # Depending on previous commands there is often a need to do the reverse, this helps find what the reverse is
        method_inverse_lookup = {
            "add": "remove",
            "remove": "add"
        }
        method_name_lookup = {
            "add": CommandDescriptions.add_command,
            "remove": CommandDescriptions.remove_command
        }
        inverse_method = method_inverse_lookup[method]
        inverse_method_name = method_name_lookup[method_inverse_lookup[method]]

        start = time.time()

        # Max date to use if no to_date is specified
        max_date = datetime.max
        max_date = max_date.replace(tzinfo=pytz.UTC)

        as_at = None

        if to_date is None:
            to_date = max_date

        # Get the membership history for the portfolio
        membership_history = self._get_portfolio_group_membership_history(
            composite_scope, composite_code)[f"{member_scope}_{member_code}"]
        command_date_ranges = self._calculate_effective_at_date_ranges_from_membership_history(membership_history)

        '''
        1) Order commands by effectiveAt date up until the start time
        '''
        # Sort by effectiveAt date in chronological order
        command_date_ranges.sort(key=lambda x: x[2])

        # If there are two commands with the same effectiveAt date choose the one with the greatest asAt date as
        # it takes precedence
        if len(command_date_ranges) > 0:

            index_to_drop = []
            previous = command_date_ranges[0]

            for index, command in enumerate(command_date_ranges):
                # Same effectiveAt date as previous and lower asAt date, discard it
                if command[2] == previous[2] and command[1] < previous[1]:
                    index_to_drop.append(index)
                # Same effectiveAt date as previous and greater asAt date, keep it and discard previous
                elif command[2] == previous[2] and command[1] > previous[1]:
                    index_to_drop.append(index-1)
                # Update the previous
                previous = command

            # Remove all redundant commands, index_to_drop is sorted so index is not affected by prior deletion
            for index in sorted(index_to_drop, reverse=True):
                del command_date_ranges[index]

        # Find all commands which precede the start of the date range to add/remove the Portfolio for
        previous_events = list(filter(lambda x: x[2] <= from_date, command_date_ranges))

        '''
        2) If immediately previous was the inverse of the desired state e.g. previous was 'remove' and you want to 'add'
        then create a command to override the effect of the previous inverse. Same if there is no previous at all
        '''
        if len(previous_events) == 0 or previous_events[-1][0] == inverse_method_name:

            as_at = getattr(self, f"_{method}_portfolio")(
                composite_scope=composite_scope,
                composite_code=composite_code,
                from_date=from_date,
                member_scope=member_scope,
                member_code=member_code
            )

        '''
        3) Identify all commands between the start date and end date, iterate through in chronological effectiveAt order 
        replacing all inverse with desired state
        '''
        # Get subsequent events after start date up until the end date
        subsequent_events = list(filter(lambda x: from_date < x[2] <= to_date, command_date_ranges))

        # Overwrite each remove/add event with an add/remove event
        for event in subsequent_events:
            if event[0] == inverse_method_name:

                as_at = getattr(self, f"_{method}_portfolio")(
                    composite_scope=composite_scope,
                    composite_code=composite_code,
                    from_date=event[2],
                    member_scope=member_scope,
                    member_code=member_code
                )

        # If there is no end date, then no further action required
        if to_date == max_date:
            return as_at

        # Also if there is
        # 1) An event immediately following the end date
        # 2) The command before the start date was the desired state and there were no subsequent commands
        # 3) There are no events before or after and the command is to remove (default behaviour of a Group)
        # 4) The last command before the end date was the same as the desired state
        # Then no further action is required
        epilogue_event = list(filter(lambda x: x[2] == to_date + timedelta(days=1), command_date_ranges))

        if len(epilogue_event) == 1 or \
                (
                        len(subsequent_events) == 0 and
                        len(previous_events) > 0 and
                        previous_events[-1][0] == method_name_lookup[method]
                ) or \
                (
                        len(subsequent_events) == 0 and
                        len(previous_events) == 0 and
                        method == "remove"
                ) or \
                (
                    len(subsequent_events) > 0 and
                    subsequent_events[-1][0] == method_name_lookup[method]
                ):
            return as_at
        '''
        4) Otherwise, place the inverse at end date plus one, this ensures that the Portfolio's inclusion/exclusion 
        does not extend outside the desired range
        '''
        as_at = getattr(self, f"_{inverse_method}_portfolio")(
            composite_scope=composite_scope,
            composite_code=composite_code,
            from_date=to_date + timedelta(days=1),
            member_scope=member_scope,
            member_code=member_code
        )

        logging.debug(f"Add Portfolio Took: {time.time() - start}")
        return as_at

    def add_composite_member(self, composite_scope: str, composite_code: str, member_scope: str, member_code: str,
                             from_date: Timestamp, to_date: Timestamp) -> Timestamp:
        """
        This function is responsible for adding a member to the composite over an effectiveAt range

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group
        :param str member_scope: The scope of the member. This is the scope of the Portfolio in LUSID.
        :param str member_code: The code of the member. Along with the member_scope it uniquely identifies the Portfolio
        in LUSID
        :param Timestamp from_date: The start date (inclusive) from which this member is part of the composite
        :param Timestamp to_date: The end date (inclusive) from which this member is part of the composite

        :return: Timestamp: The asAt date at which the member was added to the composite.
        """
        return self._update_composite_membership(
            composite_scope=composite_scope,
            composite_code=composite_code,
            method="add",
            member_scope=member_scope,
            member_code=member_code,
            from_date=from_date,
            to_date=to_date)

    def remove_composite_member(self, composite_scope: str, composite_code: str, member_scope: str, member_code: str,
                                from_date: Timestamp, to_date: Timestamp) -> Timestamp:
        """
        This function is responsible for removing a member to the composite over an effectiveAt range

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group
        :param str member_scope: The scope of the member. This is the scope of the Portfolio in LUSID.
        :param str member_code: The code of the member. Along with the member_scope it uniquely identifies the Portfolio
        in LUSID
        :param Timestamp from_date: The start date (inclusive) from which this member is not part of the composite
        :param Timestamp to_date: The end date (inclusive) from which this member is not part of the composite

        :return: Timestamp: The asAt date at which the member was removed from the composite.
        """
        return self._update_composite_membership(
            composite_scope=composite_scope,
            composite_code=composite_code,
            method="remove",
            member_scope=member_scope,
            member_code=member_code,
            from_date=from_date,
            to_date=to_date)

    @as_dates
    def get_composite_members(self, composite_scope: str, composite_code: str, start_date: Timestamp,
                              end_date: Timestamp, asat: Timestamp) -> Dict[str, List[Tuple[Timestamp, Timestamp]]]:
        """
        The responsibility of this method is to get the members of a Portfolio Group over an effectiveAt date range,
        returning the members of the group along with the date ranges at which they where members of the group.

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group
        :param Timestamp start_date: The effectiveAt date (inclusive) from which to start looking at group membership
        :param Timestamp end_date: The effectiveAt end date (inclusive) at which to stop looking at group membership
        :param Timestamp asat: The asAt date at which to look at group membership

        :return: Dict[str, List[Tuple[Timestamp, Timestamp]]] ranges: The members of the group and the
        effectiveAt date ranges (inclusive) during which they were members of the Portfolio Group inside the requested
        window
        """

        # Get the history of adding and removing members from the Portfolio Group
        history = self._get_portfolio_group_membership_history(composite_scope, composite_code)

        # Calculate the current membership of each Portfolio in the Portfolio Group
        ranges = {}
        for portfolio, event_history in history.items():
            command_date_ranges = self._calculate_effective_at_date_ranges_from_membership_history(event_history)
            ranges[portfolio] = self._calculate_current_membership_from_effective_at_date_ranges(
                command_date_ranges)

        # Filter the membership down to what is inside the requested window
        records_to_remove = []

        for portfolio, date_ranges in ranges.items():
            date_ranges_updated = [
                (max(date_range[0], start_date), min(date_range[1], end_date)) for date_range in date_ranges
                if date_range[0] <= end_date and date_range[1] >= start_date
            ]
            if len(date_ranges_updated) > 0:
                ranges[portfolio] = date_ranges_updated
            else:
                records_to_remove.append(portfolio)

        for record in records_to_remove:
            del ranges[record]

        return ranges

    def create_composite(self, composite_scope: str, composite_code: str) -> PortfolioGroup:
        """
        This function is responsible for creating a composite

        :param str composite_scope: The scope of the Porfolio Group which represents the composite in LUSID
        :param str composite_code: The code of the Portfolio Group which represents the composite in LUSID. Together
        with the scope this uniquely identifies the Portfolio Group

        :return: Timestamp: The asAt datetime at which the composite was created
        """
        try:
            # See if the Portfolio Group already exists
            response = self.api_factory.build(PortfolioGroupsApi).get_portfolio_group(
                scope=composite_scope, code=composite_code
            )
        except ApiException as e:
            if e.status == 404:
                # If not, create a new Portfolio Group
                response = self.api_factory.build(PortfolioGroupsApi).create_portfolio_group(
                    scope=composite_scope,
                    create_portfolio_group_request=CreatePortfolioGroupRequest(
                        code=composite_code,
                        created=datetime(1980, 1, 1, tzinfo=pytz.UTC),
                        display_name=f"Composite-{composite_code}"
                    )
                )
            else:
                raise e

        return response
