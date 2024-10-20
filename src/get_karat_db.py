from __future__ import annotations

import json
from datetime import date

import pandas as pd
import requests as req

from credentials.KDL_passwords import KDL

INTERNAL_SERVER_ERROR = 500
MSG_LIST = [
    "Dates are required",
    "Dates aren't required",
    """The table "{}" wasn't found""",
    "Incorrect date",
    "The first date is less than the second",
]


class MissingDatesError(Exception):
    pass


class ExtraDatesError(Exception):
    pass


class InternalServerError(Exception):
    pass


class EmptyDataframeError(Exception):
    pass


class IncorrectDateError(Exception):
    pass


def get_table_from_kdl(dbname: str, start_date: str | None = None, end_date: str | None = None):
    dtrequired = dates_required_check(dbname)
    dates_list = None
    if dtrequired:
        if start_date is None and end_date is None:
            raise MissingDatesError(MSG_LIST[0])
        if start_date is not None:
            dates_list = convert_date_format(start_date, end_date)
    if not dtrequired and start_date is not None:
        raise ExtraDatesError(MSG_LIST[1])
    response = get_response_from_kdl(dbname, dates_list)
    return convert_json_to_dataframe(response)


def convert_date_format(start_date: str, end_date: str | None):
    dates_correctness_check(start_date, end_date)
    start_dt_date_type = pd.to_datetime(start_date, format="%d.%m.%Y")
    end_dt_date_type = start_dt_date_type if end_date is None else pd.to_datetime(end_date, format="%d.%m.%Y")
    return prepare_dates_for_request(start_dt_date_type, end_dt_date_type)


def dates_correctness_check(start_date: str, end_date: str | None):
    if end_date is None:
        try:
            test_start_date = pd.to_datetime(start_date, format="%d.%m.%Y")
            if not isinstance(test_start_date, date):
                raise IncorrectDateError(MSG_LIST[3])
        except ValueError:
            raise IncorrectDateError(MSG_LIST[3]) from None
    elif end_date is not None:
        try:
            test_start_date = pd.to_datetime(start_date, format="%d.%m.%Y")
            test_end_date = pd.to_datetime(end_date, format="%d.%m.%Y")
            if not isinstance(test_start_date, date):
                raise IncorrectDateError(MSG_LIST[3])
            if not isinstance(test_end_date, date):
                raise IncorrectDateError(MSG_LIST[3])
            if test_start_date > test_end_date:
                raise IncorrectDateError(MSG_LIST[4])
        except ValueError:
            raise IncorrectDateError(MSG_LIST[3]) from None


def prepare_dates_for_request(start_date: date, end_date: date):
    dates_list = []
    if start_date.day != 1:
        dates_list.append(start_date)
    dates_list += list(pd.date_range(start_date, end_date, freq="MS"))
    if end_date.day != 1:
        dates_list.append(end_date)
    if len(dates_list) == 1:
        dates_list.append(dates_list[0])
    return dates_list


def dates_required_check(dbname: str):
    query = {"DBName": dbname, "НачалоПериода": date(1, 1, 1).isoformat(), "КонецПериода": date(1, 1, 1).isoformat()}
    with req.Session() as session:
        try:
            _ = _send_request(query, session)
        except InternalServerError:
            return False
        return True


def get_response_from_kdl(dbname: str, periods_list: list | None = None):
    if periods_list is not None:
        responses = []
        with req.Session() as session:
            for i in range(len(periods_list) - 1):
                start_date = periods_list[i]
                end_date = periods_list[i + 1]
                query = {
                    "DBName": dbname,
                    "НачалоПериода": start_date.isoformat(),
                    "КонецПериода": end_date.isoformat(),
                }
                response = _send_request(query, session)
                responses += response
        return responses
    query = {"DBName": dbname}
    with req.Session() as session:
        return _send_request(query, session)


def _send_request(args: dict, session):
    login = KDL["login"]
    login = login.encode("utf-8")
    login = login.decode("latin-1")
    password = KDL["password"]
    url = KDL["url"]
    query = json.dumps(args)
    response = session.post(
        url,
        auth=(login, password),
        json=query,
        timeout=100,
    )
    if response.status_code == INTERNAL_SERVER_ERROR:
        raise InternalServerError(MSG_LIST[2].format(args["DBName"]))
    response = response.json()
    response = json.loads(response["DBData"])
    return response["Результат"]


def convert_json_to_dataframe(response: list):
    return pd.DataFrame(response)
