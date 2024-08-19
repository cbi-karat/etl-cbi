from __future__ import annotations

from pathlib import Path
from sys import path

path.append(str(Path(__file__).resolve().parents[1]))
import json
from datetime import date

import pandas as pd
import requests as req

from credentials.KDL_passwords import KDL

INTERNAL_SERVER_ERROR = 500
MSG_LIST = [
    "The table {} wasn't found",
    "Incorrect date",
    "Dates are required",
    "Dates aren't required",
    "The error isn't defined",
    """There is no data for this period, or this table uses grouping by month.
    If the table uses month grouping, try using the format 01.MM.YYYY""",
]


def get_table_from_kdl(dbname: str, start_date: str | None = None, end_date: str | None = None):
    dtrequired = dates_required_check(dbname)
    if dtrequired and start_date is None and end_date is None:
        raise ValueError(MSG_LIST[2])
    if not dtrequired and start_date is not None:
        raise ValueError(MSG_LIST[3])
    if start_date is None and end_date is None:
        response = get_json(dbname, None)
        return convert_json_to_dataframe(response)
    if start_date is not None:
        dates_correctness_check(start_date, end_date)
        start_dt_date_type = pd.to_datetime(start_date, format="%d.%m.%Y")
        end_dt_date_type = start_dt_date_type if end_date is None else pd.to_datetime(end_date, format="%d.%m.%Y")
        dates_list = splitting_period_by_month(start_dt_date_type, end_dt_date_type)
        response = get_json(dbname, dates_list)
        return convert_json_to_dataframe(response)
    raise ValueError(MSG_LIST[4])


def dates_correctness_check(start_date: str, end_date: str | None):
    if end_date is None:
        try:
            test_start_date = pd.to_datetime(start_date, format="%d.%m.%Y")
        except ValueError as e:
            raise ValueError(MSG_LIST[1]) from e
    elif end_date is not None:
        try:
            test_start_date = pd.to_datetime(start_date, format="%d.%m.%Y")
            test_end_date = pd.to_datetime(end_date, format="%d.%m.%Y")
        except ValueError as e:
            raise ValueError(MSG_LIST[1]) from e
        else:
            if test_start_date > test_end_date:
                raise ValueError(MSG_LIST[1])


def splitting_period_by_month(start_date: date, end_date: date):
    dates_list = []
    if start_date.day != 1:
        dates_list.append(start_date)
    dates_list += list(pd.date_range(start_date, end_date, freq="MS"))
    if end_date.day != 1:
        dates_list.append(end_date)
    return dates_list


def dates_required_check(dbname: str):
    query = {"DBName": dbname, "НачалоПериода": date(1, 1, 1).isoformat(), "КонецПериода": date(1, 1, 1).isoformat()}
    query = json.dumps(query)
    login = KDL["login"]
    login = login.encode("utf-8")
    login = login.decode("latin-1")
    password = KDL["password"]
    url = KDL["url"]
    response = req.post(
        url,
        auth=(login, password),
        json=query,
        timeout=100,
    )
    return response.status_code != INTERNAL_SERVER_ERROR


def get_json(dbname: str, periods_list: list | None = None):
    login = KDL["login"]
    login = login.encode("utf-8")
    login = login.decode("latin-1")
    password = KDL["password"]
    url = KDL["url"]
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
                query = json.dumps(query)
                response = session.post(
                    url,
                    auth=(login, password),
                    json=query,
                    timeout=100,
                )
                if response.status_code == INTERNAL_SERVER_ERROR:
                    raise ValueError(MSG_LIST[0].format(dbname))
                response = response.json()
                response = json.loads(response["DBData"])
                response = response["Результат"]
                responses += response
        return responses
    query = {"DBName": dbname}
    query = json.dumps(query)
    response = req.post(
        url,
        auth=(login, password),
        json=query,
        timeout=100,
    )
    if response.status_code == INTERNAL_SERVER_ERROR:
        raise ValueError(MSG_LIST[0].format(dbname))
    response = response.json()
    response = json.loads(response["DBData"])
    return response["Результат"]


def convert_json_to_dataframe(response: list):
    if response == []:
        raise ValueError(MSG_LIST[5])
    return pd.DataFrame(response)
