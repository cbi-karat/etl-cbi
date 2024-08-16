from __future__ import annotations

from pathlib import Path
from sys import path

path.append(str(Path(__file__).resolve().parents[1]))
import json
from datetime import date

import pandas as pd
import requests as req

from credentials.KDL_passwords import KDL

MSG_LIST = [
    "The table wasn't found",
    "Incorrect date",
    "Dates are required",
    "Dates aren't required",
    "The error isn't defined",
    """There is no data for this period, or this table uses grouping by month.
    If the table uses month grouping, try using the format 01.MM.YYYY""",
]


def request(dbname: str, start_date: str | None = None, end_date: str | None = None):
    correctinput, dtrequired = get_json_test(dbname)
    if correctinput == 0:
        raise ValueError(MSG_LIST[0])
    if start_date is None and end_date is None:
        dates = 0
        return table_format_check(dates, dtrequired, dbname, start_date, end_date)
    if start_date is not None:
        return dates_correctness_check(dtrequired, dbname, start_date, end_date)
    raise ValueError(MSG_LIST[4])


def checking_date_format(date: str | date | None):
    datesymbols = 10
    return bool(
        isinstance(date, str)
        and len(date) == datesymbols
        and date[0:2].isdigit()
        and date[3:5].isdigit()
        and date[6:].isdigit()
        and date[2] == "."
        and date[5] == ".",
    )


def dates_correctness_check(dtrequired: int, dbname: str, start_date: str | date, end_date: str | date | None):
    if checking_date_format(start_date) and end_date is None:
        dates = 1
        try:
            start_date = pd.to_datetime(start_date, format="%d.%m.%Y")
        except ValueError as e:
            raise ValueError(MSG_LIST[1]) from e
        else:
            end_date = start_date
            return table_format_check(dates, dtrequired, dbname, start_date, end_date)
    elif checking_date_format(start_date) and checking_date_format(end_date):
        dates = 1
        try:
            start_date = pd.to_datetime(start_date, format="%d.%m.%Y")
            if end_date is not None:
                end_date = pd.to_datetime(end_date, format="%d.%m.%Y")
        except ValueError as e:
            raise ValueError(MSG_LIST[1]) from e
        else:
            return table_format_check(dates, dtrequired, dbname, start_date, end_date)
    else:
        raise ValueError(MSG_LIST[1])


def table_format_check(dates: int, dtrequired: int, dbname: str, start_date: date | None, end_date: date | None):
    if dates == 0 and dtrequired == 1:
        raise ValueError(MSG_LIST[2])
    if dates == 1 and dtrequired == 0:
        raise ValueError(MSG_LIST[3])
    if dates == 1 and dtrequired == 1 and start_date is not None and end_date is not None:
        if start_date > end_date:
            raise ValueError(MSG_LIST[1])
        return distribution_to_periods(dbname, start_date, end_date, 1)
    if dates == 0 and dtrequired == 0:
        return distribution_to_periods(dbname, start_date, end_date, 0)
    raise ValueError(MSG_LIST[4])


def distribution_to_periods(dbname: str, start_date: date | None, end_date: date | None, dtrequired: int):
    if dtrequired == 1:
        last_month = date(int(str(end_date)[0:4]), int(str(end_date)[5:7]), 1)
        try:
            second_month = date(int(str(start_date)[0:4]), int(str(start_date)[5:7]) + 1, 1)
        except ValueError:
            second_month = date(int(str(start_date)[0:4]) + 1, 1, 1)
        if second_month > last_month:
            periods_list = [start_date, end_date]
        elif second_month == last_month and last_month != end_date:
            periods_list = [start_date, second_month, end_date]
        elif second_month == last_month and last_month == end_date:
            periods_list = [start_date, end_date]
        else:
            periods_list = [start_date]
            interim_date = second_month
            while interim_date != last_month:
                periods_list.append(interim_date)
                try:
                    interim_date = date(int(str(interim_date)[0:4]), int(str(interim_date)[5:7]) + 1, 1)
                except ValueError:
                    interim_date = date(int(str(interim_date)[0:4]) + 1, 1, 1)
            periods_list.append(last_month)
            if last_month != end_date:
                periods_list.append(end_date)
        return get_json(dbname, periods_list)
    return get_json(dbname, None)


def get_json_test(dbname: str):
    query = {"DBName": dbname, "НачалоПериода": date(1, 1, 1).isoformat(), "КонецПериода": date(1, 1, 1).isoformat()}
    query = json.dumps(query)
    login = KDL["login"]
    login = login.encode("utf-8")
    login = login.decode("latin-1")
    password = KDL["password"]
    url = KDL["url"]
    try:
        response = req.post(
            url,
            auth=(login, password),
            json=query,
            timeout=100,
        )
        response = response.json()
    except json.JSONDecodeError:
        try:
            query = {"DBName": dbname}
            query = json.dumps(query)
            response = req.post(
                url,
                auth=(login, password),
                json=query,
                timeout=100,
            )
            response = response.json()
        except json.JSONDecodeError:
            return 0, 0
        else:
            return 1, 0
    else:
        return 1, 1


def get_json(dbname: str, periods_list: list | None):
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
                response = response.json()
                response = json.loads(response["DBData"])
                response = response["Результат"]
                responses += response
        return convert_json_to_dataframe(responses)
    query = {"DBName": dbname}
    query = json.dumps(query)
    response = req.post(
        url,
        auth=(login, password),
        json=query,
        timeout=100,
    )
    response = response.json()
    response = json.loads(response["DBData"])
    response = response["Результат"]
    return convert_json_to_dataframe(response)


def convert_json_to_dataframe(response: list):
    if response == []:
        raise ValueError(MSG_LIST[5])
    return pd.DataFrame(response)
