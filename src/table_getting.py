from __future__ import annotations

import json
from datetime import date

import pandas as pd
import requests as req

from credentials.KDL_passwords import KDL


def request(dbname, start_date: type | None = None, end_date: type | None = None):  # Проверить данные на корректность
    correctinput, dtrequired = getjson_test(dbname)
    if not correctinput:
        return "The table wasn't found"
    if start_date is None and end_date is None:
        dates = False
        return check2(dates, dtrequired, dbname, start_date, end_date)
    return check1(dtrequired, dbname, start_date, end_date)


def check1(dtrequired, dbname, start_date, end_date):  # Проверить данные на корректность
    datesymbols = 10
    if (
        isinstance(start_date, str)
        and len(start_date) == datesymbols
        and start_date[0:2].isdigit()
        and start_date[3:5].isdigit()
        and start_date[6:].isdigit()
        and start_date[2] == "."
        and start_date[5] == "."
        and end_date is None
    ):
        dates = True
        start_date = start_date.split(".")
        try:
            start_date = date(int(start_date[2]), int(start_date[1]), int(start_date[0]))
        except ValueError:
            return "Incorrect date"
        else:
            end_date = start_date
            return check2(dates, dtrequired, dbname, start_date, end_date)
    elif (
        isinstance(start_date, str)
        and len(start_date) == datesymbols
        and start_date[0:2].isdigit()
        and start_date[3:5].isdigit()
        and start_date[6:].isdigit()
        and start_date[2] == "."
        and start_date[5] == "."
        and isinstance(end_date, str)
        and len(end_date) == datesymbols
        and end_date[0:2].isdigit()
        and end_date[3:5].isdigit()
        and end_date[6:].isdigit()
        and end_date[2] == "."
        and end_date[5] == "."
    ):
        dates = True
        start_date = start_date.split(".")
        end_date = end_date.split(".")
        try:
            start_date = date(int(start_date[2]), int(start_date[1]), int(start_date[0]))
        except ValueError:
            return "Incorrect date"
        try:
            end_date = date(int(end_date[2]), int(end_date[1]), int(end_date[0]))
        except ValueError:
            return "Incorrect date"
        else:
            return check2(dates, dtrequired, dbname, start_date, end_date)
    else:
        return "Incorrect date"


def check2(dates, dtrequired, dbname, start_date, end_date):  # Проверить данные на корректность
    errors = ["Dates are required", "Dates aren't required", "The error isn't defined"]
    error = 2
    if not dates and dtrequired:
        error = 0
    elif dates and not dtrequired:
        error = 1
    elif dates and dtrequired:
        if start_date > end_date:
            return "Incorrect date"
        return distribution(dbname, start_date, end_date)
    elif not dates and not dtrequired:
        return getjson_nodates(dbname)
    return errors[error]


def distribution(dbname, start_date, end_date):  # Обработать даты
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
    return getjson(dbname, periods_list)


def getjson_test(dbname):  # Проверить данные на корректность
    query = {"DBName": dbname, "НачалоПериода": date(1, 1, 1).isoformat(), "КонецПериода": date(1, 1, 1).isoformat()}
    query = json.dumps(query)
    login = KDL["login"]
    login = login.encode("utf-8")
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
            return False, False
        else:
            return True, False
    else:
        return True, True


def getjson(dbname, periods_list):  # Получить таблицу без дат
    responses = []
    login = KDL["login"]
    login = login.encode("utf-8")
    password = KDL["password"]
    url = KDL["url"]
    with req.Session() as session:
        for i in range(len(periods_list) - 1):
            start_date = periods_list[i]
            end_date = periods_list[i + 1]
            query = {"DBName": dbname, "НачалоПериода": start_date.isoformat(), "КонецПериода": end_date.isoformat()}
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
    return convert_to_dataframe(responses)


def getjson_nodates(dbname):  # Получить таблицу с датами
    query = {"DBName": dbname}
    query = json.dumps(query)
    login = KDL["login"]
    login = login.encode("utf-8")
    password = KDL["password"]
    url = KDL["url"]
    response = req.post(
        url,
        auth=(login, password),
        json=query,
        timeout=100,
    )
    response = response.json()
    response = json.loads(response["DBData"])
    response = response["Результат"]
    return convert_to_dataframe(response)


def convert_to_dataframe(response):  # Сделать таблицу
    if response == []:
        return """There is no data for this period, or this table uses grouping by month.
If the table uses month grouping, try using the format 01.MM.YYYY"""
    return pd.DataFrame(response)
