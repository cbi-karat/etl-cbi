from __future__ import annotations

import json
from datetime import date
from secrets.KDL_passwords import KDL

import pandas as pd
import requests as req


def request(dbname, start_date: type | None = None, end_date: type | None = None):
    datesymbols = 10
    error = 2
    errors = ["Dates are required", "Dates aren't required", "The error isn't defined"]
    correctinput, dtrequired = getjson_test(dbname)
    if not correctinput:
        return "The table wasn't found"
    if start_date is None and end_date is None:
        dates = False
    elif (
        start_date[0:1].isdigit()
        and start_date[3:4].isdigit()
        and start_date[6:9].isdigit()
        and start_date[2] == "."
        and start_date[5] == "."
        and len(start_date) == datesymbols
        and end_date is None
    ):
        dates = True
        start_date = start_date.split(".")
        start_date = date(int(start_date[2]), int(start_date[1]), int(start_date[0]))
        end_date = start_date
    elif (
        start_date[0:1].isdigit()
        and start_date[3:4].isdigit()
        and start_date[6:9].isdigit()
        and start_date[2] == "."
        and start_date[5] == "."
        and len(start_date) == datesymbols
        and end_date[0:1].isdigit()
        and end_date[3:4].isdigit()
        and end_date[6:9].isdigit()
        and end_date[2] == "."
        and end_date[5] == "."
        and len(end_date) == datesymbols
    ):
        dates = True
        start_date = start_date.split(".")
        end_date = end_date.split(".")
        start_date = date(int(start_date[2]), int(start_date[1]), int(start_date[0]))
        end_date = date(int(end_date[2]), int(end_date[1]), int(end_date[0]))
    else:
        return "Incorrect date information"
    if not dates and dtrequired:
        error = 0
    elif dates and not dtrequired:
        error = 1
    elif dates and dtrequired:
        return getjson(dbname, start_date, end_date)
    elif not dates and not dtrequired:
        return getjson_nodates(dbname)
    return errors[error]


def getjson_test(dbname):
    query = {"DBName": dbname, "НачалоПериода": date(1, 1, 1).isoformat(), "КонецПериода": date(1, 1, 1).isoformat()}
    query = json.dumps(query)
    login = KDL["login"]
    login = login.encode("utf-8")
    try:
        response = req.post(
            KDL["url"],
            auth=(login, KDL["password"]),
            json=query,
            timeout=100,
        )
        response = response.json()
    except json.JSONDecodeError:
        try:
            query = {"DBName": dbname}
            query = json.dumps(query)
            response = req.post(
                KDL["url"],
                auth=(login, KDL["password"]),
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


def getjson(dbname, start_date, end_date):
    query = {"DBName": dbname, "НачалоПериода": start_date.isoformat(), "КонецПериода": end_date.isoformat()}
    query = json.dumps(query)
    login = KDL["login"]
    login = login.encode("utf-8")
    response = req.post(
        KDL["url"],
        auth=(login, KDL["password"]),
        json=query,
        timeout=100,
    )
    return convert_to_dataframe(response)


def getjson_nodates(dbname):
    query = {"DBName": dbname}
    query = json.dumps(query)
    login = KDL["login"]
    login = login.encode("utf-8")
    response = req.post(
        KDL["url"],
        auth=(login, KDL["password"]),
        json=query,
        timeout=100,
    )
    return convert_to_dataframe(response)


def convert_to_dataframe(response):
    response = response.json()
    response = json.loads(response["DBData"])
    response = response["Результат"]
    return pd.DataFrame(response)
