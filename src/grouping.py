from __future__ import annotations

from sys import path

import tables_joining

path.append("C:/projects/etl_cbi")
import contextlib
from datetime import date, datetime, timedelta

import pandas as pd
import pytz

import table_getting as tg


def grouping(dbname, time_interval, groupby_list, start_date: type | None = None, end_date: type | None = None):
    if dbname == "combined":
        return combined_table(time_interval, groupby_list, start_date, end_date)
    checkn1 = check1(time_interval, groupby_list)
    if checkn1 != ".":
        return checkn1
    table = check2(dbname, time_interval[0], start_date, end_date)
    if isinstance(table, str):
        return table
    checkn3 = check3(table, time_interval[1], groupby_list)
    if isinstance(checkn3, str):
        return checkn3
    table = checkn3[0]
    float_columns = checkn3[1]
    for i in groupby_list:
        if float_columns.count(i) != 0:
            return "It isn't possible to group a table by numeric columns"
    return table_changing(table, time_interval, groupby_list, float_columns, start_date, end_date)


def combined_table(time_interval, groupby_list, start_date, end_date):
    if start_date is not None or end_date is not None:
        return "Dates aren't required"
    float_columns = [
        "Объем",
        "ОбъемBaseLineКг",
        "ОбъемПромоКг",
        "Количество",
        "КоличествоBaseLineШт",
        "КоличествоПромоШт",
        "ВыручкаПоБПЛБезНДС",
        "ВыручкаПоПЛКБезНДС",
        "ВыручкаNetБезНДС",
        "ВыручкаNetBaseLineБезНДС",
        "ВыручкаNetПромоБезНДС",
        "ВыручкаNetсНДС",
        "СкидкаБПЛПЛКбезНДС",
        "СкидкаПЛКNetбезНДС",
        "СуммаСкидкиФакт",
        "ВыручкаПромоФакт",
        "СуммаРБПромоФакт",
        "СуммаПеременнойСебестоимостиПромоФакт",
        "СуммаЛогистикиПромоФакт",
        "СуммаРБФакт",
        "СуммаПрочиеКУФакт",
        "СуммаПеременнойСебестоимостиФакт",
        "СуммаЛогистикиФакт",
    ]
    start_date = date(2018, 1, 1).strftime("%d.%m.%Y")
    end_date = (
        datetime.now(pytz.timezone("Europe/Moscow"))
        .replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        .strftime("%d.%m.%Y")
    )
    checkn1 = check1(time_interval, groupby_list)
    if checkn1 != ".":
        return checkn1
    if (
        time_interval[0] != "Недели"
        and time_interval[0] != "Месяцы"
        and time_interval[0] != "Годы"
        and time_interval[0] != "Декады"
        and time_interval[0] != "Кварталы"
    ):
        return "The time interval is incorrect"
    table = tables_joining.get_dataframe()
    checkn3 = check3(table, time_interval[1], groupby_list)
    if isinstance(checkn3, str):
        return checkn3
    table = checkn3[0]
    return table_changing(table, time_interval, groupby_list, float_columns, start_date, end_date)


def check1(time_interval, groupby_list):
    arg2len = 2
    if not isinstance(time_interval, list):
        return "The second argument must be a list"
    if len(time_interval) != arg2len:
        return "The second argument should be a list containing 2 elements"
    if not (isinstance(time_interval[0], str) and isinstance(time_interval[1], str)):
        return "The elements of the list, which is the second argument, must be strings"
    if not isinstance(groupby_list, list):
        return "The trird argument must be a list"
    for i in groupby_list:
        if not isinstance(i, str):
            return "The elements of the list, which is the third argument, must be strings"
    return "."


def check2(dbname, time_interval, start_date, end_date):
    datesymbols = 10
    if end_date is None:
        end_date = start_date
    if (
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
        try:
            start_date = start_date.split(".")
            start_date = date(int(start_date[2]), int(start_date[1]), int(start_date[0]))
            end_date = end_date.split(".")
            end_date = date(int(end_date[2]), int(end_date[1]), int(end_date[0]))
        except ValueError:
            return get_table(dbname, start_date, end_date)
        if start_date > end_date:
            return "Incorrect date"
        start_date, end_date = get_dates(start_date, end_date, time_interval)
        if isinstance(start_date, str):
            return start_date
        start_date = start_date.strftime("%d.%m.%Y")
        end_date = end_date.strftime("%d.%m.%Y")
    return get_table(dbname, start_date, end_date)


def check3(table, time_interval, groupby_list):
    if list(table.columns).count(time_interval) == 0:
        return "The specified date column wasn't found"
    first_date = table.iloc[0][time_interval]
    error = False
    if isinstance(first_date, str):
        first_date = first_date.strip(" ")
        datelen = 10
        if len(first_date) >= datelen and first_date[4] == "-" and first_date[7] == "-":
            first_date = first_date[:10].split("-")
            for i in first_date:
                if not i.isdigit():
                    error = True
        else:
            error = True
    else:
        error = True
    if error:
        return "The specified column does not contain a date"
    groupby_set = set(groupby_list)
    if sorted(groupby_list) != sorted(groupby_set):
        return "Column names are repeated"
    if not set(groupby_list).issubset(set(table.columns)):
        return "One or more columns weren't found in the table"
    float_columns = []
    for i in table.columns:
        with contextlib.suppress(ValueError):
            table[i] = table[i].astype(float)
            float_columns.append(i)
    return [table, float_columns]


def get_table(dbname, start_date, end_date):
    response = tg.request(dbname, start_date, end_date)
    if isinstance(response, str) and response == "Dates are required":
        start_date = date(2018, 1, 1)
        end_date = datetime.now(pytz.timezone("Europe/Moscow")).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        response = tg.request(dbname, start_date, end_date)
    return response


def get_dates(start_date, end_date, time_interval):
    if time_interval == "Недели":
        start_date = start_date - timedelta(days=start_date.weekday())
        end_date = timedelta(days=6) - timedelta(days=end_date.weekday()) + end_date
    elif time_interval == "Месяцы":
        start_date = start_date.replace(day=1)
        end_date = end_date.replace(day=1)
        try:
            end_date = end_date.replace(month=int(end_date.strftime("%m")) + 1) - timedelta(days=1)
        except ValueError:
            end_date = end_date.replace(month=1)
            end_date = end_date.replace(year=int(end_date.strftime("%Y")) + 1) - timedelta(days=1)
    elif time_interval == "Годы":
        start_date = start_date.replace(month=1, day=1)
        end_date = end_date.replace(month=12, day=31)
    elif time_interval == "Декады":
        start_date, end_date = decades(start_date, end_date)
    elif time_interval == "Кварталы":
        start_date, end_date = quarters(start_date, end_date)
    else:
        return "The time interval is incorrect", None
    return start_date, end_date


def decades(start_date, end_date):
    n2 = 11
    n3 = 21
    sday = int(start_date.strftime("%d"))
    eday = int(end_date.strftime("%d"))
    if sday < n2:
        start_date = start_date.replace(day=1)
    elif sday < n3:
        start_date = start_date.replace(day=11)
    else:
        start_date = start_date.replace(day=21)
    if eday < n2:
        end_date = end_date.replace(day=10)
    elif eday < n3:
        end_date = end_date.replace(day=20)
    else:
        end_date = end_date.replace(day=1)
        try:
            end_date = end_date.replace(month=int(end_date.strftime("%m")) + 1) - timedelta(days=1)
        except ValueError:
            end_date = end_date.replace(month=1)
            end_date = end_date.replace(year=int(end_date.strftime("%Y")) + 1) - timedelta(days=1)
    return start_date, end_date


def quarters(start_date, end_date):
    n2 = 4
    n3 = 7
    n4 = 10
    smonth = int(start_date.strftime("%m"))
    emonth = int(end_date.strftime("%m"))
    if smonth < n2:
        start_date = start_date.replace(month=1)
    elif smonth < n3:
        start_date = start_date.replace(month=4)
    elif smonth < n4:
        start_date = start_date.replace(month=7)
    else:
        start_date = start_date.replace(month=10)
    if emonth < n2:
        end_date = end_date.replace(month=4, day=1) - timedelta(days=1)
    elif emonth < n3:
        end_date = end_date.replace(month=7, day=1) - timedelta(days=1)
    elif emonth < n4:
        end_date = end_date.replace(month=10, day=1) - timedelta(days=1)
    else:
        end_date = end_date.replace(month=1, day=1)
        end_date = end_date.replace(year=int(end_date.strftime("%Y")) + 1) - timedelta(days=1)
    return start_date, end_date


def splitting(interval_start, interval_end, time_interval):
    end_date = interval_end
    dates_dict = {}
    while interval_start < end_date:
        interval_start, interval_end = get_dates(interval_start, interval_start, time_interval)
        interim_date = interval_start
        temp_list = []
        while interim_date != interval_end:
            interim_date += timedelta(days=1)
            temp_list.append(interim_date.strftime("%Y-%m-%dT00:00:00"))
        dates_dict[interval_start.strftime("%Y-%m-%dT00:00:00")] = temp_list
        interval_start = interim_date + timedelta(days=1)
    return dates_dict


def table_changing(table, time_interval, groupby_list, float_columns, start_date, end_date):
    date_column = time_interval[1]
    time_interval = time_interval[0]
    groupby_list.insert(0, date_column)
    if start_date is None:
        start_date = date(2018, 1, 1)
        end_date = datetime.now(pytz.timezone("Europe/Moscow")).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    elif end_date is None:
        end_date = start_date
    start_date = start_date.split(".")
    end_date = end_date.split(".")
    start_date = date(int(start_date[2]), int(start_date[1]), int(start_date[0]))
    end_date = date(int(end_date[2]), int(end_date[1]), int(end_date[0]))
    start_date, end_date = get_dates(start_date, end_date, time_interval)
    dates_dict = splitting(start_date, end_date, time_interval)
    new_dict = {}
    keylist = list(table.keys())
    for key in keylist:
        temp_dict = {}
        for i in range(list(table[keylist[0]].keys())[-1] + 1):
            temp_dict[i] = str(table[key][i]).strip()
        new_dict[key] = temp_dict
    table = pd.DataFrame(new_dict)
    for i in table.columns:
        with contextlib.suppress(ValueError):
            table[i] = table[i].astype(float)
    for key in dates_dict:
        table = table.replace(dates_dict[key], key)
    return table.groupby(groupby_list)[float_columns].sum().reset_index()
