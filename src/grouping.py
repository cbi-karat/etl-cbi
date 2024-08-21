from __future__ import annotations

import contextlib
from datetime import date, timedelta, timezone

import pandas as pd

MSK_TZ = timezone(timedelta(hours=3), name="MSK")
MSG_LIST = [
    "The first argument must be a pandas.DataFrame",
    "The second argument must be a string",
    "The third argument must be a string",
    "The fourth argument must be a list",
    "The elements of the list, which is the fourth argument, must be strings",
    "The fifth argument must be a string, or it doesn't need to be specified",
    "The sixth argument must be a string, or it doesn't need to be specified",
    "Incorrect date",
    "The time interval is incorrect",
    "The specified date column wasn't found",
    "The specified column contains empty fields",
    "The specified column doesn't contain dates",
    "Column names are repeated",
    "One or more columns weren't found in the table",
    "It isn't possible to group a table by numeric columns",
]


def grouping(
    dataframe: pd.DataFrame,
    date_column: str,
    time_interval: str,
    groupby_list: list[str],
):
    type_checking(dataframe, date_column, time_interval, groupby_list)
    checking_column_names(dataframe, date_column, groupby_list)
    float_columns = []
    for i in dataframe.columns:
        with contextlib.suppress(ValueError):
            dataframe[i] = dataframe[i].astype(float)
            float_columns.append(i)
    for i in groupby_list:
        if i in float_columns:
            raise ValueError(MSG_LIST[14])
    return table_changing(dataframe, date_column, time_interval, groupby_list, float_columns)


def type_checking(
    table: pd.DataFrame,
    date_column: str,
    time_interval: str,
    groupby_list: list,
):
    if not isinstance(table, pd.DataFrame):
        raise TypeError(MSG_LIST[0])
    if not isinstance(date_column, str):
        raise TypeError(MSG_LIST[1])
    if not isinstance(time_interval, str):
        raise TypeError(MSG_LIST[2])
    if not isinstance(groupby_list, list):
        raise TypeError(MSG_LIST[3])
    for i in groupby_list:
        if not isinstance(i, str):
            raise TypeError(MSG_LIST[4])


def checking_column_names(table: pd.DataFrame, date_column: str, groupby_list: list):
    if date_column not in list(table.columns):
        raise ValueError(MSG_LIST[9])
    if bool(table[date_column].isna().any()):
        raise ValueError(MSG_LIST[10])
    first_date = table[date_column].to_numpy()[0]
    if isinstance(first_date, str):
        first_date = first_date[:10]
        try:
            first_date = pd.to_datetime(first_date, format="%Y-%m-%d")
        except ValueError as e:
            raise ValueError(MSG_LIST[11]) from e
    elif not isinstance(first_date, date):
        raise TypeError(MSG_LIST[11])
    groupby_set = set(groupby_list)
    if sorted(groupby_list) != sorted(groupby_set):
        raise ValueError(MSG_LIST[12])
    if not set(groupby_list).issubset(set(table.columns)):
        raise ValueError(MSG_LIST[13])


def to_period_beginning(time_interval: str, given_date: str):
    converted_date = pd.to_datetime(given_date, format="%d.%m.%Y")
    if time_interval == "Недели":
        converted_date = converted_date - timedelta(days=converted_date.weekday())
    elif time_interval == "Месяцы":
        converted_date = converted_date.replace(day=1)
    elif time_interval == "Годы":
        converted_date = converted_date.replace(month=1, day=1)
    elif time_interval == "Декады":
        converted_date = decades(converted_date)
    elif time_interval == "Кварталы":
        converted_date = quarters(converted_date)
    else:
        raise ValueError(MSG_LIST[8])
    return converted_date


def decades(converted_date: date):
    decade_2_start = 11
    decade_3_start = 21
    if converted_date.day < decade_2_start:
        converted_date = converted_date.replace(day=1)
    elif converted_date.day < decade_3_start:
        converted_date = converted_date.replace(day=11)
    else:
        converted_date = converted_date.replace(day=21)
    return converted_date


def quarters(converted_date: date):
    quarter_2_start = 4
    quarter_3_start = 7
    quarter_4_start = 10
    if converted_date.month < quarter_2_start:
        converted_date = converted_date.replace(month=1)
    elif converted_date.month < quarter_3_start:
        converted_date = converted_date.replace(month=4)
    elif converted_date.month < quarter_4_start:
        converted_date = converted_date.replace(month=7)
    else:
        converted_date = converted_date.replace(month=10)
    return converted_date


def table_changing(table: pd.DataFrame, date_column, time_interval, groupby_list, float_columns):
    table["Период"] = table["Период"].apply(
        lambda x: to_period_beginning(time_interval, pd.to_datetime(x).strftime("%d.%m.%Y")),
    )
    if date_column not in groupby_list:
        groupby_list.insert(0, date_column)
    str_columns = list(table.select_dtypes(include=["object"]).columns)
    for i in str_columns:
        table[i] = table[i].apply(lambda x: str(x).strip())
    return table.groupby(groupby_list)[float_columns].sum().reset_index()
