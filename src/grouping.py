from __future__ import annotations

import contextlib
import math
from datetime import date, timedelta, timezone

import pandas as pd

MSK_TZ = timezone(timedelta(hours=3), name="MSK")
MSG_LIST = [
    "The first argument must be a pandas.DataFrame",
    "The second argument must be a string",
    "The third argument must be a string",
    "The fourth argument must be a list",
    "The elements of the list, which is the fourth argument, must be strings",
    "The time interval is incorrect",
    "The specified date column wasn't found",
    "The specified column contains empty fields",
    "The specified column doesn't contain dates",
    "Column names are repeated",
    "One or more columns weren't found in the table",
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
        with contextlib.suppress(ValueError, TypeError):
            dataframe[i] = dataframe[i].astype(float)
            float_columns.append(i)
    for i in groupby_list:
        if i in float_columns:
            float_columns.remove(i)
    time_interval = convert_time_interval(time_interval)
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
        raise ValueError(MSG_LIST[6])
    if bool(table[date_column].isna().any()):
        raise ValueError(MSG_LIST[7])
    first_date = table[date_column].iloc[0]
    if isinstance(first_date, str):
        first_date = first_date[:10]
        try:
            first_date = pd.to_datetime(first_date, format="%Y-%m-%d")
        except ValueError as e:
            raise ValueError(MSG_LIST[8]) from e
    elif not isinstance(first_date, date):
        raise TypeError(MSG_LIST[8])
    groupby_set = set(groupby_list)
    if sorted(groupby_list) != sorted(groupby_set):
        raise ValueError(MSG_LIST[9])
    if not set(groupby_list).issubset(set(table.columns)):
        raise ValueError(MSG_LIST[10])


def convert_time_interval(time_interval: str):
    if time_interval.lower() == "week" or time_interval.lower() == "w":
        time_interval = "W"
    elif time_interval.lower() == "month" or time_interval.lower() == "m":
        time_interval = "MS"
    elif time_interval.lower() == "year" or time_interval.lower() == "y":
        time_interval = "YS"
    elif time_interval.lower() == "decades_of_month" or time_interval.lower() == "md":
        time_interval = "D"
    elif time_interval.lower() == "quarters" or time_interval.lower() == "q":
        time_interval = "QS"
    else:
        raise ValueError(MSG_LIST[5])
    return time_interval


def table_changing(
    table: pd.DataFrame,
    date_column: str,
    time_interval: str,
    groupby_list: list[str],
    float_columns: list[str],
):
    if date_column in groupby_list:
        groupby_list.remove(date_column)
    table[date_column] = pd.to_datetime(table[date_column])
    if time_interval == "W":
        table = table.groupby(groupby_list).resample("W", on=date_column)[float_columns].sum().reset_index()
        table[date_column] = table[date_column] + pd.DateOffset(days=-6)
    elif time_interval != "D":
        table = table.groupby(groupby_list).resample(time_interval, on=date_column)[float_columns].sum().reset_index()
    first_column = table.pop(date_column)
    table.insert(0, date_column, first_column)
    if time_interval == "D":
        table[date_column] = table[date_column].apply(
            lambda x: x.replace(day=(math.ceil(x.day / 10.4) - 1) * 10 + 1),
        )  # This line replaces all the dates in the table with decades of the month
        groupby_list.append(date_column)
        table = table.groupby(groupby_list)[float_columns].sum().reset_index()
        first_column = table.pop(date_column)
        table.insert(0, date_column, first_column)
    return table
