from pathlib import Path
from sys import path

import pandas as pd
import pytest

path.append(str(Path(__file__).resolve().parents[1]))
import get_karat_db


def test_missing_dates_error():
    with pytest.raises(get_karat_db.MissingDatesError):
        _ = get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт")


def test_extra_dates_error():
    with pytest.raises(get_karat_db.ExtraDatesError):
        _ = get_karat_db.get_table_from_kdl("Продажи_НСИ_SKU", "01.01.2000")


def test_table_name_error():
    with pytest.raises(get_karat_db.InternalServerError):
        _ = get_karat_db.get_table_from_kdl("Ъ")


def test_incorrect_date_error_1():
    with pytest.raises(get_karat_db.IncorrectDateError):
        _ = get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.32.2000")


def test_incorrect_date_error_2():
    with pytest.raises(get_karat_db.IncorrectDateError):
        _ = get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.02.2000", "01.01.2000")


def test_incorrect_date_error_3():
    with pytest.raises(get_karat_db.IncorrectDateError):
        _ = get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "Ъ")


def test_empty_dataframe_error():
    assert type(get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.01.2000")) is pd.DataFrame  # noqa: S101


def test_no_dates():
    assert type(get_karat_db.get_table_from_kdl("Финансы_НСИ_SKU")) is pd.DataFrame  # noqa: S101


def test_1_date():
    assert type(get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.08.2024")) is pd.DataFrame  # noqa: S101


def test_2_dates():
    assert type(get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.08.2024", "02.08.2024")) is pd.DataFrame  # noqa: S101
