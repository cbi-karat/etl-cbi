import datetime
from datetime import date, timedelta
from pathlib import Path
from sys import path

import pandas as pd

path.append(str(Path(__file__).resolve().parents[1]))
import table_getting

UPDATE_TIME = 100  # This parameter must be an integer that is greater or equal to 1
MSK_TZ = datetime.timezone(timedelta(hours=3), name="MSK")
EXCESS_COLUMNS = [
    "ВыручкаNetсНДС",
    "ВыручкаПромоФакт",
    "ИДЗаписи",
    "ИсточникДанных",
    "КоличествоBaseLineШт",
    "КоличествоПромоШт",
    "ПериодМесяц",
    "СкидкаБПЛПЛКбезНДС",
    "СкидкаПЛКNetбезНДС",
    "Склад",
    "СуммаЛогистикиПромоФакт",
    "СуммаЛогистикиФакт",
    "СуммаПеременнойСебестоимостиПромоФакт",
    "СуммаПеременнойСебестоимостиФакт",
    "СуммаПрочиеКУФакт",
    "СуммаРБПромоФакт",
    "СуммаРБФакт",
    "СуммаСкидкиФакт",
    "ТипДанных",
]
COLUMNS_TO_RENAME = {
    "ОбъемBaseLineКг": "BaseLine",
    "ОбъемПромоКг": "Promo",
    "ВыручкаNetБезНДС": "Объем_rub",
    "ВыручкаNetBaseLineБезНДС": "BaseLine_rub",
    "ВыручкаNetПромоБезНДС": "Promo_rub",
    "ВыручкаПоПЛКБезНДС": "gain_plk",
}


def main():
    try:
        df = pd.read_csv("improved_table.csv")
    except FileNotFoundError:
        df = table_getting.request(
            "Продажи_ПродажиФакт",
            "01.01.2018",
            datetime.datetime.now(tz=MSK_TZ)
            .replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            .strftime("%d.%m.%Y"),
        )
        df = df.drop(columns=EXCESS_COLUMNS)
        df = df.rename(COLUMNS_TO_RENAME)
        df["Период"] = pd.to_datetime(df["Период"])
        df.to_csv("improved_table.csv", index=False)
    last_date = datetime.datetime.now(tz=MSK_TZ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ) - timedelta(
        days=UPDATE_TIME,
    )
    last_date = last_date.strftime("%Y-%m-%dT00:00:00")
    df = df[df["Период"] <= last_date]
    last_date = last_date[0:10].split("-")
    last_date = date(int(last_date[0]), int(last_date[1]), int(last_date[2]))
    start_date = (last_date + timedelta(days=1)).strftime("%d.%m.%Y")
    end_date = datetime.datetime.now(tz=MSK_TZ).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%d.%m.%Y")
    new_data = table_getting.request("Продажи_ПродажиФакт", start_date, end_date)
    new_data = new_data.drop(columns=EXCESS_COLUMNS)
    new_data = new_data.rename(columns=COLUMNS_TO_RENAME)
    new_data["Период"] = pd.to_datetime(new_data["Период"])
    df = pd.concat([df, new_data], axis=0, ignore_index=True)
    df.to_csv("improved_table.csv", index=False)
