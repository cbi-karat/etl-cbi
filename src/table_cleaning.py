from sys import path

path.append("C:/projects/etl_cbi")

from datetime import date, datetime, timedelta
from pathlib import Path
from sys import path

import pandas as pd
import pytz

import table_getting

update_time = 2  # This parameter must be an integer that is greater or equal to 1


def main():
    try:
        df = pd.read_csv(r"C:\projects\etl_cbi\improved_table.csv")
    except FileNotFoundError:
        df = table_getting.request(
            "Продажи_ПродажиФакт",
            "01.01.2018",
            datetime.now(pytz.timezone("Europe/Moscow"))
            .replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            .strftime("%d.%m.%Y"),
        )
        df = df.drop(
            columns=[
                "ВыручкаNetBaseLineБезНДС",
                "ВыручкаNetБезНДС",
                "ВыручкаNetПромоБезНДС",
                "ВыручкаNetсНДС",
                "ВыручкаПоПЛКБезНДС",
                "ВыручкаПромоФакт",
                "ИДЗаписи",
                "ИсточникДанных",
                "КоличествоBaseLineШт",
                "КоличествоПромоШт",
                "ОбъемBaseLineКг",
                "ОбъемПромоКг",
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
            ],
        )
        df = df.rename(
            columns={
                "ОбъемBaseLineКг": "BaseLine",
                "ОбъемПромоКг": "Promo",
                "ВыручкаNetБезНДС": "Объем_rub",
                "ВыручкаNetBaseLineБезНДС": "BaseLine_rub",
                "ВыручкаNetПромоБезНДС": "Promo_rub",
                "ВыручкаПоПЛКБезНДС": "gain_plk",
            },
        )
        with Path.open("improved_table.csv", "w") as file:
            df.to_csv(file, index=False)

    last_date = datetime.now(pytz.timezone("Europe/Moscow")).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ) - timedelta(
        days=update_time,
    )
    last_date = last_date.strftime("%Y-%m-%dT00:00:00")
    df = df.iloc[0 : list(df.loc[df["Период"] == last_date].index)[-1] + 1]
    last_date = last_date[0:10].split("-")
    last_date = date(int(last_date[0]), int(last_date[1]), int(last_date[2]))
    start_date = (last_date + timedelta(days=1)).strftime("%d.%m.%Y")
    end_date = (
        datetime.now(pytz.timezone("Europe/Moscow"))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .strftime("%d.%m.%Y")
    )
    new_data = table_getting.request("Продажи_ПродажиФакт", start_date, end_date)
    if isinstance(new_data, str):
        pass
    else:
        new_data = new_data.drop(
            columns=[
                "ВыручкаNetBaseLineБезНДС",
                "ВыручкаNetБезНДС",
                "ВыручкаNetПромоБезНДС",
                "ВыручкаNetсНДС",
                "ВыручкаПоПЛКБезНДС",
                "ВыручкаПромоФакт",
                "ИДЗаписи",
                "ИсточникДанных",
                "КоличествоBaseLineШт",
                "КоличествоПромоШт",
                "ОбъемBaseLineКг",
                "ОбъемПромоКг",
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
            ],
        )
        new_data = new_data.rename(
            columns={
                "ОбъемBaseLineКг": "BaseLine",
                "ОбъемПромоКг": "Promo",
                "ВыручкаNetБезНДС": "Объем_rub",
                "ВыручкаNetBaseLineБезНДС": "BaseLine_rub",
                "ВыручкаNetПромоБезНДС": "Promo_rub",
                "ВыручкаПоПЛКБезНДС": "gain_plk",
            },
        )
        df = pd.concat([df, new_data], axis=0, ignore_index=True)
        with Path.open("improved_table.csv", "w") as file:
            df.to_csv(file, index=False)
