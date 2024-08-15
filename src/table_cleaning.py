import datetime
from datetime import timedelta
from pathlib import Path
from sys import path

import pandas as pd

path.append(str(Path(__file__).resolve().parents[1]))
import table_getting

UPDATE_TIME = 2  # This parameter must be an integer that is greater or equal to 1
MSK_TZ = datetime.timezone(timedelta(hours=3), name="MSK")
EXCESS_COLUMNS1 = [
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
EXCESS_COLUMNS2 = [
    "РасстоянияОтКаратаДоАдреса",
    "Клиент",
    "Адрес",
    "Регион",
    "ФО",
    "Канал",
    "ИсточникДанных",
    "ИДЗаписи",
    "ИНН",
]
EXCESS_COLUMNS3 = [
    "ИДЗаписи",
    "ИсточникДанных",
    "НаименованиеКлиента",
    "МастерКод",
    "МастерНаименование",
    "КодЛогическогоКлиента",
    "Менеджер1Уровня",
    "Менеджер2Уровня",
    "Канал",
    "ФорматСети",
    "НазваниеСети",
    "СтатусКонтрагента",
    "ИНН",
]
COLUMNS_TO_RENAME = {
    "ОбъемBaseLineКг": "BaseLine",
    "ОбъемПромоКг": "Promo",
    "ВыручкаNetБезНДС": "Объём_rub",
    "ВыручкаNetBaseLineБезНДС": "BaseLine_rub",
    "ВыручкаNetПромоБезНДС": "Promo_rub",
    "ВыручкаПоПЛКБезНДС": "gain_plk",
}
STR_COLUMNS = [
    "КлиентКод",
    "Артикул",
    "ФО",
    "ТипЦен",
    "ВидЦены",
    "Скидка_Наценка",
]
MODIFIED_ARTICLES = {
    "Л0214": "Л1039",
    "Л0225": "Л1040",
    "ЛК0214": "ЛК1039",
    "ЛК0225": "ЛК1040",
    "1260": "1211",
    "1221": "1212",
    "1218": "1213",
    "1247": "1214",
    "1220": "1215",
    "1219": "1216",
    "1236": "1217",
    "0300": "0340",
    "0305": "0341",
    "0307": "0342",
    "0303": "0343",
    "0308": "0344",
    "0317": "0345",
    "0320": "0346",
    "0301": "0350",
    "0306": "0351",
    "0304": "0352",
    "0309": "0353",
    "0318": "0354",
    "0321": "0355",
    "0313": "0356",
    "0316": "0341",
    "0302": "0343",
    "0315": "0342",
    "0323": "0345",
    "0324": "0346",
}


def main():
    try:
        df = pd.read_csv("improved_table.csv")
        end_date = datetime.datetime.now(tz=MSK_TZ).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=1,
        )
        last_date = end_date - timedelta(days=UPDATE_TIME)

        df = df[df["Период"] <= last_date.strftime("%d.%m.%Y")]

        start_date = (last_date + timedelta(days=1)).strftime("%d.%m.%Y")
        end_date = end_date.strftime("%d.%m.%Y")

        new_data = table_getting.request("Продажи_ПродажиФакт", start_date, end_date)
        new_data = table_transformation(new_data)

        df = pd.concat([df, new_data], axis=0, ignore_index=True)

        merging(df)

    except FileNotFoundError:
        df = table_getting.request(
            "Продажи_ПродажиФакт",
            "01.08.2024",
            (datetime.datetime.now(tz=MSK_TZ) - timedelta(days=1))
            .replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            .strftime("%d.%m.%Y"),
        )

        df = table_transformation(df)
        merging(df)


def merging(df: pd.DataFrame):
    df2 = table_getting.request("Продажи_НСИ_Контрагенты")
    df2 = df2.drop(columns=EXCESS_COLUMNS2)
    df2 = df2.rename(columns={"КодКлиента": "КлиентКод"})
    df2["КлиентКод"] = df2["КлиентКод"].apply(lambda x: str(x).strip()).copy()
    df2["КодКлиента1С"] = df2["КодКлиента1С"].apply(lambda x: str(x).strip()).copy()

    df = df.merge(df2, on="КлиентКод", how="left")

    df3 = table_getting.request("Продажи_НСИ_Клиенты")
    df3 = df3.drop(columns=EXCESS_COLUMNS3)
    df3 = df3.rename(columns={"КонтрагентПартнерКод": "КодКлиента1С"})
    df3["КодКлиента1С"] = df3["КодКлиента1С"].apply(lambda x: str(x).strip()).copy()
    df3["НаименованиеЛогическогоКлиента"] = df3["НаименованиеЛогическогоКлиента"].apply(lambda x: str(x).strip()).copy()

    df = df.merge(df3, on="КодКлиента1С", how="left")

    changing_articles(df)


def changing_articles(df: pd.DataFrame):
    df["Артикул"] = df["Артикул"].replace(MODIFIED_ARTICLES)
    df.loc[
        (df["НаименованиеЛогическогоКлиента"] == "ТАНДЕР АО (ТС Магнит)") & (df["Артикул"] == "Л1039"),
        "Артикул",
    ] = "Л0214"
    df.loc[
        (df["НаименованиеЛогическогоКлиента"] == "ТАНДЕР АО (ТС Магнит)") & (df["Артикул"] == "Л1040"),
        "Артикул",
    ] = "Л0225"
    df.to_csv("improved_table.csv", index=False)


def table_transformation(df: pd.DataFrame):
    df = df.drop(columns=EXCESS_COLUMNS1)
    df = df.rename(columns=COLUMNS_TO_RENAME)
    df["Период"] = pd.to_datetime(df["Период"])
    for column in STR_COLUMNS:
        df[column] = df[column].apply(lambda x: str(x).strip()).copy()
    df["gain_bpl"] = df["ВыручкаПоБПЛБезНДС"]
    df["gain_fact"] = df["Объём_rub"]
    return df
