import datetime
from datetime import timedelta

import pandas as pd

import src.get_karat_db as tg

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
        main_dataframe = pd.read_csv("improved_table.csv")
        end_date = datetime.datetime.now(tz=MSK_TZ).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=1,
        )
        last_date = end_date - timedelta(days=UPDATE_TIME)

        main_dataframe = main_dataframe[main_dataframe["Период"] <= last_date.strftime("%Y-%m-%d")]

        start_date = (last_date + timedelta(days=1)).strftime("%d.%m.%Y")
        end_date = end_date.strftime("%d.%m.%Y")

        new_data = tg.get_table_from_kdl("Продажи_ПродажиФакт", start_date, end_date)
        new_data = table_transformation(new_data)

        main_dataframe = pd.concat([main_dataframe, new_data], axis=0, ignore_index=True)

        merging(main_dataframe)

    except FileNotFoundError:
        main_dataframe = tg.get_table_from_kdl(
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

        main_dataframe = table_transformation(main_dataframe)

        merging(main_dataframe)


def merging(main_dataframe: pd.DataFrame):
    sales_contragents = tg.get_table_from_kdl("Продажи_НСИ_Контрагенты")
    sales_contragents = sales_contragents.drop(columns=EXCESS_COLUMNS2)
    sales_contragents = sales_contragents.rename(columns={"КодКлиента": "КлиентКод"})
    sales_contragents["КлиентКод"] = sales_contragents["КлиентКод"].apply(lambda x: str(x).strip()).copy()
    sales_contragents["КодКлиента1С"] = sales_contragents["КодКлиента1С"].apply(lambda x: str(x).strip()).copy()

    main_dataframe = main_dataframe.merge(sales_contragents, on="КлиентКод", how="left")

    sales_clients = tg.get_table_from_kdl("Продажи_НСИ_Клиенты")
    sales_clients = sales_clients.drop(columns=EXCESS_COLUMNS3)
    sales_clients = sales_clients.rename(columns={"КонтрагентПартнерКод": "КодКлиента1С"})
    sales_clients["КодКлиента1С"] = sales_clients["КодКлиента1С"].apply(lambda x: str(x).strip()).copy()
    sales_clients["НаименованиеЛогическогоКлиента"] = (
        sales_clients["НаименованиеЛогическогоКлиента"].apply(lambda x: str(x).strip()).copy()
    )

    main_dataframe = main_dataframe.merge(sales_clients, on="КодКлиента1С", how="left")

    changing_articles(main_dataframe)


def changing_articles(main_dataframe: pd.DataFrame):
    main_dataframe["Артикул"] = main_dataframe["Артикул"].replace(MODIFIED_ARTICLES)
    main_dataframe.loc[
        (main_dataframe["НаименованиеЛогическогоКлиента"] == "ТАНДЕР АО (ТС Магнит)")
        & (main_dataframe["Артикул"] == "Л1039"),
        "Артикул",
    ] = "Л0214"
    main_dataframe.loc[
        (main_dataframe["НаименованиеЛогическогоКлиента"] == "ТАНДЕР АО (ТС Магнит)")
        & (main_dataframe["Артикул"] == "Л1040"),
        "Артикул",
    ] = "Л0225"

    main_dataframe = main_dataframe.drop(columns=["КодКлиента1С", "НаименованиеЛогическогоКлиента"])

    main_dataframe.to_csv("improved_table.csv", index=False)


def table_transformation(main_dataframe: pd.DataFrame):
    main_dataframe = main_dataframe.drop(columns=EXCESS_COLUMNS1)
    main_dataframe = main_dataframe.rename(columns=COLUMNS_TO_RENAME)
    main_dataframe["Период"] = pd.to_datetime(main_dataframe["Период"])
    for column in STR_COLUMNS:
        main_dataframe[column] = main_dataframe[column].apply(lambda x: str(x).strip()).copy()
    main_dataframe["gain_bpl"] = main_dataframe["ВыручкаПоБПЛБезНДС"]
    main_dataframe["gain_fact"] = main_dataframe["Объём_rub"]
    return main_dataframe
