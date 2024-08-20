from pathlib import Path
from sys import path

path.append(str(Path(__file__).resolve().parents[1]))
import json
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
import requests as req

from credentials.KDL_passwords import KDL

update_time = 100
update_time = max(update_time, 0)
keys_list1 = [
    "КодПродукта1С",
    "НаименованиеПродукта1С",
    "Бренд",
    "Суббренд",
    "ТоварнаяКатегория",
    "ТоварнаяПодкатегория",
    "НомерФормулыРецепта",
    "НаименованиеФормулыРецепта",
    "ФасовочноеОборудование",
    "ЛинияГрупповойУпаковки",
    "ТехнологическийПроцессВарка",
    "ТехнологическийПроцессИзмельчитель",
    "ПроцессОхлаждения",
    "Участок",
    "ПроизводственнаяПлощадка",
    "Производство",
    "ФорматТипУпаковки",
    "Группировка",
    "Категория4",
    "Категория3",
    "Категория2",
    "Категория1",
    "Категория0",
    "МастерКодСКЮ",
    "МастерНаименованиеПродукта",
    "УкрупненныйКодСКЮ",
    "УкрупненноеНаименованиеСКЮ",
    "Вкус",
    "Сегмент",
    "ТипУпаковки",
    "Группировка4",
    "Группировка5",
    "Группировка6",
    "АссортиментнаяГруппа",
    "Скрывать",
    "Вид_продукции",
    "GTIN",
    "Действующий",
    "Вес",
    "КоэффициентЕдиницыДляОтчетов",
    "Вес1шт",
    "Фасовка1местаШт",
    "Вес1места",
]
keys_list2 = [
    "Период",
    "КлиентКод",
    "Артикул",
    "ПериодМесяц",
    "ИсточникДанных",
    "ИДЗаписи",
    "ФО",
    "ТипЦен",
    "ТипДанных",
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
    "Склад",
    "ВидЦены",
    "Скидка_Наценка",
]


def get_table1():  # Получить таблицу "Финансы_НСИ_SKU"
    query = {"DBName": "Финансы_НСИ_SKU"}
    query = json.dumps(query)
    login = KDL["login"]
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
    cleaning_table1(response)


def get_table2():  # Получить таблицу "Продажи_ПродажиФакт"
    with Path(r"C:\projects\etl_cbi\tables\end_date.json").open("r") as file:
        start_date = json.load(file)
    start_date = date(int(start_date[0]), int(start_date[1]), int(start_date[2]))
    end_date = datetime.now(pytz.timezone("Europe/Moscow")).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ) - timedelta(days=update_time)
    if start_date == end_date:
        pass
    else:
        start_date = start_date + timedelta(days=1)
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
        responses = []
        login = KDL["login"]
        password = KDL["password"]
        url = KDL["url"]
        with req.Session() as session:
            for i in range(len(periods_list) - 1):
                start_date = periods_list[i]
                end_date = periods_list[i + 1]
                query = {
                    "DBName": "Продажи_ПродажиФакт",
                    "НачалоПериода": start_date.isoformat(),
                    "КонецПериода": end_date.isoformat(),
                }
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
        end_date = end_date.strftime("%Y %m %d").split()
        with Path(r"C:\projects\etl_cbi\tables\end_date.json").open("w") as file:
            json.dump(end_date, file)
        cleaning_table2(responses)


def write_table1(response):  # Записать в файл таблицу "Финансы_НСИ_SKU"
    response = pd.DataFrame(response)
    response = response.to_dict()
    with Path(r"C:\projects\etl_cbi\tables\Финансы_НСИ_SKU.json").open("w") as file:
        json.dump(response, file)


def write_table2(response):  # Записать в файл таблицу "Продажи_ПродажиФакт"
    response = pd.DataFrame(response)
    with Path(r"C:\projects\etl_cbi\tables\Продажи_ПродажиФакт.json").open("r") as file:
        last_table = json.load(file)
    if last_table is not None:
        last_table = pd.DataFrame(last_table)
        new_table2 = pd.concat([last_table, response], axis=0, ignore_index=True)
    else:
        new_table2 = response
    new_table2 = new_table2.to_dict()
    with Path(r"C:\projects\etl_cbi\tables\Продажи_ПродажиФакт.json").open("w") as file:
        json.dump(new_table2, file)


def get_last_days():  # Получить последние update_time дней из таблицы "Продажи_ПродажиФакт"
    start_date = datetime.now(pytz.timezone("Europe/Moscow"))
    start_date = start_date - timedelta(days=(update_time - 1))
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = datetime.now(pytz.timezone("Europe/Moscow"))
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
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
    responses = []
    login = KDL["login"]
    password = KDL["password"]
    url = KDL["url"]
    with req.Session() as session:
        for i in range(len(periods_list) - 1):
            start_date = periods_list[i]
            end_date = periods_list[i + 1]
            query = {
                "DBName": "Продажи_ПродажиФакт",
                "НачалоПериода": start_date.isoformat(),
                "КонецПериода": end_date.isoformat(),
            }
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
    return responses


def cleaning_table1(response):  # Очистить таблицу "Финансы_НСИ_SKU" от лишних пробелов
    new_table1 = []
    for i in response:
        temp_dict = {}
        for key in keys_list1:
            temp_dict[key] = str(i[key]).strip(" ")
        new_table1.append(temp_dict)
    write_table1(new_table1)


def cleaning_table2(response):  # Очистить таблицу "Продажи_ПродажиФакт" от лишних пробелов
    new_table2 = []
    for i in response:
        temp_dict = {}
        for key in keys_list2:
            temp_dict[key] = str(i[key]).strip(" ")
        new_table2.append(temp_dict)
    write_table2(new_table2)


def reset_table1():  # Очистить файл "Финансы_НСИ_SKU.json"
    with Path(r"C:\projects\etl_cbi\tables\Финансы_НСИ_SKU.json").open("w") as file:
        json.dump(None, file)


def reset_table2():  # Очистить файлы "Продажи_ПродажиФакт.json" и "end_date.json"
    with Path(r"C:\projects\etl_cbi\tables\end_date.json").open("w") as file:
        json.dump([2017, 12, 31], file)
    with Path(r"C:\projects\etl_cbi\tables\Продажи_ПродажиФакт.json").open("w") as file:
        json.dump(None, file)


def get_dataframe():  # Объединить таблицы
    with Path(r"C:\projects\etl_cbi\tables\Финансы_НСИ_SKU.json").open("r") as file:
        table1 = json.load(file)
    if table1 is None:
        get_table1()
        with Path(r"C:\projects\etl_cbi\tables\Финансы_НСИ_SKU.json").open("r") as file:
            table1 = json.load(file)
    get_table2()
    with Path(r"C:\projects\etl_cbi\tables\Продажи_ПродажиФакт.json").open("r") as file:
        table2 = json.load(file)
    table1 = pd.DataFrame(table1)
    table2 = pd.DataFrame(table2)
    if update_time != 0:
        last_days = get_last_days()
        temp_list = []
        for i in last_days:
            temp_dict = {}
            for key in keys_list2:
                temp_dict[key] = str(i[key]).strip(" ")
            temp_list.append(temp_dict)
        temp_table = pd.DataFrame(temp_list)
        table2 = pd.concat([table2, temp_table], axis=0, ignore_index=True)
    table1 = table1.rename(columns={"КодПродукта1С": "Артикул"})
    return table2.merge(table1, on="Артикул", how="inner")
