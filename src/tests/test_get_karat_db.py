from pathlib import Path
from sys import path

import pytest

path.append(str(Path(__file__).resolve().parents[1]))
import get_karat_db


def test_missing_dates_error():
    with pytest.raises(get_karat_db.MissingDatesError):
        get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт")


def test_extra_dates_error():
    with pytest.raises(get_karat_db.ExtraDatesError):
        get_karat_db.get_table_from_kdl("Продажи_НСИ_SKU", "01.01.2000")


def test_table_name_error():
    with pytest.raises(get_karat_db.InternalServerError):
        get_karat_db.get_table_from_kdl("Ъ")


def test_incorrect_date_error():
    with pytest.raises(get_karat_db.IncorrectDateError):
        get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.32.2000")

    with pytest.raises(get_karat_db.IncorrectDateError):
        get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.02.2000", "01.01.2000")

    with pytest.raises(get_karat_db.IncorrectDateError):
        get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "Ъ")


def test_empty_dataframe_error():
    assert get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.01.2000").empty  # noqa: S101


def test_table_without_dates_getting():
    assert list(get_karat_db.get_table_from_kdl("Финансы_НСИ_SKU").columns) == [  # noqa: S101
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


def test_table_with_dates_getting():
    table = get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.08.2024")

    assert list(table.columns) == [  # noqa: S101
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

    assert table["Период"][table.first_valid_index()] == "2024-08-01T00:00:00"  # noqa: S101

    assert table["Период"][table.last_valid_index()] == "2024-08-01T00:00:00"  # noqa: S101

    table = get_karat_db.get_table_from_kdl("Продажи_ПродажиФакт", "01.08.2024", "02.08.2024")

    assert list(table.columns) == [  # noqa: S101
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

    assert table["Период"][table.first_valid_index()] == "2024-08-01T00:00:00"  # noqa: S101

    assert table["Период"][table.last_valid_index()] == "2024-08-02T00:00:00"  # noqa: S101
