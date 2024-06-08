## Аллокация добычи по пластам

Исходные данные - excel файл well_data.xlsx.


На первом листе rates представлены данные добычи продукции по скважинам (well_id) за интервал в 10 суток. Нефть – oil_rate, вода – water_rate, газ – gas_rate.

На втором листе splits – доля добычи тех же скважин (well_id) по пластам (layer_id), выраженная в процентах. Нефть – oil_split, вода – water_split, газ – gas_split. Значения этих коэффициентов (сплитов) могут изменяться ото дня ко дню, набор пластов для скважины также может изменяться.

Третий лист invalid_splits – копия второго с ошибками в заполнении данных.


### Задача валидации данных

- Сумма сплитов по одной скважине на одну дату по конкретному типу флюида (нефть, вода, газ) должна быть равна 100% для заданных пластов.

- Необходимо по данным третьего листа invalid_splits определить и записать список скважин, соответствующие даты и тип флюида, где сумма сплитов не равняется 100%.


###	Расчёт аллокации
- Для каждого пласта в каждой строчке таблицы сплитов (2 лист splits) посчитать соответствующий расход каждого флюида (нефть, вода, газ) по формуле
split_rate = rate * split / 100
Значения расходов (rate) брать из таблицы на 1 листе rates по well_id и dt


### Сохранение результатов

- Результаты второй задачи сохранить в виде excel файла, а также в json файл следующего формата. Названия полей в camelCase, даты со временем в ISO формате.


```json
{
  "allocation": {
    "data": [
      {
        "wellId": 0,
        "dt": "2022-12-01T00:00:00",
        "layerId": 4,
        "oilRate": 15.29527178593368,
        "gasRate": 80.93229959419733,
        "waterRate": 47.76733547397225}, 
      {
        "wellId": 1,
        "dt": "2022-12-01T00:00:00",
        "layerId": 0,
        "oilRate": 0.9428405445540975,
        "gasRate": 50.21562630167292,
        "waterRate": 31.57716155334533
      },
      ...
    ]
  }
}

```


Для запуска:

```shell
$ python3 main.py --file_name well_data.xlsx
```