import json
import pandas as pd
import numpy as np
from abc import (
    ABC,
    abstractmethod,
    abstractstaticmethod,
)
from typing import Dict, List


class AbstractDataWorker(ABC):
    """
    Абстрактный класс, определяющий интерфейс для работы с файлами
    """
    @abstractmethod
    def read_data(self, file_name: str) -> Dict[str, pd.DataFrame]:
        """
        Сохраняет DataFrame в Excel файл.

        :arg
            dataframe (pd.DataFrame): DataFrame, который будет сохранен.
            file_name (str): Путь к файлу, в который будут сохранены данные.
        """
        ...

    @staticmethod
    @abstractmethod  # abstract static method is deprecated in python 3.3
    def save_to_excel(dataframe: pd.DataFrame, file_name: str) -> None:
        """
        Сохраняет DataFrame в Excel файл.

        :arg
            dataframe (pd.DataFrame): DataFrame, который будет сохранен.
            file_name (str): Путь к файлу, в который будут сохранены данные.
        """
        ...

    @staticmethod
    @abstractmethod  # abstract static method is deprecated in python 3.3
    def save_to_json(dataframe: pd.DataFrame, file_name: str) -> None:
        """
        Сохраняет DataFrame в JSON файл в формате 'records'.

        :arg
            dataframe (pd.DataFrame): DataFrame, который будет сохранен.
            file_name (str): Путь к файлу, в который будут сохранены данные.
        """


class ExcelDataWorkerMixin(AbstractDataWorker):
    """
    Класс-mixin для работы с файлами Excel
    """
    def read_data(self, file_name: str) -> Dict[str, pd.DataFrame]:
        """
        Читает данные из Excel файла и возвращает словарь DataFrame'ов.

        :arg
            file_name (str): Путь к Excel файлу, из которого будут прочитаны данные.
        :return
            Dict[str, pd.DataFrame]: Словарь с DataFrame'ами, содержащими прочитанные данные.
        """
        df_map = {}
        reader_object = pd.ExcelFile(file_name)
        for sheet in reader_object.sheet_names:
            df_map[sheet] = reader_object.parse(sheet, index_col=0)
        return df_map

    @staticmethod
    def save_to_excel(dataframe: pd.DataFrame, file_name: str) -> None:
        """
        Сохраняет DataFrame в Excel файл.

        :arg
            dataframe (pd.DataFrame): DataFrame, который будет сохранен.
            file_name (str): Путь к файлу, в который будут сохранены данные.

        :return: None
        """
        dataframe.to_excel(file_name, index=False)

    @staticmethod
    def save_to_json(dataframe: pd.DataFrame, file_name: str) -> None:
        """
        Сохраняет DataFrame в JSON файл в формате 'records'.

        :arg
            dataframe (pd.DataFrame): DataFrame, который будет сохранен.
            file_name (str): Путь к файлу, в который будут сохранены данные.

        :return: None
        """
        dataframe.to_json(file_name, orient='records')


class DataProcessor(ExcelDataWorkerMixin):
    """
    Класс для обработки данных.
    """
    def __init__(self, file_name: str):
        self.data: Dict[str, pd.DataFrame] = self.read_data(file_name)
        self.tolerance = 1e-6

    def __process_invalid_data(self) -> pd.DataFrame:
        """
        Внутренний метод для обработки невалидных данных.

        :return: pd.DataFrame
        """
        grouped = self.data['invalid_splits'].groupby(['well_id', 'dt']).agg({
            'oil_split': 'sum',
            'water_split': 'sum',
            'gas_split': 'sum'
        }).reset_index()

        filtered = pd.DataFrame()
        for split in ['oil_split', 'water_split', 'gas_split']:
            filtered = pd.concat([filtered, grouped[~(np.isclose(grouped[split], 100.0, atol=self.tolerance))]], ignore_index=True)
        return filtered

    def transform_invalid_data(self) -> pd.DataFrame:
        """
        Преобразует невалидные данные и возвращает их в виде DataFrame.

        :return: pd.DataFrame
        """
        melted_df = pd.melt(self.__process_invalid_data(), id_vars=['well_id', 'dt'],
                            value_vars=['oil_split', 'water_split', 'gas_split'],
                            var_name='fluid_type',
                            value_name='split_sum')
        return melted_df.loc[melted_df['split_sum'].round(2) != 100.00]

    def allocate_calc(self) -> pd.DataFrame:
        """
        Вычисляет аллокацию для каждого пласата и возвращает результат в виде DataFrame.

        :return: pd.DataFrame
        """
        df = self.data['splits'].merge(self.data['rates'], on=['well_id',  'dt'])
        df['oil_split_rate'] = df['oil_rate'] * df['oil_split'] / 100.0
        df['water_split_rate'] = df['water_rate'] * df['water_split'] / 100.0
        df['gas_split_rate'] = df['gas_rate'] * df['gas_split'] / 100.0

        return df.drop(['oil_split', 'gas_split', 'water_split', 'oil_rate', 'gas_rate', 'water_rate'], axis=1)

    @staticmethod
    def save_to_json(dataframe: pd.DataFrame, file_name: str) -> None:
        """
        Метод для сохранения данных в JSON с измененной структурой.

        :arg
            dataframe (pd.DataFrame): DataFrame, который будет сохранен в JSON.
            file_name (str): Путь к файлу, в который будут сохранены данные.

        :return: None
        """
        data = []
        for index, row in dataframe.reset_index().iterrows():
            data.append({
                'wellId': row['well_id'],
                'dt': row['dt'].strftime('%Y-%m-%d %H:%M:%S'),
                'oilRate': row['oil_split_rate'],
                'waterRate': row['water_split_rate'],
                'gasRate': row['gas_split_rate']
            })
        data = {"allocation": {"data": data}}

        with open(file_name, 'w') as json_file:
            json.dump(data, json_file, indent=4)