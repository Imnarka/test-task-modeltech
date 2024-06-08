from typing import Any
from argparse import ArgumentParser

from file_processor import DataProcessor


def parse_args() -> Any:
    """
    Парсит аргументы командной строки.

    Returns:
        Namespace: Аргументы командной строки.
    """
    parser = ArgumentParser(description='Process well data.')
    parser.add_argument('--file_name', type=str, help='The name of the file to process')
    return parser.parse_args()


def main() -> None:

    args = parse_args()
    processor = DataProcessor(args.file_name)
    filtered_data = processor.transform_invalid_data()

    processor.save_to_excel(filtered_data, 'invalid_data.xlsx')

    df = processor.allocate_calc()
    processor.save_to_json(df, 'allocated_calc.xlsx')
    processor.save_to_json(df,  'allocated_calc.json')
    # print(processor.allocate_calc())

if __name__ == '__main__':
    main()
