from glob import glob
import pandas as pd
from loguru import logger
from logging_formatters import logger_format
import sys
import json
import os
from time import time


def sort(df, table):
    if table.get('in_out', False):
        df = df.sort_values(['mtu', 'zone_in', 'zone_out'])
    else:
        df = df.sort_values(['mtu', 'zone'])

    return df


if __name__ == '__main__':
    # setup the logging
    logger.remove()  # needed to disable the default one to overrule it
    logger.add(sys.stdout, format=logger_format, level="DEBUG", colorize=True, enqueue=False)

    with open('tables.json', 'r') as stream:
        tables = json.load(stream)

    for table in tables:
        if table.get('disabled', False):
            continue

        logger.info(table['name'])
        start_time = time()

        files = sorted(list(glob(os.path.join('data', table['name'], '*.parquet'))))

        df_all = pd.concat(
            [pd.read_parquet(f) for f in files]
            , ignore_index=True)

        os.makedirs('data_merged', exist_ok=True)

        sort(df_all, table).to_parquet(os.path.join('data_merged', f'{table["name"]}_all.parquet'), index=False)

        files_after_2020 = [pd.read_parquet(f) for f in files if os.path.basename(f).split('_')[-2] >= '2020']
        if len(files_after_2020) > 0:
            df_2020 = pd.concat(
                files_after_2020
                , ignore_index=True)
            sort(df_2020, table).to_parquet(os.path.join('data_merged', f'{table["name"]}_from2020.parquet'), index=False)

        df_last12months = pd.concat(
            [pd.read_parquet(f) for f in sorted(files[-12:])]
            , ignore_index=True)
        sort(df_last12months, table).to_parquet(
            os.path.join('data_merged', f'{table["name"]}_last12months.parquet'
                         ), index=False)

        logger.info(f'Took {round(time() - start_time, 2)} seconds')

