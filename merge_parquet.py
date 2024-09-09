from glob import glob
import pandas as pd
from loguru import logger
from logging_formatters import logger_format
import sys
import json
import os
from time import time


if __name__ == '__main__':
    # setup the logging
    logger.remove()  # needed to disable the default one to overrule it
    logger.add(sys.stdout, format=logger_format, level="DEBUG", colorize=True, enqueue=False)

    with open('tables.json', 'r') as stream:
        tables = json.load(stream)

    for table in tables:
        logger.info(table)
        start_time = time()

        files = list(glob(os.path.join('data', table['name'], '*.parquet')))

        df_all = pd.concat(
            [pd.read_parquet(f) for f in files]
            , ignore_index=True).sort_values(['mtu', 'zone'])

        os.makedirs('data_merged', exist_ok=True)
        logger.debug('All')
        df_all.to_parquet(os.path.join('data_merged', f'{table["name"]}_all.parquet'), index=False)
        logger.debug('2020+')
        df_2020 = pd.concat(
            [pd.read_parquet(f) for f in files if f.split('_')[0] >= '2020']
            , ignore_index=True).sort_values(['mtu', 'zone'])
        df_2020.to_parquet(os.path.join('data_merged', f'{table["name"]}_from2020.parquet'), index=False)
        logger.debug('last 12 months')
        df_last12months = pd.concat(
            [pd.read_parquet(f) for f in sorted(files[-12:])]
            , ignore_index=True).sort_values(['mtu', 'zone'])
        df_last12months.to_parquet(os.path.join('data_merged', f'{table["name"]}_last12months.parquet'), index=False)

        logger.info(f'Took {round(time() - start_time, 2)} seconds')

