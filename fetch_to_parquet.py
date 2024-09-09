import paramiko
import os
from paramiko.ssh_exception import SSHException
from time import sleep, time
import pandas as pd
from loguru import logger
from io import BytesIO
import sys
from logging_formatters import logger_format
import json
import custom_functions

if __name__ == '__main__':
    # setup the logging
    logger.remove()  # needed to disable the default one to overrule it
    logger.add(sys.stdout, format=logger_format, level="DEBUG", colorize=True, enqueue=False)

    entsoe_user_name = os.environ['ENTSOE_USERNAME']
    entsoe_pwd = os.environ['ENTSOE_PWD']

    logger.info(f"Fetching started at {pd.Timestamp.now(tz='Europe/Amsterdam')}")

    try:
        transport = paramiko.Transport(('sftp-transparency.entsoe.eu', 22))
        transport.connect(None, entsoe_user_name, entsoe_pwd)
    except SSHException:
        # try one time again after a wait
        sleep(60)
        transport = paramiko.Transport(('sftp-transparency.entsoe.eu', 22))
        transport.connect(None, entsoe_user_name, entsoe_pwd)
    sftp = paramiko.SFTPClient.from_transport(transport)

    logger.info("SFTP connected")

    with open('tables.json', 'r') as stream:
        tables = json.load(stream)

    with open('last_edits.json', 'r') as stream:
        last_edits = json.load(stream)

    for table in tables:
        if table.get('disabled', False):
            continue
        logger.info(table['name'])
        sftp.chdir(f'/TP_export/{table["table"]}')
        last_modified = max(
            sftp.listdir_attr('.'),
            key=lambda f: f.st_mtime
        )

        last_fetch = last_edits.get(table['name'], None)
        timestamps_parsed = []
        os.makedirs('data/' + table['name'], exist_ok=True)
        for file_info in sftp.listdir_attr('.'):
            if last_fetch is not None:
                if pd.Timestamp(file_info.st_mtime, unit='s').isoformat() <= last_fetch:
                    continue
            # data only really starts after 2014 trail period
            if file_info.filename.startswith('2014_'):
                continue
            logger.debug(file_info.filename)
            start_time = time()
            with BytesIO() as buffer:
                sftp.getfo(file_info.filename, buffer)
                buffer.seek(0)
                df = pd.read_csv(buffer, sep='\t')
            if 'DateTime' in df:
                time_column = 'DateTime'
            elif 'DateTime(UTC)' in df:
                time_column = 'DateTime(UTC)'
            else:
                raise Exception('No timecolumn defined!')

            df[time_column] = pd.to_datetime(df[time_column], utc=True).dt.tz_convert('europe/amsterdam')

            if table.get('in_out', False):
                df = df[
                    ['DateTime', 'ResolutionCode', 'OutAreaTypeCode', 'OutMapCode', 'InAreaTypeCode', 'InMapCode', table['value_column']] +
                    table.get('extra_column', [])
                ]
                df = df[(df['OutAreaTypeCode'] == 'BZN') & (df['InAreaTypeCode'] == 'BZN')].sort_values('DateTime')
                df = df.drop(columns=['OutAreaTypeCode', 'InAreaTypeCode']).rename(columns={
                    time_column: 'mtu',
                    'OutMapCode': 'zone_out',
                    'InMapCode': 'zone_in',
                    table['value_column']: 'value'
                })
                df = df[(~df['zone_out'].str.startswith('GB_')) & (~df['zone_in'].str.startswith('GB_'))]
            else:
                df = df[
                    ['DateTime', 'ResolutionCode', 'AreaTypeCode', 'MapCode', table['value_column']] +
                    table.get('extra_column', [])
                ]
                df = df[df['AreaTypeCode'] == 'BZN'].sort_values('DateTime')
                df = df.drop(columns=['AreaTypeCode']).rename(columns={
                    time_column: 'mtu',
                    'MapCode': 'zone',
                    table['value_column']: 'value'
                })
                df = df.drop_duplicates(['mtu', 'zone'], keep='first')

            if 'custom_function' in table:
                df = pd.DataFrame(getattr(custom_functions, table['custom_function'])(df))

            if 'ResolutionCode' in df and table['name'].startswith('sdac_'):
                df = pd.DataFrame(df[df['ResolutionCode'] == 'PT60M'])

            df.to_parquet('data/' + table['name'] + '/' + file_info.filename.replace('.csv', '.parquet'), index=False)

            timestamps_parsed.append(pd.Timestamp(file_info.st_mtime, unit='s').isoformat())
            logger.debug(f'Took {time() - start_time} seconds')

        last_edits[table['name']] = max(timestamps_parsed)

    with open('last_edits.json', 'w') as stream:
        json.dump(last_edits, stream)