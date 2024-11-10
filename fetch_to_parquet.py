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

        last_fetch = last_edits.get(table['name'], None)
        timestamps_parsed = []
        os.makedirs('data/' + table['name'], exist_ok=True)
        manifest_file = os.path.join('data', table['name'], 'manifest.json')
        if os.path.exists(manifest_file):
            with open(manifest_file, 'r') as stream:
                manifest = json.load(stream)
        else:
            manifest = []

        this_month = (pd.Timestamp.today() + pd.Timedelta(days=1)).strftime("%Y_%m")

        for file_info in sorted(sftp.listdir_attr('.'), key=lambda x: x.filename):
            if last_fetch is not None:
                if pd.Timestamp(file_info.st_mtime, unit='s').isoformat() <= last_fetch:
                    continue
            # data only really starts after 2014 trail period
            if file_info.filename.startswith('2014_'):
                continue

            # dont download data that was already prefilled for the future
            if file_info.filename[:7] > this_month:
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
                    [time_column, 'ResolutionCode', 'OutAreaTypeCode', 'OutMapCode', 'InAreaTypeCode', 'InMapCode', table['value_column']] +
                    table.get('extra_column', [])
                ]
                df = df[(df['OutAreaTypeCode'].str.contains('BZN')) & (df['InAreaTypeCode'].str.contains('BZN'))]
                df = df.drop(columns=['OutAreaTypeCode', 'InAreaTypeCode']).rename(columns={
                    time_column: 'mtu',
                    'OutMapCode': 'zone_out',
                    'InMapCode': 'zone_in',
                    table['value_column']: 'value'
                })
                df = df[(~df['zone_out'].str.startswith('GB_')) & (~df['zone_in'].str.startswith('GB_'))]
                df = df.drop_duplicates(['mtu', 'zone_in', 'zone_out'], keep='first')
            else:
                df = df[
                    [time_column, 'ResolutionCode', 'AreaTypeCode', 'MapCode', table['value_column']] +
                    table.get('extra_column', [])
                ]
                df = df[df['AreaTypeCode'].str.contains('BZN')]
                df = df.drop(columns=['AreaTypeCode']).rename(columns={
                    time_column: 'mtu',
                    'MapCode': 'zone',
                    table['value_column']: 'value'
                })
                df = df.drop_duplicates(['mtu', 'zone'], keep='first')

            df = df.rename(columns={'ResolutionCode': 'resolution'})

            if 'custom_function' in table:
                df = pd.DataFrame(getattr(custom_functions, table['custom_function'])(df))

            filename = f"{table['name']}_{file_info.filename.split('_')[0]}_{file_info.filename.split('_')[1]}.parquet"
            df.to_parquet(os.path.join('data', table['name'], filename), index=False)
            manifest.append({
                'url': f'https://github.com/fboerman/EntsoeParquet/raw/master/data/{table["name"]}/{filename}',
                'year': int(file_info.filename.split('_')[0]),
                'month': int(file_info.filename.split('_')[1].split('.')[0]),
                'dataset': table['name']
            })

            timestamps_parsed.append(pd.Timestamp(file_info.st_mtime, unit='s').isoformat())
            logger.debug(f'Took {round(time() - start_time, 2)} seconds')

        if len(timestamps_parsed) > 0:
            logger.info(f'Parsed {len(timestamps_parsed)} files')
            last_edits[table['name']] = max(timestamps_parsed)
            with open(manifest_file, 'w') as stream:
                json.dump(sorted(list({v['url']:v for v in manifest}.values()), key=lambda x: x['url']), stream, indent=4)
        else:
            logger.info("No new files parsed")

    with open('last_edits.json', 'w') as stream:
        json.dump(last_edits, stream, indent=4)