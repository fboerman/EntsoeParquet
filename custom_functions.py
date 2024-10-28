import pandas as pd


def fix_netposition(df):
    df = pd.DataFrame(df[df['ContractType'] == 'Daily'])
    df['value'] = df.apply(lambda row: -1 * row['value'] if row['Direction'] == 'Import' else row['value'], axis=1)
    df['mtu'] = df['mtu'].dt.tz_convert('UTC')
    df['mtu'] = df['mtu'].dt.floor('h')
    df = df.drop_duplicates(subset=['mtu', 'zone'], keep='first')
    df['mtu'] = df['mtu'].dt.tz_convert('Europe/Amsterdam')
    return df.drop(columns=['ContractType', 'Direction'])
