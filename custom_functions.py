import pandas as pd


def fix_netposition(df):
    df = pd.DataFrame(df[df['ContractType'] == 'Daily'])
    df['value'] = df.apply(lambda row: -1 * row['value'] if row['Direction'] == 'Import' else row['value'])
    df['mtu'] = df['mtu'].dt.floor('h')
    df = df.drop_duplicates(subset=['mtu', 'zone'], keep='first')

    return df.drop(columns=['ResolutionCode', 'ContractType'])
