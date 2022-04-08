import json
import requests
import pandas as pd

from azure.storage.blob import BlobServiceClient
from src.paychex_ml.utils import load_credentials
from src.paychex_ml.utils import upload_df_parquet


def get_bls_data(seriesid, startyear='2015', endyear='2022'):
    """
    :param seriesid:
    :param startyear:
    :param endyear:
    :return:
    """

    # Names dictionary
    series_dic = {"LNS14000000": 'UnemploymentRate',
                  "CES0000000001": "NFPayrolls_sa",
                  "CEU0000000001": "NFPayrolls_nsa"}

    # Request data
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": seriesid, "startyear": startyear, "endyear": endyear})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

    # Load data
    json_response = json.loads(p.text)
    json_data = json_response['Results']['series'][0]['data']

    # Data as dataframe
    list_df = []
    for s in json_response['Results']['series']:

        print("Processing: ", s['seriesID'])

        try:
            col_name = series_dic[s['seriesID']]
        except:
            col_name = s['seriesID']

        ser = pd.DataFrame(s['data']) \
            .astype({'value': 'float'}) \
            .drop(columns=['latest', 'footnotes', 'period']) \
            .rename(columns={'value': col_name}) \
            .set_index(['year', 'periodName'])
        list_df.append(ser)

    df = pd.concat(list_df, axis=1).reset_index()
    df['date'] = pd.to_datetime(df['year'] + df['periodName'] + '01', format="%Y%B%d").dt.strftime("%Y%m%d")
    df = df.drop(columns=['year', 'periodName'])
    #df = df[['date'] + list(filter(lambda x: x != 'date', df.columns.values))]
    return df.set_index('date')

def get_census_data(startyear='2015'):
    '''

    :param startyear:
    :return:
    '''

    print("Reading census data")
    url = 'http://api.census.gov/data/timeseries/eits/bfs?get=cell_value,time_slot_id&category_code=TOTAL&seasonally_adj&data_type_code=BA_BA&for=US&time=from+{}'\
        .format(startyear)
    p = requests.get(url)
    data = json.loads(p.text)

    df = pd.DataFrame(data)
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df['date'] = pd.to_datetime(df['time'] + '01', format="%Y-%m%d").dt.strftime("%Y%m%d")
    df = df.drop(['time_slot_id','category_code','data_type_code','time','us'], axis=1)\
        .set_index(['date','seasonally_adj'])\
        .unstack(1)['cell_value']\
        .rename(columns={'no': 'BusinessApplications_nsa', 'yes': 'BusinessApplications_sa'})

    return df

def get_external_data(seriesbls, startyear='2015', endyear='2022'):

    df_bls = get_bls_data(seriesbls, startyear, endyear)
    df_census = get_census_data(startyear)

    df = pd.concat([df_bls, df_census], axis=1)

    return df

if __name__ == '__main__':
    # Load credentials
    credentials = load_credentials("blob_storage")

    # Start client
    blob_service_client = BlobServiceClient.from_connection_string(credentials['conn_string'])

    # Get unemployment rate
    seriesid = ["LNS14000000", "CEU0000000001", "CES0000000001"]
    df = get_external_data(seriesid)

    # Upload data

    blob_client = upload_df_parquet(df, "external_data.parquet", blob_service_client, container='external-data')
