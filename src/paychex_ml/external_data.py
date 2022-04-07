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

    return df[['date'] + list(filter(lambda x: x != 'date', df.columns.values))]


if __name__ == '__main__':
    # Load credentials
    credentials = load_credentials("blob_storage")

    # Start client
    blob_service_client = BlobServiceClient.from_connection_string(credentials['conn_string'])

    # Get unemployment rate
    seriesid = ["LNS14000000", "CEU0000000001", "CES0000000001"]
    df = get_bls_data(seriesid)

    # Upload data

    blob_client = upload_df_parquet(df, "external_data.parquet", blob_service_client, container='external-data')
