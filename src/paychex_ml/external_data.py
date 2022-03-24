import json
import requests
import pandas as pd

from azure.storage.blob import BlobServiceClient
from src.paychex_ml.utils import load_credentials
from src.paychex_ml.utils import upload_df_parquet

def get_unemployment_rate(seriesid='LNS14000000', startyear='2015', endyear='2022'):
    """
    :param seriesid:
    :param startyear:
    :param endyear:
    :return:
    """

    # Request data
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": [seriesid],"startyear":startyear, "endyear":endyear})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

    # Load data
    json_response = json.loads(p.text)
    json_data = json_response['Results']['series'][0]['data']

    # Data as dataframe
    df = pd.DataFrame(json_data).astype({'value': 'float'})
    df = df.drop(columns='footnotes')

    print("Loaded {} data".format(seriesid))
    return df

if __name__ == '__main__':

    # Load credentials
    credentials = load_credentials("blob_storage")

    # Start client
    blob_service_client = BlobServiceClient.from_connection_string(credentials['conn_string'])

    # Get unemployment rate
    print
    df = get_unemployment_rate()

    # Upload data

    blob_client = upload_df_parquet(df, "unemployment_rate.parquet", blob_service_client, container='external-data')