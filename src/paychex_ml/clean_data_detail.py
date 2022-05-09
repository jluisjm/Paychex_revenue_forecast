import io, os
import pandas as pd
#from azure.storage.blob import BlobServiceClient

from src.paychex_ml.utils import load_credentials
from src.paychex_ml.utils import get_blob_list
from src.paychex_ml.utils import upload_df_csv


def read_mapping(file_mapping):
    return pd.read_csv(file_mapping, encoding="latin_1")


def create_date(df):
    df_date = df.copy()

    df_date[['Scenario', 'Version', 'Fiscal Year', 'Period']] = df_date['variable'].str.split("|", expand=True)

    df_date = df_date[df_date['Period'] != 'YearTotal']

    df_date['Period'] = df_date['Period'].replace(
        {'\nJun': '01', '\nJul': '02', '\nAug': '03', '\nSep': '04', '\nOct': '05', '\nNov': '06', '\nDec': '07',
         '\nJan': '08',
         '\nFeb': '09', '\nMar': '10', '\nApr': '11', '\nMay': '12'})

    df_date['Calendar Date'] = (pd.to_datetime(df_date['Fiscal Year'].str.slice(2) + df_date['Period'] + '01',
                                               format="%y%m%d").dt.to_period('M') - 7) \
        .dt.to_timestamp() \
        .apply(lambda x: x.strftime('%Y%m%d'))

    return df_date.drop(columns='variable')


def get_df(file, mapping, client=None, container="raw-data"):
    """
    :param client:
    :param file:
    :param container:
    :return:
    """

    if client is not None:
        # Get container client
        container_client = client.get_container_client(container)
        # Download the file
        stream = container_client.download_blob(file, encoding="latin-1").content_as_text(encoding="latin-1")
        file = io.StringIO(stream)
        print("Processing from Azure Storage")
    else:
        file = "./data/raw/" + file
        print("Processing {} locally".format(file))

    # Clean the dataframe
    df = pd.read_csv(file, sep="\t", header=[0, 1, 2, 4], encoding="latin-1")
    df = df.iloc[:, :186]
    column_names = dict(zip(['Unnamed: 0_level_0', 'Unnamed: 1_level_0', 'Unnamed: 2_level_0', 'Unnamed: 3_level_0'],
                            ['Level0', 'Product', 'Account', 'Detail']))
    df = df.rename(columns=column_names, level=0)
    df = df.rename(
        columns=dict(zip(['Unnamed: 0_level_1', 'Unnamed: 1_level_1', 'Unnamed: 2_level_1', 'Unnamed: 3_level_1'],
                         ['', '', '', ''])),
        level=1)
    df = df.rename(
        columns=dict(zip(['Unnamed: 0_level_2', 'Unnamed: 1_level_2', 'Unnamed: 2_level_2', 'Unnamed: 3_level_2'],
                         ['', '', '', ''])),
        level=2)
    df = df.rename(
        columns=dict(zip(['Unnamed: 0_level_3', 'Unnamed: 1_level_3', 'Unnamed: 2_level_3', 'Unnamed: 3_level_3'],
                         ['', '', '', ''])),
        level=3)
    df.columns = df.columns.map('|'.join).str.strip('|')

    df = mapping.merge(df)

    return df


def join_all(file_list, file_mapping="./data/dictionary/mapping.csv", blob_service_client=None, container="raw-data"):
    mapping = read_mapping(file_mapping)
    list_df = []

    for name in file_list:
        if name in mapping.File.unique():
            print("Processing: ", name)
            df = get_df(name, mapping, client=blob_service_client, container=container)
            list_df.append(df)
            print("{} added from {}".format(df.shape, name))
        else:
            print("No process for: ", name)

    df = pd.concat(list_df) \
        .replace({",": "", "%$": ""}, regex=True) \
        .drop(columns='Level0')
    print('All files joined')

    id_vars = df.columns.values[:5]
    value_vars = df.columns.values[5:]
    df = pd.melt(df, id_vars=id_vars, value_vars=value_vars, value_name='Value')

    df = create_date(df)
    df = df[['Calendar Date','Scenario', 'Version', 'Fiscal Year', 'Period', 'File','Product', 'Account', 'Detail','Item','Value']]

    df_predictable = df[~df['Item'].str.contains('Drivers')].reset_index(drop=True).fillna(0)
    df_drivers = df[df['Item'].str.contains('Drivers')].reset_index(drop=True)

    return df_predictable, df_drivers


if __name__ == '__main__':
    # # Load credentials
    # credentials = load_credentials("blob_storage")
    # # Start client
    # blob_service_client = BlobServiceClient.from_connection_string(credentials['conn_string'])

    # Get a list of all the files in raw-data
    # file_list = get_blob_list(blob_service_client, container="raw-data")
    file_list = os.listdir("./data/raw")

    # Download and join all data
    df_predictable, df_drivers = join_all(file_list)

    # Upload to clean data
    # upload_df_csv(df_predictable, "table_predictable.csv", blob_service_client)
    # upload_df_csv(df_drivers, "table_drivers.csv", blob_service_client)

    # Save clean data in local path
    clean_path = "./data/clean/"
    df_predictable.to_csv(clean_path+"table_predictable.csv", index=False)
    df_drivers.to_csv(clean_path+"table_drivers.csv", index=False)