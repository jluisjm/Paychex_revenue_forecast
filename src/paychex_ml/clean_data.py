import io
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from azure.storage.blob import BlobServiceClient
from src.paychex_ml.utils import load_credentials
from src.paychex_ml.utils import get_blob_list
from src.paychex_ml.upload_data import upload_data


def get_df(client, file, container="raw-data"):
    """

    :param client:
    :param file:
    :param container:
    :return:
    """

    # Get container client
    container_client = client.get_container_client(container)

    # Download the file
    stream = container_client.download_blob(file, encoding="latin-1").content_as_text(encoding="latin-1")
    file = io.StringIO(stream)

    # Clean the dataframe
    df = pd.read_csv(file, sep="\t", header=[0, 1, 2, 4], index_col=[0, 1, 2, 3])
    df = df.loc[~df.index.isin([(np.nan, np.nan, np.nan, np.nan)])] \
        .dropna(axis=0, how='all') \
        .transpose() \
        .replace({",": "", "%$": ""}, regex=True) \
        .fillna(0) \
        .astype('float')

    return df


def join_all(blob_service_client, file_list, column_names, container="raw-data"):
    """
    :param blob_service_client:
    :param file_list:
    :param column_names:
    :param container:
    :return:
    """
    list_df = []
    for f in file_list:
        try:
            df = get_df(blob_service_client, f, container=container)

        except:
            print("Download type not defined for: ", f)
            continue

        if f == '401kRevenue.txt':
            df = df.loc[:, ("Total Activity", "Total 401k Revenue", "Total Paychex", "Total Service Revenue - RW")]
        elif f == 'OnlineRevenue.txt':
            df = df.loc[:, ("Total Activity", "Total Online Svcs", "Total Paychex", "Total Service Revenue - RW")]
        elif f == 'InsuranceRevenue.txt':
            df = df.loc[:,
                 ("Total Activity", "Total Insurance Agency", "Total Paychex", "Total Service Revenue - RW")]
        elif f == 'PEORevenue.txt':
            df = df.loc[:, ("Total Activity", "Total PBS Revenue", "Total Paychex", "Total Service Revenue - RW")]
        elif f == 'OtherMgmtRevenue.txt':
            df = df.loc[:,
                 ("Total Activity", "Other Managment Solutions Revenue", "Total Paychex",
                  "Total Service Revenue - RW")]
        elif f == 'PayrollSurePayrollASOInternationalHighLevelRevenue.txt':
            df = df.groupby(level=1, axis=1).sum()
        elif f in ('BlendedProductRevenue.txt', 'InternationalRevenue.txt', 'SurePayollRevenue.txt'):
            df = df["Total Activity"].sum(axis=1).to_frame(name=f)
        else:
            print("Transformation not defined for: ", f)
            continue

        print("Added: ", f)
        list_df.append(df)

    df_join = pd.concat(list_df, axis=1)

    if type(column_names) == 'list':
        column_names = dict(zip(df_join.columns, column_names))

    df_join = df_join \
        .rename(columns=column_names) \
        .filter(regex='^[^pop]', axis=1) \
        .reset_index(-1) \
        .rename(columns={'level_3': '00 period'})

    # Correct month
    df_join['00 period'] = df_join['00 period'] \
        .replace({'\nJun': 1, '\nJul': 2, '\nAug': 3, '\nSep': 4, '\nOct': 5, '\nNov': 6, '\nDec': 7, '\nJan': 8,
                  '\nFeb': 9, '\nMar': 10, '\nApr': 11, '\nMay': 12, 'YearTotal': 0}) \
        .astype('int')

    return df_join.sort_index(axis=1)


def upload_clean_df(df, name, client, container="clean-data"):
    """
    :param df:
    :param client:
    :param container:
    :return:
    """

    container_client = client.get_container_client(container)

    table = pa.Table.from_pandas(df)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    blob_client = container_client.upload_blob(name=name,
                                               data=buf.getvalue().to_pybytes(),
                                               overwrite=True)

    print("Uploaded data")

    return blob_client


if __name__ == '__main__':

    # Load credentials
    credentials = load_credentials("blob_storage")

    # Start client
    blob_service_client = BlobServiceClient.from_connection_string(credentials['conn_string'])

    # Get a list of all the blobs in raw-data container
    blob_list = get_blob_list(blob_service_client, container="raw-data")

    # Todo: move dictionary to a json file
    column_names = {
        ('Total Activity', 'Total 401k Revenue', 'Total Paychex', 'Total Service Revenue - RW'): '20 Total 401k',
        'BlendedProductRevenue.txt': '11 Payroll Blended Products',
        'InternationalRevenue.txt': '17 Total International',
        ('Total Activity', 'Total Online Svcs', 'Total Paychex', 'Total Service Revenue - RW'): '40 Total Online Services',
        'SurePayollRevenue.txt': '16 SurePayroll',
        ('Total Activity', 'Total Insurance Agency', 'Total Paychex', 'Total Service Revenue - RW'): '70 Total Insurance Services',
        ('Total Activity', 'Total PBS Revenue', 'Total Paychex', 'Total Service Revenue - RW'): '60 Total PEO',
        ('Total Activity', 'Other Managment Solutions Revenue', 'Total Paychex', 'Total Service Revenue - RW'): '50 Other Managment Solutions',
        'HR Solutions (excl PEO)': '31 HR Solutions (excl PEO)',
        'SurePayroll Revenue': 'pop1',
        'Total Blended Products Revenue': 'pop2',
        'Total Delivery Revenue': '13 Delivery Revenue',
        'Total HR Solutions/ASO (Payroll side)': '14 ASO Allocation',
        'Total Other Processing Revenue': '15 Other Processing Revenue',
        'Total W-2 Revenue': '12 W2 Revenue'
    }

    # Download and join all data
    df = join_all(blob_service_client, blob_list, column_names)

    # Upload to clean data
    blob_client = upload_clean_df(df, "paychex_revenue.parquet", blob_service_client)