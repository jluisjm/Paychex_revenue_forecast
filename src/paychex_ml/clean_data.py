import io
import pandas as pd
import numpy as np


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
        .rename(columns={'level_3': 'period'})

    # Correct month
    df_join['period'] = df_join['period'] \
        .replace({'\nJun': 1, '\nJul': 2, '\nAug': 3, '\nSep': 4, '\nOct': 5, '\nNov': 6, '\nDec': 7, '\nJan': 8,
                  '\nFeb': 9, '\nMar': 10, '\nApr': 11, '\nMay': 12, 'YearTotal': 0}) \
        .astype('int')

    return df_join
