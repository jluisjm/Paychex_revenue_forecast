import yaml
import pyarrow as pa
import pyarrow.parquet as pq

def load_credentials(credential, file = "./credentials.yml"):
    """
    :param credential:
    :param file:
    :return:
    """

    with open(file,"r") as c:
        credentials = yaml.safe_load(c)[credential]

    return credentials

def get_blob_list(client, container="raw-data"):
    """
    Get blobs in a container
    """
    container_client = client.get_container_client(container)
    blob_list = []
    for blob in container_client.list_blobs():
        file_name = blob.name
        blob_list.append(file_name)

    return blob_list

def upload_df_parquet(df, name, client, container="clean-data"):
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

    print("Uploaded {}".format(name))

    return blob_client

def upload_df_csv(df, name, client, container="clean-data"):
    """
    :param df:
    :param client:
    :param container:
    :return:
    """

    container_client = client.get_container_client(container)

    table = df.to_csv()
    blob_client = container_client.upload_blob(name=name,
                                               data=table,
                                               overwrite=True)

    print("Uploaded {}".format(name))

    return blob_client
