import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from utils import load_credentials


def upload_data(client, path="./data", container="raw-data"):
    """
    :param client: Blob Service Client.
    :param path: Path where the data is stored.
    :param container: Container to store the data
    :return: A BlobClient to interact with the newly uploaded blob.
    """

    # Get Container Client from Blob Service Client
    container_client = client.get_container_client(container)

    # Get all files
    data_files = os.listdir(path)

    # Upload all files
    blob_clients =[]
    for file in data_files:
        print("Writing:", file)
        with open("./data/" + file, "rb") as data:
            blob_client = container_client.upload_blob(name=file, data=data, overwrite=True)
            blob_clients.append(blob_client)

    return blob_clients


if __name__ == '__main__':

    # Load credentials
    credentials = load_credentials("blob_storage")

    # Start client
    blob_service_client = BlobServiceClient.from_connection_string(credentials['conn_string'])

    # Upload all files
    upload_data(blob_service_client)
