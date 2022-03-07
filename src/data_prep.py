from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from utils import load_credentials

if __name__ == '__main__':

    # Load credentials
    credentials = load_credentials("blob_storage")

    # Start client
    blob_service_client = BlobServiceClient.from_connection_string(credentials['conn_string'])

    # List and print containers
    all_containers = blob_service_client.list_containers()
    for c in all_containers:
        print(c.name)
