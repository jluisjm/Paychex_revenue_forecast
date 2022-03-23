import yaml

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

