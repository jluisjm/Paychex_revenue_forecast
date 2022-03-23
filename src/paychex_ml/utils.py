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

