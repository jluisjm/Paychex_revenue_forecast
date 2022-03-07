import yaml

def load_credentials(credential, file = "./credentials.yml"):

    with open(file,"r") as c:
        credentials = yaml.safe_load(c)[credential]

    return credentials