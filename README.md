# Paychex Revenue Forecast

These are the instructions for running the Paychex revenue forecast model.

## Project Structure

```text
project
│  README.md
│  requirements.txt
│  credentials.yml
│
├─docs/                     # documentation of the problem
│
├─data/                     # the data directory
│  
├─notebooks/                # experimentation and exploration notebooks
│
└─src/                      # Source code

```

## Set up

1. Make sure you have installed python.

2. Project dependencies are located in the requirements.txt file.
To install them you should run:


```commandline
pip install -r requirements.txt
```

3. Create a 'credentials.yml' file with the structure as follows:
```yaml
blob_storage:
  account_key: <Account key provided by Hackett>
  conn_string: <Connection string key provided by Hackett>
```
## Upload Data

There are two ways to upload the data to the Azure Blob storage: using the [Microsoft Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/), or using the Python script in this this project.

### Microsoft Azure Storage Explorer

### Python Script

1. Place all the files in the `./data` folde.
2. In you project home directory, run the next line;

```commandline
python src\paychex_ml\upload_data.py
```
All the files in the folder will be upladed. If there's already a file with the same name it will be replaced.

## Clean data

In you project home directory, run the next line;

```commandline
python src\paychex_ml\clean_data.py
```

## Update external data

In you project home directory, run the next line;

```commandline
python src\paychex_ml\external_data.py
```

## Run models

In you project home directory, run the next line;

```commandline
python run_analysis.py
```

You can especify the item to run from the command line using the item flag:

```commandline
python run_analysis.py --item 11
```