from pathlib import Path, PurePosixPath
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import storage
import os
import csv
from os import listdir
from os.path import isfile, join

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json"

@task()
def extract_from_gcs(gsc_path: str) -> Path:
    """Download stock data from GCS"""
    gcs_block = GcsBucket.load("de-project")
    gcs_block.get_directory(from_path=gsc_path, local_path=f"./local_save/")
    
    return Path(f"./local_save/{gsc_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    df = pd.read_csv(path)       
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df["Open"] = pd.to_numeric(df["Open"]).round(2)
    df["High"] = pd.to_numeric(df["High"]).round(2)
    df["Low"] = pd.to_numeric(df["Low"]).round(2)
    df["Close"] = pd.to_numeric(df["Close"]).round(2)
    df["RealVolume"] = pd.to_numeric(df["RealVolume"])
    df["Spread"] = pd.to_numeric(df["Spread"])
    df["TickVolume"] = pd.to_numeric(df["TickVolume"])
    #print(df.head(20))
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-project-creds")

    df.to_gbq(
        destination_table="de_project_dataset.python_test",
        project_id="de-zoomcamp-project-hfelipini",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        #chunksize=500_000,
        if_exists="append",
    )

@task()
def clean_folder() -> None:
    Filepath = "./local_save/"
    onlyfiles = [f for f in listdir(Filepath) if isfile(join(Filepath, f))]
    for size in range(len(onlyfiles)):
        File = onlyfiles[size]
        os.remove(Filepath+File)

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load new data into Big Query"""

    my_bucket = "dtc_data_lake_de-zoomcamp-project-hfelipini"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_bucket)
    blobs = bucket.list_blobs()
    list_files = list()

    for blob in blobs:
        list_files.append(blob.name)
    
    with open('3_Ingestion/Check_to_BQ.csv', 'rt') as c:
        str_arr_csv = c.readlines()
    for files_in_bucket in range(len(list_files)):
        file_name = list_files[files_in_bucket]
        if str(file_name) not in str(str_arr_csv):
            path = extract_from_gcs(file_name)
            df = transform(path)
            write_bq(df)
    
    clean_folder()
    df_files = pd.DataFrame(list_files)
    df_files.to_csv('3_Ingestion/Check_to_BQ.csv',header=False,index=False)

if __name__ == "__main__":
    etl_gcs_to_bq()