import os
from os import listdir
from os.path import isfile, join
import pandas as pd
from pathlib import Path, PurePosixPath
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import storage
from google.oauth2 import service_account

# Same
credentials = service_account.Credentials.from_service_account_file(filename="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json",
                                                                    scopes=["https://www.googleapis.com/auth/cloud-platform"])
# Same
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json"

@task()
def extract_from_gcs(gsc_path: str) -> Path:
    """Download stock data from GCS"""
    gcs_block = GcsBucket.load("de-project")
    gcs_block.get_directory(from_path=gsc_path, local_path=f"./local_save/")
    
    return Path(f"./local_save/{gsc_path}")

@task()
# transform into what? Adding a bit more context on the method name is usually helpful.
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning and formatting"""
    df = pd.read_csv(path)       
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df["Open"] = pd.to_numeric(df["Open"]).round(2)
    df["High"] = pd.to_numeric(df["High"]).round(2)
    df["Low"] = pd.to_numeric(df["Low"]).round(2)
    df["Close"] = pd.to_numeric(df["Close"]).round(2)
    df["RealVolume"] = pd.to_numeric(df["RealVolume"])
    df["Spread"] = pd.to_numeric(df["Spread"])
    df["TickVolume"] = pd.to_numeric(df["TickVolume"])
    df["Year"]=pd.DatetimeIndex(df["DateTime"]).year
    df["Month"]=pd.DatetimeIndex(df["DateTime"]).month
    df["Day"]=pd.DatetimeIndex(df["DateTime"]).day
    df["Hour"]=pd.DatetimeIndex(df["DateTime"]).hour
    df["Minute"]=pd.DatetimeIndex(df["DateTime"]).minute
    df["Dayframe"]=df["Day"]+31*df["Month"]+365*df["Year"]
    df["Timeframe"]=df["Minute"]+60*df["Hour"]
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-project-creds")

    df.to_gbq(
        #Once the table is created, chance the destination table to the partitioned
        #destination_table="de_project_dataset.python_test", 
        destination_table="de_project_dataset.python_test_partitioned",
        project_id="de-zoomcamp-project-hfelipini",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )

@task()
def update_csv_and_BQ(list_files: list) -> None:
    """Update the CSV reference"""
    """Update the partitioned table at BigQuery"""
    df_files = pd.DataFrame(list_files)
    df_files.to_csv('C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project/3_Ingestion/Check_to_BQ.csv',header=False,index=False)

@task()
def clean_folder() -> None:
    """Delete all files in local folder"""
    Filepath = "./local_save/"
    onlyfiles = [f for f in listdir(Filepath) if isfile(join(Filepath, f))]
    for size in range(len(onlyfiles)):
        File = onlyfiles[size]
        os.remove(Filepath+File)

@flow()
# I'm guessing GCS could be replaced with any datastore, right?
# If you wanted to make this more generic, you could inject the datastore as parameter and abstract it away from the code.
# You can look into "repository pattern" for more details.
def etl_gcs_to_bq():
    """Main ETL flow to load new data into Big Query"""

    """Retrieve the list of files in GCS Bucket"""
    my_bucket = "dtc_data_lake_de-zoomcamp-project-hfelipini"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_bucket)
    blobs = bucket.list_blobs()
    list_files = list()

    """Save all files names in a list"""
    for blob in blobs:
        list_files.append(blob.name)
    
    """Check which files aren't in the list, so these will be uploaded to BigQuery"""
    with open('3_Ingestion/Check_to_BQ.csv', 'rt') as c:
        str_arr_csv = c.readlines()
        
    for files_in_bucket in range(len(list_files)):
        file_name = list_files[files_in_bucket]
        if str(file_name) not in str(str_arr_csv):
            path = extract_from_gcs(file_name)
            df = transform(path)
            write_bq(df)
    
    """Finally, clean the local folder for space management and save the files uploaded in CSV to compare next runs"""
    update_csv_and_BQ(list_files)
    clean_folder()

if __name__ == "__main__":
    etl_gcs_to_bq()