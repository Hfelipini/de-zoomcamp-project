from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from google.cloud import storage
import glob
import os
from os import listdir
from os.path import isfile, join

@task()
def write_gsc(Filepath: Path) -> None:
    """Upload local CSV file into GCS"""
    gcs_block = GcsBucket.load("de-project")
    onlyfiles = [f for f in listdir(Filepath) if isfile(join(Filepath, f))]

    for size in range(len(onlyfiles)):
        File = onlyfiles[size]
        gcs_block.upload_from_path(from_path=Filepath+File,to_path=File)
        
@task()
def clean_folder(Filepath) -> None:
    onlyfiles = [f for f in listdir(Filepath) if isfile(join(Filepath, f))]
    for size in range(len(onlyfiles)):
        File = onlyfiles[size]
        os.remove(Filepath+File)

@flow()
def etl_local_to_gsc():
    """The main ETL Function"""
    Filepath='C:/Users/hfeli/AppData/Roaming/MetaQuotes/Terminal/D0E8209F77C8CF37AD8BF550E51FF075/MQL5/Files/'
    write_gsc(Filepath)
    clean_folder(Filepath)

if __name__ == '__main__':
    etl_local_to_gsc()
