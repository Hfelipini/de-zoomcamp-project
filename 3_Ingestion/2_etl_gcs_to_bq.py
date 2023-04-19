from pathlib import Path, PurePosixPath
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import storage
import os
import csv

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json"

@task(retries=3)
def extract_from_gcs() -> Path:
    """Download stock data from GCS"""
    gcs_path = f"1-Raw/2023.04.14-10.08.00.csv"
        
    my_bucket = "dtc_data_lake_de-zoomcamp-project-hfelipini"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_bucket)
    blobs = bucket.list_blobs()
    list_1 = list()

    for blob in blobs:
        list_1.append(blob.name)
        
    #print(list_1, list_1[0], list_1[2])
    
    df = pd.DataFrame(list_1)
    df.to_csv('3_Ingestion/GFGGG.csv',header=False,index=False) 

    username = "2023.04.17-10.23.00.csv"
    with open('3_Ingestion/GFGGG.csv', 'rt') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if username == row[0]: # if the username shall be on column 3 (-> index 2)
                print("is in file")


    print(type(blob))   

    return Path(f"Data/{gcs_path}")

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
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-project-creds")

    df.to_gbq(
        destination_table="de_project_dataset.python_test",
        project_id="de-zoomcamp-project-hfelipini",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load new data into Big Query"""

    #Primeiro é necessário ler os arquivos no bucket
    #Salvar esses arquivos num CSV
    #Rodar somente o código com os CSVs que ainda não foram carregados para o BigQuery
    #Após subir o arquivo no BigQuery, colocar no CSV que ele já foi lido e deletar a versão local

    #Fazer laço For para passar por todos os arquivos ainda não lidos
    #Código para obter os arquivos
    path = extract_from_gcs() #Mandar o nome do arquivo
    #df = transform(path)
    #write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()