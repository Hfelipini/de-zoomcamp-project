from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs() -> Path:
    """Download stock data from GCS"""
    gcs_path = f"1-Raw/2023.04.14-10.08.00.csv"
    gcs_block = GcsBucket.load("de-project")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"Data/")

    return Path(f"Data/{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning"""
    df = pd.read_csv(path)
    #print(df["Open"].head(21))
    #print(f"columns: {df.dtypes}")
    #print(f"rows: {len(df)}")
        
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df["Open"] = pd.to_numeric(df["Open"]).round(2)
    df["High"] = pd.to_numeric(df["High"]).round(2)
    df["Low"] = pd.to_numeric(df["Low"]).round(2)
    df["Close"] = pd.to_numeric(df["Close"]).round(2)
    df["RealVolume"] = pd.to_numeric(df["RealVolume"])
    df["Spread"] = pd.to_numeric(df["Spread"])
    df["TickVolume"] = pd.to_numeric(df["TickVolume"])
    #print(df["Open"].head(21))
    #print(f"columns: {df.dtypes}")
    #print(f"rows: {len(df)}")
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
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs()
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()