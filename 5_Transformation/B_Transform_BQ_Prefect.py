#!/usr/bin/python
from pathlib import Path
from prefect import flow, task
import pandas as pd
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(filename="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json",
                                                                    scopes=["https://www.googleapis.com/auth/cloud-platform"])

@task()
def query_write() -> str:
    """Write the Query to obtain the variation of each ticker related to the previous one in the most recent update"""
    query = '''
    CREATE OR REPLACE TABLE `de-zoomcamp-project-hfelipini.de_project_dataset.minute_rate`
    AS (
      SELECT
        DISTINCT(A.ID), A.DateTime, A.Ticker, A.Close, A.Timeframe, A.Dayframe, B.Close as ClosePrevious, CAST(A.Close AS DECIMAL) / B.Close - 1 as Rate,
      FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned` A,
           `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned` B 
      WHERE 
        A.Ticker = B.Ticker
      AND 
        A.Timeframe - 1 = B.Timeframe
      AND  
        A.Dayframe = (
          SELECT MAX(Dayframe) 
          FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned`
        )
      AND 
        A.Timeframe >= (
          SELECT MAX(Timeframe) 
          FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned` 
          WHERE Dayframe = (
            SELECT MAX(Dayframe) 
            FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned`
          )
        )
      ORDER BY A.ID ASC
    );
    '''
    return query
        
@task()
def send_query(query: str) -> None:
    """Run the query in Google BigQuery to create the updated table""" 
    pd.read_gbq(credentials=credentials, query=query)

@flow()
def transform_bq():
    """The Transform Function in BigQuery"""
    query = query_write()
    send_query(query)

if __name__ == '__main__':
    transform_bq()
