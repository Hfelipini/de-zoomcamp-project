import pandas as pd
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(filename="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json",
                                                                    scopes=["https://www.googleapis.com/auth/cloud-platform"])

query = '''
    SELECT
        DISTINCT(ID), *
    FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned`
    ORDER BY Ticker ASC, DateTime DESC
'''

query_2 = '''
    CREATE OR REPLACE TABLE `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned`
    PARTITION BY DATE(DateTime)
    CLUSTER BY Ticker AS (
      SELECT * FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test`
    );
'''
#query_result2 = pd.read_gbq(credentials=credentials, query=query_2)

query_result = pd.read_gbq(credentials=credentials, query=query)
print(len(query_result))
print(query_result.head(20))