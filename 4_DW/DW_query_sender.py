import pandas as pd
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(filename="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json",
                                                                    scopes=["https://www.googleapis.com/auth/cloud-platform"])
"""Create Partitioned table for optimization"""
query = '''
    SELECT
        DISTINCT(ID), *
    FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned`
    --WHERE DateTime = "2023-04-25 15:12:00+00:00"
    ORDER BY Ticker ASC, DateTime DESC
'''

query_result = pd.read_gbq(credentials=credentials, query=query)
print(len(query_result))
print(query_result.head(20))