import pandas as pd
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(filename="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json",
                                                                    scopes=["https://www.googleapis.com/auth/cloud-platform"])
"""Create Partitioned table for optimization"""

query = '''
    CREATE OR REPLACE TABLE `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned`
    PARTITION BY DATE(DateTime)
    CLUSTER BY Ticker AS (
      SELECT * FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test`
    );
'''

query_result = pd.read_gbq(credentials=credentials, query=query)