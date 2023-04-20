from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json"
        
my_bucket = "dtc_data_lake_de-zoomcamp-project-hfelipini"
storage_client = storage.Client()
bucket = storage_client.get_bucket(my_bucket)
blobs = bucket.list_blobs()

for blob in blobs:
    print(blob.name) # if you only want to display the name of the blob