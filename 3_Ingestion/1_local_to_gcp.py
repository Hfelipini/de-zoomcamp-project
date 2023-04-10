from google.cloud import storage
import os
from os import listdir
from os.path import isfile, join

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json"

storage_client = storage.Client()

buckets = list(storage_client.list_buckets())
bucket = storage_client.get_bucket("dtc_data_lake_de-zoomcamp-project-hfelipini") # your bucket name

Filepath='1_Metatrader/Files/'
onlyfiles = [f for f in listdir(Filepath) if isfile(join(Filepath, f))]

for size in range(len(onlyfiles)):
    File=onlyfiles[size]
    blob = bucket.blob(File)
    blob.upload_from_filename(Filepath+File)
    print(buckets, size, len(onlyfiles))
    os.remove(Filepath+File)