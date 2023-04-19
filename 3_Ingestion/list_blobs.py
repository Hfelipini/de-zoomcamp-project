from google.cloud import storage
import os
#from os import listdir
#from os.path import isfile, join

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/hfeli/OneDrive/Documents/Cursos/DataEngineering/Projects/de-zoomcamp-project-important-files/de-zoomcamp-project-hfelipini-a9d06ac71bcd.json"

#storage_client = storage.Client()

#buckets = list(storage_client.list_buckets())
#bucket = storage_client.get_bucket("dtc_data_lake_de-zoomcamp-project-hfelipini") # your bucket name
"""
my_prefix = ""
my_bucket = "dtc_data_lake_de-zoomcamp-project-hfelipini"
storage_client = storage.Client()
bucket = storage_client.get_bucket(my_bucket)
blobs = bucket.list_blobs(prefix = my_prefix, delimiter = '/')

for blob in blobs:
    if(blob.name != my_prefix): # ignoring the subfolder itself 
        print(blob.name.replace(my_prefix, "")) # if you only want to display the name of the blob
"""
        
my_bucket = "dtc_data_lake_de-zoomcamp-project-hfelipini"
storage_client = storage.Client()
bucket = storage_client.get_bucket(my_bucket)
blobs = bucket.list_blobs()

for blob in blobs:
    print(blob.name) # if you only want to display the name of the blob