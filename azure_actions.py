
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import uuid
from io import StringIO
import pandas as pd


connect_str="DefaultEndpointsProtocol=https;AccountName=demoexl123;AccountKey=WzRMepyKYlwQZmj7llzQUL4AooMk8tMPi4r4tKJ5FEHcYAlX1YmQhG+SuX67PpeFSpW0fe98vJ41+ASt+IEwaA==;EndpointSuffix=core.windows.net"

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client_in = blob_service_client.get_container_client(container= 'synth-input')
container_client_out = blob_service_client.get_container_client(container= 'synth-output')

def upload_file_to_blob(filename, data, container_client): 
    blob_client = container_client.upload_blob(name= filename, data=data, overwrite=True)
            
        
def download_blob_into_df(blob_container_name,filename):
     blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=filename)
     download_stream = blob_client.download_blob().content_as_text()
     return(pd.read_csv(StringIO(download_stream)))
        

def upload_df_to_blob(filename, df_data, container_client):
    blob_client = container_client.upload_blob(name= filename, data=df_data.to_csv(index=False), overwrite=True)
    

#def upload_file_to_blob(out_file, in_file, container_client):
#    blob_client = container_client.upload_blob(out_file, in_file, overwrite=True)

