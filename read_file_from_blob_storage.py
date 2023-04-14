from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd

class Read_initial_csv_file:

    def __init__(self):
        # enter credentials
        self.account_name = 'demoexlflask'

        self.account_key = 'qwlSm7yaOCUS04PQjJpuosG+1InVQ1sgKLXZaRjDc6T/vK/m3/Tzi3soglKm5ngJJD7+2neSnO8V+AStNWbmDw=='

        self.container_name = 'filesstorage'

        # create a client to interact with blob storage
        self.connect_str = 'DefaultEndpointsProtocol=https;AccountName=demoexlflask;AccountKey=qwlSm7yaOCUS04PQjJpuosG+1InVQ1sgKLXZaRjDc6T/vK/m3/Tzi3soglKm5ngJJD7+2neSnO8V+AStNWbmDw==;EndpointSuffix=core.windows.net'


    def read_file(self,blob_folder="input_files/"):

        print(self.connect_str)
        blob_service_client = BlobServiceClient.from_connection_string(self.connect_str)

        # use the client to connect to the container
        container_client = blob_service_client.get_container_client(self.container_name)

        # get a list of all blob files in the container
        blob_list = []
        for blob_i in container_client.list_blobs(blob_folder):
            blob_list.append(blob_i.name)
        blob_i=blob_list[0]

        sas_i = generate_blob_sas(account_name=self.account_name,
                                  container_name=self.container_name,
                                  blob_name=blob_i,
                                  account_key=self.account_key,
                                  permission=BlobSasPermissions(read=True),
                                  expiry=datetime.utcnow() + timedelta(hours=1))

        sas_url = 'https://' + self.account_name + '.blob.core.windows.net/' + self.container_name + '/' + blob_i + '?' + sas_i

        df = pd.read_csv(sas_url)
        return df.to_string()
    #

