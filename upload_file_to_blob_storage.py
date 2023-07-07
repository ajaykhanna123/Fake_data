
from azure.storage.blob import BlobServiceClient
import os

class Upload_file_to_blob:
    def __init__(self):
        self.account_name = '*****'

        self.account_key = '****'

        self.container_name = 'filesstorage'

        # create a client to interact with blob storage
        self.connect_str = '******'

    def upload_file(self,filename = 'combined_df.csv',filepath = os.getcwd(),blob_folder='input_files/'):
        blob_service_client = BlobServiceClient.from_connection_string(self.connect_str)

        # use the client to connect to the container


        try:
            container_client = blob_service_client.get_container_client(container=self.container_name)

            with open(file=os.path.join(filepath, filename), mode="rb") as data:
                blob_client = container_client.upload_blob(name=blob_folder + filename, data=data, overwrite=True)
            return 1
        except FileNotFoundError:
            return 0
