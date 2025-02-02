from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import logging as log

from utilities.data_descriptor import singleton


@singleton
class BlobApi:
    def __init__(self, configuration):
        log.info("Initializing BlobApi.")
        self.configuration = configuration
        self.url = self._get_blob_storage_url()
        log.info(f"URL: {self.url}")


    def _get_blob_storage_url(self):
        return f"https://{self.configuration.account_name}.blob.core.windows.net"

    def get_blob_service_client(self):
        default_credential = DefaultAzureCredential()

        blob_service_client = BlobServiceClient(self.url, credential=default_credential)
        return blob_service_client

    def create_container(self, container_name, blob_service_client=None):
        if blob_service_client is None:
            blob_service_client = self.get_blob_service_client()
        blob_service_client.create_container(container_name)

    def get_container_client(self, container_name, blob_service_client=None):
        if blob_service_client is None:
            blob_service_client = self.get_blob_service_client()
        return blob_service_client.get_container_client(container_name)

    def get_blob_client(self, container_name, blob_name, blob_service_client=None):
        if blob_service_client is None:
            blob_service_client = self.get_blob_service_client()
        return blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    def upload_blob(self, container, blob_name, content):
        blob_client = self.get_blob_client(container, blob_name)
        container_client = self.get_container_client(container)
        if not container_client.exists():
            self.create_container(container)
            log.info("Created container %s", container)
        if blob_client.exists():
            log.info(f"Blob {blob_name} already exists")
            return
        blob_client.upload_blob(content)