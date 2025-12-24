import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

load_dotenv()

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
service = BlobServiceClient.from_connection_string(conn_str)

container_name = "bronze"
container = service.get_container_client(container_name)

try:
    container.create_container()
    print(f"container '{container_name}' created")
except ResourceExistsError:
    print(f"container '{container_name}' already exists")

container.upload_blob("healthcheck.txt", b"ok", overwrite=True)
print("upload ok")
