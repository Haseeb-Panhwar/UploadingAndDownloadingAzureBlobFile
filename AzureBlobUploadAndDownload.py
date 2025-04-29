from azure.storage.blob import BlobServiceClient, BlobBlock
import base64
import dotenv
from dotenv import load_dotenv
import os

load_dotenv()

con_string = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')


def upload_large_file(connection_string, container_name, blob_name, file_path, chunk_size=4 * 1024 * 1024):
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Create container if not exists
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass  # Container likely already exists

    blob_client = container_client.get_blob_client(blob_name)
    block_list = []

    with open(file_path, 'rb') as file:
        index = 0
        while True:
            data = file.read(chunk_size)
            if not data:
                break
            block_id = base64.b64encode(f"{index:06}".encode()).decode()
            blob_client.stage_block(block_id=block_id, data=data)
            block_list.append(BlobBlock(block_id=block_id))
            index += 1
            print(f"Uploaded chunk {index}")

    # Commit the blocks
    blob_client.commit_block_list(block_list)
    print("Upload completed.")


def download_large_file(connection_string, container_name, blob_name, download_path, chunk_size=4 * 1024 * 1024):
    # Connect to the blob service
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Get blob size from properties
    blob_props = blob_client.get_blob_properties()
    blob_size = blob_props.size

    print(f"Blob size: {blob_size / (1024 * 1024):.2f} MB")

    # Open local file for writing
    with open(download_path, "wb") as file:
        offset = 0

        while offset < blob_size:
            # Download this chunk
            stream = blob_client.download_blob(offset=offset, length=chunk_size)
            data = stream.readall()

            # Write to local file
            file.write(data)

            offset += chunk_size
            print(f"Downloaded {offset / (1024 * 1024):.2f} MB")

    print("Download complete.")




upload_large_file(
    connection_string=con_string,
    container_name="videoasblob",
    blob_name="yolo11x.pt",
    file_path="yolo11x.pt",
    chunk_size=5 * 1024 * 1024  # 5MB chunks (can be configured)
)


download_large_file(
    connection_string=con_string,
    container_name="videoasblob",
    blob_name="yolo11x.pt",
    download_path="yolo11x.pt",
    chunk_size= 5 * 1024 * 1024  # 64MB
)
