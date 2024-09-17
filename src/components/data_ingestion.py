# Imports
import os
import hashlib
from google.cloud import storage

class DataIngestion():

    def __init__(self, bucket_name, service_account_key_path, temp_dir):
        self.bucket_name = bucket_name
        self.service_account_key_path = service_account_key_path
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.service_account_key_path
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(bucket_name)
        self.temp_dir = temp_dir

    
    def compute_sha256(self, file_path):
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read and update the hash in chunks
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def compute_gcs_blob_sha256(self, blob_name):
        # Initialize a GCS client
        client = storage.Client()

        # Get the bucket and blob (file) from GCS
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.blob(blob_name)

        # Download the blob's content as a bytes object
        blob_bytes = blob.download_as_bytes()

        # Compute the SHA-256 hash
        sha256_hash = hashlib.sha256()
        sha256_hash.update(blob_bytes)

        return sha256_hash.hexdigest()
    
    def data_ingestion(self):
        blobs = self.bucket.list_blobs()
        files_in_gcs = [blob.name for blob in blobs]
        files_in_temp = os.listdir(self.temp_dir)

        for file in files_in_gcs:
            local_file_path = self.temp_dir + file
            if not os.path.exists(self.temp_dir + file) or self.compute_sha256(local_file_path) != self.compute_gcs_blob_sha256(file):
                # download the file from GCS to data/Temporary_Data
                blob = self.bucket.blob(file)
                
                blob.download_to_filename(local_file_path)
                print(f"Downloaded {file} from GCS to {local_file_path}")
            else:
                print(f"{file} already exists in data/Temporary_Data")

        for file in files_in_temp:
            if file not in files_in_gcs:
                os.remove(self.temp_dir + file)
                print(f"Removed {file} from data/Temporary_Data as it is no longer needed")


if __name__ == "__main__":
    data_ingestion = DataIngestion(
        bucket_name="credit_card_fraud_detection_jm",
        temp_dir="./data/Temporary_Data/",
        service_account_key_path="/home/jobin/Desktop/MyFiles/Projects/AnomalyDetection/Security_Keys/anomaly-detection-mlops-0a7a867fbcf0.json"
    )

    data_ingestion.data_ingestion()