import os
from google.cloud import storage

def upload_files_in_directory(local_directory, bucket_name):
    # Set the path to your service account JSON key file
    json_key_path = r"C:\Users\manish.kumar02\Downloads\mentorsko-1700569452273-3e9bdd986606.json"  
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path

    # Create a client using the service account key file
    client = storage.Client()

    # Get the target bucket
    bucket = client.get_bucket(bucket_name)

    # Iterate over files in the local directory
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_file_path = os.path.join(root, file)
            _, filename = os.path.split(local_file_path)

            # Create a blob (object) in the bucket with the same name as the local file
            blob = bucket.blob(filename)

            # Upload the local file to GCS
            blob.upload_from_filename(local_file_path)

            print(f"File {local_file_path} uploaded to {bucket_name}/{filename}")

# Example usage
if __name__ == "__main__":
    # Set the paths and names
    local_directory = r"C:\Users\manish.kumar02\Downloads\capstone_data"
    bucket_name = "capstondata"

    # Upload files from the directory to Google Cloud Storage
    upload_files_in_directory(local_directory, bucket_name)
