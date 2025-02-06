import requests
import os
import zipfile
from datetime import datetime
import shutil

def download_file(url, path):
    if not os.path.exists(path):
        os.makedirs(path)
    local_filename = os.path.join(path, url.split('/')[-1])
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"File not found: {url}")
        else:
            print(f"HTTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def extract_zip(file_path, extract_to):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted: {file_path}")
    except zipfile.BadZipFile:
        print(f"Bad zip file: {file_path}")

def generate_urls(base_url, start_year, end_year, end_month):
    urls = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > end_month:
                break
            urls.append(base_url.format(month, year % 100))
    return urls


def upload_files_to_s3(path, bucket_name, s3_prefix, profile_name='default'):
    import boto3
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client('s3', region_name='eu-west-1')
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".csv") or file.endswith(".txt"):
                local_file_path = os.path.join(root, file)
                s3_file_path = os.path.join(s3_prefix, os.path.relpath(local_file_path, path))
                try:
                    s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
                    print(f"Uploaded: {local_file_path} to s3://{bucket_name}/{s3_file_path}")
                except Exception as e:
                    print(f"Failed to upload {local_file_path}: {e}")

'''
def upload_files_to_gcp(path, bucket_name, gcp_prefix, credentials_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".csv") or file.endswith(".txt"):
                local_file_path = os.path.join(root, file)
                gcp_file_path = os.path.join(gcp_prefix, os.path.relpath(local_file_path, path))
                blob = bucket.blob(gcp_file_path)
                try:
                    blob.upload_from_filename(local_file_path)
                    print(f"Uploaded: {local_file_path} to gs://{bucket_name}/{gcp_file_path}")
                except Exception as e:
                    print(f"Failed to upload {local_file_path}: {e}")
'''

def clear_local_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)            

def main():
    start_year = 2015
    now = datetime.now()
    end_year = now.year
    end_month =  now.month

    base_urls = [
        ("https://www.ine.es/ftp/microdatos/etr/datos_{}_{}.zip", "/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_microdatos/microdatos_etr", "etr"),
        ("https://www.ine.es/ftp/microdatos/frontur/datos_{}_{}.zip", "/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_microdatos/microdatos_frontur", "frontur"),
        ("https://www.ine.es/ftp/microdatos/egatur/datos_{}_{}.zip", "/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_microdatos/microdatos_egatur", "egatur")
    ]

    bucket_name = 'osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake'
    s3_prefix_base = 'innvatur/ine_microdatos'
    profile_name = 'default'  # Replace with your AWS profile name

    for base_url, path, subfolder in base_urls:
        clear_local_folder(path) 
        urls = generate_urls(base_url, start_year, end_year, end_month)
        for url in urls:
            try:
                #clear_local_folder(path)  # Clear the local folder before processing
                print(f"Downloading {url}")
                local_file = download_file(url, path)
                if local_file:
                    extract_zip(local_file, path)
                    os.remove(local_file)  # Remove the zip file after extraction
                    print(f"Removed: {local_file}")
                    s3_prefix = os.path.join(s3_prefix_base, subfolder)
                    #upload_files_to_s3(path, bucket_name, s3_prefix, profile_name)
                print(f"Downloaded, extracted {url}")
            except Exception as e:
                print(f"Failed to download {url}: {e}")
        upload_files_to_s3(path, bucket_name, s3_prefix, profile_name)
        clear_local_folder(path) # Clear the local folder after processing
if __name__ == "__main__":
    main()
