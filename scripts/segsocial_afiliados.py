import os
import sys
import requests
import pandas as pd  # Import pandas
from datetime import datetime
from io import BytesIO


def upload_files_to_s3(path, bucket_name, s3_prefix, profile_name='default'):
    import boto3
    import sys
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


def get_url(historico):
    if historico:
        urls = [
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1224.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1124.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1024.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0924.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0824.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0724.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0624.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0524.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0424.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0324.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0224.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0124.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1223.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1123.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1023.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0923.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0823.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0723.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0623.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0523.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0423.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0323.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0223.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0123.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1222.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1122.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE1022.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0922.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0822.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0722.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0622.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0522.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0422.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0322.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0222.xlsx",
            "https://www.seg-social.es/descargas/STAT/MUNCNAE0122.xlsx"
        ]
    else: 
        year = datetime.now().year
        month = datetime.now().month
        month -= 1
        if month == 0:
            month = 12
            year -= 1
            year = str(year)[-2:]
        urls = [f"https://www.seg-social.es/descargas/STAT/MUNCNAE{month}{year}.xlsx"]
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


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'historico':
            urls = get_url(historico=True)
    else:
            urls = get_url(historico=False)

    workdir = '/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ss'
    if not os.path.exists(workdir):
        os.makedirs(workdir)
    bucket_name = 'osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake'
    s3_prefix_base = 'innvatur/ss/'
    profile_name = 'default'  # Replace with your AWS profile name

    for url in urls:
        print(f"Downloading {url}")
        response = requests.get(url)

        content_type = response.headers.get('Content-Type')

        if content_type != 'text/html':
                try:
                    df = pd.read_excel(BytesIO(response.content))
                    csv_filename = os.path.join(workdir, os.path.basename(url).replace('.xlsx', '.csv'))
                    df.to_csv(csv_filename, index=False)
                    print(f"Converted {url} to {csv_filename}")
                except ValueError as e:
                     print(f"Error processing {url}: {e}")
        
        elif content_type and 'text/html' in content_type:
            print(f"Failed to download {url}: Not an Excel file")
    
    upload_files_to_s3(workdir, bucket_name, s3_prefix_base, profile_name)
