import sys
import requests
import pandas as pd
from datetime import datetime
import json
import csv
import os

def generar_url(pagina, starttime, endtime, rrss):
    url = f"https://app.atribus.com/api/invattur/timeline?source={rrss}&categories=28,29,30,31,32,33,34,35,36,37,38,39,40,42,41,43,44,45,46,47,48,49,50,51,52,74,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,77,70,71,72,73,75,76&searchs=&startTime={starttime}&endTime={endtime}&idApp=1&page={pagina}"
    return url

def limpiar_datos(data):
    for item in data:
        for key, value in item.items():
            if isinstance(value, str):
                item[key] = value.replace(';', '|')
            elif isinstance(value, list) or isinstance(value, dict):
                item[key] = json.dumps(value).replace(';', '|')
    return data

def obtener_datos(starttime, endtime, rrss, token):
    pagina = 1
    dataframe = pd.DataFrame()
    while True:
        response = requests.get(generar_url(pagina, starttime, endtime, rrss), headers={"Authorization": f"Bearer {token}"})

        if response.status_code == 429:
            print(f"Demasiadas solicitudes ({pagina}) : {response.text}")
            break

        if response.status_code == 403:
            print(f"Acceso prohibido ({pagina}) : {response.text}")
            break

        # Process the response and accumulate in DataFrame
        data = response.json()
        if 'items' not in data:
            print(f"Clave 'items' no encontrada en la respuesta ({pagina}) : {data}")
            break

        if not data['items']:
            print(f"No se encontraron más resultados en la página ({pagina})")
            break

        # Clean the data
        cleaned_data = limpiar_datos(data['items'])
        df_temp = pd.json_normalize(cleaned_data)
        dataframe = pd.concat([dataframe, df_temp], ignore_index=True)

        pagina += 1
    print(f"Se han recopilado {len(dataframe)} registros en {pagina} páginas")
    #columns = dataframe.columns
    #array_columns = [col for col in columns if dataframe[col].apply(lambda x: isinstance(x, str) and (x.startswith('[') or x.startswith('{'))).any()]
    #ddl_statement = f"CREATE TABLE {rrss} ({', '.join([f'{col} VARIANT' if col in array_columns else f'{col} VARCHAR' for col in columns])});"
    #print(ddl_statement)
    return dataframe

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

def upload_file_to_s3(workdir, file_csv, bucket_name, s3_prefix_base, profile_name='default'):
    import boto3
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client('s3', region_name='eu-west-1')
    local_file_path = os.path.join(workdir, file_csv)
    s3_file_path = os.path.join(s3_prefix_base, file_csv)
    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
        print(f"Uploaded: {local_file_path} to s3://{bucket_name}/{s3_file_path}")
    except Exception as e:
        print(f"Failed to upload {local_file_path}: {e}")


def main():
    if len(sys.argv) != 4:
        print("uso: python ./atribus_daily.py '2024-10-21 00:00:00' '2024-10-21 23:59:59' instagram")
        sys.exit(1)

    fecha_ini = sys.argv[1]
    fecha_fin = sys.argv[2]
    rrss = sys.argv[3]
    print("*" * 80)
    print(f"Recopilando datos de {rrss} desde {fecha_ini} hasta {fecha_fin}")

    starttime = int(datetime.strptime(fecha_ini, "%Y-%m-%d %H:%M:%S").timestamp()) * 1000
    endtime = int(datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S").timestamp()) * 1000

    token = "4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0="

    workdir = '/opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_timeline/daily/'

    bucket_name = 'osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake'
    s3_prefix_base = 'innvatur/atribus/timeline/daily/'  
    profile_name = 'default'  

    # Obtener datos paginados
    dataframe = obtener_datos(starttime, endtime, rrss, token)

    # Write the accumulated DataFrame to a CSV file
    dia_ini = datetime.strptime(fecha_ini, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
    file_csv = f"{rrss}_{dia_ini}.csv"
    if dataframe.empty:
        print("No se encontraron datos para escribir en el archivo CSV.")
        return
    #dataframe.to_csv(file_csv, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    dataframe.to_csv(f'{workdir}/{file_csv}', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    upload_file_to_s3(workdir,file_csv, bucket_name, s3_prefix_base, profile_name)

if __name__ == "__main__":
    main()
