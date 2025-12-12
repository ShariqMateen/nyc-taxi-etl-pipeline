import os
import pandas as pd
import logging
import requests

logging.basicConfig(
    filename = "scripts/ETL_Logs/extract_log.log",
    level= logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

Data_Raw_Path = "data/data_raw/" # folder containing parquet files

def download_tlc_parquet(folder=Data_Raw_Path):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    os.makedirs(folder, exist_ok=True)

    for month in range(1, 7):
        mm = f"{month:02d}"
        file_name = f"yellow_tripdata_2023-{mm}.parquet"
        url = base_url + file_name
        dest = os.path.join(folder, file_name)

        print(f"Downloading {file_name} ...")

        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            print(f"Saved: {dest}")
        else:
            print(f"Failed: {response.status_code}")


def verify_parquet_files():
    results = []
    for filename in os.listdir(Data_Raw_Path):
        if filename.endswith(".parquet"):
            filepath = os.path.join(Data_Raw_Path,filename)

           
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)

            
            try:
                df = pd.read_parquet(filepath)
                row_count = len(df)
            except Exception as e:
                logging.error(f"Error reading {filename}: {e}")
                continue

           
            logging.info(
                f"File: {filename} | Size: {file_size_mb:.2f} MB | Records: {row_count}"
            )

            results.append({
                "file" : filename,
                "Size_in_MB" : file_size_mb,
                "Records" : row_count
            })

def extract_files():
    download_tlc_parquet()
    results = verify_parquet_files()
    print(results)

if __name__ == "__main__":
    extract_files()
