import os
import time
import pandas as pd
import pyarrow.parquet as pq
import logging

RAW_DIR = "data/data_raw/"
CLEAN_DIR = "data/data_clean/"
CSV_DIR = "data/data_csv/"

# ============ LOGGING ============
logging.basicConfig(
    filename="airflow_project/dags/local_ETL_logs/transform_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_dataframe(df):

    # Convert Data Types
    
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    df["VendorID"] = df["VendorID"].astype("Int64")
    df["payment_type"] = df["payment_type"].astype("Int64")
    df["RatecodeID"] = df["RatecodeID"].astype("Int64")
#    df["passenger_count"] = df["passenger_count"].astype("Int64")

    # Clean store_and_fwd_flag raw values
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].fillna("N")
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].replace(
        {"": "N", None: "N"}
    )

    # Convert store_and_fwd_flag to boolean
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": True, "N": False})

    # Remove invalid timestamp rows
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    # Remove invalid passenger count
    df.loc[df["passenger_count"] <= 0, "passenger_count"] = None

    # Remove invalid trip distance
    df = df[df["trip_distance"] > 0]

    # Keep only valid payment_type values
    valid_payment_types = [1, 2, 3, 4, 5, 6]
    df = df[df["payment_type"].isin(valid_payment_types)]

    # Check total_amount consistency
    fare_columns = [
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee"
    ]

    # If a column doesn't exist, fill it with 0
    for col in fare_columns:
        if col not in df.columns:
            df[col] = 0

    df["calculated_total"] = df[fare_columns].sum(axis=1)


    df["total_amount_diff"] = (df["total_amount"] - df["calculated_total"]).abs()

    # Remove rows where difference > $1
    df = df[df["total_amount_diff"] < 1]

    # 7. Add derived columns
    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds()
        / 60
    )

    # Remove trips that are too short or too long
    df = df[df["trip_duration_minutes"] > 1]    # > 1 minute
    df = df[df["trip_duration_minutes"] < 180]  # < 3 hours

    df["average_speed_mph"] = df["trip_distance"] / (df["trip_duration_minutes"] / 60)

    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_weekday"] = df["tpep_pickup_datetime"].dt.day_name()


    # Drop helper columns
    df = df.drop(columns=["calculated_total", "total_amount_diff"])

    return df




def transform_files():
    for file in os.listdir(RAW_DIR):
        if file.endswith(".parquet"):
            path = os.path.join(RAW_DIR, file)

            table = pq.read_table(path)
            df = table.to_pandas()

            df = clean_dataframe(df)

            clean_path = os.path.join(CLEAN_DIR, "clean_" + file)
            df.to_parquet(clean_path, index=False)

            #  CONVERT TO CSV 
            start = time.time()
            csv_path = os.path.join(CSV_DIR, file.replace(".parquet", ".csv"))
            df.to_csv(csv_path, index=False)
            end = time.time()

            # File size difference
            parquet_size = os.path.getsize(clean_path) / (1024 * 1024)
            csv_size = os.path.getsize(csv_path) / (1024 * 1024)
            size_diff = csv_size - parquet_size

            # Log
            logging.info(
                f"{file}: Cleaned={len(df)} rows | "
                f"CSV conversion time={end - start:.2f}s | "
                f"Parquet={parquet_size:.2f}MB | CSV={csv_size:.2f}MB | Diff={size_diff:.2f}MB"
            )

    print("Transformation completed successfully!")


if __name__ == "__main__":
    transform_files()
