import pandas as pd
import os
import io
import pyarrow.parquet as pq
import psycopg2
import logging


logging.basicConfig(
    filename="airflow_project/dags/local_ETL_logs/data_load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


RAW_DIR = "data/data_raw/"
CLEAN_DIR = "data/data_clean/"


# PostgresSQl DB CONNECTION 
def get_connection():
    return psycopg2.connect(
        dbname="NYC-Data",
        user="postgres",
        password="PostgresSql@123",
        host="localhost",
        port="5432"
    )


def copy_to_postgres_clean(df, table_name, cursor):
   
    output = io.StringIO()

    
    df = df[[
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee",
        "trip_duration_minutes", "average_speed_mph",
        "pickup_date", "pickup_hour", "pickup_weekday"
    ]]

    df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)

    cursor.copy_from(output, table_name, sep='\t', null='\\N')


def Load_Cleaned_data():
    logging.info("Starting cleaned data load...")

    for file in os.listdir(CLEAN_DIR):
        if not file.endswith(".parquet"):
            continue

        logging.info(f"Processing cleaned file: {file}")

        try:
            path = os.path.join(CLEAN_DIR, file)
            table = pq.read_table(path)
            df = table.to_pandas()
            df = df.where(pd.notnull(df), None)  

            rows = len(df)
                
            conn = get_connection()
            cur = conn.cursor()

            
            try:
                copy_to_postgres_clean(df, "trips_clean", cur)
                conn.commit()
                logging.info(f"Inserted cleaned data from {file}")
                logging.info(f"Rows inserted: {rows}")

            except Exception as e:
                logging.error(f"Error while inserting file {file}: {e}")
                conn.rollback()

        except Exception as e:
            logging.error(f"Error while processing cleaned file {file}: {e}")

        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass





def copy_to_postgres(df, table_name, cursor):
    output = io.StringIO()

    # Reorder dataframe columns EXACTLY like table
    df = df[[
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee"
    ]]

    df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)

    cursor.copy_from(output, table_name, sep='\t', null='\\N')


def Load_Raw_data():
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Connected to DB for inserting raw data.")

        required_columns = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "RatecodeID",
            "store_and_fwd_flag", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "improvement_surcharge", "total_amount",
            "congestion_surcharge", "airport_fee"
        ]

        for file in os.listdir(RAW_DIR):
            if file.endswith(".parquet"):
                logging.info(f"Started file: {file}")

                try:
                    path = os.path.join(RAW_DIR, file)
                    df = pq.read_table(path).to_pandas()

                    # Add missing columns
                    for col in required_columns:
                        if col not in df.columns:
                            df[col] = None

                    df = df.where(pd.notnull(df), None)
                    rows = len(df)

                    try:
                        copy_to_postgres(df, "trips_raw", cur)
                        conn.commit()
                        logging.info(f"Inserted raw data from {file} ")
                        logging.info(f"Rows inserted: {rows}")

                    except Exception as e:
                        print(f"Error while inserting file {file}: {e}")
                        conn.rollback() 

                except Exception as e:
                    print(f"Error while processing file {file}: {e}")

    finally:
        cur.close()
        conn.close()



def Load_Data():
#    Load_Raw_data()
    Load_Cleaned_data()


if __name__ == "__main__":
    Load_Data()

    print("Data load finished. Check data_load.log for details.")
    logging.info("All data load operations completed.")
