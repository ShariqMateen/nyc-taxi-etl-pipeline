import pandas as pd
import pyarrow.parquet as pq

filepath = "data/data_raw\yellow_tripdata_2023-01.parquet"


"""Load a Parquet file into a Pandas DataFrame."""
table = pq.read_table(filepath)
df = table.to_pandas()
print(df["airport_fee"].max())


