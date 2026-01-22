import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import pyarrow.parquet as pq
import requests
import tempfile
import os

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "trip_type": "Int64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', type=int, default=5432, help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database')
@click.option('--chunksize', type=int, default=100000, help='Chunk size')
@click.option('--target_table', type=str, default='green_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):
    
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    
    # Download the file to a temporary location
    response = requests.get(url)
    response.raise_for_status()
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
        tmp_file.write(response.content)
        temp_file_path = tmp_file.name
    
    try:
        pf = pq.ParquetFile(temp_file_path)
        first = True

        for batch in tqdm(pf.iter_batches(batch_size=chunksize)):
            df_chunk = batch.to_pandas()
            
            # Apply parse_dates
            for col in parse_dates:
                if col in df_chunk.columns:
                    df_chunk[col] = pd.to_datetime(df_chunk[col])
            
            # Apply dtype conversions
            df_chunk = df_chunk.astype(dtype)

            if first:
                # Create table schema (no data)
                df_chunk.head(0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists="replace"
                )
                first = False
                print("Table created")

            # Insert chunk
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append"
            )

            print("Inserted:", len(df_chunk))
    finally:
        # Clean up the temporary file
        os.unlink(temp_file_path)
    
    # Ingest taxi zone lookup
    zone_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    df_zones = pd.read_csv(zone_url)
    df_zones.to_sql(name='taxi_zones', con=engine, if_exists='replace', index=False)
    print("Inserted taxi zones:", len(df_zones))

if __name__ == '__main__':
    run()

