import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

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
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# df = pd.read_csv(
#     prefix + 'yellow_tripdata_2021-01.csv.gz',
#     dtype=dtype,
#     parse_dates=parse_dates
# )


# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', type=int, default=5432, help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database')
@click.option('--year', type=int, default=2021, help='Year')
@click.option('--month', type=int, default=1, help='Month')
@click.option('--chunksize', type=int, default=100000, help='Chunk size')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize):
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02}.csv.gz'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    target_table = "yellow_taxi_data"

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )


    first = True

    for df_chunk in df_iter:

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

if __name__ == '__main__':
    run()