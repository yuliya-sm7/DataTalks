from time import time
import pandas as pd
from prefect import flow, task


@task(retries=3, log_prints=True)
def fetch(dataset_url: str, dataset_file: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    import os, wget
    from pathlib import Path

    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    if not path.exists():
        os.makedirs(f"data/{color}", exist_ok=True)
        print('Load file: ' + dataset_url)
        wget.download(dataset_url, out=str(path))
    df = pd.read_csv(path, compression='gzip')
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == 'green':
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    elif color == 'yellow':
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif color == 'fhv':
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
    df.columns = map(str.lower, df.columns)
    # print(df.head(3))
    print(f"columns:\n{df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(retries=3)
def write_postgres(df: pd.DataFrame, table_name):
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL

    engine = create_engine(URL.create(
        username='julia',
        drivername='postgresql+psycopg2',
        password="",
        host='192.168.1.33',
        # host='185.204.0.207',
        port=45432,
        database='production',
    ))
    if color == 'green':
        id_field = 'vendorid'
        datetime = 'lpep_pickup_datetime'
    elif color == 'yellow':
        id_field = 'vendorid'
        datetime = 'tpep_pickup_datetime'
    elif color == 'fhv':
        id_field = 'dispatching_base_num'
        datetime = 'pickup_datetime'
    example_record = df.iloc[-1, :]

    if not engine.has_table(table_name, schema_name) or \
            not engine.execute(
                f"SELECT 1 FROM {schema_name}.{table_name} WHERE {id_field}::text = '{example_record[id_field]}' AND {datetime} = '{example_record[datetime]}'"
            ).scalar():
        df.to_sql(schema=schema_name, name=table_name, con=engine, if_exists='append', index=False)


@flow(log_prints=True)
def etl_web_to_postgres(color, year, month) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    table_name = f'{color}_tripdata'

    t_start = time()
    print(f'Write partition {dataset_file} to {schema_name}.{table_name}')

    df = fetch(dataset_url, dataset_file)
    df_clean = clean(df)
    write_postgres(df_clean, table_name)

    t_end = time()
    print('Inserted partition took %.3f second' % (t_end - t_start))


if __name__ == "__main__":
    schema_name = 'trips_data_all'

    for color in ('fhv', 'green', 'yellow'):
        for year in (2019, 2020):
            if color == 'fhv' and year == 2020:
                continue
            for month in range(1, 13):
                etl_web_to_postgres(color, year, month)
                print(f'Job finished successfully for day = {color}-{year}-{month}')
