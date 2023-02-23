from pathlib import Path
import pandas as pd
from prefect import flow, task


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    df.columns = map(str.lower, df.columns)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    import os
    path = Path(f"data/{color}/{dataset_file}.parquet")
    os.makedirs(f"data/{color}", exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_postgres(df: pd.DataFrame, color: str) -> int:
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL
    from sqlalchemy.schema import CreateSchema

    engine = create_engine(URL.create(
        username='julia',
        drivername='postgresql+psycopg2',
        password='tzDySSwroc',
        host='185.204.0.207',
        port=45432,
        database='production',
    ))
    schema_name = 'trips_data_all'
    try:
        engine.execute(CreateSchema(schema_name))
    except:
        pass
    return df.to_sql(schema=schema_name, name=f'{color}_tripdata', con=engine, if_exists='append', index=False)


@flow()
def etl_web_to_postgres(color="green", year=2020, month=1) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    # path = write_local(df_clean, color, dataset_file)
    write_postgres(df_clean, color)


if __name__ == "__main__":
    for color in ('green', 'yellow'):
        for year in (2019, 2020):
            for month in range(1, 12):
                etl_web_to_postgres(color, year, month)
