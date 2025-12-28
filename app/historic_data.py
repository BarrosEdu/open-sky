# historic.py
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

def historic_data(year: int=2025, month: int=12, day: int=25, limit: int = 1000):
    con = duckdb.connect()

    try:
        # Azure extension já deve estar instalada no container
        con.execute("LOAD azure;")

        con.execute(
            f"SET azure_storage_connection_string='{connection_string}';"
        )

        date_str = f"{year:04d}-{month:02d}-{day:02d}"

        df = con.execute(
            f"""
            SELECT *
            FROM read_parquet(
                'azure://bronze/open_sky/**/*.parquet',
                hive_partitioning = true
            )
            WHERE date = '{date_str}'
            LIMIT {limit}
            """
        ).df()

        return df

    finally:
        con.close()
