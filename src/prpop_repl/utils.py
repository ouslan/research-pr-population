import logging

import duckdb
import pandas as pd
import polars as pl
import tempfile
from pathlib import Path
from CensusForge import CensusAPI
from jp_tools import download


class DataUtils:
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.conn = duckdb.connect()

    def pull_dp03(self) -> pl.DataFrame:
        for _year in range(2011, 2024):
            file_path = Path(f"{self.saving_dir}raw/acs5-{_year}.parquet")
            if not file_path.exists():

                logging.info(f"pulling {_year} data")
                r = CensusAPI().query(
                    params_list=[
                        "DP03_0001E",
                    ],
                    year=_year,
                    geography="county",
                    dataset="acs-acs5-profile",
                )
                df = pl.DataFrame(r)
                df = df.rename(df.row(0, named=True))
                df = df.slice(1).with_columns(
                    pl.col("*").exclude("zip code tabulation area").cast(pl.Float32)
                )
                df = df.rename(
                    {
                        "DP03_0001E": "total_population",
                    }
                )
                df = df.with_columns(year=_year)
                df.write_parquet(file=file_path)
                logging.info(f"succesfully inserting {_year}")
        return self.conn.sql(
            f"SELECT * FROM '{self.saving_dir}raw/acs5-*.parquet';"
        ).pl()
