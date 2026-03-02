import logging
import tempfile
from pathlib import Path

import duckdb
import geopandas as gpd
import polars as pl
from CensusForge import CensusAPI
from jp_tools import download


class DataUtils:
    def __init__(
        self,
        saving_dir: str = "data/",
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
        return self.conn.sql(
            f"SELECT * FROM '{self.saving_dir}raw/acs5-*.parquet';"
        ).pl()

    def county_geom(self) -> gpd.GeoDataFrame:
        file_path = Path(f"{self.saving_dir}external/geo-county.parquet")
        if not file_path.exists():
            download(
                url="https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip",
                filename=f"{tempfile.gettempdir()}/{hash(file_path)}.zip",
            )

            # Process shape
            gdf = gpd.read_file(f"{tempfile.gettempdir()}/{hash(file_path)}.zip")
            gdf = gdf.rename(
                columns={
                    "STATEFP": "statefip",
                    "GEOID": "geoid",
                    "NAME": "name",
                }
            )
            gdf = gdf[gdf["statefip"] == "72"].reset_index()
            gdf = gdf[["statefip", "geoid", "name", "geometry"]]
            gdf.to_parquet(file_path)
        return gpd.read_parquet(path=file_path)
