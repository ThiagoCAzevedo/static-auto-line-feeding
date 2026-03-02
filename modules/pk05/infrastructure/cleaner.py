import polars as pl
from .base import PK05Base


class PK05DefineDataframe(PK05Base):
    def __init__(self, path: str):
        super().__init__()
        self.path = path

    def create_df(self) -> pl.LazyFrame:
        try:
            self.log.debug(f"Loading file: {self.path}")
            df = self.load_file(self.path).lazy()
            return df
        except Exception as e:
            self.log.error(f"Failed to load file {self.path}: {str(e)}", exc_info=True)
            raise


class PK05Cleaner(PK05Base):
    def rename_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        rename_map = {
            "Área abastec.prod.": "supply_area",
            "Depósito": "deposit",
            "Responsável": "responsible",
            "Ponto de descarga": "discharge_point",
            "Denominação SupM": "description",
        }
        
        try:
            lf = self.rename(lf, rename_map)
            return lf
        except Exception as e:
            self.log.error(f"Column rename failed: {str(e)}", exc_info=True)
            raise

    def filter_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        try:
            lf = lf.filter(
                pl.col("deposit") == "LB01",
                pl.col("takt").is_not_null() & pl.col("takt").str.starts_with("T")
            )
            return lf
        except Exception as e:
            self.log.error(f"Column filter failed: {str(e)}", exc_info=True)
            raise

    def create_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        try:
            lf = lf.with_columns(
                pl.col("description").str.extract(r"(T\d+)", 1).alias("takt")
            )
            lf = lf.with_row_index(name="id")
            return lf
        except Exception as e:
            self.log.error(f"Column creation failed: {str(e)}", exc_info=True)
            raise