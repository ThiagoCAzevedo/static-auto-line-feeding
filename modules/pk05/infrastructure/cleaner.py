import polars as pl
from .base import PK05Base


class PK05DefineDataframe(PK05Base):
    def __init__(self, path: str):
        super().__init__()
        self.path = path

    def create_df(self) -> pl.LazyFrame:
        return self.load_file(self.path).lazy()


class PK05Cleaner(PK05Base):
    def rename_columns(self, lf):
        rename_map = {
            "Área abastec.prod.": "supply_area",
            "Depósito": "deposit",
            "Responsável": "responsible",
            "Ponto de descarga": "discharge_point",
            "Denominação SupM": "description",
        }
        return self.rename(lf, rename_map)

    def filter_columns(self, lf):
        return lf.filter(
            pl.col("deposit") == "LB01",
            pl.col("takt").is_not_null() & pl.col("takt").str.starts_with("T")
        )

    def create_columns(self, lf):
        lf = lf.with_columns(
            pl.col("description").str.extract(r"(T\d+)", 1).alias("takt")
        )
        return lf.with_row_index(name="id")