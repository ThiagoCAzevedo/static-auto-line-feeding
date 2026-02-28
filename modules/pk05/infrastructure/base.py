import polars as pl
from common.logger import logger


class PK05Base:
    def __init__(self):
        self.log = logger("pk05")

    def load_file(self, path: str) -> pl.DataFrame:
        try:
            self.log.info(f"Lendo arquivo: {path}")
            
            return pl.read_excel(
                path,
                raise_if_empty=False,
                read_csv_options={"infer_schema_length": 10000}
            )

        except Exception:
            self.log.error(f"Erro ao ler arquivo {path}", exc_info=True)
            raise

    def rename(self, df: pl.LazyFrame, names: dict):
        return df.select(list(names.keys())).rename(names)