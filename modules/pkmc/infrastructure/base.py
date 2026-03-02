import polars as pl
from common.logger import logger


class PKMCBase:
    def __init__(self):
        self.log = logger("pkmc")

    def load_file(self, path: str) -> pl.DataFrame:
        try:
            self.log.debug(f"Loading Excel file: {path}")
            
            df = pl.read_excel(
                path,
                raise_if_empty=False,
                read_csv_options={"infer_schema_length": 10000}
            )
            
            row_count = df.height
            self.log.debug(f"File loaded: {row_count} rows")
            return df

        except FileNotFoundError as e:
            self.log.error(f"File not found: {path}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f"Failed to load file {path}: {str(e)}", exc_info=True)
            raise

    def rename(self, df: pl.LazyFrame, names: dict) -> pl.LazyFrame:
        return df.select(list(names.keys())).rename(names)