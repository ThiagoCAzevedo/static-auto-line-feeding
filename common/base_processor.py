import polars as pl
from common.logger import logger


class DataProcessorBase:
    def __init__(self, service_name: str):
        self.log = logger(service_name)

    def load_file(self, path: str) -> pl.DataFrame:
        try:
            self.log.debug(f"Loading Excel file: {path}")
            df = pl.read_excel(
                path,
                raise_if_empty=False,
                read_csv_options={"infer_schema_length": 10000}
            )
            self.log.debug(f"File loaded: {df.height} rows")
            return df
        except FileNotFoundError as e:
            self.log.error(f"File not found: {path}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f"Failed to load file {path}: {str(e)}", exc_info=True)
            raise

    def rename(self, df: pl.LazyFrame, names: dict) -> pl.LazyFrame:
        # log mapping and result columns for traceability
        try:
            self.log.debug(f"Renaming columns: {names}")
            result = df.select(list(names.keys())).rename(names)
            # columns property is cheap on LazyFrame
            self.log.debug(f"Columns after rename: {result.columns}")
            return result
        except Exception as e:
            self.log.error(f"Rename operation failed: {str(e)}", exc_info=True)
            raise