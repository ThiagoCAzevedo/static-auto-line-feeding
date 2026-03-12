import polars as pl
from .base import PKMCBase


class PKMCLoader(PKMCBase):
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