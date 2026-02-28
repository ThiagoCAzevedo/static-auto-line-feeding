from common.logger import logger
from modules.pk05.infrastructure.cleaner import PK05DefineDataframe, PK05Cleaner
from config.settings import settings
import polars as pl


class PK05Pipeline:
    def __init__(self):
        self.log = logger("pk05")
        self.file_path = settings.PK05_PATH

    def run(self) -> pl.LazyFrame:
        self.log.info("PK05 pipeline started")

        loader = PK05DefineDataframe(self.file_path)
        lf = loader.create_df()

        cleaner = PK05Cleaner()
        lf = cleaner.rename_columns(lf)
        lf = cleaner.create_columns(lf)
        lf = cleaner.filter_columns(lf)

        self.log.info("PK05 pipeline finished")
        return lf