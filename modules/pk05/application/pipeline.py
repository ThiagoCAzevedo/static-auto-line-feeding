from common.logger import logger
from modules.pk05.infrastructure.cleaner import PK05DefineDataframe, PK05Cleaner
from config.settings import settings
import polars as pl


class PK05Pipeline:
    def __init__(self):
        self.log = logger("pk05")
        self.file_path = settings.PK05_PATH

    def run(self) -> pl.LazyFrame:
        self.log.info(f"Starting PK05 pipeline (file: {self.file_path})")

        try:
            loader = PK05DefineDataframe(self.file_path)
            lf = loader.create_df()
            self.log.debug("DataFrame loaded successfully")

            cleaner = PK05Cleaner()
            
            print("COLUMNSSS:", lf.columns)
            lf = cleaner.rename_columns(lf)
            self.log.debug("Columns renamed")
            
            lf = cleaner.create_columns(lf)
            self.log.debug("Calculated columns created (takt extraction)")
            
            lf = cleaner.filter_columns(lf)
            self.log.debug("Rows filtered (deposit == 'LB01', takt starts with 'T')")

            self.log.info("PK05 pipeline completed successfully")
            return lf

        except Exception as e:
            self.log.error(f"PK05 pipeline failed: {str(e)}", exc_info=True)
            raise