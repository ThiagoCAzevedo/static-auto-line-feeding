from common.logger import logger
from modules.pkmc.infrastructure.cleaner import PKMCDefineDataframe, PKMCCleaner
from config.settings import settings
import polars as pl


class PKMCPipeline:
    def __init__(self):
        self.log = logger("pkmc")
        self.file_path = settings.PKMC_PATH

    def run(self) -> pl.LazyFrame:
        self.log.info("PKMC pipeline started")

        loader = PKMCDefineDataframe(self.file_path)
        lf = loader.create_df()

        cleaner = PKMCCleaner()
        lf = cleaner.rename_columns(lf)
        lf = cleaner.filter_columns(lf)
        lf = cleaner.clean_columns(lf)
        lf = cleaner.create_columns(lf)

        self.log.info("PKMC pipeline finished")
        return lf
