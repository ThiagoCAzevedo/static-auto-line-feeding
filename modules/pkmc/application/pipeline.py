from common.logger import logger
from modules.pkmc.infrastructure.loader import PKMCLoader
from modules.pkmc.infrastructure.cleaner import PKMCCleaner
from config.settings import settings
import polars as pl


class PKMCPipeline:
    def __init__(self):
        self.log = logger("pkmc")
        self.file_path = settings.PKMC_PATH

    def run(self) -> pl.LazyFrame:
        self.log.info(f"Starting PKMC pipeline (file: {self.file_path})")

        try:
            loader = PKMCLoader(self.file_path)
            lf = loader.create_df()
            self.log.debug("DataFrame loaded successfully")

            cleaner = PKMCCleaner()
            
            lf = cleaner.rename_columns(lf)
            self.log.debug("Columns renamed")
            
            lf = cleaner.filter_columns(lf)
            self.log.debug("Rows filtered (deposit_type == 'B01')")
            
            lf = cleaner.clean_columns(lf)
            self.log.debug("✓ Data cleaned (qty_max_box, partnumber)")
            
            lf = cleaner.create_columns(lf)
            self.log.debug("Calculated columns created")

            # record columns for debugging
            self.log.debug(f"Pipeline output columns: {lf.columns}")

            self.log.info("PKMC pipeline completed successfully")
            return lf

        except Exception as e:
            self.log.error(f"PKMC pipeline failed: {str(e)}", exc_info=True)
            raise