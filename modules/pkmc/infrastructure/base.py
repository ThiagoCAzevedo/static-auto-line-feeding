from common.base_processor import DataProcessorBase


class PKMCBase(DataProcessorBase):
    def __init__(self):
        super().__init__("pkmc_processor")