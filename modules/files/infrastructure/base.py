import os
from common.logger import logger
from config.settings import settings


class FilesBase:
    def __init__(self):
        self.log = logger("files")

    def get_excel_path(self) -> str:
        path = settings.EXCEL_PATH
        self.log.info(f"Excel path: {path}")
        return path
