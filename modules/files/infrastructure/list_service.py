from os import listdir
from os.path import isfile, join
from .base import FilesBase


class ListExcelFiles(FilesBase):
    def __init__(self):
        super().__init__()

    def execute(self) -> list[str]:
        path = self.get_excel_path()
        self.log.debug(f"Scanning directory: {path}")

        try:
            files = [f for f in listdir(path) if isfile(join(path, f))]
            self.log.debug(f"Found {len(files)} files")
            return files

        except FileNotFoundError as e:
            self.log.error(f"Excel directory not found: {path}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f"Failed to list files in {path}: {str(e)}", exc_info=True)
            raise