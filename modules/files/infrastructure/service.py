from os import listdir
from os.path import isfile, join
from .base import FilesBase
import os


class ListExcelFiles(FilesBase):
    def __init__(self):
        super().__init__()
        self.log.info("Initialized ListExcelFiles")

    def execute(self):
        path = self.get_excel_path()
        self.log.info(f"Listing files in directory: {path}")

        try:
            files = [f for f in listdir(path) if isfile(join(path, f))]
            self.log.info(f"Found files: {len(files)}")
            return files

        except Exception:
            self.log.error("Error listing files in EXCEL_PATH", exc_info=True)
            raise


class UploadFiles(FilesBase):
    def __init__(self):
        super().__init__()
        self.log.info("Initialized UploadFiles")

    def execute(self, file):
        path = self.get_excel_path()
        target_path = f"{path}/{file.filename}"

        self.log.info(f"Initialized file upload: {file.filename}")

        try:
            content = file.file.read()

            with open(target_path, "wb") as f:
                f.write(content)

            self.log.info(f"Upload completed: {file.filename} ({len(content)} bytes)")
            return {"filename": file.filename, "size": len(content)}

        except Exception:
            self.log.error(f"Error uploading file {file.filename}", exc_info=True)
            raise


class DeleteFiles(FilesBase):
    def __init__(self):
        super().__init__()
        self.log.info("Initialized DeleteFiles")

    def execute(self, filename: str):
        path = self.get_excel_path()
        file_path = os.path.join(path, filename)

        self.log.info(f"Delete requested to file: {filename}")

        try:
            os.remove(file_path)
            self.log.info(f"File removed successfully: {filename}")
            return {"message": f"File '{filename}' succesfully removed."}

        except FileNotFoundError:
            self.log.error(f"File '{filename}' not found for removal", exc_info=True)
            raise

        except Exception:
            self.log.error(f"Error removing file '{filename}'", exc_info=True)
            raise
