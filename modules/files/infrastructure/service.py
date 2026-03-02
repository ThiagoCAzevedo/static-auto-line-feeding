from os import listdir
from os.path import isfile, join
from .base import FilesBase
import os


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


class UploadFiles(FilesBase):
    def __init__(self):
        super().__init__()

    def execute(self, file) -> dict:
        path = self.get_excel_path()
        target_path = os.path.join(path, file.filename)

        self.log.debug(f"Uploading file: {file.filename} → {target_path}")

        try:
            content = file.file.read()
            file_size = len(content)

            with open(target_path, "wb") as f:
                f.write(content)

            self.log.debug(f"File written to disk ({file_size} bytes)")
            return {"filename": file.filename, "size": file_size}

        except IOError as e:
            self.log.error(f"I/O error writing file {file.filename}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f"Failed to upload file {file.filename}: {str(e)}", exc_info=True)
            raise


class DeleteFiles(FilesBase):
    def __init__(self):
        super().__init__()

    def execute(self, filename: str) -> dict:
        path = self.get_excel_path()
        file_path = os.path.join(path, filename)

        self.log.debug(f"Deleting file: {filename} ({file_path})")

        try:
            if not os.path.exists(file_path):
                self.log.warning(f"File not found: {filename}")
                raise FileNotFoundError(f"File '{filename}' does not exist")

            os.remove(file_path)
            self.log.debug(f"File deleted from disk")
            return {"message": f"File '{filename}' successfully removed."}

        except FileNotFoundError as e:
            self.log.error(f"File not found: {filename}", exc_info=True)
            raise
        except OSError as e:
            self.log.error(f"OS error deleting file {filename}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f"Failed to delete file {filename}: {str(e)}", exc_info=True)
            raise